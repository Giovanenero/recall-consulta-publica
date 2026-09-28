"""
fetch_TJRN.py - credor de processos do TJRN pela consulta pública do PJe 1º grau (só consulta: nada vai para o banco).

1. Abre o Google Chrome da máquina (Playwright, com janela) no perfil próprio TJRN/.chrome-profile-pje-tjrn, que
   guarda os cookies do Akamai e tem a extensão Buster instalada (o Chrome 136+ não deixa automatizar o perfil padrão).
2. Para cada processo: digita o número e clica em Pesquisar. A página tem Akamai e reCAPTCHA invisível no Pesquisar;
   se o reCAPTCHA abrir o desafio, clica uma vez no botão do Buster (resolve pelo áudio). Se o desafio seguir na tela
   ESPERA_BUSTER s depois do clique (ou o botão não aparecer), recarrega a página e tenta de novo com desafio novo
   (até NOVAS_TENTATIVAS vezes).
3. Abre o "Ver Detalhes" e lê a capa (classe, data da distribuição, órgão julgador, jurisdição...) e as partes dos
   polos ativo e passivo, separando os advogados (OAB). O PJe do TJRN mascara o CPF/CNPJ (***.974.194-**).
4. Grava só um CSV com uma linha por processo (hora, duração, se houve desafio, cliques no Buster, tentativas,
   capa e partes). Não guarda HTML nem JSON.

Bloqueio (o Google recusa o áudio com "consultas automáticas", ou o Akamai nega o acesso) interrompe o lote. Se o
Chrome fechar no meio, o lote o reabre e repete o processo (até REABERTURAS vezes).

Requisitos: pip install playwright scrapling; Google Chrome instalado (não precisa do "playwright install").

Uso (um ou vários processos na mesma sessão do navegador):
    python TJRN/fetch_TJRN.py 0826136-40.2019.8.20.5001
    python TJRN/fetch_TJRN.py 0826136-40.2019.8.20.5001 0801377-04.2018.8.20.5112 --csv TJRN/saida/lote.csv

Log: terminal e TJRN/saida/logs/fetch_TJRN_AAAAMMDD.log, no formato padrão (utils/log.py).
"""
import argparse
import ctypes
import logging
import os
import random
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils.arquivos import gravar_csv as gravar_csv_padrao  # noqa: E402
from utils.log import configurar_log  # noqa: E402

# =============================================================================== configuração

log = logging.getLogger("fetch_TJRN")

BASE = "https://pje1gconsulta.tjrn.jus.br"
URL = f"{BASE}/consultapublica/ConsultaPublica/listView.seam"
# CHROME_PERFIL aponta para outra pasta de perfil; por padrão fica ao lado do script
# e guarda os cookies do Akamai entre execuções.
PERFIL = Path(os.environ.get("CHROME_PERFIL") or AQUI / ".chrome-profile-pje-tjrn").resolve()
SAIDA = AQUI / "saida"

TIMEOUT_AKAMAI = 60
TIMEOUT_RESULTADO = 20   # sem desafio na tela e sem resultado por este tempo → desiste
ESPERA_BUSTER = 10       # s para o Buster resolver depois do clique; passou disso, recarrega a página
NOVAS_TENTATIVAS = 3     # vezes que recarrega a página e refaz o processo se o desafio não sair
PAUSA_ENTRE = (5, 10)    # s entre um processo e outro do lote, para não martelar o site
REABERTURAS = 3          # vezes que o lote reabre o Chrome se ele fechar no meio

CAMPO_PROCESSO = "#fPP\\:numProcesso-inputNumeroProcessoDecoration\\:numProcesso-inputNumeroProcesso"
BOTAO_PESQUISAR = "#fPP\\:searchProcessos"

# Dentro do iframe do desafio: o Buster remove o botão de ajuda do rodapé e põe o
# dele na mesma div, ao lado do botão de áudio, num shadow root fechado -- então o
# clique vai na div. Com .rc-doscaptcha-body ("seu computador pode estar enviando
# consultas automáticas") o Google recusou o áudio: é o bloqueio.
BOTAO_BUSTER = ".help-button-holder:not(:has(#recaptcha-help-button))"
AUDIO_BLOQUEADO = ".rc-doscaptcha-body"
# A extensão Buster (Chrome Web Store). Perfil novo precisa dela instalada uma vez à mão: o Chrome não aceita
# --load-extension (Chrome 137+) e desliga a extensão de um perfil copiado (DISABLE_CORRUPTED).
BUSTER = "mpbjkejclgfgadiemmefgebjfooflfhl"
LOJA_BUSTER = f"https://chromewebstore.google.com/detail/{BUSTER}"
ESPERA_INSTALAR_BUSTER = 15 * 60   # s esperando a pessoa instalar o Buster num perfil novo

# A página do Akamai (bm-verify) some sozinha depois da verificação.
JS_FORM_OK = "!document.getElementById('akam-logo') && !!document.getElementById('fPP')"
JS_DETALHE_OK = "!document.getElementById('akam-logo') && document.body.innerText.includes('Polo')"

# O reCAPTCHA sempre cria o iframe "bframe"; o desafio só está na tela quando
# ele fica visível e dentro da viewport.
JS_DESAFIO_VISIVEL = """
[...document.querySelectorAll('iframe[src*="bframe"]')].some(f => {
    const r = f.getBoundingClientRect();
    return getComputedStyle(f).visibility !== 'hidden' && r.width > 0 && r.height > 0 && r.top >= 0;
})
"""

# Link "Ver Detalhes" da linha do resultado (abre a popup DetalheProcessoConsultaPublica).
JS_LINK_DETALHE = """
(() => {
    const a = [...document.querySelectorAll('a')].find(a =>
        (a.getAttribute('onclick') || '').includes('DetalheProcessoConsultaPublica')
        || (a.title || '').toLowerCase().includes('ver detalhes'));
    return a ? (a.getAttribute('onclick') || a.href || '') : null;
})()
"""

# "NOME - OAB RN5644 - CPF: ***.974.194-** (ADVOGADO)" e variações sem OAB/documento.
RE_PARTE = re.compile(
    r"^(?P<nome>.+?)"
    r"(?:\s+-\s+OAB\s+(?P<oab>[A-Z]{2}\s?\d+(?:-?[A-Z])?))?"  # RN5644, RN5644-B
    r"(?:\s+-\s+(?P<tipo_doc>CPF|CNPJ):\s*(?P<documento>[\d.\-/*Xx]+))?"
    r"\s*\((?P<papel>[^()]*(?:\([^()]*\)[^()]*)*)\)\s*$"  # aceita "(DEFENSORIA (POLO ATIVO))"
)
ROTULOS_CAPA = ("Número Processo", "Data da Distribuição", "Classe Judicial", "Assunto",
                "Jurisdição", "Órgão Julgador", "Endereço")
FIM_POLOS = ("Movimentações", "Documentos juntados")
COLUNAS_CSV = ("processo", "status", "hora", "duracao_s", "desafio", "cliques_buster", "tentativas",
               "classe", "data_distribuicao", "orgao_julgador", "jurisdicao",
               "polo_ativo", "polo_ativo_papel", "polo_ativo_documento", "advogados_polo_ativo",
               "polo_passivo", "polo_passivo_papel", "polo_passivo_documento", "advogados_polo_passivo")


class Bloqueado(Exception):
    """O Google recusou o desafio de áudio ou o Akamai negou o acesso."""


class DesafioNaoResolvido(TimeoutError):
    """O Buster não resolveu o desafio (ou o botão dele não apareceu)."""

# =============================================================================== navegador e reCAPTCHA


def esperar(pagina, js: str, timeout: int) -> bool:
    """Espera o JS devolver verdadeiro (checa a cada 1 s, até timeout s)."""
    for _ in range(timeout):
        try:
            if pagina.evaluate(js):
                return True
        except PlaywrightError:  # o Akamai recarrega a página no meio da verificação
            pass
        time.sleep(1)
    return False


def clicar(pagina, alvo) -> None:
    """Leva o mouse em passos até um ponto do elemento (Locator) antes de clicar --
    faz o papel do humanize do Camoufox; o reCAPTCHA pontua o movimento do mouse.
    Vale para elemento dentro de iframe: a caixa vem em coordenadas da página."""
    alvo.scroll_into_view_if_needed(timeout=5000)
    caixa = alvo.bounding_box(timeout=5000)
    if not caixa:
        alvo.click(timeout=5000)
        return
    x = caixa["x"] + caixa["width"] * random.uniform(0.3, 0.7)
    y = caixa["y"] + caixa["height"] * random.uniform(0.3, 0.7)
    pagina.mouse.move(x, y, steps=random.randint(15, 30))
    pagina.mouse.click(x, y)


def pesquisar(pagina, processo: str, tele: dict) -> str | None:
    """Preenche o número, clica em Pesquisar e devolve o onclick/href do
    "Ver Detalhes". Se abrir o desafio, clica uma vez no Buster e soma em tele.
    None = sem resultado; DesafioNaoResolvido = o Buster falhou (quem chama
    recarrega a página); Bloqueado = áudio recusado."""
    campo = pagina.locator(CAMPO_PROCESSO)
    clicar(pagina, campo)
    campo.press_sequentially(re.sub(r"\D", "", processo), delay=80)  # campo tem máscara
    time.sleep(1)
    clicar(pagina, pagina.locator(BOTAO_PESQUISAR))

    clicou = False
    desde = None  # quando o desafio apareceu; depois do clique, quando clicou
    ultimo_sinal = time.time()
    while True:
        link = pagina.evaluate(JS_LINK_DETALHE)
        if link:
            return link
        if pagina.evaluate(JS_DESAFIO_VISIVEL):
            ultimo_sinal = time.time()
            if desde is None:
                desde = time.time()
                if not tele["desafio"]:
                    log.info("apareceu o desafio do reCAPTCHA")
                    tele["desafio"] = True
            desafio = next((f for f in pagina.frames if "bframe" in f.url), None)
            try:
                if desafio and desafio.locator(AUDIO_BLOQUEADO).count():
                    raise Bloqueado("o Google recusou o desafio de áudio (consultas automáticas)")
                if not clicou and desafio and desafio.locator(BOTAO_BUSTER).count():
                    tele["cliques_buster"] += 1
                    log.info("clicando no Buster")
                    clicar(pagina, desafio.locator(BOTAO_BUSTER).first)
                    clicou, desde = True, time.time()
            except PlaywrightError:  # o iframe do desafio recarrega no meio do áudio
                pass
            if time.time() - desde > ESPERA_BUSTER:
                raise DesafioNaoResolvido("o Buster não resolveu o desafio" if clicou
                                          else "o botão do Buster não apareceu")
        elif time.time() - ultimo_sinal > TIMEOUT_RESULTADO:
            return None
        time.sleep(1)


def buster_funciona(contexto) -> bool:
    """O Buster está ligado neste perfil? (a página de opções dele só abre com a extensão carregada e ativa)"""
    aba = contexto.new_page()
    try:
        aba.goto(f"chrome-extension://{BUSTER}/src/options/index.html", timeout=15000)
        return True
    except PlaywrightError:
        return False
    finally:
        aba.close()


def garantir_buster(contexto, pagina, deve_parar=None) -> bool:
    """Sem o Buster o desafio do reCAPTCHA não sai. Perfil sem ele (um worker novo, na 1ª vez): abre a página do
    Buster na Chrome Web Store e espera até ESPERA_INSTALAR_BUSTER s a pessoa clicar em "Usar no Chrome". Nas
    próximas execuções o perfil já tem a extensão. Devolve False se não foi instalado a tempo, se a janela do
    Chrome foi fechada antes ou se deve_parar() ficou verdadeiro (Ctrl+C); na próxima execução o worker confere de
    novo."""
    try:
        for _ in range(3):                  # logo depois de abrir o Chrome a extensão pode ainda estar carregando
            if buster_funciona(contexto):
                return True
            time.sleep(2)
        log.warning(f"o Buster não está instalado no perfil {PERFIL.name}: na janela do Chrome que abriu, clique em "
                    f"'Usar no Chrome' (a esteira espera até {ESPERA_INSTALAR_BUSTER // 60} min)")
        pagina.goto(LOJA_BUSTER, wait_until="domcontentloaded")
        fim = time.time() + ESPERA_INSTALAR_BUSTER
        while time.time() < fim and not (deve_parar and deve_parar()):
            time.sleep(10)
            if (PERFIL / "Default" / "Extensions" / BUSTER).exists() and buster_funciona(contexto):
                log.info(f"Buster instalado no perfil {PERFIL.name}")
                return True
    except PlaywrightError:
        log.warning(f"a janela do Chrome foi fechada antes de o Buster ser conferido no perfil {PERFIL.name}")
    return False


def abrir_detalhe(pagina, link: str):
    """A linha abre a popup via openPopUp('...listView.seam?ca=...'); navegar
    direto na URL é mais estável que esperar a popup."""
    m = re.search(r"(/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"]+)", link)
    if m:
        pagina.goto(BASE + m.group(1), wait_until="domcontentloaded")
        return pagina
    with pagina.context.expect_page() as nova:
        pagina.click("a[title='Ver Detalhes']")
    detalhe = nova.value
    detalhe.wait_for_load_state("domcontentloaded")
    return detalhe

# =============================================================================== leitura do detalhe


def secao(linhas: list[str], titulo: str, fins: tuple[str, ...]) -> list[str]:
    """Linhas entre o título (ex.: 'Polo ativo') e o próximo título de seção.
    Os fins valem sem caixa e pelo começo: a página usa 'Polo Passivo',
    'Movimentações do Processo' e 'Documentos juntados ao processo'."""
    try:
        ini = next(i for i, linha in enumerate(linhas) if linha.strip().lower() == titulo.lower())
    except StopIteration:
        return []
    fins = tuple(f.lower() for f in fins)
    saida = []
    for linha in linhas[ini + 1:]:
        if linha.strip().lower().startswith(fins):
            break
        saida.append(linha.strip())
    return saida


def extrair_partes(linhas: list[str]) -> tuple[list[dict], list[dict]]:
    """(partes, advogados) das linhas de um polo; advogado = tem OAB ou papel ADVOGADO."""
    partes, advogados = [], []
    for linha in linhas:
        m = RE_PARTE.match(linha)
        if not m:
            continue
        item = {k: (v.strip() if v else None) for k, v in m.groupdict().items()}
        (advogados if item["oab"] or "ADVOGADO" in item["papel"].upper() else partes).append(item)
    return partes, advogados


def linhas_da_pagina(html: str, url: str) -> list[str]:
    """Texto visível do detalhe, uma linha por elemento, sem as linhas vazias."""
    texto = Selector(content=html, url=url).css("body")[0].get_all_text(separator="\n", strip=True)
    return [linha for linha in texto.split("\n") if linha.strip()]


def extrair_capa(linhas: list[str]) -> dict:
    """Rótulo -> valor dos campos da capa (o valor é a linha seguinte ao rótulo)."""
    capa = {}
    for i, linha in enumerate(linhas[:-1]):
        rotulo = linha.strip()
        if rotulo in ROTULOS_CAPA and rotulo not in capa:
            capa[rotulo] = linhas[i + 1].strip()
    return capa


# =============================================================================== consulta


@contextmanager
def filhos_ignoram_ctrl_c():
    """Enquanto dura, este processo ignora o Ctrl+C, e os processos criados nesse meio-tempo nascem ignorando (no
    Windows a marca é herdada). Usado para subir o driver do Playwright: sem isso o Ctrl+C do console mata o driver
    junto com o robô, e o contexto.close() fica esperando para sempre um driver que já morreu."""
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleCtrlHandler(None, True)
    try:
        yield
    finally:
        kernel32.SetConsoleCtrlHandler(None, False)       # o robô volta a receber o Ctrl+C (KeyboardInterrupt)


@contextmanager
def abrir_chrome():
    """Chrome instalado com o perfil do robô, janela maximizada e extensões ligadas (o Buster). O driver do Playwright
    (e o Chrome que ele abre) não recebe o Ctrl+C do console: quem recebe é o robô, que então fecha o Chrome."""
    with filhos_ignoram_ctrl_c():
        playwright = sync_playwright().start()
    try:
        contexto = playwright.chromium.launch_persistent_context(
            str(PERFIL),
            channel="chrome",   # o Chrome instalado, não o Chromium do Playwright
            headless=False,     # o Akamai barra o Chrome headless
            locale="pt-BR",
            no_viewport=True,
            # --disable-extensions desligaria as extensões do perfil; --enable-automation
            # põe a faixa "controlado por software de teste automatizado".
            ignore_default_args=["--disable-extensions", "--enable-automation"],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
        )
        try:
            yield contexto
        finally:
            try:
                contexto.close()
            except PlaywrightError:  # o navegador já tinha fechado
                pass
    finally:
        playwright.stop()


def consultar(pagina, processo: str, tele: dict) -> dict | None:
    """Uma consulta completa, da tela de pesquisa ao detalhe. Devolve o resultado (capa e partes); None = sem
    resultado. Se o desafio não sair, recarrega a página de pesquisa e refaz o processo (até NOVAS_TENTATIVAS
    vezes); anota em tele quantas tentativas foram."""
    tele.update(desafio=False, cliques_buster=0, tentativas=0)
    for tentativa in range(1, NOVAS_TENTATIVAS + 2):
        tele["tentativas"] = tentativa
        pagina.goto(URL, wait_until="domcontentloaded")  # página nova, desafio novo
        if not esperar(pagina, JS_FORM_OK, TIMEOUT_AKAMAI):
            if "Access Denied" in pagina.content():
                raise Bloqueado("o Akamai negou o acesso (Access Denied)")
            raise TimeoutError(f"o formulário não apareceu em {TIMEOUT_AKAMAI}s")
        try:
            link = pesquisar(pagina, processo, tele)
            break
        except DesafioNaoResolvido as exc:
            if tentativa > NOVAS_TENTATIVAS:
                raise DesafioNaoResolvido(f"desafio do reCAPTCHA não resolvido em {tentativa} tentativas") from None
            log.warning(f"{exc}; recarregando a página ({tentativa}/{NOVAS_TENTATIVAS})")
    if not link:
        return None

    detalhe = abrir_detalhe(pagina, link)
    esperar(detalhe, JS_DETALHE_OK, TIMEOUT_AKAMAI)
    html, url_detalhe = detalhe.content(), detalhe.url
    if detalhe is not pagina:  # veio pela popup
        detalhe.close()

    linhas = linhas_da_pagina(html, url_detalhe)

    ativos, adv_ativo = extrair_partes(secao(linhas, "Polo ativo", ("Polo passivo",) + FIM_POLOS))
    passivos, adv_passivo = extrair_partes(secao(linhas, "Polo passivo", FIM_POLOS))
    resultado = {
        "processo": processo,
        "url_detalhe": url_detalhe,
        "capa": extrair_capa(linhas),
        "polo_ativo": ativos,
        "advogados_polo_ativo": adv_ativo,
        "polo_passivo": passivos,
        "advogados_polo_passivo": adv_passivo,
    }
    return resultado


# =============================================================================== CSV e execução


def linha_csv(processo: str, status: str, resultado: dict | None, tele: dict) -> dict:
    """Uma linha por processo; várias partes na mesma célula, separadas por ' | '
    (na mesma ordem nas colunas de nome, papel e documento)."""
    linha = {
        "processo": processo,
        "status": status,
        "hora": tele.get("hora"),
        "duracao_s": tele.get("duracao_s"),
        "desafio": "sim" if tele.get("desafio") else "não",
        "cliques_buster": tele.get("cliques_buster", 0),
        "tentativas": tele.get("tentativas"),
    }
    if not resultado:
        return linha

    def juntar(itens, campo):
        return " | ".join(i[campo] or "" for i in itens)

    def advogados(itens):
        return " | ".join(f"{a['nome']} ({a['oab']})" if a["oab"] else a["nome"] for a in itens)

    capa = resultado["capa"]
    linha.update(
        classe=capa.get("Classe Judicial"),
        data_distribuicao=capa.get("Data da Distribuição"),
        orgao_julgador=capa.get("Órgão Julgador"),
        jurisdicao=capa.get("Jurisdição"),
        polo_ativo=juntar(resultado["polo_ativo"], "nome"),
        polo_ativo_papel=juntar(resultado["polo_ativo"], "papel"),
        polo_ativo_documento=juntar(resultado["polo_ativo"], "documento"),
        advogados_polo_ativo=advogados(resultado["advogados_polo_ativo"]),
        polo_passivo=juntar(resultado["polo_passivo"], "nome"),
        polo_passivo_papel=juntar(resultado["polo_passivo"], "papel"),
        polo_passivo_documento=juntar(resultado["polo_passivo"], "documento"),
        advogados_polo_passivo=advogados(resultado["advogados_polo_passivo"]),
    )
    return linha


def gravar_csv(caminho: Path, linhas: list[dict]) -> None:
    """CSV do lote inteiro (uma linha por processo), no padrão do projeto."""
    gravar_csv_padrao(caminho, linhas, COLUNAS_CSV)


def main() -> None:
    """Consulta os processos da linha de comando numa sessão do Chrome e grava o CSV a cada processo."""
    parser = argparse.ArgumentParser(description="Credor de processos do TJRN pela consulta pública do PJe 1º grau.")
    parser.add_argument("processos", nargs="*", default=["0826136-40.2019.8.20.5001"])
    parser.add_argument("--csv", type=Path, default=SAIDA / f"lote_{datetime.now():%Y%m%d_%H%M%S}.csv")
    args = parser.parse_args()

    configurar_log(__file__, SAIDA / "logs")
    PERFIL.mkdir(parents=True, exist_ok=True)
    SAIDA.mkdir(parents=True, exist_ok=True)

    log.info("Não minimize a janela do Chrome: o Akamai barra janela minimizada.")
    pendentes = list(args.processos)
    total, linhas, reaberturas, parar = len(pendentes), [], 0, None
    while pendentes and not parar:
        with abrir_chrome() as contexto:
            pagina = contexto.pages[0] if contexto.pages else contexto.new_page()
            while pendentes and not parar:
                processo = pendentes[0]
                if linhas:
                    time.sleep(random.uniform(*PAUSA_ENTRE))
                log.info(f"[{len(linhas) + 1}/{total}] {processo}")
                tele = {"hora": datetime.now().isoformat(timespec="seconds")}
                inicio = time.time()
                try:
                    resultado = consultar(pagina, processo, tele)
                    status = "ok" if resultado else "sem resultado"
                except Bloqueado as exc:
                    resultado, status, parar = None, f"bloqueio: {exc}", f"bloqueio em {processo}"
                except (PlaywrightError, TimeoutError) as exc:
                    if pagina.is_closed():  # o Chrome fechou no meio da consulta
                        if reaberturas < REABERTURAS:
                            reaberturas += 1
                            log.warning(f"o Chrome fechou; reabrindo ({reaberturas}/{REABERTURAS}) "
                                        "e repetindo o processo")
                            break  # sai do with, reabre e tenta o mesmo processo
                        parar = f"o Chrome fechou mais de {REABERTURAS} vezes"
                    resultado, status = None, f"erro: {str(exc).strip().splitlines()[0]}"
                tele["duracao_s"] = round(time.time() - inicio)
                credor = " | ".join(p["nome"] for p in resultado["polo_ativo"]) if resultado else ""
                nivel = logging.INFO if resultado or status == "sem resultado" else logging.WARNING
                log.log(nivel, f"{processo}: {status} em {tele['duracao_s']}s{' -- ' + credor if credor else ''}")
                linhas.append(linha_csv(processo, status, resultado, tele))
                gravar_csv(args.csv, linhas)  # a cada processo: um erro no meio não perde o que já saiu
                pendentes.pop(0)

    if parar:
        log.error(f"Lote interrompido: {parar} ({len(linhas)} de {total} consultados).")
    log.info(f"Navegador fechado. CSV: {args.csv}")


if __name__ == "__main__":
    main()
