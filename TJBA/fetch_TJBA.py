"""
fetch_TJBA.py - credor dos precatórios do TJBA pela consulta pública, de ponta a ponta: lê um crédito do banco,
acha e confirma o originário (DJEN + PJe 1º grau público) e grava o resultado dele no banco na hora.

1. Fila: o robô tem software próprio (CONSULTA_PUBLICA_TJBA) na fila creditos.coleta_credor. assumir_fila() passa
   para ele as linhas do TJBA que eram do RPA (fora de lease) e deixa PENDENTE quem não tem credor. Cada crédito é
   reservado com fila_credor_pegar (lease) antes de raspar.
2. Candidatos a originário: o que já está ligado, a pista deixada pelo RPA (capa antiga ou CNJ achado) e a busca
   pelo nome do beneficiário no DJEN (beneficiário no polo ativo, ente no passivo, não mais novo que o precatório).
3. Confirma cada candidato pela capa que já está no banco (credor com CPF) ou abrindo o PJe 1º grau público
   (captcha Tencent resolvido sozinho com OpenCV). Confirmado = beneficiário no polo ativo e ente no polo passivo.
4. Decide: 1 confirmado -> liga; vários -> desempata por valor citado no DJEN ou OAB em comum com o precatório;
   sem desempate -> SUCESSO_ANALISAR (não liga o originário; se todos são a mesma pessoa, com o mesmo CPF/CNPJ, e
   nenhum candidato ficou sem conferir, liga só o credor: regra CREDOR_UNICO).
5. Grava o crédito numa transação só dele, logo depois de processado (erro desfaz só ele): originário, partes
   (registrar_capa), credor (registrar_credor), metadata (registrar_credito), capa antiga e filas mensais (legado)
   e o status na fila (fila_credor_finalizar).

Erro passageiro (captcha, portal fora, timeout) devolve o crédito para a fila (fila_credor_adiar) na hora e nunca
vira FALHA. Ctrl+C desfaz o que estava em gravação e devolve o crédito em andamento para a fila.

Vários workers na mesma máquina: --workers N sobe os workers 1..N neste terminal (um processo filho por worker), ou
--worker N roda só o worker N (um por terminal). A fila com lease garante que dois workers nunca pegam o mesmo
crédito. Cada worker tem o seu perfil do Chrome, a sua saída para o DJEN (o 1 direto, o N pelo (N-1)º
PROXY_* do .env, porque o DJEN limita por IP) e os seus arquivos (o worker 1 com os nomes de sempre, os outros com
_w<N>). Só o worker 1 assume a fila do RPA. Deadlock entre as gravações de dois workers devolve o crédito para a
fila (ADIADO), não vira FALHA.

Saídas em TJBA/saida: fetch_TJBA.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql, desfazer_fila_*.sql (todos com _w<N> nos workers 2 em diante) e o HTML de cada detalhe
aberto no PJe (pje1g_html/).

Uso:
    python fetch_TJBA.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJBA.py                        # assume a fila do TJBA e processa até acabar (Ctrl+C para parar)
    python fetch_TJBA.py --limite 30            # para depois de 30 créditos
    python fetch_TJBA.py --workers 4            # 4 workers neste terminal (Ctrl+C para todos)
    python fetch_TJBA.py --worker 2             # só o 2º worker, em outro terminal, junto com o 1

Log: terminal e TJBA/saida/logs/fetch_TJBA[_w<N>]_AAAAMMDD.log, no formato padrão (utils/log.py).
"""
import argparse
import html as H
import json
import logging
import msvcrt
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import time
import unicodedata
import urllib.request
import winreg
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin

import cv2                                                   # pip install opencv-python-headless
import numpy as np
import psycopg2
import psycopg2.errors
import requests
from dotenv import load_dotenv

try:
    from patchright.sync_api import Error as ErroNavegador, sync_playwright
except ImportError:
    from playwright.sync_api import Error as ErroNavegador, sync_playwright

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils import banco  # noqa: E402
from utils.arquivos import anexar_csv  # noqa: E402
from utils.banco import chave_texto_lote, como_dicts, partes_do_banco  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

# =============================================================================== configuração

SAIDA = AQUI / "saida"
PASTA_HTML = SAIDA / "pje1g_html"
PERFIL = AQUI / ".chrome-profile-pje-tjba"
load_dotenv(AQUI.parent / ".env")      # PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD e PROXY_*
log = logging.getLogger("fetch_TJBA")

TRIBUNAL_TJBA = 105
SOFTWARE = "CONSULTA_PUBLICA_TJBA"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER_N = 1                           # número do worker nesta máquina (--worker); definir_worker() troca os 3
WORKER = f"{socket.gethostname()}:TJBA:consulta_publica:w1:{os.getpid()}"
TETO_CREDITO = 15 * 60                 # s por crédito; passou disso, volta para a fila
LEASE = f"{TETO_CREDITO // 60 + 30} minutes"          # cobre o processamento (até o teto) e a gravação do crédito
ADIAMENTO = "30 minutes"
ADIAMENTO_CONFLITO = "5 minutes"       # deadlock com a gravação de outro worker: tenta de novo logo
ASSUMIR_A_CADA = 30 * 60               # s entre rodadas de assumir_fila
MAX_FALHAS_SEGUIDAS = 5                # falhas técnicas (ou de gravação) seguidas que param o robô
PAUSA_ENTRE_WORKERS = 10               # s entre subir um worker e o próximo (--workers): Chrome e captcha em escada
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"

DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
INTERVALO_DJEN = 1.2                   # s entre consultas (o DJEN aguenta ~1,3-2 por s por IP)
FALHAS_PARA_DESLIGAR_PROXY = 3         # falhas seguidas do proxy do worker antes de o DJEN passar a sair direto
MAX_PAGINAS_DJEN = 5                   # 100 publicações por página
MAX_CANDIDATOS_DJEN = 15               # candidatos abertos no DJEN pelo número (valor e OAB)

BASE = "https://consultapublicapje.tjba.jus.br"
URL = BASE + "/pje/ConsultaPublica/listView.seam"
HOST = "http://127.0.0.1"
CAMPO_NUMERO = "[id='fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso']"
BOTAO_PESQUISAR = "[id='fPP:searchProcessos']"
ESPERA_PJE = 90                        # s pela resposta de uma pesquisa ou detalhe
TENTATIVAS_CAPTCHA = 4
MAX_ABERTOS_PJE = 10                   # candidatos abertos no PJe por crédito
MAX_PAGINAS_PARTES = 50
PAUSA_PJE = 2

CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")
RE_ESPOLIO_REP = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?(?P<falecido>.+?)\s+(?:rep\.?|representad[oa])\s+"
                            r"(?:por\s+)?(?P<rep>.+?)\s*$", re.I)
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
RE_REP = re.compile(r"\s+(?:rep\.?|representad[oa])\s+(?:por\s+)?.*$", re.I)
RE_E_OUTROS = re.compile(r"\s+e\s+outr[oa]s?\s*$", re.I)
RE_APOSTO = re.compile(r"\s*\([^)]*\)\s*$")
CLASSE_EXECUCAO = re.compile(r"cumprimento|execu", re.I)
RE_ORGAO_PUBLICO = re.compile(r"^(?:ESTADO D|MUNICIPIO D|UNIAO\b|DISTRITO FEDERAL)|PROCURADORIA|DEFENSORIA PUBLICA|"
                              r"MINISTERIO PUBLICO|FAZENDA PUBLICA|PREFEITURA|CAMARA MUNICIPAL|TRIBUNAL D")

RE_LINK_DETALHE = re.compile(r"(/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+)")
RE_NADA = re.compile(r"n[ãa]o\s+encontrou\s+nenhum\s+processo", re.I)
# Linha de parte como o PJe escreve: "FULANO - CPF: 000.000.000-00 (AUTOR)",
# "BELTRANO - OAB BA12345 - CPF: ... (ADVOGADO)"
RE_PARTE = re.compile(
    r"([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ][A-Za-zÁÂÃÀÉÊÍÓÔÕÚÜÇáâãàéêíóôõúüç&\.\-\s']{3,90}?)"
    r"(?:\s*-\s*OAB\s*([A-Z]{2})\s*(\d+[A-Z]?))?"
    r"(?:\s*-\s*(CPF|CNPJ):\s*([\d\.\-/\*]{11,20}))?"
    r"\s*\(([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ][A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ\s/\.\-]{2,39})\)")
RE_ADVOGADO = re.compile(r"ADVOGAD|PROCURADOR|DEFENSOR", re.I)
# Campos do bloco "Dados do Processo" (os rótulos vêm colados no valor, sem dois-pontos).
ROTULOS = {
    "data_autuacao": r"(?:Data\s+da\s+[Dd]istribui[çc][ãa]o|Autuado\s+em|[ÚU]ltima\s+[Dd]istribui[çc][ãa]o)",
    "classe_judicial": r"Classe\s+[Jj]udicial",
    "assunto": r"Assunto",
    "jurisdicao": r"Jurisdi[çc][ãa]o",
    "orgao_julgador": r"[ÓO]rg[ãa]o\s+[Jj]ulgador",
}
FIM_CAMPO = "|".join(ROTULOS.values()) + (r"|N[úu]mero\s+Processo|Endere[çc]o|Compet[êe]ncia|Ju[íi]zo\s+100%"
                                          r"|Valor\s+da\s+[Cc]ausa|Segredo|Justi[çc]a\s+[Gg]ratuita|Prioridade"
                                          r"|Tutela|Polo\s+[Aa]tivo|Polo\s+[Pp]assivo|Movimenta|Documentos")
# Captcha Tencent (iframe drag_ele_global.html): #slideBg = fundo com o buraco (img_index=1);
# a peça é o .tc-fg-item quadrado, recorte do sprite img_index=0; o slider é .tc-slider-normal.
JS_CAPTCHA = r"""() => {
  const r = e => e.getBoundingClientRect(), url = s => (s.match(/url\("?(.*?)"?\)/) || [])[1];
  const bg = document.getElementById('slideBg');
  const peca = [...document.querySelectorAll('.tc-fg-item:not(.tc-slider-normal)')]
                 .find(e => r(e).width > 0 && Math.abs(r(e).width - r(e).height) < 2);
  const slider = document.querySelector('.tc-fg-item.tc-slider-normal');
  if (!bg || !peca || !slider) return null;
  const cp = getComputedStyle(peca);
  return {bg_url: url(getComputedStyle(bg).backgroundImage), bg_x: r(bg).x, bg_y: r(bg).y, bg_w: r(bg).width,
          sp_url: url(cp.backgroundImage), sp_w: parseFloat(cp.backgroundSize),
          pos: cp.backgroundPosition.split(' ').map(parseFloat),
          peca_x: r(peca).x, peca_y: r(peca).y, peca_w: r(peca).width, slider_w: r(slider).width};
}"""
JS_IFRAME_VISIVEL = ("() => { const f = document.getElementById('tcaptcha_iframe_dy'); "
                     "return !!f && f.getBoundingClientRect().y >= 0 }")

PAPEL_LEGADO = {"ATIVO": "REQUERENTE", "PASSIVO": "REQUERIDO"}    # como o RPA grava a capa antiga
POLO_BRUTO = {"ATIVO": "AUTOR", "PASSIVO": "REU"}

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "beneficiario", "ente", "ultimo_status", "resultado",
           "motivo", "originario", "regra", "fontes", "credor", "credor_documento", "candidatos", "capa_fonte",
           "banco", "legado", "credores_antes", "credores_depois", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set()}


class ErroTecnico(Exception):
    """Falha passageira (captcha, PJe/DJEN fora do ar, timeout): o crédito volta para a fila, não vira FALHA."""

# =============================================================================== utilidades


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples (para comparar nomes)."""
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", t)).strip()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE' e sem o 'REGISTRADO(A) CIVILMENTE COMO ...'."""
    return re.split(r"\bREGISTRAD[OA]\b", normal(RE_ESPOLIO.sub("", nome or "")))[0].strip()


def nomes_para_buscar(beneficiario):
    """Nome da lista -> nomes a pesquisar (espólio com inventariante: os dois)."""
    b = (beneficiario or "").strip()
    m = RE_ESPOLIO_REP.match(b)
    if m:
        nomes = [m["falecido"], m["rep"]]
    else:
        for regex in (RE_APOSTO, RE_REP, RE_ESPOLIO, RE_E_OUTROS):
            b = regex.sub("", b)
        nomes = [b]
    return [n for n in (x.strip(" -,;.") for x in nomes) if n]


def chaves_ente(ente):
    """'MUNICIPIO DE ITABUNA' -> {'MUNICIPIO DE ITABUNA', 'ITABUNA'}; 'X - SIGLA' -> {'X', 'SIGLA'}."""
    partes = [normal(p) for p in re.split(r"\s+-\s+", ente or "") if p.strip()]
    chaves = set(partes)
    for p in partes:
        m = re.match(r"MUNICIPIO D[EOA]S? (.+)", p)
        if m:
            chaves.add(m.group(1))
    return {c for c in chaves if len(c) >= 4}


def formatos_valor(valor):
    """41831.2 -> {'41.831,20', '41831,20'}."""
    br = f"{float(valor):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return {br, br.replace(".", "")}


def valor_numerico(v):
    """Valor do banco ou da lista ('41.831,20', '41831.20', 41831.2) -> float; None se não for número."""
    if v is None or isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def data_br(t):
    """Primeira data dd/mm/aaaa do texto -> date; None se não tiver."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", t or "")
    return date(int(m[3]), int(m[2]), int(m[1])) if m else None


def fmt_credores(itens):
    """{(papel, nome, documento, origem)} -> texto para o CSV."""
    return " | ".join(f"{nome} ({doc or 'sem doc'}, {papel}, {origem})" for papel, nome, doc, origem in sorted(itens))

# =============================================================================== DJEN


def proxies_do_env():
    """PROXY_01..05 do .env ('host:porta:usuário:senha' ou URL) -> [(nome, URL do proxy)], em ordem."""
    saida = []
    for chave in sorted(k for k in os.environ if re.fullmatch(r"PROXY_\d+", k)):
        v = os.environ[chave].strip()
        if not v:
            continue
        if "://" not in v:
            partes = v.split(":", 3)
            v = f"http://{partes[2]}:{partes[3]}@{partes[0]}:{partes[1]}" if len(partes) == 4 else f"http://{v}"
        saida.append((chave, v))
    return saida


def saida_djen(worker):
    """(nome, proxy) por onde o worker consulta o DJEN, que limita por IP: o worker 1 sai direto, o worker n usa o
    (n-1)º proxy do .env; sem proxy para ele, sai direto também (e divide o limite com o worker 1)."""
    if worker == 1:
        return "direta", None
    proxies = proxies_do_env()
    if worker - 2 < len(proxies):
        return proxies[worker - 2]
    log.warning(f"sem proxy no .env para o worker {worker}: o DJEN sai direto e divide o limite por IP com o worker 1")
    return "direta", None


class Djen:
    """Consulta o DJEN com intervalo fixo e nova tentativa, pela saída do worker (direta ou proxy).
    Cache só na memória (nomes repetidos entre créditos)."""

    def __init__(self, saida):
        self.sessao = requests.Session()
        self.sessao.headers["User-Agent"] = "Mozilla/5.0"
        self.ultima = 0.0
        self.cache = {}
        self.nome, proxy = saida
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self.falhas_proxy = 0

    def _proxy_falhou(self, motivo):
        """Conta a falha do proxy; depois de FALHAS_PARA_DESLIGAR_PROXY seguidas, o DJEN passa a sair direto."""
        self.falhas_proxy += 1
        if self.falhas_proxy >= FALHAS_PARA_DESLIGAR_PROXY:
            log.warning(f"DJEN: proxy {self.nome} desligado ({motivo}); saindo direto")
            self.nome, self.proxies = "direta", None

    def buscar(self, com_texto, **params):
        """Publicações da consulta (enxutas), respeitando INTERVALO_DJEN; 429 espera mais.
        ErroTecnico se não responder."""
        chave = json.dumps(params, sort_keys=True, ensure_ascii=False)
        if chave in self.cache:
            return self.cache[chave]
        for _ in range(6):
            espera = self.ultima + INTERVALO_DJEN - time.time()
            if espera > 0:
                time.sleep(espera)
            self.ultima = time.time()
            try:
                r = self.sessao.get(DJEN, params=params, proxies=self.proxies, timeout=60)
            except requests.RequestException as e:
                if self.proxies:
                    self._proxy_falhou(e.__class__.__name__)
                time.sleep(5)
                continue
            if r.status_code == 200:
                self.falhas_proxy = 0
                itens = [self._enxuto(i, com_texto) for i in r.json().get("items", [])]
                if len(self.cache) > 20000:
                    self.cache.clear()
                self.cache[chave] = itens
                return itens
            if r.status_code in (403, 407) and self.proxies:     # proxy recusado ou fora do Brasil
                self._proxy_falhou(f"HTTP {r.status_code}")
            time.sleep(30 if r.status_code == 429 else 5)
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: DJEN não respondeu")

    @staticmethod
    def _enxuto(item, com_texto):
        """Só o que o robô usa da publicação: número, classe, partes por polo, advogados com OAB e
        (se pedido) o texto."""
        advogados = []
        for a in item.get("destinatarioadvogados") or []:
            adv = a.get("advogado") or {}
            if adv.get("numero_oab"):
                advogados.append({"nome": adv.get("nome") or "", "oab": str(adv["numero_oab"]),
                                  "uf": (adv.get("uf_oab") or "").upper()})
        texto = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", item.get("texto") or "")) if com_texto else ""
        return {"numero": so_digitos(item.get("numero_processo") or item.get("numeroprocessocommascara"))[:20],
                "classe": (item.get("nomeClasse") or "").upper(),
                "partes": [[d.get("polo"), d.get("nome") or ""] for d in item.get("destinatarios") or []],
                "advogados": advogados,
                "texto": texto}

    def por_nome(self, nome):
        """Publicações do TJBA com a parte pelo nome (até MAX_PAGINAS_DJEN páginas de 100)."""
        itens = []
        for pagina in range(1, MAX_PAGINAS_DJEN + 1):
            lote = self.buscar(False, nomeParte=nome, siglaTribunal="TJBA", pagina=pagina, itensPorPagina=100)
            itens += lote
            if len(lote) < 100:
                break
        return itens

    def por_numero(self, numero20):
        """Publicações do processo, com o texto (para achar valor, nº do precatório e OAB)."""
        return self.buscar(True, numeroProcesso=numero20)


def candidatos_djen(djen, lead, nomes, chaves):
    """([(cnj20, {classe, valor, oabs})], vezes como advogado): processos com o beneficiário no polo ativo e o ente
    no passivo, anteriores ao precatório; cumprimento/execução e os mais novos primeiro. O DJEN põe os advogados no
    polo A: publicação em que o beneficiário é advogado não conta (precatório de honorários)."""
    prec20, alvo = lead["precatorio20"], {chave_nome(n) for n in nomes}
    procs, como_advogado = {}, 0
    for it in (i for n in nomes for i in djen.por_nome(n)):
        p = procs.setdefault(it["numero"], {"classe": it["classe"], "ativo": False, "ente": False})
        advogados = {chave_nome(a["nome"]) for a in it["advogados"]}
        for polo, nome in it["partes"]:
            k = chave_nome(nome)
            if polo == "A" and k in alvo:
                como_advogado += k in advogados
                p["ativo"] |= k not in advogados
            p["ente"] |= polo == "P" and any(c in normal(nome) for c in chaves)
    cands = {n: p for n, p in procs.items()
             if len(n) == 20 and n != prec20 and n[13:16] == "805" and n[16:20] != "0000"
             and int(n[9:13]) <= int(prec20[9:13]) and p["ativo"] and (p["ente"] or not chaves)}
    ordem = sorted(cands, key=lambda n: (not CLASSE_EXECUCAO.search(cands[n]["classe"]), -int(n[9:13])))
    valores = set().union(*(formatos_valor(v) for v in lead["valores"])) if lead["valores"] else set()
    saida = []
    for i, n in enumerate(ordem):
        info = {"classe": cands[n]["classe"], "valor": False, "oabs": set()}
        if i < MAX_CANDIDATOS_DJEN:
            itens = djen.por_numero(n)
            texto = " ".join(x["texto"] for x in itens)
            citados = {so_digitos(x) for x in CNJ.findall(texto)}
            info.update(valor=any(v in texto for v in valores) or prec20 in citados,
                        oabs={a["oab"] + a["uf"] for x in itens for a in x["advogados"]})
        saida.append((n, info))
    return saida, como_advogado


def oabs_do_precatorio(djen, prec20):
    """OABs (número + UF) dos advogados nas publicações do precatório."""
    return {a["oab"] + a["uf"] for x in djen.por_numero(prec20) for a in x["advogados"]}

# =============================================================================== PJe 1º grau (Chrome real por CDP)


def localizar_chrome():
    """Caminho do chrome.exe instalado (registro do Windows, pastas padrão ou PATH)."""
    chave = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe"
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(hive, chave) as k:
                caminho = winreg.QueryValueEx(k, None)[0]
                if caminho and Path(caminho).is_file():
                    return caminho
        except FileNotFoundError:
            pass
    for c in (Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
              Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
              Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe"):
        if c.is_file():
            return str(c)
    if shutil.which("chrome"):
        return shutil.which("chrome")
    raise FileNotFoundError("Chrome não encontrado nesta máquina.")


def porta_livre():
    """Uma porta TCP livre em 127.0.0.1 para a depuração remota do Chrome."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def esperar_cdp(porta, timeout=20.0):
    """Espera o Chrome abrir a porta de depuração (CDP); TimeoutError se não abrir."""
    fim = time.time() + timeout
    while time.time() < fim:
        try:
            with urllib.request.urlopen(f"{HOST}:{porta}/json/version", timeout=1):
                return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"Chrome não respondeu no CDP em {timeout}s")


def sem_script(html):
    """HTML sem os <script> e com as entidades (&aacute; ...) resolvidas."""
    return H.unescape(re.sub(r"<script\b.*?</script>", " ", html, flags=re.S | re.I))


def partes_da_pagina(html):
    """Partes e advogados das tabelas de polo ativo e passivo ('Outros interessados' fica de fora)."""
    limpo = sem_script(html)
    ini_a = limpo.find("processoPartesPoloAtivoResumido")
    ini_p = limpo.find("processoPartesPoloPassivoResumido")
    ini_o = limpo.find("processoParteOutrosInteressadosResumido", max(ini_p, 0))
    fim_a = ini_p if ini_p != -1 else len(limpo)
    fim_p = ini_o if ini_o != -1 else len(limpo)
    achados = []
    for m in RE_PARTE.finditer(limpo):
        if ini_a != -1 and ini_a <= m.start() < fim_a:
            polo = "ATIVO"
        elif ini_p != -1 and ini_p <= m.start() < fim_p:
            polo = "PASSIVO"
        else:
            continue
        achados.append({"nome": " ".join(m.group(1).split()),
                        "oab_uf": m.group(2) or "", "oab_numero": m.group(3) or "",
                        "documento": so_digitos(m.group(5)), "papel": " ".join(m.group(6).split()).upper(),
                        "polo": polo})
    return achados


def campos_da_capa(html):
    """Classe, assunto, jurisdição, órgão julgador e data de autuação do bloco 'Dados do Processo'."""
    t = " ".join(re.sub(r"<[^>]+>", " ", sem_script(html)).split())
    ini = t.rfind("Dados do Processo")
    bloco = t[ini: (t.find("Polo ativo", ini) if t.find("Polo ativo", ini) != -1 else len(t))] if ini != -1 else t
    out = {}
    for chave, rotulo in ROTULOS.items():
        m = re.search(rf"{rotulo}\s*:?\s+(.{{1,400}}?)\s*(?=(?:{FIM_CAMPO})\b|$)", bloco, flags=re.S)
        if m and m.group(1).strip(" :-"):
            out[chave] = " ".join(m.group(1).split()).strip(" :-")
    return out


def captcha_aberto(pagina, imagens):
    """(frame, geometria) do captcha visível e com as duas imagens já baixadas; senão (None, None)."""
    try:
        frame = next((f for f in pagina.frames if "drag_ele" in f.url), None)
        g = frame and pagina.evaluate(JS_IFRAME_VISIVEL) and frame.evaluate(JS_CAPTCHA)
    except Exception:                                   # frame recarregando
        return None, None
    if g and g["bg_w"] > 0 and g["bg_url"] in imagens and g["sp_url"] in imagens:
        return frame, g
    return None, None


def achar_buraco(fundo, sprite, g):
    """x (px do frame) do buraco: casa as bordas do contorno da peça com as bordas do fundo, na altura da peça."""
    k_bg = fundo.shape[1] / g["bg_w"]
    k_sp = sprite.shape[1] / g["sp_w"]
    x0, y0, w = round(-g["pos"][0] * k_sp), round(-g["pos"][1] * k_sp), round(g["peca_w"] * k_sp)
    alfa = cv2.resize(sprite[y0:y0 + w, x0:x0 + w, 3], None, fx=k_bg / k_sp, fy=k_bg / k_sp)
    topo = max(round((g["peca_y"] - g["bg_y"]) * k_bg) - 6, 0)
    faixa = cv2.cvtColor(fundo[topo: topo + alfa.shape[0] + 12], cv2.COLOR_BGR2GRAY)
    res = cv2.matchTemplate(cv2.Canny(cv2.GaussianBlur(faixa, (3, 3), 0), 50, 150), cv2.Canny(alfa, 100, 200),
                            cv2.TM_CCOEFF_NORMED)
    return g["bg_x"] + cv2.minMaxLoc(res)[3][0] / k_bg


def arrastar_slider(pagina, frame, g, alvo_x):
    """Arrasta com aceleração, tremor e leve passada do ponto (a peça anda 1:1 com o slider)."""
    caixa = frame.locator(".tc-fg-item.tc-slider-normal").bounding_box()
    dist = (alvo_x - g["peca_x"]) * caixa["width"] / g["slider_w"]
    x = caixa["x"] + caixa["width"] / 2 + random.uniform(-5, 5)
    y = caixa["y"] + caixa["height"] / 2 + random.uniform(-4, 4)
    pagina.mouse.move(x - random.uniform(20, 40), y + random.uniform(5, 15))
    pagina.mouse.move(x, y, steps=5)
    time.sleep(random.uniform(0.15, 0.35))
    pagina.mouse.down()
    time.sleep(random.uniform(0.08, 0.2))
    passos, extra = random.randint(28, 40), random.uniform(2, 5)
    for d in [(dist + extra) * (1 - (1 - i / passos) ** 3) for i in range(1, passos + 1)] + [dist + extra / 2, dist]:
        pagina.mouse.move(x + d, y + random.uniform(-1.5, 1.5))
        time.sleep(random.uniform(0.012, 0.03))
    time.sleep(random.uniform(0.2, 0.4))
    pagina.mouse.up()


class Pje:
    """Chrome instalado, com perfil próprio, controlado por CDP. Resolve o captcha Tencent sozinho."""

    def __init__(self):
        self.proc = self.pw = self.pagina = None
        self.imagens = {}                               # url -> resposta das imagens do captcha

    def abrir(self):
        """Abre o Chrome no perfil do worker, conecta por CDP e passa a guardar as imagens do captcha.
        A janela não é desacelerada quando fica atrás das dos outros workers (senão o captcha atrasa)."""
        PERFIL.mkdir(parents=True, exist_ok=True)
        for nome in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            try:
                (PERFIL / nome).unlink(missing_ok=True)   # sem isso o Chrome recusa um perfil que não fechou direito
            except OSError:
                pass
        porta = porta_livre()
        self.proc = subprocess.Popen([localizar_chrome(), f"--remote-debugging-port={porta}",
                                      f"--user-data-dir={PERFIL}", "--no-first-run",
                                      "--no-default-browser-check", "--disable-backgrounding-occluded-windows",
                                      "--disable-renderer-backgrounding", "--disable-background-timer-throttling",
                                      URL])
        esperar_cdp(porta)
        self.pw = sync_playwright().start()
        navegador = self.pw.chromium.connect_over_cdp(f"{HOST}:{porta}")
        paginas = [pg for ctx in navegador.contexts for pg in ctx.pages]
        self.pagina = next((pg for pg in paginas if "tjba.jus.br" in pg.url), paginas[0])
        self.pagina.context.on("response",
                               lambda r: self.imagens.__setitem__(r.url, r) if "getcapbysig" in r.url else None)

    def fechar(self):
        """Desliga o Playwright e mata a árvore de processos do Chrome."""
        try:
            if self.pw:
                self.pw.stop()
        except Exception:
            pass
        if self.proc:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(self.proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.proc = self.pw = self.pagina = None

    def reabrir(self):
        """Fecha e abre de novo (navegador caiu ou a aba fechou)."""
        log.warning("reabrindo o Chrome")
        self.fechar()
        self.abrir()

    def esperar(self, pagina, condicao):
        """Espera a condição no HTML resolvendo o captcha que abrir. ErroTecnico se não resolver ou não responder."""
        fim = time.time() + ESPERA_PJE
        tentadas, ultima = set(), 0.0
        while time.time() < fim:
            try:
                achou = condicao(pagina.content())
            except ErroNavegador:                       # página no meio de uma navegação
                achou = None
            if achou:
                return achou
            frame, g = captcha_aberto(pagina, self.imagens)
            if g and g["bg_url"] not in tentadas:
                if len(tentadas) >= TENTATIVAS_CAPTCHA:
                    raise ErroTecnico(f"CAPTCHA_REATIVADO: não resolvido em {TENTATIVAS_CAPTCHA} tentativas")
                tentadas.add(g["bg_url"])
                time.sleep(random.uniform(0.8, 1.5))    # a imagem acabou de aparecer
                try:
                    fundo = cv2.imdecode(np.frombuffer(self.imagens[g["bg_url"]].body(), np.uint8), cv2.IMREAD_COLOR)
                    sprite = cv2.imdecode(np.frombuffer(self.imagens[g["sp_url"]].body(), np.uint8),
                                          cv2.IMREAD_UNCHANGED)
                    arrastar_slider(pagina, frame, g, achar_buraco(fundo, sprite, g))
                except ErroNavegador:
                    raise
                except Exception:                       # imagem estranha: conta como tentativa
                    pass
                ultima = time.time()
            elif g and time.time() - ultima > 6:
                frame.evaluate("() => document.getElementById('reload')?.click()")   # errou e não trocou a imagem
                ultima = time.time()
            time.sleep(1)
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o PJe não respondeu em {ESPERA_PJE} s")

    def consultar(self, numero20):
        """{resultado: OK|NAO_ENCONTRADO, capa, partes, advogados, completa} do processo pelo número."""
        self.imagens.clear()
        pg = self.pagina
        try:
            pg.goto(URL, wait_until="domcontentloaded", timeout=60000)
            campo = pg.locator(CAMPO_NUMERO)
            campo.wait_for(timeout=30000)
            campo.click()
            campo.fill("")
            campo.press_sequentially(numero20, delay=40)
            if so_digitos(campo.input_value()) != numero20:
                campo.fill(formatar_cnj(numero20))
            pg.locator(BOTAO_PESQUISAR).click()
            achou = self.esperar(pg, lambda h: RE_LINK_DETALHE.search(h) or ("NADA" if RE_NADA.search(h) else None))
            if achou == "NADA":
                return {"resultado": "NAO_ENCONTRADO"}
            det = pg.context.new_page()
            try:
                det.goto(urljoin(BASE, achou.group(1)), wait_until="domcontentloaded", timeout=60000)
                self.esperar(det, lambda h: "processoPartesPoloAtivo" in h or "Polo ativo" in h)
                return self.ler_detalhe(det, numero20)
            finally:
                det.close()
        except ErroNavegador as e:
            raise ErroTecnico(f"PROCESSO_NAO_CARREGOU: {str(e).splitlines()[0][:150]}") from e

    @staticmethod
    def ler_detalhe(det, numero20):
        """Capa e partes do detalhe aberto, virando as páginas das tabelas de polo ativo e passivo.
        Guarda o HTML da 1ª página em saida/pje1g_html/<cnj>.html."""
        html = det.content()
        PASTA_HTML.mkdir(parents=True, exist_ok=True)
        (PASTA_HTML / f"{numero20}.html").write_text(html, encoding="utf-8")
        partes, completa = partes_da_pagina(html), True
        for tabela in ("processoPartesPoloAtivoResumido", "processoPartesPoloPassivoResumido"):
            celulas = f"[id*='{tabela}'] td.rich-datascr-inact"
            ultima = max([int(t) for t in det.locator(celulas).all_inner_texts() if t.strip().isdigit()] + [1])
            completa &= ultima <= MAX_PAGINAS_PARTES
            for n in range(2, min(ultima, MAX_PAGINAS_PARTES) + 1):
                celula = det.locator(celulas, has_text=re.compile(rf"^\s*{n}\s*$"))
                if not celula.count():
                    completa = False
                    break
                celula.first.click()
                time.sleep(1.5)
                partes += partes_da_pagina(det.content())
        unicas = list({(p["nome"], p["documento"], p["papel"], p["polo"]): p for p in partes}.values())
        return {"resultado": "OK", "capa": campos_da_capa(html), "completa": completa,
                "partes": [p for p in unicas if not RE_ADVOGADO.search(p["papel"])],
                "advogados": [p for p in unicas if RE_ADVOGADO.search(p["papel"])]}

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJBA", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, left(c.numero_norm, 20) AS precatorio20,
       tc.codigo AS tipo_credito, li.valor_lista, li.metadata->>'valor_devido' AS valor_devido,
       coalesce(li.metadata->>'entidade_nome', '') AS ente_lista, coalesce(e.nome, '') AS ente_nome,
       ARRAY(SELECT DISTINCT btrim(v.x) FROM creditos.lista_item l2,
                    LATERAL (VALUES (l2.beneficiario_nome), (l2.metadata->>'de_beneficiario')) v(x)
              WHERE l2.credito_id = c.id AND l2.removido_em IS NULL
                AND nullif(btrim(v.x), '') IS NOT NULL) AS beneficiarios,
       ARRAY(SELECT pr.numero_cnj FROM creditos.credito_originario co
                JOIN creditos.processo pr ON pr.id = co.processo_id
              WHERE co.credito_id = c.id) AS ligados,
       ARRAY(SELECT x.m FROM (SELECT cc.motivo_detalhe AS m
                              UNION ALL SELECT t.motivo_detalhe FROM creditos.coleta_credor_tentativa t
                                         WHERE t.credito_id = c.id) x WHERE x.m IS NOT NULL) AS motivos,
       (SELECT st.codigo FROM creditos.coleta_credor_tentativa t JOIN creditos.status_coleta st ON st.id = t.status_id
         WHERE t.credito_id = c.id ORDER BY t.id DESC LIMIT 1) AS ultimo_status
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc  ON tc.id = c.tipo_credito_id
  JOIN creditos.coleta_credor cc ON cc.credito_id = c.id
  LEFT JOIN creditos.ente_alias ea ON ea.id = c.ente_alias_id
  LEFT JOIN creditos.ente e        ON e.id = ea.ente_id
  LEFT JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                      WHERE x.credito_id = c.id AND x.removido_em IS NULL
                      ORDER BY (x.prioridade_id = 1), x.ordem_cronologica LIMIT 1) li ON true
 WHERE c.id = %s
"""


def ler_credito(cur, credito_id):
    """O crédito (lead) com beneficiários, ente, valores, originários já ligados e os motivos do RPA."""
    cur.execute(SQL_CREDITO, (credito_id,))
    lead = como_dicts(cur)[0]
    lead["valores"] = [v for v in (valor_numerico(lead["valor_lista"]), valor_numerico(lead["valor_devido"])) if v]
    return lead


def pistas_do_rpa(cur, motivos):
    """{cnj: OK|PARTES|VINCULO} das pistas que o RPA deixou no motivo: capa antiga com valor conferido (…:OK:<id>),
    capa antiga só com partes (…:PARTES_SEM_VALOR:<id>) e CNJ achado (…SEM_VINCULO:<cnj>)."""
    ids = {}
    for t in motivos:
        for tipo, i in re.findall(r":(PARTES_SEM_VALOR|OK):(\d+)", t or ""):
            ids[int(i)] = "OK" if tipo == "OK" or ids.get(int(i)) == "OK" else "PARTES"
    pistas = {so_digitos(c): "VINCULO" for t in motivos if "SEM_VINCULO" in (t or "") for c in CNJ.findall(t)}
    if ids:
        cur.execute("SELECT id, numero_cnj FROM originarios.processos_originarios WHERE id = ANY(%s)", (list(ids),))
        for i, n in cur.fetchall():
            d = so_digitos(n)
            pistas[d] = "OK" if "OK" in (pistas.get(d), ids[i]) else ids[i]
    return pistas


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())


def confirmar(partes, alvo, chaves):
    """(credor, ente_ok): a parte do polo ativo com o nome do beneficiário (a com documento válido primeiro)
    e se o ente está no polo passivo."""
    ativos = sorted((p for p in partes if p["polo"] == "ATIVO" and chave_nome(p["nome"]) in alvo),
                    key=lambda p: not documento_valido(p["documento"]))
    ente_ok = not chaves or any(c in normal(p["nome"]) for p in partes if p["polo"] == "PASSIVO" for c in chaves)
    return (ativos[0] if ativos else None), ente_ok

# =============================================================================== decisão (só lê: banco, DJEN, PJe)


def credor_unico(cands, confirmados):
    """Credor de vários originários confirmados sem desempate, quando todos são a mesma pessoa: o mesmo CPF/CNPJ
    válido em cada um e nenhum candidato ficou sem conferir (não aberto, não achado no PJe ou pulado), porque um
    candidato não conferido pode ser de um homônimo. Senão None. Liga o credor ao crédito sem escolher o originário."""
    documentos = {cands[n]["credor"]["documento"] for n in confirmados}
    sem_conferir = [n for n, c in cands.items() if not c.get("confirmado") and c.get("pje") != "OK"]
    if len(documentos) != 1 or None in documentos or sem_conferir:
        return None
    return dict(cands[confirmados[0]]["credor"])


def processar(cur, lead, djen, pje, inicio):
    """Acha e confirma o originário. Devolve o resultado a gravar."""
    r = {"status": "FALHA", "motivo": "", "via": "NOME", "originario": None, "regra": "", "fontes": "",
         "credor": None, "capas": [], "candidatos": [], "capa_escolhida": None, "partes_escolhido": []}
    nomes = []
    for b in lead["beneficiarios"]:
        nomes += [n for n in nomes_para_buscar(b) if chave_nome(n) not in {chave_nome(x) for x in nomes}]
    if not nomes:
        r["motivo"] = "SEM_BENEFICIARIO: crédito sem nome de beneficiário na lista"
        return r
    if all(RE_ORGAO_PUBLICO.search(normal(n)) for n in nomes):
        r["motivo"] = f"REQTE_ORGAO_PUBLICO: {nomes[0]}"
        return r
    alvo = {chave_nome(n) for n in nomes}
    chaves = chaves_ente(lead["ente_lista"]) | chaves_ente(lead["ente_nome"])
    prec20 = lead["precatorio20"]

    # evidência forte (desempata e dá SUCESSO_PROCESSO_ORIGINARIO): o DJEN cita o valor ou o nº do precatório,
    # o originário já está ligado ao crédito, ou o RPA conferiu o valor nos documentos (pista …:OK)
    cands = {}

    def somar(n, fonte, info=None, forte=False):
        """Junta um candidato (de qualquer fonte) em cands, somando fontes, valor, OABs e evidência forte."""
        n = so_digitos(n)
        if len(n) != 20 or n == prec20 or n[13:16] != "805" or n[16:20] == "0000":
            return
        c = cands.setdefault(n, {"fontes": [], "valor": False, "forte": False, "oabs": set(), "classe": ""})
        if fonte not in c["fontes"]:
            c["fontes"].append(fonte)
        c["forte"] |= forte
        if info:
            c["valor"] |= info["valor"]
            c["forte"] |= info["valor"]
            c["oabs"] |= info["oabs"]
            c["classe"] = c["classe"] or info["classe"]

    def regra_forte(n):
        """Nome da evidência forte do candidato: VALOR, LIGADO ou RPA_OK."""
        f = cands[n]["fontes"]
        return "VALOR" if cands[n]["valor"] else "LIGADO" if "LIGADO" in f else "RPA_OK"

    for n in lead["ligados"]:
        somar(n, "LIGADO", forte=True)
    for n, tipo in pistas_do_rpa(cur, lead["motivos"]).items():
        somar(n, "RPA_OK" if tipo == "OK" else "RPA", forte=tipo == "OK")
    do_djen, como_advogado = candidatos_djen(djen, lead, nomes, chaves)
    for n, info in do_djen:
        somar(n, "DJEN", info)
    nota_adv = " (o beneficiário aparece como advogado no DJEN: honorários?)" if como_advogado else ""
    if not cands:
        r["motivo"] = "PROCESSO_NAO_ENCONTRADO: nenhum candidato no DJEN nem pista do RPA" + nota_adv
        return r

    # ordem de conferência: evidência forte, o já ligado, a pista do RPA, depois a ordem do DJEN
    ordem = sorted(cands, key=lambda n: (not cands[n]["forte"], "LIGADO" not in cands[n]["fontes"],
                                         not any(f.startswith("RPA") for f in cands[n]["fontes"])))
    com_forte = [n for n in ordem if cands[n]["forte"]]
    oab_prec = None
    decisivo, regra_decisivo = None, ""
    if len(cands) == 1:
        decisivo, regra_decisivo = ordem[0], regra_forte(ordem[0]) if com_forte else "UNICO"
    elif len(com_forte) == 1:
        decisivo, regra_decisivo = com_forte[0], regra_forte(com_forte[0])
    else:
        oab_prec = oabs_do_precatorio(djen, prec20)
        com_oab = [n for n in (com_forte or ordem) if cands[n]["oabs"] & oab_prec]
        if len(com_oab) == 1:
            decisivo, regra_decisivo = com_oab[0], "OAB"

    abertos = 0
    for n in ordem:
        if time.time() - inicio > TETO_CREDITO:
            raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")
        c = cands[n]
        existentes = partes_do_banco(cur, n)
        partes_bd = [{"nome": e["nome"], "documento": e["documento"] or "", "polo": e["polo"]}
                     for e in existentes if e["papel"] != "ADVOGADO"]
        credor, ente_ok = confirmar(partes_bd, alvo, chaves)
        if credor and ente_ok and documento_valido(credor["documento"]):
            c.update(confirmado=True, capa="BANCO", partes=partes_bd,
                     credor={"nome": credor["nome"], "documento": so_digitos(credor["documento"])})
        elif abertos < MAX_ABERTOS_PJE:
            abertos += 1
            dados = pje.consultar(n)
            time.sleep(PAUSA_PJE)
            c["pje"] = dados["resultado"]
            if dados["resultado"] == "OK":
                credor, ente_ok = confirmar(dados["partes"], alvo, chaves)
                if credor and ente_ok:
                    doc = so_digitos(credor["documento"])
                    c.update(confirmado=True, capa="PJE", dados=dados, partes=dados["partes"],
                             credor={"nome": credor["nome"], "documento": doc if documento_valido(doc) else None})
        else:
            c["pje"] = "NAO_ABERTO"
        if c.get("confirmado") and n == decisivo:       # o desempate já aponta este: não precisa abrir os outros
            break

    conf = [n for n in ordem if cands[n].get("confirmado")]
    escolhido, regra = None, ""
    if len(conf) == 1:
        escolhido, regra = conf[0], regra_forte(conf[0]) if cands[conf[0]]["forte"] else "UNICO"
    elif len(conf) > 1:
        conf_forte = [n for n in conf if cands[n]["forte"]]
        if len(conf_forte) == 1:
            escolhido, regra = conf_forte[0], regra_forte(conf_forte[0])
        else:
            oab_prec = oab_prec if oab_prec is not None else oabs_do_precatorio(djen, prec20)
            conf_oab = [n for n in (conf_forte or conf) if cands[n]["oabs"] & oab_prec]
            if len(conf_oab) == 1:
                escolhido, regra = conf_oab[0], "OAB"

    r["candidatos"] = [{"cnj": formatar_cnj(n), "fontes": cands[n]["fontes"], "valor": cands[n]["valor"],
                        "forte": cands[n]["forte"], "confirmado": bool(cands[n].get("confirmado")),
                        "capa": cands[n].get("capa"), "pje": cands[n].get("pje"), "classe": cands[n]["classe"]}
                       for n in ordem[:20]]
    def capas_pje(numeros):
        """(cnj, "PJE", dados) dos candidatos cuja capa veio do PJe (vão para o registrar_capa)."""
        return [(n, "PJE", cands[n]["dados"]) for n in numeros if cands[n].get("capa") == "PJE"]

    if escolhido:
        c = cands[escolhido]
        r.update(originario=escolhido, regra=regra, via="ORIGINARIO", fontes=",".join(c["fontes"]),
                 partes_escolhido=c["partes"],
                 capas=[(escolhido, c["capa"], c.get("dados"))] + capas_pje(n for n in conf if n != escolhido),
                 capa_escolhida=c["dados"]["capa"] if c["capa"] == "PJE" else None)
        if c["credor"]["documento"]:
            r.update(credor=c["credor"],
                     status="SUCESSO_PROCESSO_ORIGINARIO" if c["forte"] else "SUCESSO_PARTES_SEM_VALOR",
                     motivo=f"cnj={formatar_cnj(escolhido)} regra={regra} fontes={r['fontes']} "
                            f"partes={len(c['partes'])} capa={c['capa']}")
        else:
            r["motivo"] = f"SEM_CPF_CREDOR: cnj={formatar_cnj(escolhido)} regra={regra} (credor sem CPF no PJe)"
    elif conf:
        com_doc = any(cands[n]["credor"]["documento"] for n in conf)
        r.update(status="SUCESSO_ANALISAR", capas=capas_pje(conf),
                 motivo=("CREDOR_COM_DOCUMENTO_SEM_VINCULO:" if com_doc else "CREDOR_POLO_ATIVO_SEM_VINCULO:")
                 + ",".join(formatar_cnj(n) for n in conf))
        credor = credor_unico(cands, conf)
        if credor:                                      # o originário fica em aberto; o credor já é certo
            r.update(credor=credor, regra="CREDOR_UNICO")
    elif decisivo and cands[decisivo].get("pje") == "NAO_ENCONTRADO" and \
            ("DJEN" in cands[decisivo]["fontes"] or cands[decisivo]["forte"]):
        # 1 candidato claro, mas o PJe público não abre o processo: liga sem capa
        r.update(originario=decisivo, regra=regra_decisivo, via="ORIGINARIO",
                 fontes=",".join(cands[decisivo]["fontes"]),
                 motivo=f"PROC_SEM_CAPA: cnj={formatar_cnj(decisivo)} regra={regra_decisivo} (não abre no PJe público)")
    else:
        r["motivo"] = f"PROCESSO_NAO_ENCONTRADO: {len(cands)} candidato(s), nenhum confirmado" + nota_adv
    return r

# =============================================================================== banco: gravação de um crédito


class Backup:
    """Guarda o 'antes' de cada mudança no legado e escreve o SQL que desfaz. Só a 1ª mudança de cada linha
    entra (e linha criada pelo robô só é apagada), então o SQL pode rodar em qualquer ordem.
    Dois níveis: o crédito em gravação (p_*) e o que já teve COMMIT."""

    def __init__(self, arquivo_sql, arquivo_csv, rodada, modo):
        self.arquivo_sql, self.arquivo_csv, self.rodada, self.modo = arquivo_sql, arquivo_csv, rodada, modo
        self.tocados, self.inseridos = set(), set()
        self.descartar()

    def descartar(self):
        """Esquece o crédito em gravação (ROLLBACK)."""
        self.p_tocados, self.p_inseridos, self.p_sql, self.p_csv = set(), set(), [], []

    def _ja_tocada(self, chave):
        """A linha já teve o 'antes' guardado (nesta rodada ou no crédito)?"""
        return chave in self.tocados | self.p_tocados

    def _criada_aqui(self, tabela, id_):
        """A linha foi criada pelo robô (o SQL de desfazer só a apaga)?"""
        return (tabela, id_) in self.inseridos | self.p_inseridos

    def update(self, cur, tabela, chave, antes):
        """Guarda o UPDATE que devolve a linha ao 'antes'."""
        k = (tabela, tuple(sorted(chave.items())))
        if not antes or self._ja_tocada(k) or self._criada_aqui(tabela, chave.get("id")):
            return
        self.p_tocados.add(k)
        onde = " AND ".join(f"{c} = %s" for c in chave)
        self.p_sql.append(cur.mogrify(f"UPDATE {tabela} SET {', '.join(f'{c} = %s' for c in antes)} WHERE {onde};",
                                      [*antes.values(), *chave.values()]).decode())
        self.p_csv.append(("UPDATE", tabela, chave, antes))

    def insert(self, cur, tabela, id_):
        """Guarda o DELETE da linha que o robô criou."""
        self.p_inseridos.add((tabela, id_))
        self.p_sql.append(cur.mogrify(f"DELETE FROM {tabela} WHERE id = %s;", [id_]).decode())
        self.p_csv.append(("INSERT", tabela, {"id": id_}, {}))

    def delete(self, cur, tabela, linha):
        """Guarda o INSERT que recria a linha apagada."""
        if self._criada_aqui(tabela, linha["id"]):
            return
        colunas = list(linha)
        self.p_sql.append(cur.mogrify(f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES "
                                      f"({', '.join(['%s'] * len(colunas))}) ON CONFLICT DO NOTHING;",
                                      list(linha.values())).decode())
        self.p_csv.append(("DELETE", tabela, {"id": linha["id"]}, linha))

    def confirmar(self, credito_id, manter):
        """Depois do COMMIT (ou do ROLLBACK da simulação): escreve o SQL e o CSV do crédito;
        manter=False (simulação) não lembra as linhas tocadas."""
        if self.p_sql:
            novo = not self.arquivo_sql.exists()
            with open(self.arquivo_sql, "a", encoding="utf-8") as f:
                if novo:
                    f.write("-- Desfaz as mudanças do fetch_TJBA.py nas tabelas antigas "
                            "(pode rodar em qualquer ordem).\n")
                f.write(f"-- crédito {credito_id}\nBEGIN;\n" + "\n".join(reversed(self.p_sql)) + "\nCOMMIT;\n")
            anexar_csv(self.arquivo_csv, COLUNAS_BACKUP,
                       [{"rodada": self.rodada, "modo": self.modo, "credito_id": credito_id, "op": op, "tabela": t,
                         "chave": json.dumps(ch, default=str), "antes": json.dumps(a, default=str, ensure_ascii=False)}
                        for op, t, ch, a in self.p_csv])
        if manter:
            self.tocados |= self.p_tocados
            self.inseridos |= self.p_inseridos
        self.descartar()


def id_do_software(cur, criar):
    """Id do software do robô; cria na 1ª vez (raspa_credor: o próprio robô busca o credor)."""
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJBA",
                                "Credor do TJBA: DJEN + PJe 1º grau público (TJBA/fetch_TJBA.py)",
                                raspa_credor=True, criar=criar)


def filas_antigas(cur):
    """Filas mensais do legado (de FILA_ANTIGA_DESDE em diante) em que o usuário pode gravar."""
    return banco.filas_antigas(cur, FILA_ANTIGA_DESDE)[0]


def partes_para_banco(cur, dados, existentes, fonte):
    """(partes, advogados) para registrar_capa. Capa do banco: reenvia como está (só para o recálculo ligar o
    credor). PJe inteiro: vale o PJe, com o CPF que faltar vindo do banco (mesmo nome). PJe cortado: só acrescenta."""
    do_banco = ([{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "papel": e["papel"],
                  "papel_bruto": e["papel_bruto"]} for e in existentes if e["papel"] != "ADVOGADO"],
                [{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "oab_uf": e["oab_uf"],
                  "oab_numero": e["oab_numero"], "papel_bruto": e["papel_bruto"]}
                 for e in existentes if e["papel"] == "ADVOGADO"])
    if fonte == "BANCO":
        return do_banco
    doc_banco = {e["chave"]: e["documento"] for e in existentes if e["chave"] and e["documento"]}
    chaves = chave_texto_lote(cur, [p["nome"] for p in dados["partes"]])
    partes = [{"nome": p["nome"], "cpf_cnpj": p["documento"] if documento_valido(p["documento"]) else doc_banco.get(ch),
               "polo": p["polo"], "papel": p["papel"], "papel_bruto": p["papel"]}
              for p, ch in zip(dados["partes"], chaves)]
    # só advogados do lado do credor: o procurador do ente viraria "advogado do lead"
    advogados = [{"nome": a["nome"], "cpf_cnpj": None, "polo": "ATIVO", "oab_uf": a["oab_uf"],
                  "oab_numero": a["oab_numero"], "papel_bruto": a["papel"]}
                 for a in dados["advogados"] if a["polo"] == "ATIVO" and a["oab_numero"]]
    if dados["completa"]:
        return partes, advogados
    nomes_banco = {e["chave"] for e in existentes}
    oabs_banco = {(e["oab_uf"], e["oab_numero"]) for e in existentes if e["oab_numero"]}
    return (do_banco[0] + [p for p, ch in zip(partes, chaves) if ch not in nomes_banco],
            do_banco[1] + [a for a in advogados if (a["oab_uf"], a["oab_numero"]) not in oabs_banco])


def capa_para_banco(capa):
    """Capa lida no PJe -> JSON da capa do registrar_capa (classe sem o código, código da classe, órgão, grau)."""
    classe = capa.get("classe_judicial") or ""
    codigo = re.search(r"\((\d+)\)\s*$", classe)
    return {"orgao_julgador": capa.get("orgao_julgador") or None,
            "classe_judicial": re.sub(r"\s*\(\d+\)\s*$", "", classe) or None,
            "classe_codigo": codigo.group(1) if codigo else None, "grau": "G1", "sistema": "PJE"}


def travar_processo(cur, cnj):
    """O mesmo lock do registrar_capa, pego antes de ler as partes: duas instâncias no mesmo processo
    não se atropelam."""
    cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)", (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if linha:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('creditos.registrar_capa'), %s::int)", (linha[0],))


def gravar_capa_legado(cur, cnj, dados, relacionar, precatorio, bk):
    """Capa antiga do originário (originarios.*), como o RPA grava."""
    cont, agora, hoje = Counter(), datetime.now(), date.today()
    capa = dados["capa"]
    cnpj = next((p["documento"] for p in dados["partes"] if p["polo"] == "PASSIVO" and len(p["documento"]) == 14), None)
    novos = {"classe_judicial": capa.get("classe_judicial"), "orgao_julgador": capa.get("orgao_julgador"),
             "jurisdicao": capa.get("jurisdicao"), "assunto": capa.get("assunto"),
             "data_autuacao": data_br(capa.get("data_autuacao")), "cnpj_entidade_devedora": cnpj,
             "origem": "TJBA", "tribunal_sigla": "TJBA"}
    prec = None
    if relacionar:                                      # índice único: não repete o precatório em outro originário
        cur.execute("SELECT 1 FROM originarios.processos_originarios WHERE precatorio_relacionado = %s::text[]",
                    ([precatorio],))
        prec = None if cur.fetchone() else [precatorio]
    cur.execute("""SELECT id, classe_judicial, orgao_julgador, jurisdicao, assunto, data_autuacao,
                          cnpj_entidade_devedora, origem, tribunal_sigla, precatorio_relacionado,
                          ultima_data_raspagem, data_raspagem
                     FROM originarios.processos_originarios
                    WHERE regexp_replace(numero_cnj::text, '\\D', '', 'g') = %s
                    ORDER BY id LIMIT 1 FOR UPDATE""", (cnj,))
    linhas = como_dicts(cur)
    if linhas:
        atual = linhas[0]
        pid = atual["id"]
        mudar = {k: v for k, v in novos.items() if v and not atual[k]}
        mudar.update(ultima_data_raspagem=hoje, data_raspagem=agora)
        if prec and not atual["precatorio_relacionado"]:
            mudar["precatorio_relacionado"] = prec
        bk.update(cur, "originarios.processos_originarios", {"id": pid}, {k: atual[k] for k in mudar})
        sets = ", ".join(f"{k} = %s" for k in mudar)
        cur.execute(f"UPDATE originarios.processos_originarios SET {sets} WHERE id = %s", [*mudar.values(), pid])
        cont["capa_atualizada"] += 1
    else:
        valores = {k: v for k, v in novos.items() if v}
        valores.update(numero_cnj=formatar_cnj(cnj), ultima_data_raspagem=hoje, data_raspagem=agora)
        if prec:
            valores["precatorio_relacionado"] = prec
        cur.execute(f"INSERT INTO originarios.processos_originarios ({', '.join(valores)}) "
                    f"VALUES ({', '.join(['%s'] * len(valores))}) RETURNING id", list(valores.values()))
        pid = cur.fetchone()[0]
        bk.insert(cur, "originarios.processos_originarios", pid)
        cont["capa_criada"] += 1

    # partes e advogados: trocados pelo PJe quando a lista veio inteira; senão só acrescenta
    cur.execute("SELECT * FROM originarios.partes_processuais WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    antigas = como_dicts(cur)
    cpf_antigo = {(a["polo"], normal(a["nome"])): a["cpf_cnpj"] for a in antigas if a["cpf_cnpj"]}
    novas = [(pid, p["polo"], p["nome"],
              p["documento"] if documento_valido(p["documento"]) else cpf_antigo.get((p["polo"], normal(p["nome"]))),
              PAPEL_LEGADO[p["polo"]], "TJBA", agora, p["papel"], POLO_BRUTO[p["polo"]]) for p in dados["partes"]]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    advs_antigos = como_dicts(cur)
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJBA", agora,
                                            "ADVOGADO", "AUTOR")
            for a in dados["advogados"] if a["polo"] == "ATIVO" and a["oab_numero"]}
    if dados["completa"]:
        for tabela, linhas_antigas in (("originarios.partes_processuais", antigas),
                                       ("originarios.advogados", advs_antigos)):
            for a in linhas_antigas:
                bk.delete(cur, tabela, a)
            cur.execute(f"DELETE FROM {tabela} WHERE processo_id = %s", (pid,))
        cont["partes_trocadas"] += 1
    else:
        ja = {(a["polo"], normal(a["nome"])) for a in antigas}
        novas = [n for n in novas if (n[1], normal(n[2])) not in ja]
        ja_oab = {(a["oab_uf"], a["oab_numero"]) for a in advs_antigos}
        advs = {k: v for k, v in advs.items() if k not in ja_oab}
    for n in novas:
        cur.execute("""INSERT INTO originarios.partes_processuais
                         (processo_id, polo, nome, cpf_cnpj, papel, origem, data_raspagem, papel_bruto, polo_bruto)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""", n)
        bk.insert(cur, "originarios.partes_processuais", cur.fetchone()[0])
    for a in advs.values():
        cur.execute("""INSERT INTO originarios.advogados
                         (processo_id, polo, nome, oab_numero, oab_uf, origem, data_raspagem, papel_bruto, polo_bruto)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""", a)
        bk.insert(cur, "originarios.advogados", cur.fetchone()[0])
    cont["partes_inseridas"] += len(novas)
    cont["advogados_inseridos"] += len(advs)
    return cont


def atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk):
    """Status, motivo e originário nas linhas do precatório nas filas mensais (pula o que o RPA está processando)."""
    cont = Counter()
    status = status_legado[r["status"]]
    originario = [formatar_cnj(r["originario"])] if r["originario"] else None
    for fila in filas:
        # o índice pu_AAAA_MM_numprec_lpad_idx acha as linhas; a 2ª condição confere a identidade do crédito
        cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead,
                               numero_originario, ultima_atualizacao
                          FROM {fila}
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJBA'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJBA"}
            bk.update(cur, fila, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                             "numero_originario", "ultima_atualizacao")})
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJBA'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Capa sem coluna própria e o resumo da decisão em credito_fonte.metadata (a função troca o JSON inteiro)."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "fontes": r["fontes"], "candidatos": r["candidatos"]}
    if r["capa_escolhida"]:
        meta["capa_originario"] = r["capa_escolhida"]
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJBA', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def gravar(cur, lead, r, filas, status_legado, bk):
    """Grava o resultado de um crédito (quem chama faz COMMIT ou ROLLBACK). Devolve o resumo para o CSV."""
    cid = lead["credito_id"]
    id_do_software(cur, criar=True)                     # na simulação ele nasce e morre nesta transação
    antes = credores_do_credito(cur, cid)
    resumo, legado, proc_escolhido = [], Counter(), None
    if r["originario"]:
        cur.execute("SELECT creditos.fila_credor_registrar_originario(%s, %s, %s)",
                    (cid, WORKER, [formatar_cnj(r["originario"])]))
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)",
                    (formatar_cnj(r["originario"]),))
        proc_escolhido = cur.fetchone()[0]
    if r["credor"]:
        cur.execute("SELECT creditos.documento_de_parte(%s)", (r["credor"]["documento"],))
        # antes do registrar_capa: o vínculo fica com origem FONTE, que o recálculo das partes não apaga
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, r["credor"]["nome"], r["credor"]["documento"], proc_escolhido, SOFTWARE))
    for cnj, fonte, dados in r["capas"]:
        travar_processo(cur, cnj)
        partes, advogados = partes_para_banco(cur, dados, partes_do_banco(cur, cnj), fonte)
        capa = capa_para_banco(dados["capa"]) if fonte == "PJE" else None
        cur.execute("SELECT creditos.registrar_capa(%s, %s::jsonb, %s::jsonb, %s::jsonb)",
                    (formatar_cnj(cnj), json.dumps(partes, ensure_ascii=False),
                     json.dumps(advogados, ensure_ascii=False),
                     json.dumps(capa, ensure_ascii=False) if capa else None))
        res = cur.fetchone()[0]
        resumo.append(f"{formatar_cnj(cnj)} ({fonte}): partes +{res['partes_inseridas']}/-{res['partes_removidas']} "
                      f"credores +{res['credores_inseridos']}/-{res['credores_removidos']}")
        if fonte == "PJE":
            legado += gravar_capa_legado(cur, cnj, dados, r["status"] == "SUCESSO_PROCESSO_ORIGINARIO"
                                         and cnj == r["originario"], lead["precatorio"], bk)
    registrar_metadata(cur, lead, r)
    legado += atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "fontes": r["fontes"], "candidatos": r["candidatos"],
               "capa": r["capa_escolhida"],
               "partes": [{k: p.get(k) for k in ("nome", "polo", "papel", "documento")}
                          for p in r["partes_escolhido"]][:60],
               "credores_antes": fmt_credores(antes), "credores_depois": fmt_credores(depois)}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => %s, p_sistema => 'PJE',
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (cid, WORKER, r["status"], r["motivo"][:2000], r["via"],
                 formatar_cnj(r["originario"]) if r["originario"] else None,
                 json.dumps(detalhe, ensure_ascii=False, default=str), socket.gethostname()))
    return {"banco": " | ".join(resumo), "legado": ", ".join(f"{k}={v}" for k, v in sorted(legado.items())),
            "antes": antes, "depois": depois}

# =============================================================================== banco: gravação de cada crédito


def desfazer_transacao(rod):
    """ROLLBACK da gravação em andamento e esquece o backup dela (a conexão pode já ter caído: não falha)."""
    try:
        rod.con.rollback()
    except psycopg2.Error:
        pass
    rod.bk.descartar()


def na_fila(rod, sql, params):
    """Roda uma função da fila numa transação curta própria (depois do ROLLBACK do crédito). Se até isso falhar, só
    registra no log: o crédito volta para a fila quando o lease expirar."""
    try:
        with rod.con.cursor() as cur:
            cur.execute(sql, params)
        rod.con.commit()
    except psycopg2.Error as e:
        desfazer_transacao(rod)
        log.error(f"fila: não consegui atualizar o crédito ({(str(e).splitlines() or [''])[0][:200]})")


def marcar_falha(rod, credito_id, erro):
    """FALHA na fila para o crédito que não gravou."""
    na_fila(rod, """SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s,
                       p_status => 'FALHA', p_motivo => %s, p_sistema => 'PJE', p_host => %s)""",
            (credito_id, WORKER, erro, socket.gethostname()))


def adiar_por_conflito(rod, credito_id, motivo):
    """Crédito que perdeu um deadlock para a gravação de outro worker: volta para a fila daqui a ADIAMENTO_CONFLITO."""
    na_fila(rod, "SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
            (credito_id, WORKER, ADIAMENTO_CONFLITO, motivo))


def gravar_credito(rod, item):
    """Grava um crédito numa transação só dele: COMMIT (ROLLBACK na simulação) e, depois, o SQL de desfazer do
    legado. Erro desfaz a transação inteira: lease perdido vira LEASE_PERDIDO; deadlock com outro worker vira ADIADO
    (volta logo para a fila); outro erro vira FALHA (na fila também, fora da simulação). Devolve True se gravou."""
    lead, r = item["lead"], item["r"]
    try:
        with rod.con.cursor() as cur:
            item["gravado"] = gravar(cur, lead, r, rod.filas, rod.status_legado, rod.bk)
        if rod.simulacao:
            rod.con.rollback()
        else:
            rod.con.commit()
    except psycopg2.errors.LockNotAvailable:
        desfazer_transacao(rod)
        item["r"] = {**r, "status": "LEASE_PERDIDO", "motivo": "outro worker reservou o crédito"}
    except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure) as e:
        desfazer_transacao(rod)
        motivo = f"CONFLITO_DE_LOCK: {e.__class__.__name__} com a gravação de outro worker"
        if not rod.simulacao:
            adiar_por_conflito(rod, lead["credito_id"], motivo)
        item["r"] = {**r, "status": "ADIADO", "motivo": motivo}
    except Exception as e:
        desfazer_transacao(rod)
        erro = f"PERSISTENCIA_CAPA: {(str(e).splitlines() or [''])[0][:300]}"
        if not rod.simulacao:
            marcar_falha(rod, lead["credito_id"], erro)
        item["r"] = {**r, "status": "FALHA", "motivo": erro}
        log.warning(f"{lead['credito_id']}: gravação desfeita ({erro})", exc_info=not isinstance(e, psycopg2.Error))
    else:
        # fora do try: o crédito já teve COMMIT, então um erro aqui (disco) não pode virar FALHA na fila
        rod.bk.confirmar(lead["credito_id"], manter=not rod.simulacao)
        return True
    item["gravado"] = SEM_GRAVACAO
    return False


def registrar_credito(rod, item, gravou):
    """Depois da gravação: completa e escreve a linha do CSV, os credores trocados e o resultado no log.
    MAX_FALHAS_SEGUIDAS gravações seguidas em FALHA param o robô (banco com problema)."""
    linha, lead, r, g = item["linha"], item["lead"], item["r"], item["gravado"]
    saiu, entrou = g["antes"] - g["depois"], g["depois"] - g["antes"]
    if saiu or entrou:
        anexar_csv(rod.arq_trocas, COLUNAS_TROCAS,
                   [{"processado_em": linha["processado_em"], "modo": rod.modo, "credito_id": lead["credito_id"],
                     "precatorio": lead["precatorio"], "credores_antes": fmt_credores(g["antes"]),
                     "credores_depois": fmt_credores(g["depois"]), "saiu": fmt_credores(saiu),
                     "entrou": fmt_credores(entrou)}])
    linha.update(resultado=r["status"], motivo=r["motivo"],
                 originario=formatar_cnj(r["originario"]) if r["originario"] else "", regra=r["regra"],
                 fontes=r["fontes"], credor=(r["credor"] or {}).get("nome", ""),
                 credor_documento=(r["credor"] or {}).get("documento", ""),
                 candidatos=" | ".join(f"{c['cnj']}[{','.join(c['fontes'])}{' forte' if c['forte'] else ''}"
                                       f"{' ok' if c['confirmado'] else ''}{' ' + c['capa'] if c['capa'] else ''}]"
                                       for c in r["candidatos"]),
                 capa_fonte=";".join(f for _, f, _ in r["capas"]), banco=g["banco"], legado=g["legado"],
                 credores_antes=fmt_credores(g["antes"]), credores_depois=fmt_credores(g["depois"]))
    anexar_csv(rod.arq_credito, COLUNAS, [linha])
    rod.resultados[r["status"]] += 1
    rod.falhas_gravacao = 0 if gravou else rod.falhas_gravacao + (r["status"] == "FALHA")
    gravacao = ("ROLLBACK, simulação" if rod.simulacao else "COMMIT") if gravou else "não gravado"
    log.log(logging.INFO if gravou else logging.WARNING,
            f"[{rod.n}] {lead['credito_id']} {lead['precatorio']} -> {r['status']} "
            f"{formatar_cnj(r['originario']) if r['originario'] else ''} "
            f"{('CPF ' + r['credor']['documento']) if r['credor'] else ''} | {linha['segundos']} s | {gravacao}")
    if rod.falhas_gravacao >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} gravações seguidas em FALHA: robô parado (banco com problema?).")
        rod.parar = True

# =============================================================================== fila


def assumir_fila(con, id_software, rodada):
    """Passa para o robô as linhas do TJBA do RPA fora de lease; PENDENTE para quem não tem credor.
    Guarda backup e o SQL que devolve tudo ao RPA."""
    with con.cursor() as cur:
        cur.execute("""SELECT cc.credito_id, cc.status_id, cc.disponivel_em,
                              (c.saiu_da_lista_em IS NULL AND NOT EXISTS (
                                 SELECT 1 FROM creditos.credito_credor k
                                  WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)) AS sem_credor
                         FROM creditos.coleta_credor cc
                         JOIN creditos.credito c ON c.id = cc.credito_id
                        WHERE cc.tribunal_id = %s AND cc.software_id = %s AND cc.status_id <> 2
                          FOR UPDATE OF cc SKIP LOCKED""", (TRIBUNAL_TJBA, SOFTWARE_RPA))
        linhas = cur.fetchall()
        if not linhas:
            con.commit()
            return 0, 0
        ids = [linha[0] for linha in linhas]
        pendentes = [linha[0] for linha in linhas if linha[3]]
        cur.execute("UPDATE creditos.coleta_credor SET software_id = %s, updated_at = now() WHERE credito_id = ANY(%s)",
                    (id_software, ids))
        cur.execute("UPDATE creditos.coleta_credor SET status_id = 1, disponivel_em = now() WHERE credito_id = ANY(%s)",
                    (pendentes,))
        colunas = ["credito_id", "status_id", "disponivel_em", "sem_credor"]
        anexar_csv(SAIDA / f"fetch_fila_backup_{rodada}.csv", colunas,
                   [dict(zip(colunas, linha)) for linha in linhas])
        with open(SAIDA / f"desfazer_fila_{rodada}.sql", "a", encoding="utf-8") as f:
            f.write("-- Devolve ao RPA (software 2), com o status de antes, as linhas que o robô assumiu.\nBEGIN;\n")
            for i in range(0, len(linhas), 1000):
                valores = ",\n".join(cur.mogrify("(%s, %s, %s::timestamptz)", linha[:3]).decode()
                                     for linha in linhas[i:i + 1000])
                f.write(f"UPDATE creditos.coleta_credor cc SET software_id = {SOFTWARE_RPA}, status_id = v.status_id, "
                        f"disponivel_em = v.disponivel_em, updated_at = now()\n  FROM (VALUES\n{valores}\n) "
                        f"v(credito_id, status_id, disponivel_em)\n WHERE cc.credito_id = v.credito_id "
                        f"AND cc.software_id = {id_software} AND cc.status_id <> 2;\n")
            f.write("COMMIT;\n")
    con.commit()
    return len(ids), len(pendentes)


def pegar(con):
    """Reserva o próximo crédito do TJBA na fila (lease de LEASE para este worker); None se a fila está vazia."""
    with con.cursor() as cur:
        cur.execute("""SELECT credito_id
                         FROM creditos.fila_credor_pegar(p_tribunal => 'TJBA', p_worker => %s, p_quantidade => 1,
                                                         p_lease => %s::interval, p_software => %s)""",
                    (WORKER, LEASE, SOFTWARE))
        linha = cur.fetchone()
    con.commit()
    return linha[0] if linha else None


def devolver(con, credito_id, motivo=None):
    """Erro passageiro: volta para a fila daqui a ADIAMENTO; sem motivo (Ctrl+C), volta já."""
    with con.cursor() as cur:
        if motivo:
            cur.execute("SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
                        (credito_id, WORKER, ADIAMENTO, motivo[:2000]))
        else:
            cur.execute("SELECT creditos.fila_credor_liberar(%s, %s)", (credito_id, WORKER))
    con.commit()


def amostra_simulacao(con, quantidade, worker):
    """Créditos do escopo (ativos, sem credor), alternando status e motivo, fora de lease. Só leitura.
    Cada worker pega a sua fatia da mesma ordem (a simulação não reserva, então sem isso repetiriam os créditos)."""
    with con.cursor() as cur:
        cur.execute("""SELECT credito_id FROM (
                         SELECT cc.credito_id, row_number() OVER (PARTITION BY cc.status_id, cc.motivo_id
                                                                  ORDER BY md5(cc.credito_id::text)) AS vez
                           FROM creditos.coleta_credor cc JOIN creditos.credito c ON c.id = cc.credito_id
                          WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2
                            AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor k
                                             WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)) x
                        ORDER BY vez, credito_id LIMIT %s OFFSET %s""",
                    (TRIBUNAL_TJBA, quantidade, (worker - 1) * quantidade))
        return [i for (i,) in cur.fetchall()]

# =============================================================================== execução


class Rodada:
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, DJEN, Chrome e contadores.
    Ao nascer prepara o banco (software, filas antigas, status do legado), a fila (amostra na simulação;
    no modo real só o worker 1 roda assumir_fila) e abre o Chrome. Os arquivos levam o sufixo do worker."""

    def __init__(self, simulacao, limite):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite = simulacao, limite
        self.modo = "SIMULACAO" if simulacao else "REAL"
        sufixo = sufixo_worker() + ("_simulacao" if simulacao else "")
        self.rodada = f"{datetime.now():%Y%m%d_%H%M%S}{sufixo_worker()}"
        self.assume_fila = not simulacao and WORKER_N == 1
        self.arq_credito = SAIDA / f"fetch_TJBA{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = Backup(SAIDA / f"desfazer_legado_{self.rodada}{'_simulacao' if simulacao else ''}.sql",
                         SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo)
        self.resultados, self.falhas_seguidas, self.falhas_gravacao, self.n, self.parar = Counter(), 0, 0, 0, False

        self.con, self.con_l = conectar(escrita=True), conectar()
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        self.djen = Djen(saida_djen(WORKER_N))
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | "
                 f"DJEN: {self.djen.nome} | Chrome: {PERFIL.name} | filas antigas: {', '.join(self.filas)}")
        if simulacao:
            self.fila_simulada = amostra_simulacao(self.con_l, limite or AMOSTRA_SIMULACAO, WORKER_N)
            log.info(f"amostra: {len(self.fila_simulada)} créditos (nada é reservado; cada crédito é desfeito no fim)")
        else:
            with self.con.cursor() as cur:
                cur.execute("SELECT creditos.fila_credor_expirar_leases()")
            self.con.commit()
            if self.assume_fila:
                self.assumir()
            else:
                log.info("fila: quem assume as linhas do RPA é o worker 1; este só pega da fila")

        self.pje = Pje()
        self.pje.abrir()

    def assumir(self):
        """Roda assumir_fila e anota a hora (para repetir a cada ASSUMIR_A_CADA)."""
        movidas, pendentes = assumir_fila(self.con, self.id_software, self.rodada)
        self.ultima_assumida = time.time()
        log.info(f"fila assumida: {movidas} linhas do RPA passaram para o robô ({pendentes} sem credor, em PENDENTE)")


def proximo_credito(rod):
    """Id do próximo crédito, ou None para parar (amostra acabou, fila vazia, --limite, parada por falhas).
    Fora da simulação reserva o crédito com lease e, no worker 1, a cada ASSUMIR_A_CADA assume de novo a fila do RPA."""
    if rod.parar or (rod.limite and rod.n >= rod.limite):
        return None
    if rod.simulacao:
        return rod.fila_simulada.pop(0) if rod.fila_simulada else None
    if rod.assume_fila and time.time() - rod.ultima_assumida > ASSUMIR_A_CADA:
        rod.assumir()
    credito_id = pegar(rod.con)
    if not credito_id:
        log.info("fila do TJBA vazia.")
    return credito_id


def processar_credito(rod, credito_id):
    """Lê o crédito no banco e decide originário e credor (DJEN + PJe), sem gravar nada. Devolve o item a gravar
    ({linha, lead, r}). Erro passageiro: devolve o crédito para a fila, escreve a linha ADIADO no CSV e devolve None;
    MAX_FALHAS_SEGUIDAS erros seguidos param o robô."""
    rod.n += 1
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    try:
        with rod.con_l.cursor() as cur:
            lead = ler_credito(cur, credito_id)
            linha.update(precatorio=lead["precatorio"], beneficiario=" | ".join(lead["beneficiarios"]),
                         ente=lead["ente_lista"] or lead["ente_nome"], ultimo_status=lead["ultimo_status"])
            r = processar(cur, lead, rod.djen, rod.pje, inicio)
    except Exception as e:                              # erro passageiro (ou inesperado): volta para a fila
        tecnico = isinstance(e, ErroTecnico)
        motivo = str(e) if tecnico else \
            f"ERRO_DESCONHECIDO: {e.__class__.__name__}: {(str(e).splitlines() or [''])[0][:200]}"
        rod.falhas_seguidas += 1
        linha.update(resultado="ADIADO", motivo=motivo, segundos=round(time.time() - inicio))
        if not rod.simulacao:
            devolver(rod.con, credito_id, motivo)
        anexar_csv(rod.arq_credito, COLUNAS, [linha])
        rod.resultados["ADIADO"] += 1
        log.warning(f"[{rod.n}] {credito_id} -> ADIADO: {motivo}", exc_info=not tecnico)  # inesperado: traceback
        if re.search(r"closed|Target|browser", motivo, re.I):
            rod.pje.reabrir()
        if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
            log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (PJe/DJEN fora ou bloqueando).")
            rod.parar = True
        return None
    rod.falhas_seguidas = 0
    linha["segundos"] = round(time.time() - inicio)
    return {"linha": linha, "lead": lead, "r": r}


def encerrar(rod):
    """Fecha o Chrome e as conexões e registra o resumo (fila vazia, --limite, Ctrl+C, parada por falhas)."""
    try:
        rod.pje.fechar()
    finally:
        rod.con.close()
        rod.con_l.close()
    log.info(f"{rod.modo}: {rod.n} crédito(s) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def travar_worker(n):
    """Trava exclusiva em saida/worker_<n>.lock enquanto o processo roda: um 2º processo com o mesmo --worker usaria
    o mesmo perfil do Chrome e os mesmos arquivos, então para aqui. Devolve o arquivo aberto (a trava dura enquanto
    ele estiver aberto, e o Windows a solta sozinho se o processo morrer)."""
    SAIDA.mkdir(exist_ok=True)
    trava = open(SAIDA / f"worker_{n}.lock", "a+")
    trava.seek(0)
    try:
        msvcrt.locking(trava.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        trava.close()
        raise SystemExit(f"o worker {n} já está rodando nesta máquina: use outro --worker.")
    return trava


def definir_worker(n):
    """Liga este processo ao worker n: nome na fila (WORKER) e perfil do Chrome (o worker 1 usa o perfil de sempre,
    os outros .chrome-profile-pje-tjba-<n>, cada um com cookies e sessão próprios)."""
    global WORKER_N, WORKER, PERFIL, log
    WORKER_N = n
    WORKER = f"{socket.gethostname()}:TJBA:consulta_publica:w{n}:{os.getpid()}"
    PERFIL = AQUI / (".chrome-profile-pje-tjba" if n == 1 else f".chrome-profile-pje-tjba-{n}")
    log = logging.getLogger(f"fetch_TJBA_w{n}")     # com vários workers no terminal, cada linha diz de qual é


def sufixo_worker():
    """Sufixo dos arquivos e do log do worker: o 1 mantém os nomes de sempre, os outros ganham _w<N>."""
    return "" if WORKER_N == 1 else f"_w{WORKER_N}"


def ler_argumentos():
    """--simulacao (faz tudo e desfaz no banco), --limite N (para depois de N créditos), --workers N (sobe N workers
    neste terminal) e --worker N (roda só o worker N, quando cada um fica num terminal)."""
    ap = argparse.ArgumentParser(description="Credor do TJBA pela consulta pública (DJEN + PJe 1º grau), "
                                             "gravado no banco crédito a crédito.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None,
                    help="para depois de N créditos, em cada worker (padrão: até a fila acabar)")
    quantos = ap.add_mutually_exclusive_group()
    quantos.add_argument("--workers", type=int, default=None,
                         help="sobe os workers 1..N neste terminal, cada um com o seu Chrome, proxy do DJEN e arquivos")
    quantos.add_argument("--worker", type=int, default=1,
                         help="roda só o worker N (1, 2, ...): perfil do Chrome, proxy do DJEN e arquivos próprios; "
                              "só o 1 assume a fila do RPA (padrão: 1)")
    args = ap.parse_args()
    if args.worker < 1 or (args.workers is not None and args.workers < 1):
        ap.error("--worker e --workers precisam ser 1 ou mais")
    return args


def main():
    """Lê os argumentos. --workers N: supervisiona os N workers. Senão roda o worker com a trava dele pega (solta ao
    sair)."""
    args = ler_argumentos()
    if args.workers:
        supervisionar(args)
        return
    with travar_worker(args.worker):
        rodar(args)


def supervisionar(args):
    """--workers N: sobe os workers 1..N como processos filhos neste terminal, cada um igual ao --worker i (Chrome,
    proxy do DJEN, arquivos, log e trava próprios; só o 1 assume a fila), um a cada PAUSA_ENTRE_WORKERS s, e espera
    todos acabarem. O Ctrl+C do terminal chega a todos (mesmo console): cada worker desfaz o que estava gravando,
    devolve o crédito em andamento e sai. Um 2º Ctrl+C mata os que ainda não saíram."""
    sup = configurar_log("fetch_TJBA_workers", SAIDA / "logs")
    saidas_djen = 1 + len(proxies_do_env())
    if args.workers > saidas_djen:
        sup.warning(f"{args.workers} workers e {saidas_djen} saída(s) para o DJEN (direta + PROXY_*): do worker "
                    f"{saidas_djen + 1} em diante o DJEN sai direto e divide o limite por IP (fica mais lento)")
    extras = (["--simulacao"] if args.simulacao else []) + (["--limite", str(args.limite)] if args.limite else [])
    workers = {}
    try:
        for n in range(1, args.workers + 1):
            if n > 1:
                time.sleep(PAUSA_ENTRE_WORKERS)
            workers[n] = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--worker", str(n), *extras])
            sup.info(f"worker {n} de {args.workers} subiu (pid {workers[n].pid})")
        esperar_workers(sup, workers)
    except KeyboardInterrupt:
        sup.warning("Ctrl+C: cada worker devolve o crédito em andamento e encerra "
                    "(Ctrl+C de novo mata os que faltarem)")
        try:
            esperar_workers(sup, workers)
        except KeyboardInterrupt:
            for n, processo in workers.items():
                if processo.poll() is None:
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(processo.pid)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    sup.error(f"worker {n} encerrado à força: o crédito dele volta para a fila quando o lease "
                              f"expirar ({LEASE})")
    sup.info("workers encerrados: " + ", ".join(f"{n}={p.poll()}" for n, p in workers.items())
             + " (0 = saiu normal). Resultados nos CSVs e logs de cada worker em TJBA/saida.")


def esperar_workers(sup, workers):
    """Espera todos os workers saírem, registrando cada um que termina. Espera com timeout: no Windows, a espera sem
    timeout não atende o Ctrl+C."""
    avisados = set()
    while True:
        for n, processo in workers.items():
            if processo.poll() is not None and n not in avisados:
                avisados.add(n)
                nivel = logging.INFO if processo.returncode == 0 else logging.WARNING
                sup.log(nivel, f"worker {n} terminou (código {processo.returncode})")
        if len(avisados) == len(workers):
            return
        time.sleep(1)


def rodar(args):
    """Laço principal: pega um crédito, processa e grava na hora (uma transação por crédito).
    Ctrl+C desfaz o que estava em gravação e devolve o crédito em andamento para a fila."""
    definir_worker(args.worker)
    configurar_log(f"fetch_TJBA{sufixo_worker()}", SAIDA / "logs")
    rod = Rodada(args.simulacao, args.limite)
    credito_id = None
    try:
        while (credito_id := proximo_credito(rod)):
            item = processar_credito(rod, credito_id)
            if item:
                registrar_credito(rod, item, gravar_credito(rod, item))
            credito_id = None                           # gravado (ou devolvido): não volta mais para a fila aqui
    except KeyboardInterrupt:
        desfazer_transacao(rod)
        if credito_id and not rod.simulacao:
            try:
                devolver(rod.con, credito_id)
                log.warning(f"interrompido: o crédito {credito_id} voltou para a fila.")
            except psycopg2.Error:
                log.error(f"interrompido: não consegui devolver o crédito {credito_id}; ele volta quando o lease "
                          f"expirar ({LEASE}).")
    finally:
        encerrar(rod)


if __name__ == "__main__":
    main()
