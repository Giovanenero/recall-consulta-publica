"""
fetch_TJMA.py - credor dos precatórios do TJMA pela consulta pública, de ponta a ponta, sem navegador e sem login:
lê o banco, acha o credor (DJEN), acha e confirma o originário (PJe 1º grau público, HTTP puro) e grava no banco em
lotes de LOTE créditos (LOTE_GRAVACAO_TJMA no .env). No lugar do modo credor do RPA_SISTEMAS (token A3 no PJe 2º grau).

A lista do TJMA não traz o beneficiário (a Res. CNJ 303 veda) e o PJe 2º grau público não abre precatório, então:
1. Fila: ordem própria, não pagos primeiro (lista vigente e PDFs de pagos do hotsite do TJMA), e dentro de cada faixa
   campanha, sem credor antes de com credor e maior valor. Cada crédito é reservado com lease (reservar) e só nessa
   hora passa do RPA para o software próprio (CONSULTA_PUBLICA_TJMA); o que o RPA tinha fica em desfazer_fila_*.sql.
2. DJEN pelo nº do precatório: o nome do credor (o texto traz 'CREDOR: E. S. D. A.' e os destinatários trazem o nome
   inteiro; as iniciais escolhem o destinatário certo), as OABs dos advogados e os CNJs de 1º grau citados no texto.
   Sem nenhuma publicação ainda (precatório novo): volta para a fila em ADIAMENTO_SEM_PUBLICACAO.
3. PJe 1º grau público (reCAPTCHA desligado no portal: 'if (false)'): pesquisa pelo nome do credor (até 30
   resultados; saturou, corta pela data de autuação e soma a busca pelo nome no DJEN). Candidato: CNJ de 1º grau do
   TJMA, não mais novo que o precatório, ente no polo passivo e classe que gera precatório. Somam-se o originário já
   ligado, os CNJs citados no DJEN (evidência forte) e as pistas do RPA.
4. Confirma cada candidato pela capa que já está no banco ou abrindo o detalhe público (CPF completo do polo ativo).
   Confirmado = credor no polo ativo e ente no polo passivo.
5. Decide: 1 confirmado -> liga; vários -> desempata por CNJ citado no DJEN, OAB em comum com o precatório ou DJEN do
   candidato citando o precatório/valor; sem desempate e o mesmo CPF em todos -> grava o credor sem ligar o originário
   (SUCESSO_ANALISAR); CPFs diferentes -> SUCESSO_ANALISAR sem credor.
6. Junta LOTE créditos processados e grava o lote numa transação só, cada crédito no seu SAVEPOINT (erro desfaz só
   ele): originário, partes (registrar_capa), credor (registrar_credor), metadata (registrar_credito), capa antiga e
   filas mensais (legado) e o status na fila (fila_credor_finalizar).

Rápido: WORKERS threads raspam ao mesmo tempo (cada uma com a sua sessão do PJe e a sua conexão de leitura);
só a thread principal pega da fila e grava. O DJEN tem um relógio por saída (direta e, com --proxies, PROXY_01..05
do .env); os proxies precisam sair pelo Brasil (o DJEN bloqueia fora do país).

Erro passageiro (DJEN/PJe fora, timeout, captcha religado) devolve o crédito para a fila (fila_credor_adiar) na hora,
nunca vira FALHA e não entra no lote. Ctrl+C devolve os créditos em andamento e grava o lote que já estava processado.
O texto das publicações do DJEN não é gravado em lugar nenhum (há dado de saúde nas decisões de superpreferência).

Saídas em TJMA/saida: fetch_TJMA.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql, desfazer_fila_*.sql e o HTML de cada detalhe aberto no PJe (pje1g_html/).

Uso:
    python fetch_TJMA.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJMA.py                        # processa a fila do TJMA até acabar (Ctrl+C para parar)
    python fetch_TJMA.py --limite 30            # para depois de 30 créditos
    python fetch_TJMA.py --workers 12           # workers (threads) raspando ao mesmo tempo (padrão 6)
    python fetch_TJMA.py --proxies              # soma PROXY_01..05 como saídas extras do DJEN
"""
import argparse
import html as H
import io
import json
import logging
import os
import re
import socket
import sys
import threading
import time
import unicodedata
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import psycopg2
import psycopg2.errors
import requests
from dotenv import load_dotenv

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
PASTA_PDF = SAIDA / "pdf_tjma"          # números já extraídos de cada PDF do hotsite (o nome do arquivo muda a cada edição)
load_dotenv(AQUI.parent / ".env")      # PG_*, LOTE_GRAVACAO_TJMA e (com --proxies) PROXY_01..05
log = logging.getLogger("fetch_TJMA")


def lote_do_env():
    """Créditos por transação de gravação, de LOTE_GRAVACAO_TJMA no .env (inteiro maior que zero)."""
    valor = os.environ.get("LOTE_GRAVACAO_TJMA", "").strip()
    if not valor.isdigit() or int(valor) < 1:
        raise SystemExit(f"LOTE_GRAVACAO_TJMA no .env precisa ser um inteiro maior que zero (veio {valor!r}).")
    return int(valor)


TRIBUNAL_TJMA = 110
SOFTWARE = "CONSULTA_PUBLICA_TJMA"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJMA:consulta_publica:{os.getpid()}"
LOTE = lote_do_env()                   # créditos por transação de gravação
WORKERS = 6                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 5 * 60                  # s por crédito; passou disso, volta para a fila
ADIAMENTO = "30 minutes"               # erro passageiro
ADIAMENTO_SEM_PUBLICACAO = "15 days"   # precatório ainda sem publicação no DJEN
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
MAX_FALHAS_SEGUIDAS = 8                # falhas técnicas seguidas que param o robô
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"

DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
INTERVALO_DJEN = 1.2                   # s entre consultas por saída (sobe sozinho quando o DJEN devolve 429)
PAUSA_429 = 10                         # s que a saída fica parada depois de um 429 (o intervalo também sobe)
INTERVALO_DJEN_MAX = 6.0
FALHAS_PARA_DESLIGAR_PROXY = 3
MAX_PAGINAS_DJEN_PRECATORIO = 3        # 100 publicações por página
MAX_PAGINAS_DJEN_NOME = 5
MAX_VALOR_DJEN = 5                     # candidatos confirmados que ganham 1 consulta no DJEN para o desempate por valor
MAX_CACHE_DJEN = 5000

HOTSITE = "https://www.tjma.jus.br/midia/prec/pagina/hotsite/500708"
RE_PDF_LISTA = re.compile(r"estado_do_maranhao_administracao_direta_e_indireta_atualizada_ate_(\d{2})(\d{2})(\d{4})")
RE_PDF_PAGOS = re.compile(r"pagos|processo_de_pagamento", re.I)
# ordem da fila do robô: faixa menor primeiro (a prioridade de campanha vale dentro da faixa)
FAIXAS = {1: "NAO_PAGO", 2: "SEM_INFO_PAGAMENTO", 3: "PAGO_PARCIAL"}
FORA_DA_LISTA = "FORA_DA_LISTA_VIGENTE"   # grupo do Estado que não está na lista vigente: quitado ou retirado, não é pego

BASE = "https://pje.tjma.jus.br"
URL = BASE + "/pje/ConsultaPublica/listView.seam"
CAMPO_NUMERO = "fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso"
CAMPO_NOME = "fPP:dnp:nomeParte"
CAMPO_AUTUACAO_ATE = "fPP:dataAutuacaoDecoration:dataAutuacaoFimInputDate"
LIMITE_PESQUISA = 30                   # o portal corta a pesquisa em 30 resultados
TIMEOUT_PJE = 90
MAX_ABERTOS_PJE = 8                    # detalhes abertos no PJe por crédito
MAX_PAGINAS_PARTES = 60
MAX_PAGINAS_PASSIVO = 5
TETO_DETALHE = 90                      # s virando páginas de partes num detalhe (ação coletiva enorme)
PAUSA_PJE = 0.5
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"

CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")
RE_ESPOLIO_REP = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?(?P<falecido>.+?)\s+(?:rep\.?|representad[oa])\s+"
                            r"(?:por\s+)?(?P<rep>.+?)\s*$", re.I)
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
RE_REP = re.compile(r"\s+(?:rep\.?|representad[oa])\s+(?:por\s+)?.*$", re.I)
RE_E_OUTROS = re.compile(r"\s+e\s+outr[oa]s?(?:\s*\(\d+\))?\s*$", re.I)
RE_APOSTO = re.compile(r"\s*\([^)]*\)\s*$")
CLASSE_EXECUCAO = re.compile(r"CUMPRIMENTO|EXECU", re.I)
# classes que não geram precatório contra o ente (o nome do credor aparece nelas, mas não é o originário)
RE_CLASSE_FORA = re.compile(r"CARTA PRECATORIA|CARTA DE ORDEM|JUIZADO ESPECIAL CIVEL|PRECATORIO|PEQUENO VALOR|"
                            r"HOMOLOGACAO DA TRANSACAO|TUTELA ANTECIPADA ANTECEDENTE|TUTELA CAUTELAR|ALVARA|"
                            r"INVENTARIO|ARROLAMENTO|DIVORCIO|ALIMENTOS|BUSCA E APREENSAO|INQUERITO|ACAO PENAL|"
                            r"TERMO CIRCUNSTANCIADO|MEDIDAS PROTETIVAS|EXECUCAO FISCAL")
RE_ORGAO_PUBLICO = re.compile(r"^(?:ESTADO D|MUNICIPIO D|UNIAO\b|DISTRITO FEDERAL)|PROCURADORIA|DEFENSORIA PUBLICA|"
                              r"MINISTERIO PUBLICO|FAZENDA PUBLICA|PREFEITURA|CAMARA MUNICIPAL|TRIBUNAL D")
# autarquia/empresa estadual no polo passivo de precatório do grupo 'Estado do Maranhão'
RE_ENTE_ESTADUAL = re.compile(r"^(?:ESTADO DO MARANHAO|INSTITUTO|UNIVERSIDADE ESTADUAL|DEPARTAMENTO ESTADUAL|AGENCIA|"
                              r"FUNDACAO|JUNTA COMERCIAL|EMPRESA MARANHENSE|COMPANHIA DE SANEAMENTO|SECRETARIA|FUNDO|"
                              r"DETRAN|IPREV|UEMA|CAEMA|EMSERH|JUCEMA|FUNAC|ITERMA|IEMA|AGED)")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA")

# DJEN: rótulo do credor no texto ('CREDOR:', 'Credor(a):', 'CREDOR(A)/REQUERENTE:', 'CREDOR(A):/REQUERENTE:')
_FIM_ROTULO = (r"(?=\s*(?:Advogad[oa]s?\s*(?:\(|/|do\b|da\b|:)|Devedor|Requerid|Requerente\s*:|Procurador|Natureza|"
               r"Decis[aã]o|Despacho|D\s?E\s?C\s?I\s?S|D\s?E\s?S\s?P|Cession|Cedente|Interessad|Executad|Ente\s+devedor)"
               r"|$)")
RE_ROTULO_CREDOR = re.compile(r"\bCREDOR(?:\s*\((?:A|ES|AS)\))?\s*(?::\s*/\s*REQUERENTE\s*:|/\s*REQUERENTE\s*:|:)"
                              r"\s*(?P<v>.{2,160}?)" + _FIM_ROTULO, re.I | re.S)
RE_CESSIONARIO = re.compile(r"\bCession[aá]ri[oa]s?(?:\s*\((?:a|s|as)\))*\s*:\s*(?P<v>.{2,160}?)" + _FIM_ROTULO,
                            re.I | re.S)

RE_LINK_DETALHE = re.compile(r"(/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+)")
RE_QTD = re.compile(r"(\d*)\s*resultados\s+encontrados")
RE_LINHA = re.compile(r'<tr class="rich-table-row[^"]*">(.*?)</tr>', re.S)
# Linha de parte como o PJe escreve: "FULANO - CPF: 000.000.000-00 (AUTOR)",
# "BELTRANO - OAB MA12345 - CPF: ... (ADVOGADO)"
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

SEMELHANCA_MESMA_PESSOA = 0.9          # nomes com grafia próxima (SOUSA x SOUZA) contam como a mesma pessoa
PAPEL_LEGADO = {"ATIVO": "REQUERENTE", "PASSIVO": "REQUERIDO"}    # como o RPA grava a capa antiga
POLO_BRUTO = {"ATIVO": "AUTOR", "PASSIVO": "REU"}

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "pagamento", "caminho", "credor_djen", "ente", "ultimo_status",
           "resultado",
           "motivo", "originario", "regra", "fontes", "credor", "credor_documento", "candidatos", "capa_fonte",
           "publicacoes_djen", "oabs_precatorio", "cessao", "credor_corrigido", "coletiva", "banco", "legado", "credores_antes", "credores_depois",
           "lote", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set(), "corrigidos": [], "coletiva": 0}


class ErroTecnico(Exception):
    """Falha passageira (PJe/DJEN fora do ar, timeout, captcha religado): o crédito volta para a fila, não vira FALHA."""

# =============================================================================== utilidades


def sem_acento(t):
    """Maiúsculas e sem acento, com a pontuação preservada (para as iniciais)."""
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples (para comparar nomes)."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", sem_acento(t))).strip()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE' e sem o 'REGISTRADO(A) CIVILMENTE COMO ...'."""
    return re.split(r"\bREGISTRAD[OA]\b", normal(RE_ESPOLIO.sub("", nome or "")))[0].strip()


def nomes_para_buscar(credor):
    """Nome do credor -> nomes a pesquisar (espólio com inventariante: os dois)."""
    b = (credor or "").strip()
    m = RE_ESPOLIO_REP.match(b)
    if m:
        nomes = [m["falecido"], m["rep"]]
    else:
        for regex in (RE_APOSTO, RE_REP, RE_ESPOLIO, RE_E_OUTROS):
            b = regex.sub("", b)
        nomes = [b]
    return [n for n in (x.strip(" -,;.") for x in nomes) if n]


def chaves_ente(ente):
    """'MUNICIPIO DE CODO' -> {'MUNICIPIO DE CODO', 'CODO'}; 'ESTADO DO MARANHÃO (Adm. Direta)' -> {'ESTADO DO MARANHAO'};
    'X - SIGLA' -> {'X', 'SIGLA'}."""
    partes = [normal(p) for p in re.split(r"\s+-\s+", re.sub(r"\([^)]*\)", " ", ente or "")) if p.strip()]
    chaves = set(partes)
    for p in partes:
        m = re.match(r"MUNICIPIO D[EOA]S? (.+)", p)
        if m:
            chaves.add(m.group(1))
    return {c for c in chaves if len(c) >= 4}


def ente_no_nome(nome, chaves, estadual):
    """A parte é o ente devedor? Grupo do Estado: também a autarquia/empresa estadual (UEMA, IPREV, DETRAN...)."""
    n = normal(nome)
    return any(c in n for c in chaves) or (estadual and "MARANHAO" in n and bool(RE_ENTE_ESTADUAL.search(n))) or \
        (estadual and bool(re.match(r"(?:DETRAN|IPREV|UEMA|CAEMA|EMSERH|JUCEMA|FUNAC|ITERMA|IEMA|AGED)\b", n)))


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


def eh_iniciais(t):
    """'E. S. D. A.' / 'M. G. &. V. B. A. A.' -> True (o TJMA anonimiza o credor no texto da publicação)."""
    tokens = re.findall(r"[^\s.]+", t or "")
    return "." in (t or "") and len(tokens) >= 2 and all(len(x) == 1 for x in tokens)


def iniciais(t):
    """Primeira letra de cada palavra: 'ELSON SOUSA DOS ANJOS' e 'E. S. d. A.' -> 'ESDA'."""
    return "".join(x[0] for x in re.findall(r"[^\s.]+", sem_acento(t)))


def cnj_1g_tjma(n):
    """CNJ (20 dígitos) de processo do TJMA fora do 2º grau (o final 0000 é o próprio tribunal)."""
    return len(n) == 20 and n[13:16] == "810" and n[16:20] != "0000"


def limpar_rotulo(v):
    """Valor do rótulo 'CREDOR:' sem sobras ('REQUERENTE:' na frente, '(a)' no fim); '' se parecer frase e não nome
    (ex.: 'do novo credor: (a) Certidão de óbito ...; (b) ...')."""
    v = re.sub(r"\s+", " ", re.sub(r"\(\s*a\s*\)\s*$", "", v or "", flags=re.I)).strip(" :;,-/")
    v = re.sub(r"^(?:/?\s*REQUERENTE\s*:\s*)+", "", v, flags=re.I).strip(" :;,-/")
    minusculas = [x for x in v.split() if len(x) > 2 and re.search(r"[a-zà-ü]", x)]
    if not v or len(v.split()) > 14 or re.search(r"[;()]", v) or len(minusculas) >= 2:
        return ""
    return v

# =============================================================================== DJEN


class Saida:
    """Uma saída para o DJEN (direta ou proxy) com o seu próprio relógio."""

    def __init__(self, nome, proxy=None):
        self.nome, self.proxies = nome, ({"http": proxy, "https": proxy} if proxy else None)
        self.proxima, self.intervalo, self.falhas, self.ativa = 0.0, INTERVALO_DJEN, 0, True


def proxies_do_env():
    """PROXY_01..05 do .env ('host:porta:usuário:senha' ou URL) -> URLs de proxy."""
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


class Djen:
    """Consulta o DJEN respeitando o relógio de cada saída (compartilhado entre as threads), com nova tentativa.
    Cache só na memória e só das consultas que se repetem entre créditos (nome e candidato)."""

    def __init__(self, parar, usar_proxies):
        self.parar = parar
        self.saidas = [Saida("direta")] + ([Saida(k, v) for k, v in proxies_do_env()] if usar_proxies else [])
        self.trava = threading.Lock()
        self.cache = {}
        self.local = threading.local()
        self.n_429 = 0

    def _sessao(self):
        if not getattr(self.local, "sessao", None):
            self.local.sessao = requests.Session()
            self.local.sessao.headers["User-Agent"] = "Mozilla/5.0"
        return self.local.sessao

    def _reservar(self):
        """A saída livre mais cedo, já com a vez marcada; dorme até a vez chegar."""
        with self.trava:
            ativas = [s for s in self.saidas if s.ativa]
            s = min(ativas, key=lambda x: x.proxima)
            vez = max(time.time(), s.proxima)
            s.proxima = vez + s.intervalo
        while (espera := vez - time.time()) > 0:
            if self.parar.is_set():
                raise ErroTecnico("INTERROMPIDO")
            time.sleep(min(espera, 0.5))
        return s

    def _falhou(self, s, motivo):
        """Proxy que falha FALHAS_PARA_DESLIGAR_PROXY vezes seguidas sai do rodízio (a direta nunca sai)."""
        with self.trava:
            s.falhas += 1
            if s.proxies and s.falhas >= FALHAS_PARA_DESLIGAR_PROXY and s.ativa:
                s.ativa = False
                log.warning(f"DJEN: saída {s.nome} desligada ({motivo})")

    def buscar(self, com_texto, usar_cache, **params):
        """Publicações da consulta (enxutas). ErroTecnico se não responder."""
        chave = json.dumps(params, sort_keys=True, ensure_ascii=False)
        if usar_cache and chave in self.cache:
            return self.cache[chave]
        for _ in range(8):
            s = self._reservar()
            try:
                r = self._sessao().get(DJEN, params=params, proxies=s.proxies, timeout=60)
            except requests.RequestException as e:
                self._falhou(s, e.__class__.__name__)
                time.sleep(3)
                continue
            if r.status_code == 200:
                with self.trava:
                    s.falhas, s.intervalo = 0, max(INTERVALO_DJEN, s.intervalo * 0.95)
                itens = [self._enxuto(i, com_texto) for i in r.json().get("items", [])]
                if usar_cache:
                    with self.trava:
                        if len(self.cache) > MAX_CACHE_DJEN:
                            self.cache.clear()
                        self.cache[chave] = itens
                return itens
            if r.status_code == 429:
                with self.trava:
                    self.n_429 += 1
                    s.intervalo = min(s.intervalo * 1.5, INTERVALO_DJEN_MAX)
                    s.proxima = time.time() + PAUSA_429
            elif r.status_code in (403, 407) and s.proxies:
                self._falhou(s, f"HTTP {r.status_code}")
            else:
                time.sleep(5)
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: DJEN não respondeu")

    @staticmethod
    def _enxuto(item, com_texto):
        """Só o que o robô usa da publicação: número, classe, partes por polo, advogados com OAB e
        (se pedido) o texto, que fica só na memória."""
        advogados = []
        for a in item.get("destinatarioadvogados") or []:
            adv = a.get("advogado") or {}
            if adv.get("numero_oab"):
                advogados.append({"nome": adv.get("nome") or "", "oab": str(adv["numero_oab"]),
                                  "uf": (adv.get("uf_oab") or "").upper()})
        texto = H.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", item.get("texto") or ""))) if com_texto else ""
        return {"numero": so_digitos(item.get("numero_processo") or item.get("numeroprocessocommascara"))[:20],
                "classe": (item.get("nomeClasse") or "").upper(),
                "partes": [[d.get("polo"), d.get("nome") or ""] for d in item.get("destinatarios") or []],
                "advogados": advogados,
                "texto": texto}

    def paginado(self, com_texto, usar_cache, max_paginas, **params):
        """Todas as páginas (100 por página) até max_paginas."""
        itens = []
        for pagina in range(1, max_paginas + 1):
            lote = self.buscar(com_texto, usar_cache, pagina=pagina, itensPorPagina=100, **params)
            itens += lote
            if len(lote) < 100:
                break
        return itens

    def do_precatorio(self, prec20):
        """Publicações do precatório, com o texto (credor, OAB e CNJ citado). Sem cache: não se repete."""
        return self.paginado(True, False, MAX_PAGINAS_DJEN_PRECATORIO, numeroProcesso=prec20, siglaTribunal="TJMA")

    def por_nome(self, nome):
        """Publicações do TJMA com a parte pelo nome."""
        return self.paginado(False, True, MAX_PAGINAS_DJEN_NOME, nomeParte=nome, siglaTribunal="TJMA")

    def por_numero(self, numero20):
        """Publicações de um candidato, com o texto (para achar o nº do precatório ou o valor)."""
        return self.buscar(True, True, numeroProcesso=numero20)


def credor_do_djen(itens, chaves, estadual):
    """O que as publicações do precatório dizem: nomes do credor, OABs, CNJs de 1º grau citados e se há cessão.
    O credor sai do rótulo 'CREDOR:' do texto; se o rótulo vem em iniciais, do destinatário com as mesmas iniciais;
    sem rótulo, do destinatário (fora ente, advogados e cessionários) que mais aparece."""
    rotulos, cessionarios, destinatarios = Counter(), set(), Counter()
    advogados, oabs, citados = set(), set(), set()
    for it in itens:
        texto = it["texto"]
        for m in RE_ROTULO_CREDOR.finditer(texto):
            v = limpar_rotulo(m["v"])
            if v:
                rotulos[v] += 1
        for m in RE_CESSIONARIO.finditer(texto):
            v = limpar_rotulo(m["v"])
            if v and not eh_iniciais(v):
                cessionarios.add(chave_nome(v))
        advogados |= {chave_nome(a["nome"]) for a in it["advogados"]}
        oabs |= {a["oab"] + a["uf"] for a in it["advogados"]}
        for _polo, nome in it["partes"]:
            if nome.strip():
                destinatarios[" ".join(nome.split())] += 1
        citados |= {n for n in (so_digitos(c) for c in CNJ.findall(texto)) if cnj_1g_tjma(n)}
    possiveis = Counter({n: q for n, q in destinatarios.items()
                         if chave_nome(n) not in advogados and chave_nome(n) not in cessionarios
                         and not ente_no_nome(n, chaves, estadual) and not RE_ORGAO_PUBLICO.search(normal(n))})
    nomes = []
    for v, _ in rotulos.most_common():
        if eh_iniciais(v):
            nomes += [n for n, _ in possiveis.most_common() if iniciais(n) == iniciais(v)]
        else:
            nomes.append(v)
    if not rotulos:
        nomes += [n for n, _ in possiveis.most_common(1)]
    unicos, vistos = [], set()
    for n in nomes:
        if chave_nome(n) and chave_nome(n) not in vistos:
            vistos.add(chave_nome(n))
            unicos.append(n)
    return {"nomes": unicos, "oabs": oabs, "citados": citados, "cessao": bool(cessionarios),
            "rotulos": list(rotulos), "publicacoes": len(itens)}


def candidatos_djen_nome(djen, prec20, nomes, alvo, chaves, estadual):
    """CNJs em que o credor está no polo ativo e o ente no passivo, pelo nome no DJEN (fonte extra quando a pesquisa
    do PJe satura em 30 resultados). O DJEN põe os advogados no polo A: publicação em que o credor é advogado não conta."""
    procs = {}
    for it in (i for n in nomes for i in djen.por_nome(n)):
        p = procs.setdefault(it["numero"], {"classe": it["classe"], "ativo": False, "ente": False})
        advogados = {chave_nome(a["nome"]) for a in it["advogados"]}
        for polo, nome in it["partes"]:
            k = chave_nome(nome)
            p["ativo"] |= polo == "A" and k in alvo and k not in advogados
            p["ente"] |= polo == "P" and ente_no_nome(nome, chaves, estadual)
    return {n: p["classe"] for n, p in procs.items()
            if cnj_1g_tjma(n) and n != prec20 and int(n[9:13]) <= int(prec20[9:13]) and p["ativo"] and p["ente"]
            and not RE_CLASSE_FORA.search(normal(p["classe"]))}

# =============================================================================== PJe 1º grau público (HTTP puro)


def sem_script(html):
    """HTML sem os <script> e com as entidades (&aacute; ...) resolvidas."""
    return H.unescape(re.sub(r"<script\b.*?</script>", " ", html, flags=re.S | re.I))


def texto_de(html):
    """HTML -> texto plano (as tags viram espaço)."""
    return " ".join(re.sub(r"<[^>]+>", " ", sem_script(html)).split())


def partes_da_pagina(html):
    """Partes e advogados das tabelas de polo ativo e passivo ('Outros interessados' fica de fora)."""
    limpo = sem_script(html)
    ini_a = limpo.find("processoPartesPoloAtivoResumido")
    ini_p = limpo.find("processoPartesPoloPassivoResumido")
    ini_o = limpo.find("processoParteOutrosInteressadosResumido", max(ini_p, 0))
    fim_a = ini_p if ini_p > ini_a else len(limpo)
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
    t = texto_de(html)
    ini = t.rfind("Dados do Processo")
    bloco = t[ini: (t.find("Polo ativo", ini) if t.find("Polo ativo", ini) != -1 else len(t))] if ini != -1 else t
    out = {}
    for chave, rotulo in ROTULOS.items():
        m = re.search(rf"{rotulo}\s*:?\s+(.{{1,400}}?)\s*(?=(?:{FIM_CAMPO})\b|$)", bloco, flags=re.S)
        if m and m.group(1).strip(" :-"):
            out[chave] = " ".join(m.group(1).split()).strip(" :-")
    return out


def linhas_da_pesquisa(html):
    """Resultado da pesquisa -> [{numero, classe, ativo, passivo, link}]. A célula é
    'CLASSE <a ...><b>Sigla CNJ - Assunto</b></a> AUTOR [e outros (N)] X RÉU [e outros (N)]'."""
    linhas = []
    for bloco in RE_LINHA.findall(html):
        link = RE_LINK_DETALHE.search(bloco)
        celula = re.search(r"<td[^>]*>((?:(?!</td>).)*btn-block(?:(?!</td>).)*)</td>", bloco, re.S)
        if not link or not celula:
            continue
        antes, _, depois = celula.group(1).partition("<a")
        cnj = CNJ.search(H.unescape(re.sub(r"<[^>]+>", " ", celula.group(1))))
        partes = " ".join(H.unescape(re.sub(r"<[^>]+>", " ", depois.split("</a>")[-1])).split())
        ativo, _, passivo = partes.partition(" X ")
        linhas.append({"numero": so_digitos(cnj.group(0)) if cnj else "", "classe": " ".join(antes.split()).upper(),
                       "ativo": ativo, "passivo": passivo, "link": link.group(1).replace("&amp;", "&")})
    return linhas


def captcha_ligado(html):
    """O portal voltou a exigir o reCAPTCHA? Hoje é 'if (false) { grecaptcha.execute(); ... }'."""
    return "grecaptcha.execute" in html and not re.search(r"if\s*\(\s*false\s*\)\s*\{\s*grecaptcha\.execute", html)


class Pje:
    """Consulta pública do PJe 1º grau do TJMA por requests (uma instância por thread: jsessionid próprio)."""

    def __init__(self, parar):
        self.parar = parar
        self.nova_sessao()

    def nova_sessao(self):
        """Sessão nova (cookies e ViewState zerados)."""
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9"})

    def _http(self, metodo, url, **kw):
        """GET/POST com 3 tentativas; ErroTecnico se não responder."""
        erro = ""
        for tentativa in range(3):
            if self.parar.is_set():
                raise ErroTecnico("INTERROMPIDO")
            try:
                r = self.s.request(metodo, url, timeout=TIMEOUT_PJE, **kw)
                if r.status_code == 200:
                    return r
                erro = f"HTTP {r.status_code}"
            except requests.RequestException as e:
                erro = e.__class__.__name__
            time.sleep(3 * (tentativa + 1))
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o PJe não respondeu ({erro})")

    def pesquisar(self, campos):
        """(linhas, quantidade) da pesquisa com os campos dados. O submit de verdade é o a4j:jsFunction
        executarPesquisa (o botão só chama o captcha, que está desligado)."""
        for tentativa in (1, 2):
            html = self._http("GET", URL).text
            if captcha_ligado(html):
                raise ErroTecnico("CAPTCHA_REATIVADO: a consulta pública do TJMA voltou a exigir reCAPTCHA")
            vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]*)"', html)
            # sem cookie o destino vem no actionUrl do JS (com ;jsessionid); com a sessão aberta, só no action do form
            acao = re.search(r"'actionUrl':'([^']*listView\.seam[^']*)'", html) or \
                re.search(r'<form id="fPP"[^>]*action="([^"]*listView\.seam[^"]*)"', html)
            gatilho = re.search(r"executarPesquisa=function\(\)\{A4J\.AJAX\.Submit\('fPP',null,"
                                r"\{'similarityGroupingId':'([^']+)'", html)
            if not (vs and acao and gatilho):
                raise ErroTecnico("PESQUISA_SEM_RESPOSTA: formulário da consulta pública mudou (PJe mudou de versão?)")
            dados = {k: "" for k in re.findall(r'name="(fPP:[^"]+)"', html)
                     if "searchProcessos" not in k and "CurrentDate" not in k}
            dados.update({"AJAXREQUEST": "_viewRoot", "fPP": "fPP", "javax.faces.ViewState": vs.group(1),
                          gatilho.group(1): gatilho.group(1)})
            dados.update(campos)
            url_acao = BASE + re.sub(r"\\x([0-9A-Fa-f]{2})", lambda m: chr(int(m.group(1), 16)), acao.group(1))
            r = self._http("POST", url_acao, data=dados, headers={"Referer": URL})
            qtd = RE_QTD.search(r.text)
            if qtd:
                return linhas_da_pesquisa(r.text), int(qtd.group(1) or 0)
            self.nova_sessao()                          # o portal não respondeu à pesquisa: sessão nova e mais uma vez
            time.sleep(3)
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: o PJe devolveu a pesquisa sem resultado e sem contagem")

    def por_numero(self, numero20):
        """Linha do processo pelo número; None se o portal não acha (não existe no PJe, físico ou segredo)."""
        linhas, _ = self.pesquisar({CAMPO_NUMERO: formatar_cnj(numero20)})
        return next((x for x in linhas if x["numero"] == numero20), linhas[0] if linhas else None)

    def por_nome(self, nome, autuado_ate=None):
        """(linhas, saturou) da pesquisa pelo nome da parte (opcionalmente autuados até a data). O portal compara com
        acento: 'JOSÉ' não acha o 'JOSE' cadastrado, então a pesquisa vai sem acento."""
        campos = {CAMPO_NOME: " ".join(sem_acento(nome).split())}
        if autuado_ate:
            campos[CAMPO_AUTUACAO_ATE] = autuado_ate.strftime("%d/%m/%Y")
        linhas, qtd = self.pesquisar(campos)
        return linhas, qtd >= LIMITE_PESQUISA

    def detalhe(self, link, numero20, alvo, prazo=None):
        """Capa e partes do detalhe. Vira as páginas do polo ativo até achar o credor (ou acabar) e as do passivo até
        MAX_PAGINAS_PASSIVO, sem passar de TETO_DETALHE nem do prazo do crédito. completa=False se alguma tabela
        não foi lida inteira."""
        prazo = min(prazo or float("inf"), time.time() + TETO_DETALHE)
        url = urljoin(BASE, link)
        html = self._http("GET", url, headers={"Referer": URL}).text
        if "processoPartesPolo" not in html:
            raise ErroTecnico("PROCESSO_NAO_CARREGOU: detalhe do PJe sem as tabelas de partes")
        PASTA_HTML.mkdir(parents=True, exist_ok=True)
        (PASTA_HTML / f"{numero20}.html").write_text(html, encoding="utf-8")
        partes, completa = partes_da_pagina(html), True
        vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]*)"', html)
        limpo = sem_script(html)
        ini_p = limpo.find("processoPartesPoloPassivoResumido")
        segmentos = {"ATIVO": limpo[:ini_p if ini_p != -1 else len(limpo)], "PASSIVO": limpo[max(ini_p, 0):]}
        for polo, teto in (("ATIVO", MAX_PAGINAS_PARTES), ("PASSIVO", MAX_PAGINAS_PASSIVO)):
            marcador = "processoPartesPoloAtivoResumido" if polo == "ATIVO" else "processoPartesPoloPassivoResumido"
            param = re.search(rf"'parameters':\{{'([^']*{marcador}[^']*)':event\.memo\.page", html)
            if not param or "rich-datascr-inact" not in segmentos[polo]:
                continue
            param = param.group(1)
            vistas = {(p["nome"], p["documento"], p["papel"]) for p in partes if p["polo"] == polo}
            for pg in range(2, teto + 2):
                if polo == "ATIVO" and alvo and any(p["polo"] == "ATIVO" and chave_nome(p["nome"]) in alvo
                                                    for p in partes):
                    completa = False                    # achou o credor: o resto do polo ativo não muda a decisão
                    break
                if pg > teto or time.time() > prazo:
                    completa = False
                    break
                time.sleep(PAUSA_PJE)
                corpo = {"AJAXREQUEST": "_viewRoot", param.split(":")[0]: param.split(":")[0], param: str(pg),
                         "ajaxSingle": param, "autoScroll": "", "javax.faces.ViewState": vs.group(1) if vs else "j_id1"}
                novas = [p for p in partes_da_pagina(self._http("POST", url, data=corpo, headers={"Referer": url}).text)
                         if p["polo"] == polo and (p["nome"], p["documento"], p["papel"]) not in vistas]
                if not novas:
                    break
                vistas |= {(p["nome"], p["documento"], p["papel"]) for p in novas}
                partes += novas
        unicas = list({(p["nome"], p["documento"], p["papel"], p["polo"]): p for p in partes}.values())
        return {"resultado": "OK", "capa": campos_da_capa(html), "completa": completa,
                "partes": [p for p in unicas if not RE_ADVOGADO.search(p["papel"])],
                "advogados": [p for p in unicas if RE_ADVOGADO.search(p["papel"])]}

# =============================================================================== situação de pagamento (PDFs do hotsite)


def numeros_do_pdf(url):
    """Números CNJ (20 dígitos) de um PDF do hotsite de precatórios. Guarda o resultado em PASTA_PDF/<arquivo>.json:
    cada edição sai com outro nome de arquivo, então o que já foi lido não é baixado de novo."""
    cache = PASTA_PDF / (url.rsplit("/", 1)[-1] + ".json")
    if cache.exists():
        return set(json.loads(cache.read_text(encoding="utf-8")))
    r = requests.get(url, headers={"User-Agent": UA}, timeout=180)
    r.raise_for_status()
    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        texto = " ".join(p.extract_text() or "" for p in pdf.pages)
    # qualquer CNJ: a lista do Estado também traz precatório de outro tribunal pago por ele (TRT22, TJTO...)
    numeros = sorted({so_digitos(c) for c in CNJ.findall(texto)})
    PASTA_PDF.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(numeros), encoding="utf-8")
    return set(numeros)


def pagamentos_tjma():
    """Do hotsite do TJMA: a lista cronológica vigente do Estado (só pendentes) e os PDFs de 'Precatórios Pagos ou em
    Processo de Pagamento'. {vigente, data_lista, pagos: {numero20: [arquivos]}}; None se o hotsite não responder
    (aí a fila anda sem a ordem por pagamento)."""
    try:
        html = requests.get(HOTSITE, headers={"User-Agent": UA}, timeout=60).text
        urls = sorted(set(re.findall(r'href="(https://novogerenciador\.tjma\.jus\.br/[^"]+\.pdf)"', html)))
        listas = [(date(int(m[3]), int(m[2]), int(m[1])), u) for u in urls if (m := RE_PDF_LISTA.search(u))]
        if not listas:
            raise ValueError("lista cronológica do Estado não achada no hotsite")
        data_lista, url_lista = max(listas)
        inicio = time.time()
        vigente = numeros_do_pdf(url_lista)
        pagos = {}
        for u in (u for u in urls if RE_PDF_PAGOS.search(u.rsplit("/", 1)[-1])):
            for n in numeros_do_pdf(u):
                pagos.setdefault(n, []).append(u.rsplit("/", 1)[-1][:90])
    except Exception as e:
        log.warning(f"situação de pagamento indisponível ({e.__class__.__name__}: {str(e)[:150]}): "
                    "a fila anda sem a ordem por pagamento")
        return None
    log.info(f"pagamento: lista vigente de {data_lista:%d/%m/%Y} com {len(vigente)} precatórios; {len(pagos)} "
             f"precatórios em PDFs de pagos ({time.time() - inicio:.0f}s)")
    return {"vigente": vigente, "data_lista": data_lista, "pagos": pagos}


SQL_ESCOPO = """
SELECT cc.credito_id, left(c.numero_norm, 20) AS numero20, cc.prioridade, cc.valor_referencia,
       coalesce((SELECT li.metadata->>'entidade_valor' FROM creditos.lista_item li
                  WHERE li.credito_id = c.id AND li.removido_em IS NULL LIMIT 1), '') AS grupo,
       EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2) AS tem_credor
  FROM creditos.coleta_credor cc JOIN creditos.credito c ON c.id = cc.credito_id
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2 AND {filtro}
"""


def situacao_pagamento(linha, pag):
    """Faixa da fila (1 não pago, 2 sem informação, 3 pago em parte) ou FORA_DA_LISTA, e os PDFs de pagos do crédito.
    Só o grupo do Estado tem lista e pagos publicados no hotsite; os municípios ficam sem informação."""
    if not pag:
        return 1, []
    arquivos = pag["pagos"].get(linha["numero20"], [])
    if linha["grupo"] != "estado":
        return (3 if arquivos else 2), arquivos
    if linha["numero20"] not in pag["vigente"]:
        return FORA_DA_LISTA, arquivos
    return (3 if arquivos else 1), arquivos


def ordenar_escopo(con, pag, filtro):
    """(ids na ordem do robô, {credito_id: {faixa, situacao, arquivos}}, contagem por situação).
    Ordem: faixa, prioridade de campanha, sem credor antes de com credor, maior valor, id."""
    with con.cursor() as cur:
        cur.execute(SQL_ESCOPO.format(filtro=filtro), (TRIBUNAL_TJMA,))
        linhas = como_dicts(cur)
    info, contagem = {}, Counter()
    for x in linhas:
        faixa, arquivos = situacao_pagamento(x, pag)
        situacao = FAIXAS.get(faixa, FORA_DA_LISTA)
        contagem[situacao] += 1
        info[x["credito_id"]] = {"faixa": faixa, "situacao": situacao, "arquivos": arquivos[:5], "linha": x}
    ordem = sorted((i for i, v in info.items() if v["faixa"] != FORA_DA_LISTA),
                   key=lambda i: (info[i]["faixa"], info[i]["linha"]["prioridade"], info[i]["linha"]["tem_credor"],
                                  -float(info[i]["linha"]["valor_referencia"] or 0), i))
    return ordem, info, contagem

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJMA", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, left(c.numero_norm, 20) AS precatorio20,
       tc.codigo AS tipo_credito, li.valor_lista, li.metadata->>'valor_devido' AS valor_devido,
       coalesce(li.metadata->>'entidade_nome', '') AS ente_lista, coalesce(li.metadata->>'Ente', '') AS ente_sigla,
       coalesce(li.metadata->>'entidade_valor', '') AS ente_grupo, coalesce(e.nome, '') AS ente_nome,
       coalesce(li.metadata->>'Recebimento', '') AS recebimento, c.data_apresentacao,
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
    """O crédito (lead) com ente, valores, data de recebimento, originários já ligados e os motivos do RPA."""
    cur.execute(SQL_CREDITO, (credito_id,))
    lead = como_dicts(cur)[0]
    lead["valores"] = [v for v in (valor_numerico(lead["valor_lista"]), valor_numerico(lead["valor_devido"])) if v]
    lead["chaves"] = chaves_ente(lead["ente_lista"]) | chaves_ente(lead["ente_nome"]) | \
        ({normal(lead["ente_sigla"])} if len(normal(lead["ente_sigla"])) >= 4 else set())
    lead["estadual"] = lead["ente_grupo"] == "estado" or "ESTADO DO MARANHAO" in lead["chaves"]
    lead["autuado_ate"] = data_br(lead["recebimento"]) or lead["data_apresentacao"]
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


def confirmar(partes, alvo, lead, iniciais_credor=(), unico_autor=False):
    """(credor, ente_ok, como): a parte do polo ativo que é o credor e se o ente está no polo passivo.
    O credor é achado pelo nome (a com documento válido primeiro); sem nome, pelas iniciais que o DJEN publicou
    (só se uma pessoa bater); e, só quando permitido (originário já ligado ao crédito, sem nome nem iniciais),
    quando o polo ativo tem um autor só. como = NOME | INICIAIS | UNICO_AUTOR."""
    ente_ok = not lead["chaves"] or any(ente_no_nome(p["nome"], lead["chaves"], lead["estadual"])
                                        for p in partes if p["polo"] == "PASSIVO")
    ativos = [p for p in partes if p["polo"] == "ATIVO" and not RE_ORGAO_PUBLICO.search(normal(p["nome"]))
              and not ente_no_nome(p["nome"], lead["chaves"], lead["estadual"])]
    por_nome = sorted((p for p in ativos if chave_nome(p["nome"]) in alvo),
                      key=lambda p: not documento_valido(p["documento"]))
    if por_nome:
        return por_nome[0], ente_ok, "NOME"
    if iniciais_credor:
        por_iniciais = {chave_nome(p["nome"]): p for p in ativos if iniciais(p["nome"]) in iniciais_credor}
        if len(por_iniciais) == 1:
            return next(iter(por_iniciais.values())), ente_ok, "INICIAIS"
    if unico_autor:
        autores = {chave_nome(p["nome"]): p for p in ativos}
        if len(autores) == 1:
            return next(iter(autores.values())), ente_ok, "UNICO_AUTOR"
    return None, ente_ok, ""

# =============================================================================== decisão (só lê: banco, DJEN, PJe)


def resultado_vazio():
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "NOME", "originario": None, "regra": "", "fontes": "",
            "credor": None, "capas": [], "candidatos": [], "capa_escolhida": None, "partes_escolhido": [],
            "credor_djen": "", "publicacoes": 0, "oabs_precatorio": "", "cessao": False, "caminho": ""}


def processar(cur, lead, djen, pje, inicio):
    """Acha o credor e o originário. Com originário conhecido (ligado no banco, citado no DJEN ou pista do RPA), abre
    ele direto pelo número; só se nenhum confirmar é que pesquisa o nome do credor no PJe. Devolve o resultado a gravar
    (status ADIAR = volta para a fila)."""
    r = resultado_vazio()
    prec20 = lead["precatorio20"]
    if prec20[13:16] != "810":
        # precatório de outro tribunal (TRT, TJ de outro estado) na lista do Maranhão: nem o DJEN do TJMA nem o PJe
        # do TJMA o têm
        r["motivo"] = f"CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO: {formatar_cnj(prec20)} não é do TJMA"
        return r

    def tempo():
        if time.time() - inicio > TETO_CREDITO:
            raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")

    # 1. DJEN do precatório: quem é o credor (nome inteiro, ou só as iniciais), OABs e CNJs citados
    info = credor_do_djen(djen.do_precatorio(prec20), lead["chaves"], lead["estadual"])
    r.update(publicacoes=info["publicacoes"], oabs_precatorio=",".join(sorted(info["oabs"])), cessao=info["cessao"],
             credor_djen=" | ".join(info["nomes"]))
    nomes = []
    for c in info["nomes"][:2]:
        nomes += [n for n in nomes_para_buscar(c) if chave_nome(n) not in {chave_nome(x) for x in nomes}]
    alvo = {chave_nome(n) for n in nomes}
    # sem nome, as iniciais do rótulo 'CREDOR: M. J. D. P.' ainda acham o credor dentro de um originário conhecido
    iniciais_credor = set() if nomes else {iniciais(v) for v in info["rotulos"] if eh_iniciais(v)}
    if nomes and all(RE_ORGAO_PUBLICO.search(normal(n)) for n in nomes):
        r["motivo"] = f"REQTE_ORGAO_PUBLICO: {nomes[0]}"
        return r
    if nomes and all(RE_SOCIEDADE_ADV.search(normal(n)) for n in nomes):
        r["motivo"] = f"SEM_CPF_CREDOR: credor é sociedade de advogados (honorários): {nomes[0]}"
        return r

    cands, abertos = {}, [0]

    def somar(n, fonte, forte=False, linha=None, classe=""):
        """Junta um candidato (de qualquer fonte) em cands, somando fontes e evidência forte."""
        n = so_digitos(n)
        if not cnj_1g_tjma(n) or n == prec20:
            return
        c = cands.setdefault(n, {"fontes": [], "valor": False, "forte": False, "oabs": set(), "classe": "",
                                 "linha": None, "primeiro": False})
        if fonte not in c["fontes"]:
            c["fontes"].append(fonte)
        c["forte"] |= forte
        c["classe"] = c["classe"] or classe or (linha or {}).get("classe", "")
        if linha:
            c["linha"] = c["linha"] or linha
            c["primeiro"] |= chave_nome(RE_E_OUTROS.sub("", linha["ativo"])) in alvo

    def regra_forte(n):
        """Nome da evidência forte do candidato: DJEN_CITA, VALOR, LIGADO ou RPA_OK."""
        f = cands[n]["fontes"]
        return "DJEN_CITA" if "DJEN_CITA" in f else "VALOR" if cands[n]["valor"] else \
            "LIGADO" if "LIGADO" in f else "RPA_OK"

    def abrir(n):
        """Confirma um candidato: pelas partes que o banco já tem ou abrindo o detalhe no PJe (até MAX_ABERTOS_PJE)."""
        tempo()
        c = cands[n]
        c["aberto"] = True
        unico = not nomes and not iniciais_credor and "LIGADO" in c["fontes"]
        existentes = partes_do_banco(cur, formatar_cnj(n))
        partes_bd = [{"nome": e["nome"], "documento": e["documento"] or "", "polo": e["polo"]}
                     for e in existentes if e["papel"] != "ADVOGADO"]
        credor, ente_ok, como = confirmar(partes_bd, alvo, lead, iniciais_credor, unico)
        if credor and ente_ok and documento_valido(credor["documento"]):
            c.update(confirmado=True, capa="BANCO", partes=partes_bd, como=como,
                     credor={"nome": credor["nome"], "documento": so_digitos(credor["documento"])})
            c["oabs"] |= {f"{e['oab_numero']}{e['oab_uf']}" for e in existentes
                          if e["papel"] == "ADVOGADO" and e["polo"] == "ATIVO" and e["oab_numero"]}
            return
        if abertos[0] >= MAX_ABERTOS_PJE:
            c["pje"] = "NAO_ABERTO"
            return
        abertos[0] += 1
        linha = c["linha"] or pje.por_numero(n)
        if not linha:
            c["pje"] = "NAO_ENCONTRADO"
            return
        dados = pje.detalhe(linha["link"], n, alvo, prazo=inicio + TETO_CREDITO - 15)
        time.sleep(PAUSA_PJE)
        c["pje"] = "OK"
        c["classe"] = c["classe"] or (dados["capa"].get("classe_judicial") or "").upper()
        c["oabs"] |= {a["oab_numero"] + a["oab_uf"] for a in dados["advogados"]
                      if a["polo"] == "ATIVO" and a["oab_numero"]}
        credor, ente_ok, como = confirmar(dados["partes"], alvo, lead, iniciais_credor, unico)
        if credor and ente_ok:
            doc = so_digitos(credor["documento"])
            c.update(confirmado=True, capa="PJE", dados=dados, partes=dados["partes"], como=como,
                     credor={"nome": credor["nome"], "documento": doc if documento_valido(doc) else None})

    def ordenados():
        """Forte, ligado, credor como 1º autor, classe de execução e os mais novos primeiro."""
        return sorted(cands, key=lambda n: (not cands[n]["forte"], "LIGADO" not in cands[n]["fontes"],
                                            not cands[n]["primeiro"], not CLASSE_EXECUCAO.search(cands[n]["classe"]),
                                            -int(n[9:13])))

    # 2. originário conhecido: abre direto pelo número, sem pesquisar o nome
    for n in lead["ligados"]:
        somar(n, "LIGADO", forte=True)
    for n, tipo in pistas_do_rpa(cur, lead["motivos"]).items():
        somar(n, "RPA_OK" if tipo == "OK" else "RPA", forte=tipo == "OK")
    for n in info["citados"]:
        if int(n[9:13]) <= int(prec20[9:13]):
            somar(n, "DJEN_CITA", forte=True)
    conhecidos = ordenados()
    for n in conhecidos:
        abrir(n)
    r["caminho"] = "ORIGINARIO_CONHECIDO" if conhecidos else ""

    # 3. nenhum originário conhecido confirmou: método pelo nome do credor
    if not any(cands[n].get("confirmado") for n in conhecidos):
        if not info["publicacoes"]:
            r.update(status="ADIAR", motivo="AINDA_SEM_PUBLICACAO: precatório sem publicação no DJEN"
                     + (f" (originário conhecido não confirmou: {len(conhecidos)})" if conhecidos else ""))
            return r
        if not nomes:
            r["motivo"] = (f"SEM_BENEFICIARIO: credor não identificado nas {info['publicacoes']} publicações do DJEN "
                           f"(rótulos: {', '.join(info['rotulos'])[:200] or 'nenhum'})"
                           + (f"; {len(conhecidos)} originário(s) conhecido(s) sem o credor" if conhecidos else ""))
            return r
        r["caminho"] = "ORIGINARIO_CONHECIDO+NOME" if conhecidos else "NOME"
        saturou_tudo = False
        for nome in nomes:
            tempo()
            linhas, saturou = pje.por_nome(nome)
            if saturou and lead["autuado_ate"]:
                linhas, saturou = pje.por_nome(nome, lead["autuado_ate"])
            saturou_tudo |= saturou
            for x in linhas:
                if x["numero"] and cnj_1g_tjma(x["numero"]) and int(x["numero"][9:13]) <= int(prec20[9:13]) \
                        and not RE_CLASSE_FORA.search(normal(x["classe"])) \
                        and (ente_no_nome(RE_E_OUTROS.sub("", x["passivo"]), lead["chaves"], lead["estadual"])
                             or RE_E_OUTROS.search(x["passivo"])):
                    somar(x["numero"], "PJE_NOME", linha=x)
        if saturou_tudo or not cands:
            tempo()
            for n, classe in candidatos_djen_nome(djen, prec20, nomes, alvo, lead["chaves"], lead["estadual"]).items():
                somar(n, "DJEN_NOME", classe=classe)
        if not cands:
            r["motivo"] = f"PROCESSO_NAO_ENCONTRADO: nenhum candidato no PJe nem no DJEN para {nomes[0]}"
            return r
        ordem = ordenados()
        com_forte = [n for n in ordem if cands[n]["forte"]]
        decisivo = ordem[0] if len(cands) == 1 else com_forte[0] if len(com_forte) == 1 else None
        for n in ordem:
            if not cands[n].get("aberto"):
                abrir(n)
            if cands[n].get("confirmado") and n == decisivo:   # o desempate já aponta este: não abre os outros
                break
    ordem = ordenados()
    com_forte = [n for n in ordem if cands[n]["forte"]]
    decisivo = ordem[0] if len(cands) == 1 else com_forte[0] if len(com_forte) == 1 else None

    # 4. decide
    conf = [n for n in ordem if cands[n].get("confirmado")]
    escolhido, regra = None, ""
    if len(conf) == 1:
        escolhido, regra = conf[0], regra_forte(conf[0]) if cands[conf[0]]["forte"] else "UNICO"
    elif len(conf) > 1:
        conf_forte = [n for n in conf if cands[n]["forte"]]
        conf_oab = [n for n in (conf_forte or conf) if cands[n]["oabs"] & info["oabs"]]
        if len(conf_forte) == 1:
            escolhido, regra = conf_forte[0], regra_forte(conf_forte[0])
        elif len(conf_oab) == 1:
            escolhido, regra = conf_oab[0], "OAB"
        else:
            # último desempate: o DJEN do candidato cita o nº do precatório ou o valor dele
            alvos_texto = {prec20, formatar_cnj(prec20)} | (set().union(*(formatos_valor(v) for v in lead["valores"]))
                                                            if lead["valores"] else set())
            for n in (conf_oab or conf_forte or conf)[:MAX_VALOR_DJEN]:
                tempo()
                texto = " ".join(x["texto"] for x in djen.por_numero(n))
                if any(a in texto for a in alvos_texto):
                    cands[n].update(valor=True, forte=True)
            conf_valor = [n for n in conf if cands[n]["valor"]]
            if len(conf_valor) == 1:
                escolhido, regra = conf_valor[0], "VALOR"

    r["candidatos"] = [{"cnj": formatar_cnj(n), "fontes": cands[n]["fontes"], "valor": cands[n]["valor"],
                        "forte": cands[n]["forte"], "confirmado": bool(cands[n].get("confirmado")),
                        "capa": cands[n].get("capa"), "pje": cands[n].get("pje"), "classe": cands[n]["classe"],
                        "credor_por": cands[n].get("como"),
                        "credor_doc": (cands[n].get("credor") or {}).get("documento")}
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
                     motivo=f"cnj={formatar_cnj(escolhido)} regra={regra} fontes={r['fontes']} credor_por={c['como']} "
                            f"partes={len(c['partes'])} capa={c['capa']}")
        else:
            r["motivo"] = f"SEM_CPF_CREDOR: cnj={formatar_cnj(escolhido)} regra={regra} (credor sem CPF no PJe)"
    elif conf:
        docs = {cands[n]["credor"]["documento"] for n in conf}
        lista = ",".join(formatar_cnj(n) for n in conf)
        if len(docs) == 1 and None not in docs:
            # o originário é ambíguo, mas o credor não: mesmo nome e mesmo CPF em todos os candidatos
            r.update(status="SUCESSO_ANALISAR", capas=capas_pje(conf), credor=cands[conf[0]]["credor"],
                     fontes=",".join(sorted({f for n in conf for f in cands[n]["fontes"]})),
                     partes_escolhido=cands[conf[0]]["partes"],
                     motivo=f"CREDOR_CPF_UNANIME_SEM_VINCULO:{lista}")
        else:
            r.update(status="SUCESSO_ANALISAR", capas=capas_pje(conf),
                     motivo=("CREDOR_COM_DOCUMENTO_SEM_VINCULO:" if docs - {None} else "CREDOR_POLO_ATIVO_SEM_VINCULO:")
                     + lista)
    elif decisivo and cands[decisivo].get("pje") == "NAO_ENCONTRADO" and cands[decisivo]["forte"]:
        # 1 candidato claro, mas o PJe público não abre o processo (físico ou em segredo): liga sem capa
        r.update(originario=decisivo, regra=regra_forte(decisivo), via="ORIGINARIO",
                 fontes=",".join(cands[decisivo]["fontes"]),
                 motivo=f"PROC_SEM_CAPA: cnj={formatar_cnj(decisivo)} regra={regra_forte(decisivo)} "
                        f"(não abre no PJe público)")
    else:
        r["motivo"] = f"PROCESSO_NAO_ENCONTRADO: {len(cands)} candidato(s), nenhum confirmado para " \
                      f"{nomes[0] if nomes else 'o credor'}"
    return r

# =============================================================================== banco: gravação de um crédito


class Backup:
    """Guarda o 'antes' de cada mudança no legado e escreve o SQL que desfaz. Só a 1ª mudança de cada linha
    entra (e linha criada pelo robô só é apagada), então o SQL pode rodar em qualquer ordem.
    Três níveis: o crédito em gravação (p_*), o lote aberto (l_*) e o que já teve COMMIT."""

    def __init__(self, arquivo_sql, arquivo_csv, rodada, modo):
        self.arquivo_sql, self.arquivo_csv, self.rodada, self.modo = arquivo_sql, arquivo_csv, rodada, modo
        self.tocados, self.inseridos = set(), set()
        self.descartar_lote()

    def descartar(self):
        """Esquece o crédito em gravação (ROLLBACK TO SAVEPOINT)."""
        self.p_tocados, self.p_inseridos, self.p_sql, self.p_csv = set(), set(), [], []

    def descartar_lote(self):
        """Esquece o lote inteiro (ROLLBACK)."""
        self.l_tocados, self.l_inseridos, self.l_creditos = set(), set(), []
        self.descartar()

    def _ja_tocada(self, chave):
        """A linha já teve o 'antes' guardado (nesta rodada, no lote ou no crédito)?"""
        return chave in self.tocados | self.l_tocados | self.p_tocados

    def _criada_aqui(self, tabela, id_):
        """A linha foi criada pelo robô (o SQL de desfazer só a apaga)?"""
        return (tabela, id_) in self.inseridos | self.l_inseridos | self.p_inseridos

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

    def fechar_credito(self, credito_id):
        """O crédito gravou sem erro (RELEASE SAVEPOINT): o que ele guardou passa para o lote."""
        self.l_tocados |= self.p_tocados
        self.l_inseridos |= self.p_inseridos
        if self.p_sql:
            self.l_creditos.append((credito_id, self.p_sql, self.p_csv))
        self.descartar()

    def confirmar_lote(self, manter):
        """Depois do COMMIT (ou do ROLLBACK da simulação): escreve o SQL e o CSV de cada crédito do lote;
        manter=False (simulação) não lembra as linhas tocadas."""
        for credito_id, sql, linhas in self.l_creditos:
            novo = not self.arquivo_sql.exists()
            with open(self.arquivo_sql, "a", encoding="utf-8") as f:
                if novo:
                    f.write("-- Desfaz as mudanças do fetch_TJMA.py nas tabelas antigas "
                            "(pode rodar em qualquer ordem).\n")
                f.write(f"-- crédito {credito_id}\nBEGIN;\n" + "\n".join(reversed(sql)) + "\nCOMMIT;\n")
            anexar_csv(self.arquivo_csv, COLUNAS_BACKUP,
                       [{"rodada": self.rodada, "modo": self.modo, "credito_id": credito_id, "op": op, "tabela": t,
                         "chave": json.dumps(ch, default=str), "antes": json.dumps(a, default=str, ensure_ascii=False)}
                        for op, t, ch, a in linhas])
        if manter:
            self.tocados |= self.l_tocados
            self.inseridos |= self.l_inseridos
        self.descartar_lote()


def id_do_software(cur, criar):
    """Id do software do robô; cria na 1ª vez (raspa_credor: o próprio robô busca o credor)."""
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJMA",
                                "Credor do TJMA: DJEN + PJe 1º grau público (TJMA/fetch_TJMA.py)",
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
             "origem": "TJMA", "tribunal_sigla": "TJMA"}
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
              PAPEL_LEGADO[p["polo"]], "TJMA", agora, p["papel"], POLO_BRUTO[p["polo"]]) for p in dados["partes"]]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    advs_antigos = como_dicts(cur)
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJMA", agora,
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
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJMA'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJMA"}
            bk.update(cur, fila, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                             "numero_originario", "ultima_atualizacao")})
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJMA'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Capa sem coluna própria e o resumo da decisão em credito_fonte.metadata (a função troca o JSON inteiro).
    Do DJEN só entram o nome do credor, a contagem de publicações e as OABs (o texto não é guardado)."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "fontes": r["fontes"], "candidatos": r["candidatos"],
                     "djen": {"credor": r["credor_djen"], "publicacoes": r["publicacoes"],
                              "oabs": r["oabs_precatorio"], "cessao": r["cessao"]}}
    if r["capa_escolhida"]:
        meta["capa_originario"] = r["capa_escolhida"]
    if lead.get("pagamento"):
        meta["pagamento"] = lead["pagamento"]           # situação no hotsite do TJMA quando o robô passou
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJMA', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def mesma_pessoa(a, b):
    """Mesmo nome, ou grafia próxima o bastante (SOUSA x SOUZA, acento, 'FILHO' faltando não conta)."""
    a, b = chave_nome(a), chave_nome(b)
    return a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA


def corrigir_credor(cur, lead, credor, bk):
    """A fonte vence o banco: com o CPF do credor confirmado na fonte, apaga os vínculos de CREDOR do crédito que são
    a mesma pessoa (mesmo nome ou grafia próxima) com outro documento, e troca o CPF na capa antiga do precatório
    (senão o espelho do legado recriaria o vínculo errado). Herdeiro, cessionário, sucessor e advogado não são tocados;
    credor com outro nome (outra pessoa) fica. O banco audita cada DELETE em creditos.auditoria_alteracao e o
    desfazer_legado_*.sql guarda o INSERT que o recria. Devolve o que foi corrigido (para o CSV)."""
    cur.execute("""SELECT cc.*, p.nome AS _nome, p.documento::text AS _documento
                     FROM creditos.credito_credor cc JOIN creditos.pessoa p ON p.id = cc.pessoa_id
                    WHERE cc.credito_id = %s AND cc.papel_id = 1
                      AND p.documento IS DISTINCT FROM creditos.documento_normalizar(%s)
                      FOR UPDATE OF cc""", (lead["credito_id"], credor["documento"]))
    corrigidos = []
    for linha in como_dicts(cur):
        nome, doc = linha.pop("_nome"), linha.pop("_documento")
        if not mesma_pessoa(nome, credor["nome"]):
            continue
        bk.delete(cur, "creditos.credito_credor", linha)
        cur.execute("DELETE FROM creditos.credito_credor WHERE id = %s", (linha["id"],))
        corrigidos.append(f"{nome} ({doc or 'sem doc'}, {linha['origem']}) -> {credor['documento']}")
    cur.execute("""SELECT x.id, x.nome, x.cpf_cnpj
                     FROM precatorios.partes_processuais x
                     JOIN precatorios.processos_precatorios pp ON pp.id = x.processo_id
                    WHERE regexp_replace(pp.numero_cnj::text, '\\D', '', 'g') = %s AND x.polo = 'ATIVO'
                      AND x.cpf_cnpj IS DISTINCT FROM %s
                      FOR UPDATE OF x""", (lead["precatorio20"], credor["documento"]))
    for pid, nome, doc in cur.fetchall():
        if mesma_pessoa(nome, credor["nome"]) and so_digitos(doc) != credor["documento"]:
            bk.update(cur, "precatorios.partes_processuais", {"id": pid}, {"cpf_cnpj": doc})
            cur.execute("UPDATE precatorios.partes_processuais SET cpf_cnpj = %s WHERE id = %s",
                        (credor["documento"], pid))
            corrigidos.append(f"capa antiga do precatório: {nome} ({doc or 'sem doc'}) -> {credor['documento']}")
    return corrigidos


def autores(dados, lead):
    """Pessoas distintas do polo ativo que não são o ente (mais de 1 = ação coletiva)."""
    return {chave_nome(p["nome"]) for p in dados["partes"]
            if p["polo"] == "ATIVO" and not ente_no_nome(p["nome"], lead["chaves"], lead["estadual"])}


def recorte_do_credor(dados, credor, oabs_precatorio):
    """Ação coletiva: só o credor confirmado, o polo passivo e os advogados do precatório (OAB no DJEN), com
    completa=False (só acrescenta). Gravar todos os autores faria o recálculo do banco ligar cada um deles ao precatório
    como credor, e mexer nos credores dos outros precatórios do mesmo processo."""
    return {**dados, "completa": False,
            "partes": [p for p in dados["partes"]
                       if p["polo"] == "PASSIVO" or mesma_pessoa(p["nome"], credor["nome"])],
            "advogados": [a for a in dados["advogados"]
                          if a["polo"] == "ATIVO" and a["oab_numero"] + a["oab_uf"] in oabs_precatorio]}


def gravar(cur, lead, r, filas, status_legado, bk):
    """Grava o resultado de um crédito (quem chama cuida do SAVEPOINT e do COMMIT). Devolve o resumo para o CSV."""
    cid = lead["credito_id"]
    id_do_software(cur, criar=True)                     # na simulação ele nasce e morre nesta transação
    antes = credores_do_credito(cur, cid)
    resumo, legado, proc_escolhido, corrigidos = [], Counter(), None, []
    if r["originario"]:
        cur.execute("SELECT creditos.fila_credor_registrar_originario(%s, %s, %s)",
                    (cid, WORKER, [formatar_cnj(r["originario"])]))
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)",
                    (formatar_cnj(r["originario"]),))
        proc_escolhido = cur.fetchone()[0]
    if r["credor"] and r["credor"]["documento"]:
        cur.execute("SELECT creditos.documento_de_parte(%s)", (r["credor"]["documento"],))
        # antes do registrar_capa: o vínculo fica com origem FONTE, que o recálculo das partes não apaga
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, r["credor"]["nome"], r["credor"]["documento"], proc_escolhido, SOFTWARE))
            corrigidos = corrigir_credor(cur, lead, r["credor"], bk)
    # capa só do originário escolhido e só quando veio do PJe (a do banco não traz nada novo e o registrar_capa
    # recalcularia os credores do processo inteiro); na ação coletiva, só o credor
    coletiva = 0
    for cnj, fonte, dados in r["capas"]:
        if cnj != r["originario"] or fonte != "PJE":
            continue
        coletiva = len(autores(dados, lead))
        if coletiva > 1:
            if not (r["credor"] and r["credor"]["documento"]):
                continue                                # coletiva sem o CPF do credor: não há o que recortar
            dados = recorte_do_credor(dados, r["credor"], set(r["oabs_precatorio"].split(",")) - {""})
        travar_processo(cur, cnj)
        partes, advogados = partes_para_banco(cur, dados, partes_do_banco(cur, formatar_cnj(cnj)), fonte)
        capa = capa_para_banco(dados["capa"])
        cur.execute("SELECT creditos.registrar_capa(%s, %s::jsonb, %s::jsonb, %s::jsonb)",
                    (formatar_cnj(cnj), json.dumps(partes, ensure_ascii=False),
                     json.dumps(advogados, ensure_ascii=False),
                     json.dumps(capa, ensure_ascii=False) if capa else None))
        res = cur.fetchone()[0]
        resumo.append(f"{formatar_cnj(cnj)} ({fonte}): partes +{res['partes_inseridas']}/-{res['partes_removidas']} "
                      f"credores +{res['credores_inseridos']}/-{res['credores_removidos']}")
        legado += gravar_capa_legado(cur, cnj, dados, r["status"] == "SUCESSO_PROCESSO_ORIGINARIO",
                                     lead["precatorio"], bk)
    registrar_metadata(cur, lead, r)
    legado += atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "fontes": r["fontes"], "candidatos": r["candidatos"],
               "capa": r["capa_escolhida"], "credor_djen": r["credor_djen"],
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
            "antes": antes, "depois": depois, "corrigidos": corrigidos, "coletiva": coletiva}

# =============================================================================== banco: gravação em lote


def marcar_falha(cur, credito_id, erro):
    """FALHA na fila para o crédito que não gravou, dentro da transação do lote (num SAVEPOINT próprio: se até isso
    falhar, o lote segue)."""
    cur.execute("SAVEPOINT falha")
    try:
        cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s,
                          p_status => 'FALHA', p_motivo => %s, p_sistema => 'PJE', p_host => %s)""",
                    (credito_id, WORKER, erro, socket.gethostname()))
        cur.execute("RELEASE SAVEPOINT falha")
    except psycopg2.Error:
        cur.execute("ROLLBACK TO SAVEPOINT falha")


def gravar_credito_no_lote(cur, rod, item):
    """Grava um crédito dentro da transação do lote, no seu SAVEPOINT. Erro desfaz só este crédito: lease perdido
    vira LEASE_PERDIDO; outro erro vira FALHA (e FALHA na fila, fora da simulação). Devolve True se gravou."""
    lead, r = item["lead"], item["r"]
    cur.execute("SAVEPOINT credito")
    try:
        item["gravado"] = gravar(cur, lead, r, rod.filas, rod.status_legado, rod.bk)
        cur.execute("RELEASE SAVEPOINT credito")
        rod.bk.fechar_credito(lead["credito_id"])
        return True
    except psycopg2.errors.LockNotAvailable:
        cur.execute("ROLLBACK TO SAVEPOINT credito")
        rod.bk.descartar()
        item["r"] = {**r, "status": "LEASE_PERDIDO", "motivo": "outro worker reservou o crédito"}
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT credito")
        rod.bk.descartar()
        erro = f"PERSISTENCIA_CAPA: {(str(e).splitlines() or [''])[0][:300]}"
        log.warning(f"{lead['credito_id']} não gravou: {erro}")
        if not rod.simulacao:
            marcar_falha(cur, lead["credito_id"], erro)
        item["r"] = {**r, "status": "FALHA", "motivo": erro}
    item["gravado"] = SEM_GRAVACAO
    return False


def gravar_lote(rod, lote):
    """Grava os créditos do lote numa transação só: COMMIT no fim (ROLLBACK na simulação) e, depois, os arquivos de
    desfazer. Se a transação cair no meio (conexão, Ctrl+C), desfaz tudo e repassa o erro. Devolve quantos gravaram."""
    rod.n_lote += 1
    try:
        with rod.con.cursor() as cur:
            gravados = sum(gravar_credito_no_lote(cur, rod, item) for item in lote)
        if rod.simulacao:
            rod.con.rollback()
        else:
            rod.con.commit()
    except BaseException:
        try:
            rod.con.rollback()
        except Exception:
            pass
        rod.bk.descartar_lote()
        raise
    rod.bk.confirmar_lote(manter=not rod.simulacao)
    return gravados


def registrar_lote(rod, lote, gravados):
    """Depois do lote gravado: completa e escreve as linhas do CSV, os credores trocados e o resumo na tela.
    Se nenhum crédito do lote gravou, o banco está com problema: marca para parar."""
    trocas = []
    for item in lote:
        linha, lead, r, g = item["linha"], item["lead"], item["r"], item["gravado"]
        saiu, entrou = g["antes"] - g["depois"], g["depois"] - g["antes"]
        if saiu or entrou:
            trocas.append({"processado_em": linha["processado_em"], "modo": rod.modo, "credito_id": lead["credito_id"],
                           "precatorio": lead["precatorio"], "credores_antes": fmt_credores(g["antes"]),
                           "credores_depois": fmt_credores(g["depois"]), "saiu": fmt_credores(saiu),
                           "entrou": fmt_credores(entrou)})
        linha.update(resultado=r["status"], motivo=r["motivo"],
                     originario=formatar_cnj(r["originario"]) if r["originario"] else "", regra=r["regra"],
                     fontes=r["fontes"], credor=(r["credor"] or {}).get("nome", ""),
                     credor_documento=(r["credor"] or {}).get("documento", ""),
                     candidatos=" | ".join(f"{c['cnj']}[{','.join(c['fontes'])}{' forte' if c['forte'] else ''}"
                                           f"{' ok' if c['confirmado'] else ''}{' ' + c['capa'] if c['capa'] else ''}]"
                                           for c in r["candidatos"]),
                     capa_fonte=";".join(f for _, f, _ in r["capas"]), banco=g["banco"], legado=g["legado"],
                     credor_corrigido=" | ".join(g["corrigidos"]), coletiva=g["coletiva"] or "",
                     credores_antes=fmt_credores(g["antes"]), credores_depois=fmt_credores(g["depois"]),
                     lote=rod.n_lote)
        rod.resultados[r["status"]] += 1
    anexar_csv(rod.arq_credito, COLUNAS, [item["linha"] for item in lote])
    if trocas:
        anexar_csv(rod.arq_trocas, COLUNAS_TROCAS, trocas)
    status = Counter(item["r"]["status"] for item in lote)
    log.info(f"== lote {rod.n_lote}: {gravados}/{len(lote)} gravado(s) "
             f"({'ROLLBACK, simulação' if rod.simulacao else 'COMMIT'}) | "
             + ", ".join(f"{k}: {v}" for k, v in status.most_common()))
    if lote and not gravados:
        log.error("nenhum crédito do lote gravou: robô parado (banco com problema?).")
        rod.parar.set()


def descarregar(rod, pendentes):
    """Grava o lote pendente, registra nos CSVs e esvazia a lista."""
    gravados = gravar_lote(rod, pendentes)
    registrar_lote(rod, pendentes, gravados)
    pendentes.clear()

# =============================================================================== fila


# o que o robô pode pegar: do RPA em PENDENTE, do RPA sem credor em qualquer status (menos EM_ANDAMENTO) e o que já é
# do robô em PENDENTE; sempre vencido o disponivel_em. O que já é do robô com status final (SUCESSO/FALHA) não volta.
FILTRO_PEGAVEL = f"""cc.disponivel_em <= now() AND (
       (cc.software_id = {SOFTWARE_RPA}
        AND (cc.status_id = 1 OR NOT EXISTS (SELECT 1 FROM creditos.credito_credor k
                                              WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)))
    OR (cc.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}') AND cc.status_id = 1))"""


def reservar(con, credito_id, lease, id_software):
    """Reserva UM crédito (lease para este worker) se ele ainda é pegável, passando-o para o software do robô nesse
    momento (a fila do RPA só muda lead a lead, no que o robô de fato pega). UPDATE atômico: se outra instância já o
    pegou, não afeta nenhuma linha. Devolve (software, status, disponivel_em) de antes, ou None."""
    with con.cursor() as cur:
        cur.execute(f"""WITH alvo AS (
                          SELECT cc.credito_id, cc.software_id, cc.status_id, cc.disponivel_em
                            FROM creditos.coleta_credor cc
                            JOIN creditos.credito c ON c.id = cc.credito_id
                           WHERE cc.credito_id = %s AND cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL
                             AND cc.status_id <> 2 AND {FILTRO_PEGAVEL}
                             FOR UPDATE OF cc SKIP LOCKED)
                        UPDATE creditos.coleta_credor cc
                           SET status_id = 2, software_id = %s, lease_worker = %s,
                               lease_ate = now() + %s::interval, reservado_em = now(), updated_at = now()
                          FROM alvo
                         WHERE cc.credito_id = alvo.credito_id
                        RETURNING alvo.software_id, alvo.status_id, alvo.disponivel_em""",
                    (credito_id, TRIBUNAL_TJMA, id_software, WORKER, lease))
        antes = cur.fetchone()
    con.commit()
    return antes


def anotar_desfazer_fila(rod, credito_id, antes):
    """Lead que era do RPA: guarda o UPDATE que o devolve ao RPA com o status de antes (desfazer_fila_<rodada>.sql)."""
    software, status, disponivel = antes
    if software != SOFTWARE_RPA:
        return
    arquivo = SAIDA / f"desfazer_fila_{rod.rodada}.sql"
    novo = not arquivo.exists()
    with rod.con.cursor() as cur, open(arquivo, "a", encoding="utf-8") as f:
        if novo:
            f.write("-- Devolve ao RPA (software 2), com o status de antes, os leads que o robô pegou.\n")
        f.write(cur.mogrify(f"UPDATE creditos.coleta_credor SET software_id = {SOFTWARE_RPA}, status_id = %s, "
                            f"disponivel_em = %s::timestamptz, updated_at = now() WHERE credito_id = %s "
                            f"AND software_id = {rod.id_software} AND status_id <> 2;\n",
                            (status, disponivel, credito_id)).decode())


def pegar(rod):
    """Próximo crédito na ordem do robô (não pagos primeiro) que consegue reservar. Acabou a lista: refaz a ordem uma
    vez (entram os adiados que venceram); vazia de novo, a fila acabou (None)."""
    for tentativa in (1, 2):
        while rod.posicao < len(rod.ordem):
            credito_id = rod.ordem[rod.posicao]
            rod.posicao += 1
            antes = reservar(rod.con, credito_id, rod.lease, rod.id_software)
            if antes:
                anotar_desfazer_fila(rod, credito_id, antes)
                return credito_id
        if tentativa == 1:
            rod.reordenar()
    return None


def devolver(con, credito_id, motivo=None, intervalo=ADIAMENTO):
    """Volta para a fila daqui a `intervalo` (erro passageiro, sem publicação); sem motivo (Ctrl+C), volta já."""
    with con.cursor() as cur:
        if motivo:
            cur.execute("SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
                        (credito_id, WORKER, intervalo, motivo[:2000]))
        else:
            cur.execute("SELECT creditos.fila_credor_liberar(%s, %s)", (credito_id, WORKER))
    con.commit()


# =============================================================================== execução


class Rodada:
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, DJEN, PJe por thread e contadores.
    Ao nascer prepara o banco (software, filas antigas, status do legado) e a ordem da fila (na simulação, a amostra
    sai dela e nada é reservado)."""

    def __init__(self, simulacao, limite, workers, usar_proxies, creditos=()):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers = simulacao, limite, workers
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJMA{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = Backup(SAIDA / f"desfazer_legado_{self.rodada}{sufixo}.sql",
                         SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo)
        self.resultados, self.falhas_seguidas, self.n, self.n_lote = Counter(), 0, 0, 0
        self.parar, self.fila_vazia = threading.Event(), False
        # o 1º crédito do lote espera os outros LOTE-1, que andam `workers` por vez
        self.lease = f"{(LOTE // workers + 2) * TETO_CREDITO // 60 + 30} minutes"
        self.local, self.conexoes, self.trava = threading.local(), [], threading.Lock()

        self.con = conectar(escrita=True)
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        self.djen = Djen(self.parar, usar_proxies)
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | lease {self.lease} | DJEN: "
                 f"{', '.join(s.nome for s in self.djen.saidas)} | filas antigas: {', '.join(self.filas)}")
        self.pagamentos, self.info = pagamentos_tjma(), {}
        if simulacao:
            ordem = self.ordenar()
            self.fila_simulada = list(creditos) or ordem[:limite or AMOSTRA_SIMULACAO]
            log.info(f"amostra: {len(self.fila_simulada)} créditos "
                     f"({'os informados em --creditos' if creditos else 'os primeiros da ordem do robô'}; "
                     f"nada é reservado; cada lote é desfeito no fim)")
        else:
            with self.con.cursor() as cur:
                cur.execute("SELECT creditos.fila_credor_expirar_leases()")
            self.con.commit()
            self.reordenar()

    def ordenar(self):
        """Ordem do robô (não pagos primeiro) e a situação de pagamento de cada crédito pegável."""
        con_l = conectar()
        try:
            ordem, info, contagem = ordenar_escopo(con_l, self.pagamentos, FILTRO_PEGAVEL)
        finally:
            con_l.close()
        self.info = info
        log.info(f"ordem da fila: {len(ordem)} créditos | " + ", ".join(
            f"{s}: {contagem[s]}" for s in [*FAIXAS.values(), FORA_DA_LISTA]) + " (fora da lista não é pego)")
        return ordem

    def reordenar(self):
        """Remonta a ordem da fila e volta ao começo dela (repete a cada REORDENAR_A_CADA)."""
        self.ordem, self.posicao, self.ultima_ordem = self.ordenar(), 0, time.time()

    def da_thread(self):
        """(conexão de leitura, PJe) da thread que chama; criados na 1ª vez."""
        if not getattr(self.local, "con", None):
            self.local.con, self.local.pje = conectar(), Pje(self.parar)
            with self.trava:
                self.conexoes.append(self.local.con)
        return self.local.con, self.local.pje

    def fechar(self):
        """Fecha as conexões (escrita e as de leitura das threads)."""
        for con in [self.con] + self.conexoes:
            try:
                con.close()
            except Exception:
                pass


def proximo_credito(rod):
    """Id do próximo crédito, ou None (amostra acabou, fila vazia, --limite, parada por falhas).
    Fora da simulação reserva o crédito com lease e, a cada REORDENAR_A_CADA, remonta a ordem da fila."""
    if rod.parar.is_set() or rod.fila_vazia or (rod.limite and rod.n >= rod.limite):
        return None
    if rod.simulacao:
        credito_id = rod.fila_simulada.pop(0) if rod.fila_simulada else None
    else:
        if time.time() - rod.ultima_ordem > REORDENAR_A_CADA:
            rod.reordenar()
        credito_id = pegar(rod)
    if credito_id:
        rod.n += 1
    else:
        rod.fila_vazia = True
        log.info("fila do TJMA vazia." if not rod.simulacao else "amostra acabou.")
    return credito_id


def processar_credito(rod, credito_id, n):
    """(Numa thread) Lê o crédito e decide credor e originário (DJEN + PJe), sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r} ou {tipo: ADIAR, credito_id, motivo, intervalo, linha, tecnico}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    try:
        con, pje = rod.da_thread()
        with con.cursor() as cur:
            lead = ler_credito(cur, credito_id)
            pag = rod.info.get(credito_id) or {}
            lead["pagamento"] = {"situacao": pag.get("situacao"), "pdfs_pagos": pag.get("arquivos") or [],
                                 "lista_vigente": rod.pagamentos and f"{rod.pagamentos['data_lista']:%d/%m/%Y}"}
            linha.update(precatorio=lead["precatorio"], ente=lead["ente_nome"] or lead["ente_lista"],
                         ultimo_status=lead["ultimo_status"], pagamento=pag.get("situacao") or "")
            r = processar(cur, lead, rod.djen, pje, inicio)
    except Exception as e:                              # erro passageiro (ou inesperado): volta para a fila
        tecnico = isinstance(e, ErroTecnico)
        motivo = str(e) if tecnico else \
            f"ERRO_DESCONHECIDO: {e.__class__.__name__}: {(str(e).splitlines() or [''])[0][:200]}"
        if not tecnico:
            log.warning(f"[{n}] {credito_id}: erro inesperado", exc_info=True)
        try:
            rod.local.pje.nova_sessao()
        except Exception:
            pass
        linha.update(resultado="ADIADO", motivo=motivo, segundos=round(time.time() - inicio))
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": motivo, "intervalo": ADIAMENTO, "linha": linha,
                "tecnico": True}
    linha["segundos"] = round(time.time() - inicio)
    linha.update(caminho=r["caminho"], credor_djen=r["credor_djen"], publicacoes_djen=r["publicacoes"], oabs_precatorio=r["oabs_precatorio"],
                 cessao="sim" if r["cessao"] else "")
    log.info(f"[{n}] {credito_id} {lead['precatorio']} -> {r['status']} "
             f"{formatar_cnj(r['originario']) if r['originario'] else ''} "
             f"{('CPF ' + r['credor']['documento']) if r['credor'] and r['credor']['documento'] else ''} "
             f"| {r['credor_djen'][:60]} | {linha['segundos']} s")
    if r["status"] == "ADIAR":
        linha.update(resultado="ADIADO_SEM_PUBLICACAO", motivo=r["motivo"])
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": r["motivo"],
                "intervalo": ADIAMENTO_SEM_PUBLICACAO, "linha": linha, "tecnico": False}
    return {"tipo": "LOTE", "linha": linha, "lead": lead, "r": r}


def tratar(rod, item, pendentes):
    """(Na thread principal) Resultado de um crédito: vai para o lote ou volta para a fila."""
    if item["tipo"] == "LOTE":
        rod.falhas_seguidas = 0
        pendentes.append(item)
        return
    if not rod.simulacao:
        devolver(rod.con, item["credito_id"], item["motivo"], item["intervalo"])
    anexar_csv(rod.arq_credito, COLUNAS, [item["linha"]])
    rod.resultados[item["linha"]["resultado"]] += 1
    if not item["tecnico"]:
        rod.falhas_seguidas = 0
        return
    rod.falhas_seguidas += 1
    log.warning(f"{item['credito_id']} -> ADIADO: {item['motivo']}")
    if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (PJe/DJEN fora ou bloqueando).")
        rod.parar.set()


def encerrar(rod, pendentes, inicio):
    """Grava o lote incompleto que sobrou (fila vazia, --limite, Ctrl+C, parada por falhas), fecha as conexões e
    imprime o resumo. Se o último lote não gravar, os créditos dele voltam para a fila."""
    try:
        if pendentes:
            log.info(f"gravando o último lote ({len(pendentes)} crédito(s))...")
            descarregar(rod, pendentes)
    except BaseException as e:
        log.error(f"último lote não gravado ({e.__class__.__name__}): os créditos voltam para a fila.")
        if not rod.simulacao:
            for item in pendentes:
                try:
                    devolver(rod.con, item["lead"]["credito_id"])
                except Exception:
                    pass
    finally:
        rod.fechar()
    minutos = max((time.time() - inicio) / 60, 1 / 60)
    log.info(f"DJEN: {rod.djen.n_429} resposta(s) 429 (limite de ritmo)")
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --creditos e --proxies."""
    ap = argparse.ArgumentParser(description="Credor do TJMA pela consulta pública (DJEN + PJe 1º grau), "
                                             f"gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS}); "
                         "diferente do TJBA, não sobe um processo por worker")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
    ap.add_argument("--proxies", action="store_true",
                    help="soma PROXY_01..05 do .env como saídas do DJEN (precisam sair pelo Brasil)")
    return ap.parse_args()


def main():
    """Laço principal: mantém `workers` créditos raspando, junta os prontos no lote e grava a cada LOTE.
    Ctrl+C devolve os créditos em andamento para a fila; o lote já processado é gravado ao encerrar."""
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    inicio = time.time()
    creditos = [int(x) for x in re.findall(r"\d+", args.creditos)]
    if creditos and not args.simulacao:
        raise SystemExit("--creditos só vale com --simulacao")
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), args.proxies, creditos)
    pendentes, em_voo = [], {}
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjma")
    try:
        while True:
            while len(em_voo) < rod.workers and (credito_id := proximo_credito(rod)):
                em_voo[executor.submit(processar_credito, rod, credito_id, rod.n)] = credito_id
            if not em_voo:
                break
            prontos, _ = wait(em_voo, return_when=FIRST_COMPLETED)
            for futuro in prontos:
                em_voo.pop(futuro)
                tratar(rod, futuro.result(), pendentes)
            if len(pendentes) >= LOTE:
                descarregar(rod, pendentes)
    except KeyboardInterrupt:
        rod.parar.set()
        if not rod.simulacao:
            for credito_id in em_voo.values():
                try:
                    devolver(rod.con, credito_id)
                except Exception:
                    pass
        log.warning(f"interrompido: {len(em_voo)} crédito(s) em andamento voltaram para a fila.")
    finally:
        rod.parar.set()
        executor.shutdown(wait=True, cancel_futures=True)
        encerrar(rod, pendentes, inicio)


if __name__ == "__main__":
    main()
