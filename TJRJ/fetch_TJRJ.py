"""
fetch_TJRJ.py - credor dos precatórios do TJRJ pela consulta pública, sem login e SEM token A3: lê o banco, consulta o
processo originário (DCP ou PJe 1º grau, HTTP puro), decide o credor do precatório e grava no banco em lotes de LOTE
créditos (LOTE_GRAVACAO_TJRJ no .env). No lugar do modo credor do RPA_SISTEMAS para o TJRJ.

O originário já vem da lista do TJRJ (lista_item.metadata.NumeroProcOriginario, ligado em credito_originario), então:
1. Fila: ordem própria (T1 não pagos na fila cronológica -> T2 outros não pagos -> T3 pagos; maior valor primeiro).
   Só entram leads com originário CNJ do TJRJ (com ou sem máscara) e com o número normalizado de 10 dígitos; sem
   originário (ou de outro tribunal) fica para o RPA. Cada crédito é reservado com lease (reservar) e só nessa hora
   passa do RPA para o software próprio (CONSULTA_PUBLICA_TJRJ); o que o RPA tinha fica em desfazer_fila_*.sql.
2. Originário '08...' (ou só no PJe): consulta pública do PJe 1º grau (reCAPTCHA desligado, 'if (false)'), que traz o
   polo ativo com CPF completo.
3. Originário do 2º grau ('.8.19.0000'): o DCP dá o número antigo do eJUD e a página do eJUD 2º grau, aberta no
   Chrome instalado (--chromes; liberado pelo usuário em 01/10/2026), dá personagens, advogados (OAB), precatórios
   autuados, segredo e capa. A própria página roda o reCAPTCHA v3 invisível (o Google libera sozinho um navegador
   normal); o robô não resolve nada: score baixo -> o crédito é adiado e o Chrome descansa. Sem CPF.
4. Demais: API pública da consulta processual do TJRJ (DCP; hoje sem reCAPTCHA: recuperar-site-key devolve vazio):
   partes, advogados (OAB), precatórios vinculados ao processo e as certidões dos movimentos (beneficiário do
   precatório, valor bruto, data de nascimento e, raramente, o CPF). O DCP não mostra CPF nas partes.
5. Credor: certidão com o valor bruto igual ao ValorHistorico da lista, ou autor pessoa física único (sem herdeiro,
   habilitado ou sucessor). Ação coletiva sem certidão que decida -> SUCESSO_ANALISAR (nada é gravado como credor).
   Cessão ou herdeiro no precatório (portal de precatórios) -> SUCESSO_ANALISAR.
6. Grava: credor com CPF -> SUCESSO_PROCESSO_ORIGINARIO (registrar_credor, origem FONTE); credor só com nome ->
   SUCESSO_INCOMPLETO com motivo 'SUCESSO_SEM_CPF: ...' (o banco não aceita credor sem CPF/OAB em credito_credor: o
   nome vai para a capa do originário, como parte sem pessoa, e para credito_fonte.metadata).
   A capa só acrescenta (partes do banco + as novas); na ação coletiva vai só o credor confirmado, o polo passivo e o
   advogado daquele precatório (página do precatório no eJUD), para o recálculo do banco não ligar os outros autores
   e advogados aos precatórios dos outros credores. Legado como o TJMA: capa antiga (originarios.*) e status nas filas
   mensais (processos_unificados_*), com o 'antes' em desfazer_legado_*.sql.

Erro passageiro (DCP/PJe/eJUD fora, timeout, captcha) devolve o crédito para a fila (fila_credor_adiar) na hora,
nunca vira FALHA e não entra no lote. Ctrl+C devolve os créditos em andamento e grava o lote que já estava processado.
CPF não vai para log, CSV, motivo nem detalhe da fila (só para credito_credor/pessoa e capas).

Saídas em TJRJ/saida: fetch_TJRJ.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql, desfazer_fila_*.sql e o log em saida/logs.

Uso:
    python fetch_TJRJ.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJRJ.py --simulacao --creditos 1160917,1192966   # só esses créditos, desfazendo no fim
    python fetch_TJRJ.py                        # processa a fila do TJRJ até acabar (Ctrl+C para parar)
    python fetch_TJRJ.py --limite 50            # para depois de 50 créditos
    python fetch_TJRJ.py --workers 4            # workers (threads) raspando ao mesmo tempo (padrão 6)
    python fetch_TJRJ.py --chromes 0            # sem o eJUD 2º grau (os leads do 2º grau ficam para o RPA)
"""
import argparse
import html as H
import json
import logging
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.request
import winreg
from collections import Counter, OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from concurrent.futures import TimeoutError as TempoEsgotado
from datetime import date, datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urljoin

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
from utils.workers import matar_chrome_do_perfil  # noqa: E402

# =============================================================================== configuração

SAIDA = AQUI / "saida"
load_dotenv(AQUI.parent / ".env")      # PG_* e LOTE_GRAVACAO_TJRJ
log = logging.getLogger("fetch_TJRJ")


def lote_do_env():
    """Créditos por transação de gravação, de LOTE_GRAVACAO_TJRJ no .env (inteiro maior que zero)."""
    valor = os.environ.get("LOTE_GRAVACAO_TJRJ", "").strip()
    if not valor.isdigit() or int(valor) < 1:
        raise SystemExit(f"LOTE_GRAVACAO_TJRJ no .env precisa ser um inteiro maior que zero (veio {valor!r}).")
    return int(valor)


TRIBUNAL_TJRJ = 119
SOFTWARE = "CONSULTA_PUBLICA_TJRJ"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJRJ:consulta_publica:{os.getpid()}"
LOTE = lote_do_env()                   # créditos por transação de gravação
WORKERS = 6                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 5 * 60                  # s por crédito; passou disso, volta para a fila
ADIAMENTO = "30 minutes"               # erro passageiro
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
CHECAR_CAPTCHA_A_CADA = 30 * 60        # s entre conferências do reCAPTCHA do DCP
MAX_FALHAS_SEGUIDAS = 12               # falhas técnicas seguidas que param o robô
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"
FAIXAS = {1: "T1_NAO_PAGO_NA_FILA", 2: "T2_NAO_PAGO_FORA_DA_FILA", 3: "T3_PAGO"}

DCP_API = "https://www3.tjrj.jus.br/consultaprocessual/api"
EJUD = "https://www3.tjrj.jus.br/ejud/"
PORTAL = "https://www3.tjrj.jus.br/PortalConhecimento/api/precatorios"
TIMEOUT_HTTP = 90
# ritmo: teto de requisições por segundo somando todos os workers (medido em 30/09/2026: 8,2 req/s por 1 min sem erro
# nem lentidão no DCP). Erro de servidor corta o ritmo pela metade e pausa todos; respostas boas sobem aos poucos.
RITMO_TJRJ = 6.0                       # req/s para www3.tjrj.jus.br (DCP, eJUD, portal) (--ritmo)
RITMO_PJE = 2.0                        # req/s para o PJe (já deu 502 com carga)
RITMO_MINIMO = 0.5
PAUSA_ERRO = 60                        # s que todos os workers param depois de um erro de servidor
SUBIR_A_CADA = 200                     # respostas boas seguidas para subir o ritmo em 10% (até o teto)
HTTP_FREIA = {403, 429, 502, 503, 504}  # respostas que indicam carga/bloqueio: freiam o ritmo
MAX_CACHE_DCP = 20000                  # originários do DCP guardados (já interpretados) para os precatórios irmãos
TOLERANCIA_VALOR = 1.00                # R$ de diferença entre o valor bruto da certidão e o ValorHistorico da lista
# autor único com vários precatórios no mesmo originário: o precatório pequeno costuma ser o de honorários do advogado
# (10-30% do principal). Valor do precatório / maior precatório do originário:
RAZAO_PRINCIPAL = 0.50                 # a partir daqui é o principal (ou complementar/parcela): credor = autor
RAZAO_HONORARIOS = 0.35                # até aqui, possível honorários; entre as duas, zona de dúvida (analisar)

# eJUD 2º grau no Chrome instalado (medido em 01/10/2026: 40 de 40 páginas, 1,7 s cada, score do reCAPTCHA 0,9)
EJUD_PROCESSO = EJUD + "ConsultaProcesso.aspx?N={}"
CHROMES = 2                            # Chromes abertos para o eJUD 2º grau (--chromes; 0 = 2º grau fica para o RPA)
TIMEOUT_EJUD = 45                      # s esperando a página trazer os dados do processo
INTERVALO_EJUD = 1.0                   # s entre duas páginas no mesmo Chrome
SCORE_MINIMO_EJUD = 0.3                # a própria página recusa abaixo disso ("Interação não humana detectada")
PAUSA_EJUD = 5 * 60                    # s que um Chrome descansa depois de o reCAPTCHA recusar
ERROS_PARA_REABRIR = 3                 # erros seguidos num Chrome que o fazem fechar e abrir de novo
HOST = "http://127.0.0.1"              # depuração remota (CDP) dos Chromes

BASE = "https://tjrj.pje.jus.br"
URL = BASE + "/pje/ConsultaPublica/listView.seam"
CAMPO_NUMERO = "fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso"
TIMEOUT_PJE = 90
MAX_PAGINAS_PARTES = 60
MAX_PAGINAS_PASSIVO = 5
TETO_DETALHE = 90                      # s virando páginas de partes num detalhe (ação coletiva enorme)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"

UFS = {"AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ",
       "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}
CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")
RE_ORIGINARIO = re.compile(r"^(?:\d{7}-\d{2}\.\d{4}\.8\.19\.\d{4}|\d{13}819\d{4})$")   # a lista às vezes vem sem máscara
RE_OAB_EJUD = re.compile(r"^([A-Z]{2})\s*(\d{2,7}[A-Z]?)\s*-\s*(.+)$")                   # 'RJ111585 - NOME' no eJUD
RE_PRECATORIO = re.compile(r"^(\d{4})\.(\d{5})-\d$")
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
# ente público no polo ativo (a lista do TJRJ tem ente cobrando ente, ex.: execução fiscal do Estado contra município)
RE_ORGAO_PUBLICO = re.compile(
    r"^(?:ESTADO D|MUNICIPIO D|PREFEITURA|UNIAO\b|DISTRITO FEDERAL|INSS\b|INSTITUTO NACIONAL|RIO ?PREVID|PREVI ?RIO\b|"
    r"DETRAN|IPERJ\b|DER\b)|PROCURADORIA|DEFENSORIA PUBLICA|MINISTERIO PUBLICO|FAZENDA PUBLICA|CAMARA MUNICIPAL|"
    r"TRIBUNAL D|^(?:INSTITUTO|FUNDACAO|FUNDO|DEPARTAMENTO|AUTARQUIA|SERVICO|GUARDA|UNIVERSIDADE|COMPANHIA|EMPRESA)\b"
    r".*\b(?:ESTADO|ESTADUAL|MUNICIP|RIO DE JANEIRO|PREFEITURA|PUBLIC)")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA|ESCRITORIO DE ADVOCACIA")
# entidade civil com o nome do ente no nome ('ASSOCIAÇÃO DE DOCENTES DA UNIVERSIDADE DO ESTADO DO RIO DE JANEIRO'):
# não é órgão público
RE_ENTIDADE_CIVIL = re.compile(r"^(?:ASSOCIACAO|ASSOC|SINDICATO|SIND|FEDERACAO|CONFEDERACAO|CLUBE|COOPERATIVA|"
                               r"CENTRO DOS|UNIAO DOS|UNIAO DAS|ORDEM DOS)\b")
# papéis do DCP (descPers, sem acento): quem pode ser o credor, quem indica sucessão, polo passivo
RE_PAPEL_CREDOR = re.compile(r"^(?:AUTOR|AUTORA|AUTORES|EXEQUENTE|REQUERENTE|IMPETRANTE|EMBARGADO|EMBARGADA|"
                             r"BENEFICIARI|RECLAMANTE|CREDOR)")
RE_PAPEL_SUCESSAO = re.compile(r"HERDEIR|HABILITAD|SUCESSOR|INVENTARIANTE|FALECID|ESPOLIO")
RE_PAPEL_PASSIVO = re.compile(r"^(?:REU|RE\b|EXECUTAD|REQUERID|IMPETRAD|EMBARGANTE|RECLAMAD|DEVEDOR)")
# certidões do DCP (texto sem acento, maiúsculo): 'Beneficiário do precatório: NOME b) Parte executada ...',
# '* BENEFICIARIO: NOME * DATA DE NASCIMENTO: ... * CPF: ...', 'Valor bruto: R$ 144.943,70'
RE_BENEF = re.compile(r"BENEFICIARI[OA](?: DO PRECATORIO)?\s*:\s*([A-Z][A-Z \.'\-]{3,120}?)\s*"
                      r"(?=[,;*(]|\s-\s|\s[A-Z]\)|\sPARTE EXECUTADA|\sCPF|\sDATA DE NASCIMENTO|\sVALOR|\sRG\b|$)")
RE_BRUTO = re.compile(r"VALOR (?:BRUTO|TOTAL)[^R$]{0,25}R\$\s*([\d\.]+,\d{2})")
RE_NASC = re.compile(r"DATA DE NASCIMENTO(?: DO BENEFICIARIO)?\s*:?\s*(\d{2}/\d{2}/\d{4})")
RE_CPF = re.compile(r"(?<!\d)(\d{3})\.?(\d{3})\.?(\d{3})-?(\d{2})(?!\d)")
RE_CPF_ROTULO = re.compile(r"CPF[^0-9]{0,25}(\d{3}\.?\d{3}\.?\d{3}-?\d{2})(?!\d)")
# o CPF logo depois de um nome não é dele se no meio (ou logo antes do nome) aparece um destes
RE_OUTRA_PESSOA = re.compile(r"PERIT|ADVOG|\bOAB\b|CURADOR|PATRONO|PROCURADOR|INVENTARIANTE|REPRESENTANTE")

# PJe (mesma consulta pública JSF do TJMA)
RE_LINK_DETALHE = re.compile(r"(/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+)")
RE_QTD = re.compile(r"(\d*)\s*resultados\s+encontrados")
RE_LINHA = re.compile(r'<tr class="rich-table-row[^"]*">(.*?)</tr>', re.S)
RE_PARTE = re.compile(
    r"([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ][A-Za-zÁÂÃÀÉÊÍÓÔÕÚÜÇáâãàéêíóôõúüç&\.\-\s']{3,90}?)"
    r"(?:\s*-\s*OAB\s*([A-Z]{2})\s*(\d+[A-Z]?))?"
    r"(?:\s*-\s*(CPF|CNPJ):\s*([\d\.\-/\*]{11,20}))?"
    r"\s*\(([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ][A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ\s/\.\-]{2,39})\)")
RE_ADVOGADO = re.compile(r"ADVOGAD|PROCURADOR|DEFENSOR", re.I)
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
STATUS_COM_CREDOR = ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_INCOMPLETO")
# sistema do originário -> creditos.sistema_processual (o DCP não tem código lá: não grava sistema)
SISTEMA_BANCO = {"PJE": "PJE", "EPROC": "EPROC"}

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "faixa", "ente", "originario", "sistema", "resultado",
           "motivo", "regra", "fontes", "credor", "cpf_encontrado", "data_nascimento", "n_autores", "n_precatorios",
           "coletiva", "advogados", "banco", "legado", "credor_corrigido", "credores_antes", "credores_depois", "lote",
           "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set(), "corrigidos": []}


class ErroTecnico(Exception):
    """Falha passageira (DCP/PJe/eJUD fora do ar, timeout, captcha): o crédito volta para a fila, não vira FALHA."""

# =============================================================================== utilidades


def sem_acento(t):
    """Maiúsculas e sem acento, com a pontuação preservada."""
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples (para comparar nomes)."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", sem_acento(t))).strip()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE', sem o 'e outro(s)...' e sem o 'REGISTRADO(A) CIVILMENTE COMO'."""
    n = normal(RE_ESPOLIO.sub("", nome or ""))
    n = re.sub(r"\s+E OUTR[OA]S?\b.*$", "", n)
    return re.split(r"\bREGISTRAD[OA]\b", n)[0].strip()


def mesma_pessoa(a, b):
    """Mesmo nome, ou grafia próxima o bastante (SOUSA x SOUZA, acento)."""
    a, b = chave_nome(a), chave_nome(b)
    return bool(a and b) and (a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA)


def chaves_ente(ente):
    """(nomes do ente, nomes da cidade) para achar o ente devedor entre as partes:
    'MUNICÍPIO DE BARRA MANSA' -> ({'MUNICIPIO DE BARRA MANSA'}, {'BARRA MANSA'});
    'RIO-PREVIDÊNCIA (03.066.219/0001-81)' -> ({'RIO PREVIDENCIA'}, set())."""
    partes = [normal(p) for p in re.split(r"\s+-\s+", re.sub(r"\([^)]*\)", " ", ente or "")) if p.strip()]
    cidades = {m.group(1) for p in partes if (m := re.match(r"MUNICIPIO D[EOA]S? (.+)", p))}
    return {c for c in partes if len(c) >= 4}, {c for c in cidades if len(c) >= 4}


def eh_orgao_publico(nome, lead):
    """A parte é ente público (ou o próprio ente devedor do precatório)? Associação, sindicato e afins nunca são, mesmo
    com o nome do ente no nome. O nome da cidade só vale sozinho ('MESQUITA'), nunca dentro do nome de uma pessoa
    ('JOSE MESQUITA', 'MARIA DO CARMO'); nome de uma palavra só ('UERJ', 'FAETEC') só no começo."""
    n = normal(nome)
    if RE_ENTIDADE_CIVIL.search(n):
        return False
    nomes, cidades = lead["chaves_ente"]
    return bool(RE_ORGAO_PUBLICO.search(n)) or n in cidades or any(
        (n == c or n.startswith(c + " ")) if " " not in c else re.search(rf"\b{re.escape(c)}\b", n) for c in nomes)


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
    try:
        return date(int(m[3]), int(m[2]), int(m[1])) if m else None
    except ValueError:
        return None


def cnj_precatorio(precatorio):
    """'2023.10055-5' -> '0010055-27.2023.8.19.0801' (CNJ do precatório no TJRJ; conferido com o eJUD)."""
    m = RE_PRECATORIO.match(precatorio or "")
    if not m:
        return None
    seq, ano = int(m.group(2)), m.group(1)
    dv = 98 - int(f"{seq:07d}{ano}8190801" + "00") % 97
    return f"{seq:07d}-{dv:02d}.{ano}.8.19.0801"


def fmt_credores(itens):
    """{(papel, nome, documento, origem)} -> texto para CSV/detalhe, SEM o documento (só se tem ou não)."""
    return " | ".join(f"{nome} ({'com doc' if doc else 'sem doc'}, {papel}, {origem})"
                      for papel, nome, doc, origem in sorted(itens))

# =============================================================================== TJRJ por HTTP (DCP, eJUD, portal)


class Ritmo:
    """Teto de requisições por segundo a um servidor, dividido entre todos os workers, que se ajusta sozinho:
    resposta de carga/bloqueio (403, 429, 502-504, timeout, conexão) corta o ritmo pela metade (até RITMO_MINIMO) e
    pausa todos os workers por PAUSA_ERRO; SUBIR_A_CADA respostas boas seguidas sobem 10% (até o teto)."""

    def __init__(self, nome, teto):
        self.nome, self.teto, self.atual = nome, teto, teto
        self.trava = threading.Lock()
        self.proximo = self.pausa_ate = 0.0
        self.boas = self.freadas = 0

    def esperar(self, parar):
        """Bloqueia até a vez desta requisição (a fila é única para todas as threads)."""
        with self.trava:
            agora = time.monotonic()
            vez = max(agora, self.proximo, self.pausa_ate)
            self.proximo = vez + 1.0 / self.atual
        if vez > agora and parar.wait(vez - agora):
            raise ErroTecnico("INTERROMPIDO")

    def ok(self):
        with self.trava:
            self.boas += 1
            if self.boas >= SUBIR_A_CADA and self.atual < self.teto:
                self.atual, self.boas = min(self.teto, self.atual * 1.1), 0
                log.info(f"ritmo {self.nome}: subiu para {self.atual:.1f} req/s")

    def freia(self, motivo):
        with self.trava:
            self.boas, self.freadas = 0, self.freadas + 1
            antes, self.atual = self.atual, max(RITMO_MINIMO, self.atual / 2)
            self.pausa_ate = max(self.pausa_ate, time.monotonic() + PAUSA_ERRO)
        log.warning(f"ritmo {self.nome}: {motivo} -> {antes:.1f} para {self.atual:.1f} req/s e pausa de {PAUSA_ERRO}s")


class Http:
    """Sessão HTTP de um worker para o TJRJ (DCP, eJUD e portal de precatórios): respeita o ritmo global e tenta 3
    vezes."""

    def __init__(self, parar, ritmo):
        self.parar, self.ritmo = parar, ritmo
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9"})

    def pedir(self, metodo, url, **kw):
        """GET/POST com 3 tentativas; ErroTecnico se não responder. 200 e 204 valem como resposta."""
        erro = ""
        for tentativa in range(3):
            self.ritmo.esperar(self.parar)
            try:
                r = self.s.request(metodo, url, timeout=TIMEOUT_HTTP, **kw)
                if r.status_code in (200, 204):
                    self.ritmo.ok()
                    return r
                erro = f"HTTP {r.status_code}"
                if "captcha" in r.text[:2000].lower():
                    raise ErroTecnico("CAPTCHA_REATIVADO: o TJRJ passou a exigir captcha nesta consulta")
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except requests.RequestException as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(2 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: {url.split('/')[2]} não respondeu ({erro})")


H_DCP = {"Accept": "application/json, text/plain, */*", "Origin": "https://www3.tjrj.jus.br",
         "Referer": "https://www3.tjrj.jus.br/consultaprocessual/"}


def dcp_post(http, caminho, corpo):
    """POST na API da consulta processual; None para resposta vazia (204)."""
    r = http.pedir("POST", f"{DCP_API}/{caminho}", json=corpo, headers=H_DCP)
    if r.status_code == 204 or not r.content:
        return None
    try:
        d = r.json()
    except ValueError:
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: DCP devolveu algo que não é JSON em {caminho}")
    if isinstance(d, dict) and "captcha" in json.dumps(d.get("error") or d.get("mensagem") or "").lower():
        raise ErroTecnico("CAPTCHA_REATIVADO: o DCP recusou a consulta por captcha")
    return d


def captcha_dcp_ligado(http):
    """O DCP voltou a exigir reCAPTCHA? Hoje recuperar-site-key devolve vazio (204) e o site consulta sem token."""
    r = http.pedir("POST", f"{DCP_API}/security/recuperar-site-key", headers=H_DCP)
    return bool((r.text or "").strip().strip('"'))


def polo_dcp(papel):
    """Papel do DCP (sem acento) -> ATIVO, PASSIVO ou OUTRO (perito, interessado, procurador...)."""
    if RE_PAPEL_CREDOR.search(papel) or RE_PAPEL_SUCESSAO.search(papel) or papel.startswith("REPRESENTANTE"):
        return "ATIVO"
    if RE_PAPEL_PASSIVO.search(papel):
        return "PASSIVO"
    return "OUTRO"


def texto_dos_movimentos(movs):
    """Movimentos do DCP -> um texto só (sem acento, maiúsculo, pontuação preservada, espaços simples)."""
    pedacos = [f"{y.get('codigo') or ''} {y.get('descricao') or ''}"
               for mv in movs for x in (mv.get("movimentosExibicao") or []) for y in (x.get("detalhesMovimento") or [])]
    return re.sub(r"\s+", " ", sem_acento(" ".join(pedacos)))


def certidoes_do_texto(texto):
    """Certidões de expedição do precatório: [{nome, valor, nascimento, cpf}] (cpf só se vier logo depois do nome,
    sem perito/advogado no meio)."""
    achadas = []
    ocorrencias = list(RE_BENEF.finditer(texto))
    for i, m in enumerate(ocorrencias):
        fim = ocorrencias[i + 1].start() if i + 1 < len(ocorrencias) else len(texto)
        janela = texto[m.end(): min(fim, m.end() + 1500)]
        valor = RE_BRUTO.search(janela)
        nasc = RE_NASC.search(janela[:600])
        cpf = None
        trecho = janela[:200]
        c = RE_CPF_ROTULO.search(trecho)
        if c and not RE_OUTRA_PESSOA.search(trecho[:c.start()]):
            d = so_digitos(c.group(1))
            cpf = d if len(d) == 11 and documento_valido(d) else None
        achadas.append({"nome": " ".join(m.group(1).split()).strip(" .-"),
                        "valor": valor_numerico(valor.group(1)) if valor else None,
                        "nascimento": nasc.group(1) if nasc else None, "cpf": cpf})
    return achadas


def cpfs_do_texto(texto):
    """[(160 caracteres antes, CPF)] de cada CPF válido no texto (para achar depois o CPF logo depois de um nome)."""
    out = []
    for m in RE_CPF.finditer(texto):
        d = "".join(m.groups())
        if documento_valido(d):
            out.append((texto[max(0, m.start() - 160):m.start()], d))
    return out


def cpf_do_nome(nome, trechos):
    """CPF que aparece logo depois do nome da pessoa (até 60 caracteres, sem perito/advogado/patrono no meio nem logo
    antes do nome). Mais de um CPF diferente: None (não dá para saber qual)."""
    chave = re.sub(r"\s+", " ", sem_acento(nome)).strip()[:40]
    if len(chave) < 8:
        return None
    achados = set()
    for antes, cpf in trechos:
        k = antes.rfind(chave)
        if k < 0:
            continue
        entre = antes[k + len(chave):]
        if len(entre) <= 60 and not RE_OUTRA_PESSOA.search(entre) and \
                not RE_OUTRA_PESSOA.search(antes[max(0, k - 60):k]):
            achados.add(cpf)
    return achados.pop() if len(achados) == 1 else None


def consultar_dcp(http, cnj20):
    """Originário no DCP -> dados já interpretados (o que vai para o cache): sistemas em que o CNJ aparece, partes,
    advogados, precatórios vinculados, certidões e CPFs do texto dos movimentos, capa."""
    itens = dcp_post(http, "processos/por-numeracao-unica", {"tipoProcesso": "1", "codigoProcesso": formatar_cnj(cnj20)})
    itens = [i for i in (itens or []) if isinstance(i, dict)]
    tipos = sorted({i.get("tipoProcesso") for i in itens if i.get("tipoProcesso") is not None})
    p1 = next((i for i in itens if i.get("tipoProcesso") == 1), None)
    out = {"achou": bool(p1), "tipos": tipos, "pje": 13 in tipos}
    if not p1:
        return out
    corpo = {"tipoProcesso": "1", "codigoProcesso": p1["numProcesso"]}
    d = dcp_post(http, "processos/por-numero/publica", corpo) or {}
    m = dcp_post(http, "processos/por-numero/movimentos",
                 {**corpo, "indProcVolumoso": "N", "ultimaOrdemExibida": None}) or {}
    partes, advogados, polo_atual = [], [], None
    for x in d.get("personagensProcesso") or []:
        nome = " ".join((x.get("nome") or "").split())
        papel = normal(x.get("descPers"))
        if "ADVOGAD" in papel:
            ma = re.match(r"^\((\w+)\)\s*(.+)$", nome)
            oab, nome_adv = (ma.group(1), ma.group(2)) if ma else ("", nome)
            mo = re.match(r"^([A-Z]{2})(\d{2,7}[A-Z]?)$", oab or "")
            if mo and mo.group(1) in UFS:
                advogados.append({"nome": nome_adv, "oab_uf": mo.group(1), "oab_numero": mo.group(2),
                                  "polo": polo_atual or "ATIVO", "papel": "ADVOGADO", "documento": ""})
            continue
        polo = polo_dcp(papel)
        if polo != "OUTRO":
            polo_atual = polo
        partes.append({"nome": nome, "papel": papel, "polo": polo, "documento": ""})
    oabs = {(a["oab_uf"], a["oab_numero"]) for a in advogados}
    for a in d.get("advogados") or []:                  # a lista própria de advogados (lado do autor)
        mo = re.match(r"^([A-Z]{2})(\d{2,7}[A-Z]?)$", (a.get("numOab") or "").strip())
        if mo and mo.group(1) in UFS and (mo.group(1), mo.group(2)) not in oabs:
            advogados.append({"nome": " ".join((a.get("nomeAdv") or "").split()), "oab_uf": mo.group(1),
                              "oab_numero": mo.group(2), "polo": "ATIVO", "papel": "ADVOGADO", "documento": ""})
    texto = texto_dos_movimentos(m.get("movimentosProc") or [])
    out.update({
        "numero_dcp": p1["numProcesso"], "segredo": d.get("indSegrJust") == "S",
        "migrado_eproc": bool(d.get("processoMigradoDcpParaEproc")), "volumoso": bool(m.get("processoVolumoso")),
        "vinculados": [x.get("codPrecatorio") for x in (d.get("listaPrecatorioVinculado") or [])],
        "partes": partes, "advogados": advogados, "certidoes": certidoes_do_texto(texto), "cpfs": cpfs_do_texto(texto),
        "n_movimentos": len(m.get("movimentosProc") or []),
        "capa": {"classe_judicial": d.get("descRito") or d.get("txtAcao"), "assunto": d.get("txtAssunto"),
                 "orgao_julgador": d.get("descServ") or d.get("descVara"), "jurisdicao": d.get("nome"),
                 "data_autuacao": d.get("dataDis")}})
    return out


def advogado_do_precatorio(http, precatorio):
    """Nome do advogado na página do precatório no eJUD (sem captcha) ou ''."""
    n = so_digitos(precatorio)
    pagina = f"{EJUD}processarprecatorio.aspx?N={n}"
    http.pedir("GET", pagina)
    r = http.pedir("POST", f"{EJUD}ProcessarPrecatorio.aspx/ExecutarConsultarPrecatorio", data="",
                   headers={"Content-Type": "application/json; charset=utf-8", "X-Requested-With": "XMLHttpRequest",
                            "Referer": pagina})
    try:
        d = (r.json() or {}).get("d") or {}
    except ValueError:
        return ""
    campos = {x.get("Descricao"): x.get("Conteudo") for x in d.get("ProcessosPrecatorio") or []}
    return " ".join((campos.get("Advogado") or "").split())


def detalhes_do_portal(http, precatorio):
    """Portal de precatórios: {cessao, herdeiro} ('Sim' no PossuiCessao / PossuiHerdeiro)."""
    r = http.pedir("GET", f"{PORTAL}/detalhes", params={"numPrecatorio": precatorio}, headers={"Accept": "application/json"})
    try:
        d = r.json() if r.content else {}
    except ValueError:
        d = {}
    d = d or {}
    return {"cessao": normal(d.get("PossuiCessao")) == "SIM", "herdeiro": normal(d.get("PossuiHerdeiro")) == "SIM"}


class CacheDcp:
    """Originários do DCP já consultados (dados interpretados), compartilhados pelos workers: os precatórios da mesma
    ação coletiva não refazem a consulta."""

    def __init__(self):
        self.d, self.trava = OrderedDict(), threading.Lock()

    def pegar(self, cnj):
        with self.trava:
            v = self.d.get(cnj)
            if v is not None:
                self.d.move_to_end(cnj)
            return v

    def por(self, cnj, v):
        with self.trava:
            self.d[cnj] = v
            while len(self.d) > MAX_CACHE_DCP:
                self.d.popitem(last=False)

# =============================================================================== eJUD 2º grau (Chrome instalado por CDP)


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


class Ejud:
    """eJUD 2º grau (ConsultaProcesso.aspx) no Chrome instalado, por CDP (liberado pelo usuário em 01/10/2026).
    A página roda o próprio reCAPTCHA v3 invisível (o Google libera sozinho um navegador normal) e só então busca os
    dados em WS/ConsultaEjud.asmx/DadosProcesso_1: o robô abre a página e lê esse JSON. Nada é resolvido nem pulado:
    se o reCAPTCHA recusa (score abaixo de SCORE_MINIMO_EJUD, a página nem busca os dados), o crédito é adiado e
    aquele Chrome descansa PAUSA_EJUD. Cada Chrome roda numa thread própria (o Playwright síncrono não troca de
    thread) e com perfil próprio; os workers pedem com consultar() e esperam. Os Chromes só abrem no 1º pedido."""

    def __init__(self, parar, n):
        self.parar, self.n = parar, n
        self.fila, self.trava, self.threads = queue.Queue(), threading.Lock(), []
        self.pausa_ate = [0.0] * n                      # monotonic até quando cada Chrome descansa
        self.paginas = self.recusas = 0

    def consultar(self, numero, prazo):
        """JSON do processo (DadosProcesso_1) pelo número antigo do eJUD ('2024.231.02441'), ou None se o eJUD não
        devolve o processo. ErroTecnico se não deu (todos em pausa, tempo esgotado, parada)."""
        with self.trava:
            if not self.threads:
                self.threads = [threading.Thread(target=self._rodar, args=(i,), name=f"ejud{i + 1}", daemon=True)
                                for i in range(self.n)]
                for t in self.threads:
                    t.start()
            if min(self.pausa_ate) > time.monotonic() + 10:
                raise ErroTecnico("CAPTCHA_EJUD: todos os Chromes do eJUD 2º grau estão em pausa (reCAPTCHA recusou)")
        futuro = Future()
        self.fila.put((numero, futuro))
        while True:
            try:
                return futuro.result(timeout=1)
            except TempoEsgotado:
                if self.parar.is_set() or time.time() > prazo:
                    if futuro.cancel() or not futuro.done():
                        raise ErroTecnico("INTERROMPIDO" if self.parar.is_set() else
                                          "TIMEOUT_EJUD: o eJUD 2º grau não devolveu o processo a tempo") from None

    def fechar(self):
        """Para os Chromes (cada thread fecha o seu) e espera as threads."""
        for _ in self.threads:
            self.fila.put(None)
        for t in self.threads:
            t.join(timeout=30)

    def _rodar(self, i):
        """Thread de um Chrome: abre, atende a fila e reabre se cair, até a parada. No fim, recusa o que sobrou."""
        perfil = AQUI / f".chrome-profile-ejud-{i + 1}"
        while not self.parar.is_set():
            try:
                if self._sessao(i, perfil):
                    break
            except Exception as e:
                self.pausa_ate[i] = max(self.pausa_ate[i], time.monotonic() + 30)
                log.warning(f"eJUD Chrome {i + 1}: {e.__class__.__name__}: {str(e)[:200]} -> reabrindo em 30 s")
                if self.parar.wait(30):
                    break
        while True:
            try:
                item = self.fila.get_nowait()
            except queue.Empty:
                break
            if item and item[1].set_running_or_notify_cancel():
                item[1].set_exception(ErroTecnico("INTERROMPIDO"))

    def _sessao(self, i, perfil):
        """Um Chrome aberto atendendo a fila. True = fim pedido (parada); exceção = fechar e abrir de novo."""
        try:
            from patchright.sync_api import sync_playwright
        except ImportError:
            from playwright.sync_api import sync_playwright
        perfil.mkdir(parents=True, exist_ok=True)
        if orfaos := matar_chrome_do_perfil(perfil):
            log.warning(f"eJUD: Chrome de uma execução anterior ainda aberto com o perfil {perfil.name}: fechado "
                        f"({orfaos} processo(s))")
        for nome in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            try:
                (perfil / nome).unlink(missing_ok=True)   # sem isso o Chrome recusa um perfil que não fechou direito
            except OSError:
                pass
        porta = porta_livre()
        proc = subprocess.Popen([localizar_chrome(), f"--remote-debugging-port={porta}", f"--user-data-dir={perfil}",
                                 "--no-first-run", "--no-default-browser-check",
                                 "--disable-backgrounding-occluded-windows", "--disable-renderer-backgrounding",
                                 "--disable-background-timer-throttling", "about:blank"])
        pw = None
        try:
            esperar_cdp(porta)
            pw = sync_playwright().start()
            ctx = pw.chromium.connect_over_cdp(f"{HOST}:{porta}").contexts[0]
            pagina = ctx.pages[0] if ctx.pages else ctx.new_page()
            caixa = {}

            def guardar(resp):
                if resp.request.method == "POST" and ("DadosProcesso" in resp.url or "RecaptchaVerify" in resp.url):
                    try:
                        corpo = resp.body().decode("utf-8", "replace")
                    except Exception:
                        corpo = ""
                    caixa["dados" if "DadosProcesso" in resp.url else "captcha"] = (resp.status, corpo)
            ctx.on("response", guardar)
            log.info(f"eJUD Chrome {i + 1}: aberto (perfil {perfil.name})")
            erros = 0
            while not self.parar.is_set():
                espera = self.pausa_ate[i] - time.monotonic()
                if espera > 0 and self.parar.wait(espera):
                    break
                try:
                    item = self.fila.get(timeout=1)
                except queue.Empty:
                    continue
                if item is None:
                    break
                numero, futuro = item
                if not futuro.set_running_or_notify_cancel():
                    continue                            # o worker desistiu (tempo esgotado)
                try:
                    futuro.set_result(self._pagina(i, pagina, caixa, numero))
                    erros = 0
                except Exception as e:
                    futuro.set_exception(e if isinstance(e, ErroTecnico) else
                                         ErroTecnico(f"PESQUISA_SEM_RESPOSTA: eJUD 2º grau ({e.__class__.__name__})"))
                    erros += 1
                    if not isinstance(e, ErroTecnico) or erros >= ERROS_PARA_REABRIR:
                        raise
                if self.parar.wait(INTERVALO_EJUD):
                    break
            return True
        finally:
            try:
                if pw:
                    pw.stop()
            except Exception:
                pass
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _pagina(self, i, pagina, caixa, numero):
        """Abre a página do processo e espera o JSON que ela mesma busca. dict, ou None se o eJUD não devolve dados."""
        caixa.clear()
        inicio, conferido = time.time(), False
        try:
            pagina.goto(EJUD_PROCESSO.format(numero), wait_until="commit", timeout=TIMEOUT_EJUD * 1000)
        except Exception as e:
            if pagina.is_closed():
                raise                                   # aba/Chrome caiu: _sessao reabre
            raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o eJUD 2º grau não abriu ({e.__class__.__name__})") from None
        while "dados" not in caixa:
            if "captcha" in caixa and not conferido:
                conferido = True
                try:
                    v = json.loads(json.loads(caixa["captcha"][1])["d"])
                except (ValueError, KeyError, TypeError):
                    v = {}
                score = v.get("score")
                if not v.get("success") or not isinstance(score, (int, float)) or score < SCORE_MINIMO_EJUD:
                    with self.trava:
                        self.recusas += 1
                    self.pausa_ate[i] = time.monotonic() + PAUSA_EJUD
                    log.warning(f"eJUD Chrome {i + 1}: o reCAPTCHA recusou o navegador (score {score}) -> "
                                f"pausa de {PAUSA_EJUD // 60} min")
                    raise ErroTecnico(f"CAPTCHA_EJUD: o reCAPTCHA do eJUD 2º grau recusou o navegador (score {score})")
            if self.parar.is_set():
                raise ErroTecnico("INTERROMPIDO")
            if time.time() - inicio > TIMEOUT_EJUD:
                raise ErroTecnico("TIMEOUT_EJUD: a página do eJUD 2º grau não trouxe os dados do processo")
            pagina.wait_for_timeout(200)
        status, corpo = caixa["dados"]
        with self.trava:
            self.paginas += 1
        if status != 200:
            raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: eJUD 2º grau respondeu HTTP {status}")
        try:
            d = json.loads(corpo).get("d")
        except (ValueError, AttributeError):
            raise ErroTecnico("PESQUISA_SEM_RESPOSTA: o eJUD 2º grau devolveu algo que não é JSON") from None
        return d if isinstance(d, dict) else None


def consultar_ejud(http, ejud, cnj20, prazo):
    """Originário do 2º grau -> dados já interpretados, no mesmo formato de consultar_dcp: o DCP dá o número antigo
    do eJUD (tipoProcesso 2) e a página do eJUD 2º grau dá personagens, advogados (OAB), precatórios autuados no
    processo, segredo e capa. Sem movimentos (não há certidão nem CPF)."""
    itens = dcp_post(http, "processos/por-numeracao-unica", {"tipoProcesso": "1", "codigoProcesso": formatar_cnj(cnj20)})
    itens = [i for i in (itens or []) if isinstance(i, dict)]
    p2 = next((i for i in itens if i.get("tipoProcesso") == 2 and i.get("numProcesso")), None)
    out = {"achou": False, "tipos": sorted({i.get("tipoProcesso") for i in itens if i.get("tipoProcesso") is not None})}
    if not p2:
        return out
    d = ejud.consultar(p2["numProcesso"], prazo)
    if not d or so_digitos(d.get("CodCNJ")) != cnj20:
        return out
    partes, advogados, polo_atual = [], [], None
    for x in d.get("Personagens") or d.get("Partes") or []:
        nome = " ".join((x.get("Nome") or "").split())
        papel = normal(x.get("Tipo"))
        if "ADVOGAD" in papel:                          # 'RJ111585 - NOME', logo depois da parte que ele representa
            ma = RE_OAB_EJUD.match(nome)
            if ma and ma.group(1) in UFS:
                advogados.append({"nome": ma.group(3).strip(), "oab_uf": ma.group(1), "oab_numero": ma.group(2),
                                  "polo": polo_atual or "ATIVO", "papel": "ADVOGADO", "documento": ""})
            continue
        polo = polo_dcp(papel)                          # AUTOR/EXEQUENTE, RÉU/EXECUTADO; PROC. DO ESTADO = OUTRO
        if polo != "OUTRO":
            polo_atual = polo
        partes.append({"nome": nome, "papel": papel, "polo": polo, "documento": ""})
    out.update({
        "achou": True, "numero_ejud": p2["numProcesso"],
        "segredo": bool(d.get("SegredoJustica") or d.get("Sigiloso") or d.get("SuperSigiloso")),
        "migrado_eproc": False, "volumoso": False,
        "vinculados": [x.get("Cod_Prec") for x in d.get("PrecatoriosAutuados") or []],
        "partes": partes, "advogados": advogados, "certidoes": [], "cpfs": [], "n_movimentos": 0,
        "principal": d.get("NumProcPrimInst") or None,
        "capa": {"classe_judicial": d.get("DescrClasse"), "assunto": d.get("DescrAssunto"),
                 "orgao_julgador": d.get("OrgaoJulgador") or d.get("LocalAtualProcesso"),
                 "jurisdicao": d.get("DescrComarca"), "data_autuacao": d.get("DtAutuaStr")}})
    return out

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
    """Resultado da pesquisa -> [{numero, classe, ativo, passivo, link}]."""
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
    """Consulta pública do PJe 1º grau do TJRJ por requests (uma instância por thread: jsessionid próprio).
    O HTML do detalhe não é guardado (tem CPF)."""

    def __init__(self, parar, ritmo):
        self.parar, self.ritmo = parar, ritmo
        self.nova_sessao()

    def nova_sessao(self):
        """Sessão nova (cookies e ViewState zerados)."""
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9"})

    def _http(self, metodo, url, **kw):
        """GET/POST no ritmo do PJe, com 3 tentativas; ErroTecnico se não responder."""
        erro = ""
        for tentativa in range(3):
            self.ritmo.esperar(self.parar)
            try:
                r = self.s.request(metodo, url, timeout=TIMEOUT_PJE, **kw)
                if r.status_code == 200:
                    self.ritmo.ok()
                    return r
                erro = f"HTTP {r.status_code}"
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except requests.RequestException as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(3 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o PJe não respondeu ({erro})")

    def pesquisar(self, campos):
        """(linhas, quantidade) da pesquisa com os campos dados (o submit é o a4j:jsFunction executarPesquisa)."""
        for tentativa in (1, 2):
            html = self._http("GET", URL).text
            if captcha_ligado(html):
                raise ErroTecnico("CAPTCHA_REATIVADO: a consulta pública do PJe do TJRJ voltou a exigir reCAPTCHA")
            vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]*)"', html)
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
            self.nova_sessao()
            time.sleep(3)
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: o PJe devolveu a pesquisa sem resultado e sem contagem")

    def por_numero(self, numero20):
        """Linha do processo pelo número; None se o portal não acha (não existe no PJe ou segredo)."""
        linhas, _ = self.pesquisar({CAMPO_NUMERO: formatar_cnj(numero20)})
        return next((x for x in linhas if x["numero"] == numero20), linhas[0] if linhas else None)

    def detalhe(self, link, prazo=None):
        """Capa e partes do detalhe (vira as páginas do polo ativo e do passivo até o teto)."""
        prazo = min(prazo or float("inf"), time.time() + TETO_DETALHE)
        url = urljoin(BASE, link)
        html = self._http("GET", url, headers={"Referer": URL}).text
        if "processoPartesPolo" not in html:
            raise ErroTecnico("PROCESSO_NAO_CARREGOU: detalhe do PJe sem as tabelas de partes")
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
                if pg > teto or time.time() > prazo:
                    completa = False
                    break
                corpo = {"AJAXREQUEST": "_viewRoot", param.split(":")[0]: param.split(":")[0], param: str(pg),
                         "ajaxSingle": param, "autoScroll": "", "javax.faces.ViewState": vs.group(1) if vs else "j_id1"}
                novas = [p for p in partes_da_pagina(self._http("POST", url, data=corpo, headers={"Referer": url}).text)
                         if p["polo"] == polo and (p["nome"], p["documento"], p["papel"]) not in vistas]
                if not novas:
                    break
                vistas |= {(p["nome"], p["documento"], p["papel"]) for p in novas}
                partes += novas
        unicas = list({(p["nome"], p["documento"], p["papel"], p["polo"]): p for p in partes}.values())
        return {"capa": campos_da_capa(html), "completa": completa,
                "partes": [p for p in unicas if not RE_ADVOGADO.search(p["papel"])],
                "advogados": [p for p in unicas if RE_ADVOGADO.search(p["papel"])]}

# =============================================================================== fila (ordem do robô)


# o que o robô pode pegar: do RPA em PENDENTE e o que já é do robô em PENDENTE (vencido o disponivel_em).
# O que já é do robô com status final (SUCESSO/FALHA) não volta.
FILTRO_PEGAVEL = f"""cc.disponivel_em <= now() AND (
       (cc.software_id = {SOFTWARE_RPA} AND cc.status_id = 1)
    OR (cc.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}') AND cc.status_id = 1))"""

# escopo: originário CNJ do TJRJ na lista (com ou sem máscara; o 2º grau só com o eJUD ligado) e número normalizado
# de 10 dígitos. Sem originário ou de outro tribunal fica para o RPA.
SQL_ESCOPO = r"""
SELECT cc.credito_id, cc.valor_referencia, li.valor_lista,
       li.metadata->>'Pago' AS pago, li.metadata->>'SituacaoTratada' AS situacao,
       li.metadata->>'OrdemPagamento' AS ordem_pagamento
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c ON c.id = cc.credito_id
  JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                 WHERE x.credito_id = c.id ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2
   AND c.numero_norm ~ '^\d{10}$'
   AND li.metadata->>'NumeroProcOriginario' ~ '^(\d{7}-\d{2}\.\d{4}\.8\.19\.\d{4}|\d{13}819\d{4})$'
   AND ({com_2grau} OR regexp_replace(li.metadata->>'NumeroProcOriginario', '\D', '', 'g') !~ '0000$')
   AND {filtro}
"""


def faixa_da_linha(pago, situacao, ordem_pagamento):
    """T1 não pago na fila cronológica, T2 outros não pagos, T3 pagos."""
    if pago == "False" and situacao == "Ativo" and (valor_numerico(ordem_pagamento) or 0) > 0:
        return 1
    return 2 if pago == "False" else 3


def ordenar_escopo(con, filtro, com_2grau):
    """(ids na ordem do robô, {credito_id: faixa}, contagem por faixa). Ordem: faixa, maior valor, id."""
    sql = SQL_ESCOPO.replace("{filtro}", filtro).replace("{com_2grau}", "true" if com_2grau else "false")
    with con.cursor() as cur:
        cur.execute(sql, (TRIBUNAL_TJRJ,))              # .format quebraria o \d{10}
        linhas = como_dicts(cur)
    faixas = {x["credito_id"]: faixa_da_linha(x["pago"], x["situacao"], x["ordem_pagamento"]) for x in linhas}
    valor = {x["credito_id"]: float(x["valor_lista"] or x["valor_referencia"] or 0) for x in linhas}
    ordem = sorted(faixas, key=lambda i: (faixas[i], -valor[i], i))
    return ordem, faixas, Counter(FAIXAS[f] for f in faixas.values())

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJRJ", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, tc.codigo AS tipo_credito,
       li.valor_lista, COALESCE(li.metadata, '{}'::jsonb) AS lista,
       (SELECT st.codigo FROM creditos.coleta_credor_tentativa t JOIN creditos.status_coleta st ON st.id = t.status_id
         WHERE t.credito_id = c.id ORDER BY t.id DESC LIMIT 1) AS ultimo_status
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc ON tc.id = c.tipo_credito_id
  LEFT JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                      WHERE x.credito_id = c.id ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE c.id = %s
"""


def ler_credito(cur, credito_id):
    """O crédito (lead) com o que a lista do TJRJ traz: originário, ente, valor histórico, prioridade, pagamento; e
    quantos créditos o banco liga ao mesmo originário (mais de 1 = ação coletiva)."""
    cur.execute(SQL_CREDITO, (credito_id,))
    linhas = como_dicts(cur)
    if not linhas:
        raise RuntimeError(f"crédito {credito_id} não existe")
    lead = linhas[0]
    lista = lead["lista"] or {}
    orig = (lista.get("NumeroProcOriginario") or "").strip()
    lead["originario"] = so_digitos(orig) if RE_ORIGINARIO.match(orig) else ""
    lead["ente"] = lista.get("EntidadeDevedora") or lista.get("entidade_nome") or ""
    lead["chaves_ente"] = chaves_ente(lead["ente"])
    lead["valor_historico"] = valor_numerico(lista.get("ValorHistorico")) or valor_numerico(lead["valor_lista"])
    lead["faixa"] = FAIXAS[faixa_da_linha(lista.get("Pago"), lista.get("SituacaoTratada"),
                                          lista.get("OrdemPagamento"))]
    lead["irmaos"] = {}                                 # {credito_id: valor da lista} dos precatórios do mesmo originário
    if lead["originario"]:
        cur.execute("""SELECT co.credito_id,
                              (SELECT x.valor_lista FROM creditos.lista_item x WHERE x.credito_id = co.credito_id
                                ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1)
                         FROM creditos.processo pr
                         JOIN creditos.credito_originario co ON co.processo_id = pr.id
                        WHERE pr.numero_cnj = creditos.cnj_normalizar(%s)""", (formatar_cnj(lead["originario"]),))
        lead["irmaos"] = {cid: valor_numerico(v) for cid, v in cur.fetchall()}
    lead["n_creditos_originario"] = len(lead["irmaos"])
    return lead


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())

# =============================================================================== decisão (só lê: DCP, PJe, eJUD, portal)


def resultado_vazio(lead):
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "ORIGINARIO", "originario": lead["originario"] or None,
            "sistema": "DCP", "regra": "", "fontes": [], "credor": None, "capa": None, "advogados": [],
            "autores": [], "coletiva": False, "n_precatorios": 0, "flags": {}}


def advogados_do_credor(http, lead, advogados, coletiva, fontes):
    """(advogados do credor, advogados que vão para a capa). Originário só deste precatório: todos do polo ativo (com
    OAB), também na capa. Ação coletiva: só o advogado que a página do precatório no eJUD mostra, e NENHUM na capa -
    o recálculo do banco liga todo advogado da capa a todos os precatórios do processo (inclusive os dos outros
    credores); ele fica só no metadata deste crédito."""
    do_ativo = [a for a in advogados if a["polo"] == "ATIVO" and a["oab_numero"]]
    if not coletiva:
        return do_ativo, do_ativo
    nome = advogado_do_precatorio(http, lead["precatorio"])
    fontes.append("EJUD")
    return [a for a in do_ativo if nome and mesma_pessoa(a["nome"], nome)], []


def razao_do_maior(lead):
    """(valor deste precatório / maior precatório do mesmo originário, quantos têm valor), ou (None, n) se não dá
    para comparar (valor desconhecido ou só este precatório no banco)."""
    valores = [v for v in lead["irmaos"].values() if v]
    meu = lead["irmaos"].get(lead["credito_id"]) or valor_numerico(lead["valor_lista"])
    if not meu or len(valores) < 2:
        return None, len(valores)
    return meu / max(max(valores), meu), len(valores)


def e_o_principal(r, lead):
    """Autor único, vários precatórios no originário: este precatório é o do autor? Só quando ele é o maior do processo
    ou pelo menos RAZAO_PRINCIPAL dele (principal, complementar, parcela). Menor que isso pode ser o de honorários do
    advogado (credor = advogado) -> SUCESSO_ANALISAR, sem gravar o autor como credor."""
    cnj = formatar_cnj(lead["originario"])
    razao, n = razao_do_maior(lead)
    r["flags"]["razao_maior_precatorio"] = round(razao, 3) if razao is not None else None
    if razao is None:
        r.update(status="SUCESSO_ANALISAR", credor=None,
                 motivo=f"PRECATORIOS_SEM_VALOR_PARA_COMPARAR: cnj={cnj} precs_com_valor={n}")
        return False
    if razao < RAZAO_PRINCIPAL:
        codigo = "POSSIVEL_HONORARIOS" if razao <= RAZAO_HONORARIOS else "PRECATORIO_MENOR_DO_PROCESSO"
        r.update(status="SUCESSO_ANALISAR", credor=None, motivo=f"{codigo}: cnj={cnj} razao={razao:.2f} precs={n}")
        return False
    return True


def conferir_portal(http, lead, r):
    """Cessão ou herdeiro no precatório (portal de precatórios): o autor pode não ser mais o credor -> analisar."""
    det = detalhes_do_portal(http, lead["precatorio"])
    r["fontes"].append("PORTAL")
    r["flags"].update(det)
    if det["cessao"] or det["herdeiro"]:
        r.update(status="SUCESSO_ANALISAR", capa=None,
                 motivo=f"CESSAO_OU_HERDEIRO_NO_PRECATORIO: cnj={formatar_cnj(lead['originario'])} "
                        f"cessao={'sim' if det['cessao'] else 'nao'} herdeiro={'sim' if det['herdeiro'] else 'nao'}")
        return False
    return True


def finalizar_credor(r, lead, regra, fonte):
    """Status e motivo de quem tem credor definido: com CPF -> SUCESSO_PROCESSO_ORIGINARIO; só com nome ->
    SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF: ...'. O motivo só leva códigos (aparece na vw_processos_unificados)."""
    base = f"cnj={formatar_cnj(lead['originario'])} regra={regra} fontes={'+'.join(r['fontes'])}"
    if r["credor"]["documento"]:
        r.update(status="SUCESSO_PROCESSO_ORIGINARIO", motivo=base)
    else:
        r.update(status="SUCESSO_INCOMPLETO", motivo=f"SUCESSO_SEM_CPF: {base}")
    r["regra"] = regra


def processar_pje(lead, pje, http, r, inicio):
    """Originário no PJe: o polo ativo vem com CPF. Credor = requerente pessoa física único."""
    orig = lead["originario"]
    r.update(sistema="PJE")
    r["fontes"].append("PJE")
    lin = pje.por_numero(orig)
    if not lin or not lin.get("link"):
        r.update(status="FALHA", motivo=f"PROCESSO_NAO_ENCONTRADO: cnj={formatar_cnj(orig)} sistema=PJE")
        return r
    d = pje.detalhe(lin["link"], prazo=inicio + TETO_CREDITO - 15)
    ativos = [p for p in d["partes"] if p["polo"] == "ATIVO"]
    pf = [p for p in ativos if not eh_orgao_publico(p["nome"], lead) and not RE_SOCIEDADE_ADV.search(normal(p["nome"]))]
    if ativos and not pf:
        r.update(status="FALHA", motivo=f"REQTE_ORGAO_PUBLICO: cnj={formatar_cnj(orig)} sistema=PJE")
        return r
    autores = {chave_nome(p["nome"]): p for p in sorted(pf, key=lambda p: not documento_valido(p["documento"]))}
    r["autores"] = [p["nome"] for p in autores.values()]
    r["coletiva"] = len(autores) > 1 or lead["n_creditos_originario"] > 1
    if len(autores) != 1:
        r.update(status="SUCESSO_ANALISAR" if autores else "FALHA",
                 motivo=(f"CREDOR_POLO_ATIVO_SEM_VINCULO: cnj={formatar_cnj(orig)} autores={len(autores)} sistema=PJE"
                         if autores else f"PROC_SEM_CAPA: cnj={formatar_cnj(orig)} polo ativo vazio no PJe"))
        return r
    p = next(iter(autores.values()))
    doc = p["documento"] if len(p["documento"]) == 11 and documento_valido(p["documento"]) else ""
    regra = "AUTOR_UNICO" if lead["n_creditos_originario"] <= 1 else "AUTOR_UNICO_VARIOS_PREC"
    r["credor"] = {"nome": p["nome"], "documento": doc, "como": regra, "nascimento": None,
                   "papel_bruto": p["papel"]}
    if regra == "AUTOR_UNICO_VARIOS_PREC" and not e_o_principal(r, lead):
        return r
    if not conferir_portal(http, lead, r):
        return r
    r["advogados"], na_capa = advogados_do_credor(http, lead, d["advogados"], r["coletiva"], r["fontes"])
    r["capa"] = {"partes": [{"nome": p["nome"], "documento": doc, "polo": "ATIVO", "papel": "CREDOR",
                             "papel_bruto": p["papel"]}] + passivo_para_capa(d["partes"]),
                 "advogados": na_capa, "capa": d["capa"], "completa": False, "sistema": "PJE"}
    finalizar_credor(r, lead, regra, "PJE")
    return r


def passivo_para_capa(partes):
    """Polo passivo para a capa, sempre com papel REU (o texto da fonte fica em papel_bruto): o banco tira o papel
    do texto sem olhar o polo, e um 'EXEQUENTE'/'BENEFICIÁRIO' do passivo viraria CREDOR do precatório."""
    return [{**p, "papel": "REU", "papel_bruto": p.get("papel_bruto") or p["papel"] or "REU"}
            for p in partes if p["polo"] == "PASSIVO"]


def processar(lead, http, pje, ejud, cache, inicio):
    """Decide o credor do precatório pelo originário (DCP, PJe ou eJUD 2º grau). Devolve o resultado a gravar."""
    r = resultado_vazio(lead)
    orig = lead["originario"]
    segundo_grau = orig.endswith("0000")
    if not orig or orig[13:16] != "819" or (segundo_grau and not ejud.n):
        r.update(status="FORA_DO_ESCOPO", motivo="originário ausente, de outro tribunal ou do 2º grau sem o eJUD "
                                                 "(fica para o RPA)")
        return r
    if orig.startswith("08"):                           # numeração do PJe
        return processar_pje(lead, pje, http, r, inicio)
    if segundo_grau:
        return processar_ejud(lead, http, ejud, cache, r, inicio)
    dcp = cache.pegar(orig)
    if dcp is None:
        dcp = consultar_dcp(http, orig)
        cache.por(orig, dcp)
    r["fontes"].append("DCP")
    if not dcp["achou"]:
        if dcp["pje"]:
            return processar_pje(lead, pje, http, r, inicio)
        r.update(status="FALHA", motivo=f"PROCESSO_NAO_ENCONTRADO: cnj={formatar_cnj(orig)} "
                                        f"sistemas={','.join(map(str, dcp['tipos'])) or 'nenhum'}")
        return r
    r["sistema"] = "EPROC" if dcp["migrado_eproc"] else "DCP"
    r["flags"].update(migrado_eproc=dcp["migrado_eproc"], volumoso=dcp["volumoso"])
    return decidir(lead, http, dcp, r)


def processar_ejud(lead, http, ejud, cache, r, inicio):
    """Originário do 2º grau: eJUD 2º grau no Chrome (personagens e precatórios autuados; sem CPF)."""
    orig = lead["originario"]
    r["sistema"] = "EJUD"
    dados = cache.pegar(orig)
    if dados is None:
        dados = consultar_ejud(http, ejud, orig, inicio + TETO_CREDITO - 15)
        cache.por(orig, dados)
    r["fontes"].append("EJUD2G")
    if not dados["achou"]:
        r.update(status="FALHA", motivo=f"PROCESSO_NAO_ENCONTRADO: cnj={formatar_cnj(orig)} sistema=EJUD "
                                        f"tipos={','.join(map(str, dados['tipos'])) or 'nenhum'}")
        return r
    r["flags"]["processo_principal"] = dados["principal"]
    return decidir(lead, http, dados, r)


def decidir(lead, http, dcp, r):
    """Credor pelos dados interpretados do originário (DCP ou eJUD 2º grau): partes, advogados, precatórios
    vinculados ao processo e, no DCP, as certidões dos movimentos."""
    cnj = formatar_cnj(lead["originario"])
    if dcp["segredo"]:
        r.update(status="FALHA", motivo=f"SEGREDO_DE_JUSTICA: cnj={cnj}")
        return r
    vinc = dcp["vinculados"]
    r["n_precatorios"] = len(vinc)
    ativos = [p for p in dcp["partes"] if p["polo"] == "ATIVO"]
    candidatos = [p for p in ativos if RE_PAPEL_CREDOR.search(p["papel"]) and not eh_orgao_publico(p["nome"], lead)
                  and not RE_SOCIEDADE_ADV.search(normal(p["nome"]))]
    if ativos and not candidatos and any(eh_orgao_publico(p["nome"], lead) for p in ativos):
        r.update(status="FALHA", motivo=f"REQTE_ORGAO_PUBLICO: cnj={cnj}")
        return r
    sucessao = any(RE_PAPEL_SUCESSAO.search(p["papel"]) for p in ativos)
    autores = {chave_nome(p["nome"]): p for p in candidatos}
    r["autores"] = [p["nome"] for p in autores.values()]
    r["coletiva"] = len(autores) > 1 or len(vinc) > 1 or lead["n_creditos_originario"] > 1
    if lead["precatorio"] not in vinc:
        r.update(status="SUCESSO_ANALISAR", motivo=f"VINCULO_NAO_CONFIRMADO: cnj={cnj} vinculados={len(vinc)}")
        return r
    # 1) certidão de expedição com o valor bruto igual ao da lista; 2) autor pessoa física único sem sucessão
    valor = lead["valor_historico"]
    certidoes = [c for c in dcp["certidoes"]
                 if valor and c["valor"] is not None and abs(c["valor"] - valor) <= TOLERANCIA_VALOR]
    credor, regra = None, ""
    if certidoes and len({chave_nome(c["nome"]) for c in certidoes}) == 1:
        c = certidoes[0]
        credor = {"nome": c["nome"], "documento": c["cpf"] or "", "como": "CERTIDAO_VALOR",
                  "nascimento": next((x["nascimento"] for x in certidoes if x["nascimento"]), None),
                  "papel_bruto": "BENEFICIARIO DO PRECATORIO"}
        regra = "CERTIDAO_VALOR"
    elif len(autores) == 1 and not sucessao:
        p = next(iter(autores.values()))
        regra = "AUTOR_UNICO" if len(vinc) <= 1 else "AUTOR_UNICO_VARIOS_PREC"
        credor = {"nome": p["nome"], "documento": "", "como": regra, "nascimento": None,
                  "papel_bruto": p["papel"]}
        if regra == "AUTOR_UNICO_VARIOS_PREC" and not e_o_principal(r, lead):
            return r
    if not credor:
        r.update(status="SUCESSO_ANALISAR",
                 motivo=f"CREDOR_POLO_ATIVO_SEM_VINCULO: cnj={cnj} autores={len(autores)} precs={len(vinc)} "
                        f"sucessao={'sim' if sucessao else 'nao'} certidoes={len(dcp['certidoes'])}")
        return r
    r["credor"] = credor
    if any(mesma_pessoa(credor["nome"], a["nome"]) for a in dcp["advogados"]):
        # honorários: o beneficiário é o advogado (fica como advogado; o banco não cria CREDOR sem CPF)
        r["credor"] = {**credor, "documento": ""}
        r["fontes"].append("CERTIDAO")
        finalizar_credor(r, lead, "HONORARIOS_ADVOGADO", r["sistema"])
        return r
    if not credor["documento"]:
        credor["documento"] = cpf_do_nome(credor["nome"], dcp["cpfs"]) or ""
    if credor.get("nascimento") is None:
        credor["nascimento"] = next((c["nascimento"] for c in dcp["certidoes"]
                                     if c["nascimento"] and mesma_pessoa(c["nome"], credor["nome"])), None)
    if regra == "CERTIDAO_VALOR":
        r["fontes"].append("CERTIDAO")
    if not conferir_portal(http, lead, r):
        return r
    r["advogados"], na_capa = advogados_do_credor(http, lead, dcp["advogados"], r["coletiva"], r["fontes"])
    r["capa"] = {"partes": [{"nome": credor["nome"], "documento": credor["documento"], "polo": "ATIVO",
                             "papel": "CREDOR", "papel_bruto": credor["papel_bruto"]}] + passivo_para_capa(dcp["partes"]),
                 "advogados": na_capa, "capa": dcp["capa"], "completa": False, "sistema": r["sistema"]}
    finalizar_credor(r, lead, regra, r["sistema"])
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
        return chave in self.tocados | self.l_tocados | self.p_tocados

    def _criada_aqui(self, tabela, id_):
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
                    f.write("-- Desfaz as mudanças do fetch_TJRJ.py nas tabelas antigas "
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
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJRJ",
                                "Credor do TJRJ: DCP + PJe 1º grau públicos, sem A3 (TJRJ/fetch_TJRJ.py)",
                                raspa_credor=True, criar=criar)


def filas_antigas(cur):
    """Filas mensais do legado (de FILA_ANTIGA_DESDE em diante) em que o usuário pode gravar."""
    return banco.filas_antigas(cur, FILA_ANTIGA_DESDE)[0]


def partes_para_banco(cur, dados, existentes):
    """(partes, advogados) para registrar_capa, SÓ ACRESCENTANDO: o que o banco já tem do processo (reenviado como
    está) + as partes e advogados novos (o registrar_capa troca o conjunto inteiro; mandar só o novo apagaria o resto).
    O CPF que faltar vem do banco (mesmo nome)."""
    do_banco = ([{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "papel": e["papel"],
                  "papel_bruto": e["papel_bruto"]} for e in existentes if e["papel"] != "ADVOGADO"],
                [{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "oab_uf": e["oab_uf"],
                  "oab_numero": e["oab_numero"], "papel_bruto": e["papel_bruto"]}
                 for e in existentes if e["papel"] == "ADVOGADO"])
    doc_banco = {e["chave"]: e["documento"] for e in existentes if e["chave"] and e["documento"]}
    chaves = chave_texto_lote(cur, [p["nome"] for p in dados["partes"]])
    partes = [{"nome": p["nome"], "cpf_cnpj": p["documento"] if documento_valido(p["documento"]) else doc_banco.get(ch),
               "polo": p["polo"], "papel": p["papel"], "papel_bruto": p.get("papel_bruto") or p["papel"]}
              for p, ch in zip(dados["partes"], chaves)]
    advogados = [{"nome": a["nome"], "cpf_cnpj": None, "polo": "ATIVO", "oab_uf": a["oab_uf"],
                  "oab_numero": a["oab_numero"], "papel_bruto": "ADVOGADO"}
                 for a in dados["advogados"] if a["polo"] == "ATIVO" and a["oab_numero"]]
    nomes_banco = {e["chave"] for e in existentes}
    oabs_banco = {(e["oab_uf"], e["oab_numero"]) for e in existentes if e["oab_numero"]}
    return (do_banco[0] + [p for p, ch in zip(partes, chaves) if ch not in nomes_banco],
            do_banco[1] + [a for a in advogados if (a["oab_uf"], a["oab_numero"]) not in oabs_banco])


def capa_para_banco(capa, sistema):
    """Capa (DCP, PJe ou eJUD 2º grau) -> JSON da capa do registrar_capa (classe sem o código, código da classe,
    órgão, grau e o sistema quando ele existe em creditos.sistema_processual)."""
    classe = capa.get("classe_judicial") or ""
    codigo = re.search(r"\((\d+)\)\s*$", classe)
    return {"orgao_julgador": capa.get("orgao_julgador") or None,
            "classe_judicial": re.sub(r"\s*\(\d+\)\s*$", "", classe) or None,
            "classe_codigo": codigo.group(1) if codigo else None, "grau": "G2" if sistema == "EJUD" else "G1",
            "sistema": SISTEMA_BANCO.get(sistema)}


def travar_processo(cur, cnj):
    """O mesmo lock do registrar_capa, pego antes de ler as partes: duas instâncias no mesmo processo
    não se atropelam."""
    cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)", (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if linha:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('creditos.registrar_capa'), %s::int)", (linha[0],))


def gravar_capa_legado(cur, cnj, dados, relacionar, precatorio, bk):
    """Capa antiga do originário (originarios.*), como o RPA grava; só acrescenta partes e advogados."""
    cont, agora, hoje = Counter(), datetime.now(), date.today()
    capa = dados["capa"]
    cnpj = next((p["documento"] for p in dados["partes"] if p["polo"] == "PASSIVO" and len(p["documento"]) == 14), None)
    novos = {"classe_judicial": capa.get("classe_judicial"), "orgao_julgador": capa.get("orgao_julgador"),
             "jurisdicao": capa.get("jurisdicao"), "assunto": capa.get("assunto"),
             "data_autuacao": data_br(capa.get("data_autuacao")), "cnpj_entidade_devedora": cnpj,
             "origem": "TJRJ", "tribunal_sigla": "TJRJ"}
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
    # partes e advogados: só acrescenta (o recorte do credor vale também aqui: o espelho do legado recriaria o resto)
    cur.execute("SELECT * FROM originarios.partes_processuais WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    antigas = como_dicts(cur)
    cpf_antigo = {(a["polo"], normal(a["nome"])): a["cpf_cnpj"] for a in antigas if a["cpf_cnpj"]}
    ja = {(a["polo"], normal(a["nome"])) for a in antigas}
    novas = [(pid, p["polo"], p["nome"],
              p["documento"] if documento_valido(p["documento"]) else cpf_antigo.get((p["polo"], normal(p["nome"]))),
              PAPEL_LEGADO[p["polo"]], "TJRJ", agora, p.get("papel_bruto") or p["papel"], POLO_BRUTO[p["polo"]])
             for p in dados["partes"] if p["polo"] in PAPEL_LEGADO and (p["polo"], normal(p["nome"])) not in ja]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    ja_oab = {(a["oab_uf"], a["oab_numero"]) for a in como_dicts(cur)}
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJRJ", agora,
                                            "ADVOGADO", "AUTOR")
            for a in dados["advogados"] if a["polo"] == "ATIVO" and a["oab_numero"]
            and (a["oab_uf"], a["oab_numero"]) not in ja_oab}
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
    """Status, motivo e originário nas linhas do precatório nas filas mensais (pula o que o RPA está processando),
    para o RPA antigo não reprocessar (com A3) o que o robô já resolveu."""
    cont = Counter()
    status = status_legado[r["status"]]
    originario = [formatar_cnj(r["originario"])] if r["originario"] else None
    for fila in filas:
        cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead,
                               numero_originario, ultima_atualizacao
                          FROM {fila}
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJRJ'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJRJ"}
            bk.update(cur, fila, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                             "numero_originario", "ultima_atualizacao")})
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJRJ'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Resumo da decisão em credito_fonte.metadata do software (a função troca o JSON inteiro: mescla aqui).
    O nome do credor só com nome fica aqui (credito_credor exige CPF/OAB); o CPF não entra."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    lista = lead["lista"] or {}
    credor = r["credor"]
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "fontes": "+".join(r["fontes"]), "sistema": r["sistema"],
                     "coletiva": r["coletiva"], "n_autores": len(r["autores"]), "n_precatorios_originario":
                     r["n_precatorios"], "n_creditos_originario_banco": lead["n_creditos_originario"]}
    meta["credor"] = ({"nome": credor["nome"], "como": credor["como"], "papel": credor.get("papel_bruto"),
                       "data_nascimento": credor.get("nascimento"), "cpf_encontrado": bool(credor["documento"])}
                      if credor else None)
    meta["advogados"] = [{"nome": a["nome"], "oab": f"{a['oab_uf']}{a['oab_numero']}"} for a in r["advogados"]]
    meta["autores"] = r["autores"][:50] if r["status"] == "SUCESSO_ANALISAR" else []
    meta["precatorio_cnj"] = cnj_precatorio(lead["precatorio"])
    meta["flags"] = r["flags"]
    meta["lista"] = {"faixa": lead["faixa"], "prioridade": lista.get("Prioridade"),
                     "precatorio_pai": lista.get("NumeroPrecatorioPai"), "valor_historico": lead["valor_historico"]}
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJRJ', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def corrigir_credor(cur, lead, credor, bk):
    """A fonte vence o banco: com o CPF do credor confirmado na fonte, apaga os vínculos de CREDOR do crédito que são
    a mesma pessoa (mesmo nome ou grafia próxima) com outro documento (o banco audita o DELETE e o desfazer_legado_*.sql
    guarda o INSERT que o recria). Herdeiro, cessionário, sucessor, advogado e credor com outro nome não são tocados.
    Devolve o que foi corrigido (para o CSV, sem documento)."""
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
        corrigidos.append(f"{nome} ({'outro doc' if doc else 'sem doc'}, {linha['origem']}) -> CPF da fonte")
    return corrigidos


def gravar(cur, lead, r, filas, status_legado, bk):
    """Grava o resultado de um crédito (quem chama cuida do SAVEPOINT e do COMMIT). Devolve o resumo para o CSV."""
    cid = lead["credito_id"]
    id_do_software(cur, criar=True)                     # na simulação ele nasce e morre nesta transação
    antes = credores_do_credito(cur, cid)
    resumo, legado, proc, corrigidos = "", Counter(), None, []
    if r["originario"]:
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)",
                    (formatar_cnj(r["originario"]),))
        linha = cur.fetchone()
        proc = linha[0] if linha else None
    credor = r["credor"]
    if credor and credor["documento"] and r["status"] == "SUCESSO_PROCESSO_ORIGINARIO":
        cur.execute("SELECT creditos.documento_de_parte(%s)", (credor["documento"],))
        # antes do registrar_capa: o vínculo fica com origem FONTE, que o recálculo das partes não apaga
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, credor["nome"], credor["documento"], proc, SOFTWARE))
            corrigidos = corrigir_credor(cur, lead, credor, bk)
    if r["capa"] and r["status"] in STATUS_COM_CREDOR and r["originario"]:
        cnj = r["originario"]
        travar_processo(cur, cnj)
        partes, advogados = partes_para_banco(cur, r["capa"], partes_do_banco(cur, formatar_cnj(cnj)))
        capa = capa_para_banco(r["capa"]["capa"], r["capa"]["sistema"])
        cur.execute("SELECT creditos.registrar_capa(%s, %s::jsonb, %s::jsonb, %s::jsonb)",
                    (formatar_cnj(cnj), json.dumps(partes, ensure_ascii=False),
                     json.dumps(advogados, ensure_ascii=False), json.dumps(capa, ensure_ascii=False)))
        res = cur.fetchone()[0]
        resumo = (f"{formatar_cnj(cnj)}: partes +{res['partes_inseridas']}/-{res['partes_removidas']} "
                  f"credores +{res['credores_inseridos']}/-{res['credores_removidos']}")
        legado += gravar_capa_legado(cur, cnj, r["capa"], True, lead["precatorio"], bk)
    registrar_metadata(cur, lead, r)
    legado += atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "fontes": "+".join(r["fontes"]), "sistema": r["sistema"],
               "coletiva": r["coletiva"], "n_autores": len(r["autores"]), "n_precatorios": r["n_precatorios"],
               "credores_antes": fmt_credores(antes), "credores_depois": fmt_credores(depois)}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => %s, p_sistema => %s,
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (cid, WORKER, r["status"], r["motivo"][:2000], r["via"], SISTEMA_BANCO.get(r["sistema"]),
                 formatar_cnj(r["originario"]) if r["originario"] else None,
                 json.dumps(detalhe, ensure_ascii=False, default=str), socket.gethostname()))
    return {"banco": resumo, "legado": ", ".join(f"{k}={v}" for k, v in sorted(legado.items())),
            "antes": antes, "depois": depois, "corrigidos": corrigidos}

# =============================================================================== banco: gravação em lote


def marcar_falha(cur, credito_id, erro):
    """FALHA na fila para o crédito que não gravou, dentro da transação do lote (num SAVEPOINT próprio: se até isso
    falhar, o lote segue)."""
    cur.execute("SAVEPOINT falha")
    try:
        cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s,
                          p_status => 'FALHA', p_motivo => %s, p_host => %s)""",
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
        linha.update(resultado=r["status"], motivo=r["motivo"], banco=g["banco"], legado=g["legado"],
                     credor_corrigido=" | ".join(g["corrigidos"]), credores_antes=fmt_credores(g["antes"]),
                     credores_depois=fmt_credores(g["depois"]), lote=rod.n_lote)
        rod.resultados[r["status"]] += 1
    anexar_csv(rod.arq_credito, COLUNAS, [item["linha"] for item in lote])
    if trocas:
        anexar_csv(rod.arq_trocas, COLUNAS_TROCAS, trocas)
    status = Counter(item["r"]["status"] for item in lote)
    log.info(f"== lote {rod.n_lote}: {gravados}/{len(lote)} gravado(s) "
             f"({'ROLLBACK, simulação' if rod.simulacao else 'COMMIT'}) | "
             + ", ".join(f"{k}: {v}" for k, v in status.most_common())
             + f" | ritmo TJRJ {rod.ritmo_tjrj.atual:.1f} req/s ({rod.ritmo_tjrj.freadas} freada(s)), "
               f"PJe {rod.ritmo_pje.atual:.1f} ({rod.ritmo_pje.freadas})"
             + (f", eJUD {rod.ejud.paginas} página(s) ({rod.ejud.recusas} recusa(s) do reCAPTCHA)"
                if rod.ejud.threads else ""))
    if lote and not gravados:
        log.error("nenhum crédito do lote gravou: robô parado (banco com problema?).")
        rod.parar.set()


def descarregar(rod, pendentes):
    """Grava o lote pendente, registra nos CSVs e esvazia a lista."""
    gravados = gravar_lote(rod, pendentes)
    registrar_lote(rod, pendentes, gravados)
    pendentes.clear()

# =============================================================================== fila: reserva


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
                    (credito_id, TRIBUNAL_TJRJ, id_software, WORKER, lease))
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
                            f"disponivel_em = %s, updated_at = now() WHERE credito_id = %s "
                            f"AND software_id = {rod.id_software} AND status_id <> 2;\n",
                            (status, disponivel, credito_id)).decode())


def pegar(rod):
    """Próximo crédito na ordem do robô que consegue reservar. Acabou a lista: refaz a ordem uma vez (entram os
    adiados que venceram); vazia de novo, a fila acabou (None)."""
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


def devolver_ao_rpa(con, credito_id):
    """Lead reservado que está fora do escopo do robô (sem originário; 2º grau com --chromes 0): volta para o RPA
    em PENDENTE."""
    with con.cursor() as cur:
        cur.execute(f"""UPDATE creditos.coleta_credor
                           SET software_id = {SOFTWARE_RPA}, status_id = 1, lease_worker = NULL, lease_ate = NULL,
                               reservado_em = NULL, updated_at = now()
                         WHERE credito_id = %s AND lease_worker = %s""", (credito_id, WORKER))
    con.commit()


def devolver(con, credito_id, motivo=None, intervalo=ADIAMENTO):
    """Volta para a fila daqui a `intervalo` (erro passageiro); sem motivo (Ctrl+C), volta já."""
    with con.cursor() as cur:
        if motivo:
            cur.execute("SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
                        (credito_id, WORKER, intervalo, motivo[:2000]))
        else:
            cur.execute("SELECT creditos.fila_credor_liberar(%s, %s)", (credito_id, WORKER))
    con.commit()

# =============================================================================== execução


class Rodada:
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, cache do DCP, PJe/HTTP por thread, os
    Chromes do eJUD 2º grau e contadores. Ao nascer confere o reCAPTCHA do DCP, prepara o banco (software, filas
    antigas, status do legado) e a ordem da fila (na simulação, a amostra sai dela e nada é reservado)."""

    def __init__(self, simulacao, limite, workers, ritmo=RITMO_TJRJ, creditos=(), chromes=CHROMES):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers = simulacao, limite, workers
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJRJ{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = Backup(SAIDA / f"desfazer_legado_{self.rodada}{sufixo}.sql",
                         SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo)
        self.resultados, self.falhas_seguidas, self.n, self.n_lote = Counter(), 0, 0, 0
        self.parar, self.fila_vazia = threading.Event(), False
        # até 2x workers créditos em voo (os da fila do executor esperam): o lease cobre o lote e a espera
        self.lease = f"{(LOTE // workers + 4) * TETO_CREDITO // 60 + 30} minutes"
        self.ritmo_tjrj, self.ritmo_pje = Ritmo("TJRJ", ritmo), Ritmo("PJe", min(RITMO_PJE, ritmo))
        self.local, self.conexoes, self.trava = threading.local(), [], threading.Lock()
        self.cache, self.faixas = CacheDcp(), {}
        self.ejud = Ejud(self.parar, chromes)            # os Chromes só abrem no 1º lead do 2º grau
        self.http = Http(self.parar, self.ritmo_tjrj)   # da thread principal (conferência do captcha)
        if captcha_dcp_ligado(self.http):
            raise SystemExit("O DCP do TJRJ voltou a exigir reCAPTCHA (recuperar-site-key devolveu uma chave): "
                             "o robô não roda assim.")
        self.ultimo_captcha = time.time()

        self.con = conectar(escrita=True)
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | ritmo {ritmo:.1f} req/s (PJe {self.ritmo_pje.teto:.1f}) | "
                 f"eJUD 2º grau: {f'{chromes} Chrome(s)' if chromes else 'desligado'} | lease {self.lease} | "
                 f"filas antigas: {', '.join(self.filas)}")
        if simulacao:
            ordem = [] if creditos else self.ordenar()
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
        """Ordem do robô (T1, T2, T3; maior valor primeiro) dos créditos pegáveis e dentro do escopo."""
        con_l = conectar()
        try:
            ordem, self.faixas, contagem = ordenar_escopo(con_l, FILTRO_PEGAVEL, com_2grau=bool(self.ejud.n))
        finally:
            con_l.close()
        log.info(f"ordem da fila: {len(ordem)} créditos | " + ", ".join(f"{s}: {contagem[s]}" for s in FAIXAS.values()))
        return ordem

    def reordenar(self):
        """Remonta a ordem da fila e volta ao começo dela (repete a cada REORDENAR_A_CADA)."""
        self.ordem, self.posicao, self.ultima_ordem = self.ordenar(), 0, time.time()

    def da_thread(self):
        """(conexão de leitura, PJe, HTTP) da thread que chama; criados na 1ª vez."""
        if not getattr(self.local, "con", None):
            self.local.con, self.local.pje, self.local.http = (conectar(), Pje(self.parar, self.ritmo_pje),
                                                               Http(self.parar, self.ritmo_tjrj))
            with self.trava:
                self.conexoes.append(self.local.con)
        return self.local.con, self.local.pje, self.local.http

    def fechar(self):
        """Fecha os Chromes do eJUD e as conexões (escrita e as de leitura das threads)."""
        self.ejud.fechar()
        for con in [self.con] + self.conexoes:
            try:
                con.close()
            except Exception:
                pass


def proximo_credito(rod):
    """Id do próximo crédito, ou None (amostra acabou, fila vazia, --limite, parada por falhas ou captcha).
    Fora da simulação reserva o crédito com lease e, a cada REORDENAR_A_CADA, remonta a ordem da fila."""
    if rod.parar.is_set() or rod.fila_vazia or (rod.limite and rod.n >= rod.limite):
        return None
    if time.time() - rod.ultimo_captcha > CHECAR_CAPTCHA_A_CADA:
        rod.ultimo_captcha = time.time()
        try:
            if captcha_dcp_ligado(rod.http):
                log.error("o DCP do TJRJ voltou a exigir reCAPTCHA: robô parado.")
                rod.parar.set()
                return None
        except ErroTecnico as e:
            log.warning(f"não deu para conferir o captcha do DCP agora: {e}")
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
        log.info("fila do TJRJ vazia." if not rod.simulacao else "amostra acabou.")
    return credito_id


def processar_credito(rod, credito_id, n):
    """(Numa thread) Lê o crédito e decide o credor (DCP/PJe/eJUD/portal), sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r}, {tipo: ADIAR, ...} ou {tipo: IGNORAR, ...}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    try:
        con, pje, http = rod.da_thread()
        with con.cursor() as cur:
            lead = ler_credito(cur, credito_id)
        linha.update(precatorio=lead["precatorio"], faixa=lead["faixa"], ente=lead["ente"],
                     originario=formatar_cnj(lead["originario"]) if lead["originario"] else "")
        r = processar(lead, http, pje, rod.ejud, rod.cache, inicio)
        if time.time() - inicio > TETO_CREDITO:
            raise ErroTecnico("TIMEOUT_PAGINA_PROCESSO: o crédito passou do tempo máximo")
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
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": motivo, "linha": linha, "tecnico": True}
    credor = r["credor"] or {}
    linha.update(segundos=round(time.time() - inicio), sistema=r["sistema"], regra=r["regra"],
                 fontes="+".join(r["fontes"]), credor=credor.get("nome", ""),
                 cpf_encontrado="sim" if credor.get("documento") else ("nao" if credor else ""),
                 data_nascimento=credor.get("nascimento") or "", n_autores=len(r["autores"]),
                 n_precatorios=r["n_precatorios"], coletiva="sim" if r["coletiva"] else "",
                 advogados=" | ".join(f"{a['nome']} ({a['oab_uf']}{a['oab_numero']})" for a in r["advogados"]))
    log.info(f"[{n}] {credito_id} {lead['precatorio']} ({lead['faixa']}) -> {r['status']} "
             f"{formatar_cnj(r['originario']) if r['originario'] else ''} {r['sistema']} {r['regra']} "
             f"{'CPF sim' if credor.get('documento') else ''} | {linha['segundos']} s")
    if r["status"] == "FORA_DO_ESCOPO":
        linha.update(resultado=r["status"], motivo=r["motivo"])
        return {"tipo": "IGNORAR", "credito_id": credito_id, "linha": linha}
    return {"tipo": "LOTE", "linha": linha, "lead": lead, "r": r}


def tratar(rod, item, pendentes):
    """(Na thread principal) Resultado de um crédito: vai para o lote, volta para a fila ou é deixado para o RPA."""
    if item["tipo"] == "LOTE":
        rod.falhas_seguidas = 0
        pendentes.append(item)
        return
    if not rod.simulacao:
        if item["tipo"] == "IGNORAR":
            devolver_ao_rpa(rod.con, item["credito_id"])    # fora do escopo: volta para o RPA
        else:
            devolver(rod.con, item["credito_id"], item["motivo"], ADIAMENTO)
    anexar_csv(rod.arq_credito, COLUNAS, [item["linha"]])
    rod.resultados[item["linha"]["resultado"]] += 1
    if item["tipo"] == "IGNORAR":
        return
    rod.falhas_seguidas += 1
    log.warning(f"{item['credito_id']} -> ADIADO: {item['motivo']}")
    if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (DCP/PJe/eJUD fora ou bloqueando).")
        rod.parar.set()


def encerrar(rod, pendentes, inicio):
    """Grava o lote incompleto que sobrou (fila vazia, --limite, Ctrl+C, parada), fecha as conexões e imprime o
    resumo. Se o último lote não gravar, os créditos dele voltam para a fila."""
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
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --ritmo, --chromes N e --creditos."""
    ap = argparse.ArgumentParser(description="Credor do TJRJ pela consulta pública (DCP + PJe 1º grau + eJUD 2º grau, "
                                             f"sem A3), gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS})")
    ap.add_argument("--ritmo", type=float, default=RITMO_TJRJ,
                    help=f"teto de requisições por segundo ao TJRJ, somando todos os workers (padrão {RITMO_TJRJ}); o robô freia sozinho quando o servidor reclama")
    ap.add_argument("--chromes", type=int, default=CHROMES,
                    help=f"Chromes abertos para o eJUD 2º grau (padrão {CHROMES}); 0 deixa os leads do 2º grau para o RPA")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
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
    if args.ritmo <= 0:
        raise SystemExit("--ritmo precisa ser maior que zero")
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), args.ritmo, creditos, max(0, args.chromes))
    pendentes, em_voo = [], {}
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjrj")
    try:
        while True:
            # 2x workers em voo: enquanto a thread principal grava um lote, os workers seguem com a fila do executor
            while len(em_voo) < 2 * rod.workers and (credito_id := proximo_credito(rod)):
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
