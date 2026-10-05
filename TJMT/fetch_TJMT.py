"""
fetch_TJMT.py - credor dos precatórios do TJMT pela consulta pública, de ponta a ponta, sem navegador e sem login:
lê o banco, acha o credor (DJEN + consulta processual do TJMT), confirma o originário e grava no banco em lotes de
LOTE créditos (LOTE_GRAVACAO_TJMT no .env). No lugar do modo credor do RPA_SISTEMAS, que não abre os precatórios do
TJMT (são sigilosos no PJe: 12 mil tentativas do RPA deram PROCESSO_NAO_ENCONTRADO).

A lista do TJMT não traz o beneficiário nem o originário, e o precatório é sigiloso, então:
1. Fila: não pagos primeiro (Situação da lista), depois prioridade de campanha, sem credor antes de com credor e maior
   valor. Cada crédito é reservado com lease (reservar) e só nessa hora passa do RPA para o software próprio
   (CONSULTA_PUBLICA_TJMT); o que o RPA tinha fica em desfazer_fila_*.sql.
2. DJEN pelo nº do precatório: o credor vem só em INICIAIS no polo A ('J. P. S.', com as partículas: 'J. P. D. S.' =
   JOAO PEREIRA DA SILVA) e os advogados vêm com nome e OAB. Precatório antigo (sistema legado, até 2019): a consulta
   processual traz o nome inteiro do credor e o advogado. Sem publicação: volta para a fila em ADIAMENTO_SEM_PUBLICACAO.
3. Consulta processual do TJMT (API pública hellsgate, sem captcha; devolve as partes com CPF/CNPJ completo): os
   processos de cada advogado do precatório contra o ente devedor (NomeOab + parteNome; sem nenhum, só o advogado com
   um ente público no polo passivo). Candidato = parte do polo ativo com as mesmas iniciais (ou o mesmo nome).
4. Evidência de que o candidato é o originário deste precatório, no DJEN do candidato: o nº do precatório citado, o
   valor requisitado exato, ou a certidão do 'formulário (espelho) do precatório' / 'Expeça-se o precatório' perto da
   data de envio do precatório (JANELA_ESPELHO).
5. Decide: 1 pessoa com evidência -> liga o originário e o credor (SUCESSO_PROCESSO_ORIGINARIO); 1 pessoa só pelas
   iniciais, sem evidência -> SUCESSO_ANALISAR (CREDOR_SO_POR_INICIAIS, regra do usuário de 01/10/2026; o candidato vai
   só para o metadata); várias pessoas -> a única com evidência, ou SUCESSO_ANALISAR (CANDIDATOS_POR_INICIAIS).
6. Junta LOTE créditos processados e grava o lote numa transação só, cada crédito no seu SAVEPOINT (erro desfaz só
   ele): originário, credor (registrar_credor, corrigindo o CPF divergente do banco), capa recortada do originário
   (só o credor, o polo passivo e os advogados do precatório: o recálculo do banco ligaria todos os autores de uma
   ação coletiva), metadata (registrar_credito), capa antiga e filas mensais (legado) e o status (fila_credor_finalizar).

Rápido: WORKERS threads raspam ao mesmo tempo; só a thread principal pega da fila e grava. O DJEN tem um relógio por
saída (direta e, com --proxies, PROXY_01..05 do .env, que precisam sair pelo Brasil). A consulta do TJMT tem um ritmo
global que se ajusta sozinho (--ritmo). Os processos de cada advogado ficam em cache (o mesmo advogado aparece em
muitos precatórios).

Erro passageiro (DJEN/TJMT fora, timeout) devolve o crédito para a fila (fila_credor_adiar) na hora, nunca vira FALHA.
Ctrl+C devolve os créditos em andamento e grava o lote que já estava processado. O texto das publicações do DJEN não é
gravado em lugar nenhum.

Saídas em TJMT/saida: fetch_TJMT.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql e desfazer_fila_*.sql.

Uso:
    python fetch_TJMT.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJMT.py                        # processa a fila do TJMT até acabar (Ctrl+C para parar)
    python fetch_TJMT.py --limite 30            # para depois de 30 créditos
    python fetch_TJMT.py --workers 6            # workers (threads) raspando ao mesmo tempo (padrão 4)
    python fetch_TJMT.py --ritmo 6              # req/s na consulta do TJMT (padrão 4)
    python fetch_TJMT.py --proxies              # soma PROXY_01..05 como saídas extras do DJEN
"""
import argparse
import html as H
import json
import logging
import os
import re
import socket
import sys
import threading
import time
import unicodedata
from collections import Counter, OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path

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
load_dotenv(AQUI.parent / ".env")      # PG_*, LOTE_GRAVACAO_TJMT e (com --proxies) PROXY_01..05
log = logging.getLogger("fetch_TJMT")


def lote_do_env():
    """Créditos por transação de gravação, de LOTE_GRAVACAO_TJMT no .env (inteiro maior que zero)."""
    valor = os.environ.get("LOTE_GRAVACAO_TJMT", "").strip()
    if not valor.isdigit() or int(valor) < 1:
        raise SystemExit(f"LOTE_GRAVACAO_TJMT no .env precisa ser um inteiro maior que zero (veio {valor!r}).")
    return int(valor)


TRIBUNAL_TJMT = 111
SOFTWARE = "CONSULTA_PUBLICA_TJMT"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJMT:consulta_publica:{os.getpid()}"
LOTE = lote_do_env()                   # créditos por transação de gravação
WORKERS = 4                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 10 * 60                 # s por crédito; passou disso, volta para a fila (1ª lista de um advogado grande)
ADIAMENTO = "30 minutes"               # erro passageiro
ADIAMENTO_SEM_PUBLICACAO = "15 days"   # precatório ainda sem publicação no DJEN
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
MAX_FALHAS_SEGUIDAS = 8                # falhas técnicas seguidas que param a rodada
MAX_REINICIOS = 30                     # rodadas que param (fonte fora, banco caiu) antes de desistir de vez
PAUSA_REINICIO = 5 * 60                # s entre uma rodada que parou e a próxima
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"
# Situação da lista: não pagos primeiro
NAO_PAGOS = {"AGUARDANDO PAGAMENTO", "PAGAMENTO PREFERENCIAL", "AUTUADO", "PROVISIONADO"}
FAIXAS = {1: "NAO_PAGO", 2: "PAGO_EM_PARTE_OU_OUTRA_SITUACAO"}

DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
INTERVALO_DJEN = 0.8                   # s entre consultas por saída (sobe sozinho quando o DJEN devolve 429)
PAUSA_429 = 10                         # s que a saída fica parada depois de um 429 (o intervalo também sobe)
INTERVALO_DJEN_MAX = 6.0
FALHAS_PARA_DESLIGAR_PROXY = 3
MAX_PAGINAS_DJEN_PRECATORIO = 3        # 100 publicações por página
MAX_CACHE_DJEN = 5000
# pré-carga: todas as publicações da Presidência do TJMT (só PRECATÓRIO), mês a mês, no lugar de 1 consulta por lead
ORGAO_PRESIDENCIA = 31259
DJEN_DESDE = date(2022, 1, 1)          # antes disso o DJEN não tem publicação da Presidência do TJMT
TETO_DJEN_CONSULTA = 10000             # o DJEN não passa disso numa consulta: mês acima disso é dividido
PASTA_DJEN = SAIDA / "djen_presidencia"
UM_DIA = timedelta(days=1)

CONSULTA = "https://hellsgate.tjmt.jus.br/consultaprocessual/ProcessosJudiciais/v2"
H_CONSULTA = {"Accept": "application/json, text/plain, */*", "Origin": "https://consultaprocessual.tjmt.jus.br",
              "Referer": "https://consultaprocessual.tjmt.jus.br/"}
TAKE = 60                              # a API recusa Take acima de 60
MAX_PAGINAS_ADVOGADO = 100             # 6.000 processos por advogado (+ ente) no máximo
MAX_PAGINAS_NOME = 5                   # pesquisa pelo nome inteiro do credor (precatório antigo)
MAX_ADVOGADOS = 6                      # advogados do precatório pesquisados (o DJEN os lista em ordem qualquer)
MAX_CACHE_ADVOGADOS = 300              # listas de processos (enxutas) guardadas entre créditos
TIMEOUT_HTTP = 90
RITMO_CONSULTA = 4.0                   # req/s para a consulta do TJMT (--ritmo)
RITMO_MINIMO = 0.5
PAUSA_ERRO = 60                        # s que todos os workers param depois de um erro de servidor
SUBIR_A_CADA = 200                     # respostas boas seguidas para subir o ritmo em 10% (até o teto)
HTTP_FREIA = {403, 429, 502, 503, 504}  # respostas que indicam carga/bloqueio: freiam o ritmo

MAX_ORIGINARIOS_DJEN = 8               # candidatos a originário consultados no DJEN (evidência) por crédito
MAX_ORIGINARIOS_PESSOA = 3             # ... por pessoa candidata
JANELA_ESPELHO = (-200, 20)            # dias (publicação - envio do precatório) em que a certidão do espelho conta
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"

CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")
PARTICULAS = {"DE", "DA", "DO", "DAS", "DOS", "E", "D"}
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
# classes que não geram precatório contra o ente, e as recursais (o mesmo número aparece também no 1º grau)
RE_CLASSE_FORA = re.compile(r"PENAL|CRIMIN|CARTA PRECATORIA|CARTA DE ORDEM|INQUERITO|ALVARA|INVENTARIO|ARROLAMENTO|"
                            r"DIVORCIO|ALIMENTOS|TERMO CIRCUNSTANCIADO|MEDIDAS PROTETIVAS|EXECUCAO FISCAL|"
                            r"BUSCA E APREENSAO|PRECATORIO|REQUISICAO|PEQUENO VALOR|APELACAO|AGRAVO|RECURSO|"
                            r"EMBARGOS DE DECLARACAO|REMESSA NECESSARIA|CONFLITO DE COMPETENCIA")
RE_ORGAO_PUBLICO = re.compile(r"^(?:ESTADO D|MUNICIPIO D|UNIAO\b|DISTRITO FEDERAL)|PROCURADORIA|DEFENSORIA PUBLICA|"
                              r"MINISTERIO PUBLICO|FAZENDA PUBLICA|PREFEITURA|CAMARA MUNICIPAL|TRIBUNAL D|"
                              r"INSTITUTO NACIONAL DO SEGURO SOCIAL")
# polo passivo de originário de precatório (modo sem o ente na pesquisa)
RE_PASSIVO_PUBLICO = re.compile(r"MUNICIPIO|ESTADO D|INSTITUTO|FUNDACAO|AUTARQUIA|SECRETARI|PREVID|DEPARTAMENTO|"
                                r"UNIVERSIDADE|CAMARA|DETRAN|FAZENDA|PREFEITURA|AGENCIA|EMPRESA MATOGROSSENSE|MTPREV")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA|ESCRITORIO DE ADVOCACIA")
RE_JUIZO = re.compile(r"\bVARA\b|\bJUIZO\b|\bJUIZADO\b|\bCOMARCA\b|\bSDCR\b|TRIBUNAL")
RE_EXECUCAO = re.compile(r"CUMPRIMENTO|EXECU")
# certidão do espelho do precatório (Ofício Circular 24/2024-PRES): sai quando ESTE precatório é formado
RE_ESPELHO = re.compile(r"espelho\)?\s+do\s+precat|formul[aá]rio\s*\(espelho\)", re.I)
# ordem de expedição: mais fraca (às vezes é texto padrão: 'se ultrapassado o teto da RPV, expeça-se precatório')
RE_EXPEDICAO = re.compile(r"expe[çc]a-se\s+(?:o\s+)?(?:of[ií]cio\s+)?(?:precat|requisit)|expedi[çc][aã]o\s+d[oae]\s+"
                          r"(?:of[ií]cio\s+)?(?:precat|requisit)|precat[óo]rio\s+expedido|of[ií]cio\s+requisit[oó]rio",
                          re.I)
# força de cada evidência (>= FORCA_MINIMA confirma o originário)
FORCA = {"NUM_PRECATORIO": 4, "VALOR": 3, "ESPELHO_PERTO": 3, "EXPEDICAO_PERTO": 2, "ESPELHO_LONGE": 1,
         "EXPEDICAO_LONGE": 1}
FORCA_MINIMA = 2
PAPEIS_CREDOR = {"AUTOR", "AUTORA", "EXEQUENTE", "REQUERENTE", "IMPETRANTE", "RECLAMANTE", "EMBARGADO", "CREDOR"}
UFS = {"AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
       "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}
RE_PREFIXO_POLO = re.compile(r"^(?:(?:ATIVO|PASSIVO|OUTROS?)\s*/\s*)+", re.I)

SEMELHANCA_MESMA_PESSOA = 0.9          # nomes com grafia próxima (SOUSA x SOUZA) contam como a mesma pessoa
PAPEL_LEGADO = {"ATIVO": "REQUERENTE", "PASSIVO": "REQUERIDO"}    # como o RPA grava a capa antiga
POLO_BRUTO = {"ATIVO": "AUTOR", "PASSIVO": "REU"}
STATUS_COM_VINCULO = ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_PARTES_SEM_VALOR", "SUCESSO_INCOMPLETO")

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "faixa", "ente", "ultimo_status", "resultado", "motivo",
           "originario", "regra", "evidencia", "credor", "credor_documento", "credor_djen", "publicacoes_djen",
           "advogados_djen", "n_pessoas", "candidatos", "credor_corrigido", "banco", "legado", "credores_antes",
           "credores_depois", "lote", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set(), "corrigidos": []}


class ErroTecnico(Exception):
    """Falha passageira (DJEN/TJMT fora do ar, timeout): o crédito volta para a fila, não vira FALHA."""

# =============================================================================== utilidades


def sem_acento(t):
    """Maiúsculas e sem acento, com a pontuação preservada."""
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples (para comparar nomes)."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", sem_acento(t))).strip()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE' e sem o 'REGISTRADO(A) CIVILMENTE COMO ...'."""
    return re.split(r"\bREGISTRAD[OA]\b", normal(RE_ESPOLIO.sub("", nome or "")))[0].strip()


def mesma_pessoa(a, b):
    """Mesmo nome, ou grafia próxima o bastante (SOUSA x SOUZA, acento)."""
    a, b = chave_nome(a), chave_nome(b)
    return a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA


def eh_iniciais(t):
    """'E. J. S.' / 'M. G. &. V. B. A. A.' -> True (o DJEN publica o credor do precatório sigiloso assim)."""
    tokens = re.findall(r"[^\s.]+", t or "")
    return "." in (t or "") and len(tokens) >= 2 and all(len(x) == 1 for x in tokens)


def iniciais(nome, particulas=True):
    """Primeira letra de cada palavra: 'JOAO PEREIRA DA SILVA' -> 'JPDS' (sem partículas: 'JPS')."""
    return "".join(w[0] for w in chave_nome(nome).split() if particulas or w not in PARTICULAS)


def iniciais_publicadas(t):
    """'E. J. D. S.' -> 'EJDS' (só as letras)."""
    return re.sub(r"[^A-Z]", "", sem_acento(t))


def casa_iniciais(nome, alvo):
    """2 = iniciais idênticas às publicadas (com partículas, como o DJEN faz); 1 = o DJEN tem um 'D' de partícula a
    mais ('R. R. D. C.' para RAUL RAMOS CORTES); 0 = não casa."""
    if iniciais(nome) == alvo:
        return 2
    sem = iniciais(nome, False)
    return 1 if len(sem) >= 2 and len(alvo) - len(sem) <= 1 and sem == alvo[0] + alvo[1:].replace("D", "") else 0


def termo_do_ente(ente):
    """Texto da pesquisa pelo ente na consulta (busca por trecho, sem acento): a cidade do município, o nome do
    Estado, 'SEGURO SOCIAL' para o INSS (a lista escreve 'SEGURIDADE'); sem a sigla depois do ' - ' ('DEPARTAMENTO
    DE ÁGUA E ESGOTO DE VÁRZEA GRANDE - DAE/VG')."""
    n = normal(ente)
    if re.search(r"\bINSS\b|SEGURIDADE SOCIAL|SEGURO SOCIAL", n):
        return "SEGURO SOCIAL"
    n = normal(re.split(r"\s+-\s+", ente)[0]) or n
    if "MTPREV" in n or "MATO GROSSO PREVIDENCIA" in n:
        return "PREVIDENCIA"
    m = re.match(r"MUNICIPIO D[EOA]S? (.+)", n)
    return m.group(1) if m else n


def formatos_valor(valor):
    """41831.2 -> {'41.831,20', '41831,20'}."""
    br = f"{float(valor):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return {br, br.replace(".", "")}


def valor_numerico(v):
    """Valor do banco ou da lista ('R$ 41.831,20', '41831.20', 41831.2) -> float; None se não for número."""
    if v is None or isinstance(v, (int, float)):
        return v
    s = re.sub(r"[^\d,.\-]", "", str(v))
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


def papel_ativo(descricao):
    """'Autor(a)' -> 'AUTOR', 'EXEQUENTE' -> 'EXEQUENTE'; papel fora de PAPEIS_CREDOR (RECONVINTE, ESPÓLIO...) vira
    AUTOR, senão o banco o classifica como OUTRO e o recálculo nunca o reconhece como credor. O texto cru vai em
    papel_bruto."""
    p = normal(re.sub(r"\(.*?\)", "", descricao or "")).split()
    return p[0] if p and p[0] in PAPEIS_CREDOR else "AUTOR"


def oab_da_api(texto):
    """OAB como a consulta escreve -> (uf, número) como o banco guarda (só os dígitos: o TJMT escreve 12027-O no DJEN
    e 12027 na consulta): '12027' e '33774/O' -> MT; '7898-B/MT' e '2569/MT' -> MT; '147427 RJ' e '107.016/RJ' -> RJ
    (ponto de milhar fora). UF é a 1ª sigla de estado depois do número; sem nenhuma, MT."""
    s = sem_acento(texto).replace(".", "")
    m = re.search(r"\d{2,7}", s)
    if not m:
        return "", ""
    siglas = [x for x in re.findall(r"(?<![A-Z])[A-Z]{2}(?![A-Z])", s[m.end():]) if x in UFS] or \
        [x for x in re.findall(r"(?<![A-Z])[A-Z]{2}(?![A-Z])", s[:m.start()]) if x in UFS]
    uf = siglas[0] if siglas else "MT"
    return uf, m.group(0)

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
    Cache só na memória e só das consultas que se repetem entre créditos (originário candidato)."""

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
        """Publicações da consulta (enxutas). ErroTecnico se não responder. O DJEN às vezes devolve 500 'muito
        ocupado': conta como resposta ruim e tenta de novo."""
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
        """Só o que o robô usa da publicação: número, data, partes por polo, advogados com OAB e (se pedido) o texto,
        que fica só na memória."""
        advogados = []
        for a in item.get("destinatarioadvogados") or []:
            adv = a.get("advogado") or {}
            if adv.get("nome"):
                advogados.append({"nome": re.split(r"\s+REGISTRAD[OA]\(?A?\)?\s+CIVILMENTE", adv["nome"])[0].strip(),
                                  "oab": str(adv.get("numero_oab") or ""), "uf": (adv.get("uf_oab") or "").upper()})
        texto = H.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", item.get("texto") or ""))) if com_texto else ""
        return {"numero": so_digitos(item.get("numero_processo") or item.get("numeroprocessocommascara"))[:20],
                "data": (item.get("data_disponibilizacao") or "")[:10],
                "partes": [[d.get("polo"), d.get("nome") or ""] for d in item.get("destinatarios") or []],
                "advogados": advogados, "texto": texto}

    def _periodo(self, ini, fim):
        """Publicações da Presidência do TJMT entre ini e fim (datas); período que satura o DJEN é dividido ao meio."""
        itens, pagina = [], 1
        while True:
            lote = self.buscar(False, False, siglaTribunal="TJMT", orgaoId=ORGAO_PRESIDENCIA, pagina=pagina,
                               itensPorPagina=100, dataDisponibilizacaoInicio=ini.isoformat(),
                               dataDisponibilizacaoFim=fim.isoformat())
            itens += lote
            if len(lote) < 100:
                return itens
            pagina += 1
            if pagina * 100 > TETO_DJEN_CONSULTA and fim > ini:
                meio = ini + (fim - ini) / 2
                return self._periodo(ini, meio) + self._periodo(meio + UM_DIA, fim)

    def carregar_presidencia(self):
        """Índice {precatório20: [publicações]} com tudo o que a Presidência do TJMT publicou desde DJEN_DESDE. Cada
        mês fica em PASTA_DJEN/AAAA-MM.json: mês fechado não é baixado de novo; o mês corrente e o anterior sim
        (publicação nova)."""
        PASTA_DJEN.mkdir(parents=True, exist_ok=True)
        hoje, inicio, baixados = date.today(), time.time(), 0
        mes_anterior = (date(hoje.year, hoje.month, 1) - UM_DIA).replace(day=1)
        mes, indice = DJEN_DESDE, {}
        while mes <= hoje:
            prox = (mes + timedelta(days=32)).replace(day=1)
            arquivo = PASTA_DJEN / f"{mes:%Y-%m}.json"
            if arquivo.exists() and mes < mes_anterior:
                itens = json.loads(arquivo.read_text(encoding="utf-8"))
            else:
                itens = self._periodo(mes, min(prox - UM_DIA, hoje))
                arquivo.write_text(json.dumps(itens, ensure_ascii=False), encoding="utf-8")
                baixados += 1
            for it in itens:
                indice.setdefault(it["numero"], []).append(it)
            mes = prox
        self.indice = indice
        log.info(f"DJEN da Presidência: {sum(len(v) for v in indice.values())} publicações de {len(indice)} "
                 f"precatórios desde {DJEN_DESDE:%m/%Y} ({baixados} mês(es) baixado(s), {time.time() - inicio:.0f} s)")

    def do_precatorio(self, prec20):
        """Publicações do precatório (sigiloso: sem texto útil): do índice da Presidência; fora dele, o DJEN direto
        (publicação de hoje ou de outro órgão)."""
        if prec20 in getattr(self, "indice", {}):
            return self.indice[prec20]
        itens = []
        for pagina in range(1, MAX_PAGINAS_DJEN_PRECATORIO + 1):
            lote = self.buscar(False, False, pagina=pagina, itensPorPagina=100, numeroProcesso=prec20,
                               siglaTribunal="TJMT")
            itens += lote
            if len(lote) < 100:
                break
        return itens

    def do_originario(self, numero20):
        """Publicações de um candidato a originário, com o texto (nº do precatório, valor, espelho)."""
        return self.buscar(True, True, numeroProcesso=numero20, itensPorPagina=100)

# =============================================================================== consulta processual do TJMT (API)


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


def enxugar(p):
    """Processo da API -> só o que o robô usa (fica em cache): número, sistema, capa e partes por polo."""
    partes = {"A": [], "P": []}
    for x in p.get("partes") or []:
        polo = (x.get("tipoParticipacaoProcessual") or "")[:1]
        if polo not in partes or not x.get("nome"):
            continue
        doc = next((so_digitos(d.get("numero")) for d in x.get("documentos") or []
                    if (d.get("descricaoTipo") or "").upper() in ("CPF", "CNPJ")
                    and documento_valido(d.get("numero"))), "")
        advs = []
        for a in x.get("advogados") or []:
            docs = a.get("documentos") or []
            oab = next((d.get("numero") for d in docs if (d.get("descricaoTipo") or "") == "OAB"), "")
            cpf = next((so_digitos(d.get("numero")) for d in docs if (d.get("descricaoTipo") or "").upper() == "CPF"
                        and documento_valido(d.get("numero"))), "")
            uf, numero = oab_da_api(oab) if oab else ("", "")
            advs.append({"nome": " ".join((a.get("nome") or "").split()), "oab_uf": uf, "oab_numero": numero,
                         "cpf": cpf})
        partes[polo].append({"nome": " ".join(x["nome"].split()), "documento": doc,
                             "papel_bruto": ((x.get("tipo") or {}).get("descricao") or "").strip(),
                             "nascimento": (x.get("dataNascimento") or "")[:10] or None, "advogados": advs})
    classe = p.get("classe") or {}
    return {"numero": p.get("numeroUnico") or "", "sistema": p.get("idSistema"),
            "classe": (classe.get("nome") or "").strip(), "classe_codigo": classe.get("codigo") or None,
            "grau": "G2" if p.get("idInstancia") == 2 else "G1", "segredo": bool(p.get("segredo")),
            "orgao": p.get("orgaoJulgador") or "", "jurisdicao": (p.get("jurisdicao") or {}).get("nome") or "",
            "data": (p.get("dataHoraInicio") or "")[:10], "valor_causa": p.get("valorCausa"),
            "arquivado": bool(p.get("arquivado")), "ativo": partes["A"], "passivo": partes["P"]}


class Consulta:
    """API pública da consulta processual do TJMT (hellsgate), dividida entre as threads: ritmo global, 3 tentativas
    por página e cache das listas de processos por (advogado, ente)."""

    def __init__(self, parar, ritmo):
        self.parar, self.ritmo = parar, Ritmo("TJMT", ritmo)
        self.local = threading.local()
        self.cache, self.travas, self.trava = OrderedDict(), {}, threading.Lock()

    def _sessao(self):
        if not getattr(self.local, "sessao", None):
            self.local.sessao = requests.Session()
            self.local.sessao.headers.update({"User-Agent": UA, **H_CONSULTA})
        return self.local.sessao

    def pagina(self, **params):
        """Uma página da pesquisa ({totalRegistros, itens}). ErroTecnico se não responder."""
        erro = ""
        for tentativa in range(3):
            self.ritmo.esperar(self.parar)
            try:
                r = self._sessao().get(CONSULTA, params=params, timeout=TIMEOUT_HTTP)
                if r.status_code == 200:
                    self.ritmo.ok()
                    return r.json()
                erro = f"HTTP {r.status_code}"
                if "captcha" in r.text[:2000].lower():
                    raise ErroTecnico("CAPTCHA_REATIVADO: a consulta processual do TJMT passou a exigir captcha")
                if r.status_code == 422:
                    raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: a API recusou a pesquisa ({r.text[:150]})")
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except (requests.RequestException, ValueError) as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(2 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: a consulta do TJMT não respondeu ({erro})")

    def todos(self, prazo, max_paginas=MAX_PAGINAS_ADVOGADO, **params):
        """(processos enxutos, total, cortou) de todas as páginas da pesquisa, até max_paginas."""
        processos, total = [], 0
        for pg in range(max_paginas):
            if time.time() > prazo:
                raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")
            d = self.pagina(Skip=pg * TAKE, Take=TAKE, ExibirArquivados="true", **params)
            itens = d.get("itens") or []
            total = d.get("totalRegistros") or 0
            processos += [enxugar(p) for p in itens]
            if not itens or (pg + 1) * TAKE >= total:
                return processos, total, False
        return processos, total, True

    def do_advogado(self, advogado, termo_ente, prazo):
        """Processos do advogado (pelo nome) com o ente numa das partes (termo_ente=None: todos). Em cache; duas
        threads pedindo o mesmo advogado esperam uma só busca."""
        chave = (chave_nome(advogado), termo_ente)
        with self.trava:
            if chave in self.cache:
                self.cache.move_to_end(chave)
                return self.cache[chave]
            trava = self.travas.setdefault(chave, threading.Lock())
        with trava:
            with self.trava:
                if chave in self.cache:
                    return self.cache[chave]
            params = {"NomeOab": chave[0]}
            if termo_ente:
                params["parteNome"] = termo_ente
            resultado = self.todos(prazo, **params)
            with self.trava:
                self.cache[chave] = resultado
                while len(self.cache) > MAX_CACHE_ADVOGADOS:
                    self.cache.popitem(last=False)
                self.travas.pop(chave, None)
            return resultado

    def por_numero(self, numero20):
        """Processos com este número (o mesmo número aparece no 1º e no 2º grau)."""
        return [enxugar(p) for p in self.pagina(numeroUnico=numero20, Skip=0, Take=10).get("itens") or []]

    def por_nome(self, nome, termo_ente, prazo):
        """Processos com a parte pelo nome inteiro (e o ente no polo passivo). Nome comum dá milhares: só as
        MAX_PAGINAS_NOME primeiras páginas."""
        processos, _, _ = self.todos(prazo, max_paginas=MAX_PAGINAS_NOME, parteNome=nome)
        if termo_ente:
            processos = [p for p in processos if any(termo_ente in normal(x["nome"]) for x in p["passivo"])]
        return processos

# =============================================================================== fila


# o que o robô pode pegar: do RPA em PENDENTE, do RPA sem credor em qualquer status (menos EM_ANDAMENTO) e o que já é
# do robô em PENDENTE; sempre vencido o disponivel_em. O que já é do robô com status final (SUCESSO/FALHA) não volta.
FILTRO_PEGAVEL = f"""cc.disponivel_em <= now() AND (
       (cc.software_id = {SOFTWARE_RPA}
        AND (cc.status_id = 1 OR NOT EXISTS (SELECT 1 FROM creditos.credito_credor k
                                              WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)))
    OR (cc.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}') AND cc.status_id = 1))"""

SQL_ESCOPO = """
SELECT cc.credito_id, cc.prioridade, cc.valor_referencia, li.valor_lista, li.metadata->>'Situação' AS situacao,
       EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2) AS tem_credor
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c ON c.id = cc.credito_id
  LEFT JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                      WHERE x.credito_id = c.id ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2 AND {filtro}
"""


def faixa_da_situacao(situacao):
    """1 não pago (aguardando, preferencial, autuado, provisionado); 2 o resto (pago em parte, quitação, suspenso)."""
    return 1 if normal(situacao) in NAO_PAGOS else 2


def ordenar_escopo(con, filtro):
    """(ids na ordem do robô, {credito_id: faixa}, contagem por faixa).
    Ordem: faixa, prioridade de campanha, sem credor antes de com credor, maior valor, id."""
    with con.cursor() as cur:
        cur.execute(SQL_ESCOPO.format(filtro=filtro), (TRIBUNAL_TJMT,))
        linhas = como_dicts(cur)
    faixas = {x["credito_id"]: faixa_da_situacao(x["situacao"]) for x in linhas}
    chave = {x["credito_id"]: (faixas[x["credito_id"]], x["prioridade"], x["tem_credor"],
                               -float(x["valor_lista"] or x["valor_referencia"] or 0), x["credito_id"]) for x in linhas}
    ordem = sorted(chave, key=chave.get)
    return ordem, faixas, Counter(FAIXAS[f] for f in faixas.values())

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJMT", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, left(c.numero_norm, 20) AS precatorio20,
       tc.codigo AS tipo_credito, li.valor_lista, COALESCE(li.metadata, '{}'::jsonb) AS lista,
       ARRAY(SELECT x.valor_lista FROM creditos.lista_item x
              WHERE x.credito_id = c.id AND x.removido_em IS NULL AND x.valor_lista > 0) AS valores,
       (SELECT st.codigo FROM creditos.coleta_credor_tentativa t JOIN creditos.status_coleta st ON st.id = t.status_id
         WHERE t.credito_id = c.id ORDER BY t.id DESC LIMIT 1) AS ultimo_status
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc ON tc.id = c.tipo_credito_id
  LEFT JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                      WHERE x.credito_id = c.id ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE c.id = %s
"""


def ler_credito(cur, credito_id):
    """O crédito (lead) com o que a lista do TJMT traz: ente, valor requisitado, data de envio, situação."""
    cur.execute(SQL_CREDITO, (credito_id,))
    linhas = como_dicts(cur)
    if not linhas:
        raise RuntimeError(f"crédito {credito_id} não existe")
    lead = linhas[0]
    lista = lead["lista"] or {}
    lead["ente"] = " ".join((lista.get("entidade_nome") or lista.get("Orgão devedor") or "").split())
    lead["termo_ente"] = termo_do_ente(lead["ente"]) if lead["ente"] else None
    lead["envio"] = data_br(lista.get("Data Envio"))
    lead["situacao"] = lista.get("Situação") or ""
    lead["preferencia"] = normal(lista.get("Preferência"))      # IDADE: credor tinha 60+ anos no envio
    lead["faixa"] = FAIXAS[faixa_da_situacao(lead["situacao"])]
    lead["valores"] = sorted({float(v) for v in (lead["valores"] or [])} |
                             ({valor_numerico(lead["valor_lista"])} if valor_numerico(lead["valor_lista"]) else set()))
    return lead


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())

# =============================================================================== decisão (só lê: DJEN e consulta)


def resultado_vazio():
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "NOME", "originario": None, "regra": "", "evidencia": "",
            "credor": None, "capa": None, "candidatos": [], "credor_djen": "", "publicacoes": 0,
            "advogados_djen": [], "advogados_precatorio": [], "n_pessoas": 0, "fonte_nome": ""}


def do_djen(itens):
    """Do DJEN do precatório: credores do polo A (iniciais ou nome inteiro) e advogados (nome, OAB)."""
    alvos, advogados = Counter(), {}
    for it in itens:
        for polo, nome in it["partes"]:
            if polo == "A" and nome.strip():
                alvos[" ".join(nome.split())] += 1
        for a in it["advogados"]:
            advogados.setdefault(chave_nome(a["nome"]), a)
    # destinatário do polo A que é um dos advogados (o DJEN às vezes intima o advogado como parte) não é credor
    alvos = Counter({n: q for n, q in alvos.items() if chave_nome(n) not in advogados})
    return alvos, list(advogados.values())


def do_legado(consulta, prec20):
    """Precatório do sistema antigo (até 2019) aberto na consulta: nomes inteiros do polo ativo (sem o juízo
    requisitante) e os advogados deles."""
    nomes, advogados = [], {}
    for p in consulta.por_numero(prec20):
        for x in p["ativo"]:
            if RE_JUIZO.search(normal(x["nome"])) or "REQUISITANTE" in normal(x["papel_bruto"]):
                continue
            nomes.append(x["nome"])
            for a in x["advogados"]:
                advogados.setdefault(chave_nome(a["nome"]), {"nome": a["nome"], "oab": a["oab_numero"],
                                                             "uf": a["oab_uf"], "cpf": a["cpf"]})
    return list(dict.fromkeys(nomes)), list(advogados.values())


def qualidade(nome, alvos_nome, alvos_ini):
    """3 = nome inteiro igual; 2 = iniciais iguais; 1 = iniciais com um 'D' a mais no DJEN; 0 = não casa."""
    k = chave_nome(nome)
    if k in alvos_nome:
        return 3
    return max((casa_iniciais(nome, a) for a in alvos_ini), default=0)


def candidatos_dos_processos(processos, alvos_nome, alvos_ini, livre, cands, fonte):
    """Junta em cands {numero: candidato} os processos com uma parte do polo ativo que casa com o credor publicado.
    livre=True: a pesquisa não filtrou o ente, então exige um ente público no polo passivo."""
    for p in processos:
        if not p["numero"] or RE_CLASSE_FORA.search(normal(p["classe"])):
            continue
        if livre and not any(RE_PASSIVO_PUBLICO.search(normal(x["nome"])) for x in p["passivo"]):
            continue
        for x in p["ativo"]:
            q = qualidade(x["nome"], alvos_nome, alvos_ini)
            if not q:
                continue
            c = cands.get(p["numero"])
            if not c or q > c["q"] or (q == c["q"] and documento_valido(x["documento"])
                                       and not documento_valido(c["credor"]["documento"])):
                cands[p["numero"]] = {"numero": p["numero"], "q": q, "proc": p, "credor": x, "fonte": fonte,
                                      "ev": set(), "consultado": False}


def idade_na(nascimento, dia):
    """Anos completos em `dia` de quem nasceu em `nascimento` ('AAAA-MM-DD'); None sem as duas datas."""
    try:
        n = date.fromisoformat(nascimento)
    except (TypeError, ValueError):
        return None
    return dia.year - n.year - ((dia.month, dia.day) < (n.month, n.day)) if dia else None


def anotar_cpfs_advogados(processos, cpfs):
    """{(uf, número da OAB): {(nome, CPF)}} dos advogados que a consulta mostra com CPF (para gravar o advogado do
    precatório com o documento). Guarda todas as grafias do nome que aparecem para a mesma OAB."""
    for p in processos:
        for x in p["ativo"]:
            for a in x["advogados"]:
                if a["cpf"] and a["oab_numero"]:
                    cpfs.setdefault((a["oab_uf"], a["oab_numero"]), set()).add((a["nome"], a["cpf"]))


def mesmo_advogado(a, b):
    """Mesmo nome com folga para sobrenome a mais ou a menos ('ANA F. MOREIRA' x 'ANA F. MOREIRA LIMA'):
    grafia próxima, ou um nome contido no outro com o mesmo primeiro nome. A OAB já é a mesma."""
    ka, kb = chave_nome(a).split(), chave_nome(b).split()
    if not ka or not kb:
        return False
    curto, longo = sorted((ka, kb), key=len)
    return mesma_pessoa(a, b) or (ka[0] == kb[0] and len(curto) >= 2 and set(curto) <= set(longo))


def advogados_do_precatorio(advogados, cpfs):
    """Advogados do precatório (DJEN: nome e OAB '12027-O'/MT) prontos para o registrar_credor, com o CPF quando a
    consulta mostrou o mesmo advogado (mesma OAB e mesmo nome)."""
    saida = {}
    for a in advogados:
        numero = (re.match(r"\d+", a.get("oab") or "") or [""])[0]
        uf = (a.get("uf") or "").upper()
        if not (numero and len(uf) == 2):
            continue
        cpfs_oab = {cpf for nome, cpf in cpfs.get((uf, numero), set()) if mesmo_advogado(nome, a["nome"])}
        cpf = a.get("cpf") or (next(iter(cpfs_oab)) if len(cpfs_oab) == 1 else None)   # do próprio precatório antigo
        saida[(uf, numero)] = {"nome": a["nome"], "oab_uf": uf, "oab_numero": numero, "cpf": cpf}
    return list(saida.values())


def pessoas_dos_candidatos(cands):
    """{chave da pessoa: [candidatos]}: pelo CPF/CNPJ; sem documento, pelo nome (junta com a de mesmo nome que tem)."""
    pessoas = {}
    for c in cands.values():
        pessoas.setdefault(c["credor"]["documento"] or chave_nome(c["credor"]["nome"]), []).append(c)
    for k in [k for k in pessoas if not k.isdigit()]:
        dono = next((k2 for k2, cs in pessoas.items() if k2.isdigit() and chave_nome(cs[0]["credor"]["nome"]) == k), None)
        if dono:
            pessoas[dono] += pessoas.pop(k)
    return pessoas


def forca(c):
    """Maior força das evidências do candidato (FORCA): 4 nº do precatório citado; 3 valor exato ou certidão do
    espelho perto do envio; 2 ordem de expedição perto do envio; 1 espelho/expedição longe; 0 nada."""
    return max((FORCA[e] for e in c["ev"]), default=0)


def evidencias(djen, c, lead, alvos_texto):
    """Lê o DJEN do candidato e anota as evidências de que ele é o originário deste precatório."""
    c["consultado"] = True
    for it in djen.do_originario(c["numero"]):
        t = it["texto"]
        if any(x in t for x in alvos_texto["numero"]):
            c["ev"].add("NUM_PRECATORIO")
        if any(v in t for v in alvos_texto["valor"]):
            c["ev"].add("VALOR")
        for tipo, regex in (("ESPELHO", RE_ESPELHO), ("EXPEDICAO", RE_EXPEDICAO)):
            if regex.search(t):
                perto = False
                if lead["envio"] and it["data"]:
                    dias = (date.fromisoformat(it["data"]) - lead["envio"]).days
                    perto = JANELA_ESPELHO[0] <= dias <= JANELA_ESPELHO[1]
                c["ev"].add(f"{tipo}_PERTO" if perto else f"{tipo}_LONGE")


def ordem_dos_candidatos(cs):
    """Melhor primeiro: casa melhor, classe de execução/cumprimento, 1º grau do PJe, mais novo."""
    return sorted(cs, key=lambda c: (-c["q"], not RE_EXECUCAO.search(normal(c["proc"]["classe"])),
                                     c["proc"]["sistema"] != 1, -int(c["numero"][9:13] or 0)))


def processar(lead, djen, consulta, inicio):
    """Acha o credor e o originário. Devolve o resultado a gravar (status ADIAR = volta para a fila)."""
    r = resultado_vazio()
    prec20 = lead["precatorio20"]
    prazo = inicio + TETO_CREDITO - 15
    if prec20[13:16] != "811":
        r["motivo"] = f"CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO: {formatar_cnj(prec20)} não é do TJMT"
        return r

    def tempo():
        if time.time() > prazo:
            raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")

    # 1. quem é o credor: iniciais (ou nome) no DJEN; precatório antigo: nome inteiro na consulta
    itens = djen.do_precatorio(prec20)
    alvos, advogados = do_djen(itens)
    nomes_legado = []
    if prec20.startswith("0"):
        nomes_legado, advs_legado = do_legado(consulta, prec20)
        for a in advs_legado:
            if chave_nome(a["nome"]) not in {chave_nome(x["nome"]) for x in advogados}:
                advogados.append(a)
    alvos_ini = {iniciais_publicadas(n) for n in alvos if eh_iniciais(n)}
    alvos_nome = {chave_nome(n) for n in alvos if not eh_iniciais(n)} | {chave_nome(n) for n in nomes_legado}
    alvos_ini |= {iniciais(n) for n in alvos_nome}
    cpfs_adv = {}
    r.update(publicacoes=len(itens), credor_djen=" | ".join(list(alvos) + nomes_legado)[:300],
             advogados_djen=[f"{a['nome']} ({a['oab']}{a['uf']})" for a in advogados],
             fonte_nome="LEGADO" if nomes_legado else "DJEN",
             advogados_precatorio=advogados_do_precatorio(advogados, cpfs_adv))
    if not alvos_ini and not alvos_nome:
        if not itens:
            r.update(status="ADIAR", motivo="AINDA_SEM_PUBLICACAO: precatório sem publicação no DJEN")
        else:
            r["motivo"] = f"SEM_BENEFICIARIO: polo ativo vazio nas {len(itens)} publicações do DJEN"
        return r
    if alvos_nome and all(RE_ORGAO_PUBLICO.search(n) for n in alvos_nome):
        r["motivo"] = f"REQTE_ORGAO_PUBLICO: {next(iter(alvos_nome))}"
        return r

    if not advogados and not alvos_nome:
        r["motivo"] = "PROCESSO_NAO_ENCONTRADO: o DJEN do precatório não traz advogado para achar o originário"
        return r

    # 2. candidatos: processos dos advogados do precatório contra o ente; sem nenhum, só pelo advogado (com um ente
    # público no polo passivo)
    cands = {}
    modos = ([(lead["termo_ente"], False)] if lead["termo_ente"] else []) + [(None, True)]
    for termo, livre in modos:
        for a in advogados[:MAX_ADVOGADOS]:
            tempo()
            processos, _, _ = consulta.do_advogado(a["nome"], termo, prazo)
            anotar_cpfs_advogados(processos, cpfs_adv)
            candidatos_dos_processos(processos, alvos_nome, alvos_ini, livre, cands,
                                     "ADVOGADO_SEM_ENTE" if livre else "ADVOGADO")
        if cands:
            break
    nomes_inteiros = list(dict.fromkeys(nomes_legado + [n for n in alvos if not eh_iniciais(n)]))
    for nome in nomes_inteiros[:2]:                    # nome inteiro: pesquisa a parte direto
        tempo()
        processos = consulta.por_nome(nome, lead["termo_ente"], prazo)
        anotar_cpfs_advogados(processos, cpfs_adv)
        candidatos_dos_processos(processos, alvos_nome, alvos_ini, False, cands, "NOME")
    r["advogados_precatorio"] = advogados_do_precatorio(advogados, cpfs_adv)
    # preferência por idade: o credor tinha 60+ anos no envio; candidato mais novo (pela consulta) sai
    if lead["preferencia"] == "IDADE" and lead["envio"]:
        cands = {n: c for n, c in cands.items()
                 if (idade_na(c["credor"]["nascimento"], lead["envio"]) or 60) >= 60}
    # fica só o melhor nível de casamento (nome > iniciais > iniciais com 'D' a mais)
    if cands:
        melhor_q = max(c["q"] for c in cands.values())
        cands = {n: c for n, c in cands.items() if c["q"] == melhor_q}
    pessoas = pessoas_dos_candidatos(cands)
    r["n_pessoas"] = len(pessoas)
    if not pessoas:
        r["motivo"] = (f"PROCESSO_NAO_ENCONTRADO: nenhum processo dos advogados ({', '.join(a['nome'] for a in advogados[:MAX_ADVOGADOS])}) "
                       f"contra {lead['ente'] or 'o ente'} com autor {', '.join(sorted(alvos_ini | alvos_nome))[:120]}")
        return r

    # 3. evidência no DJEN dos candidatos: por pessoa, todos os originários dela (até MAX_ORIGINARIOS_PESSOA), porque
    # a mesma pessoa com dois processos igualmente prováveis deixa o originário em dúvida
    alvos_texto = {"numero": {formatar_cnj(prec20), prec20},
                   "valor": set().union(*(formatos_valor(v) for v in lead["valores"])) if lead["valores"] else set()}
    consultados = 0
    for chave in sorted(pessoas, key=lambda k: -len(pessoas[k])):
        for c in ordem_dos_candidatos(pessoas[chave])[:MAX_ORIGINARIOS_PESSOA]:
            if consultados >= MAX_ORIGINARIOS_DJEN:
                break
            tempo()
            evidencias(djen, c, lead, alvos_texto)
            consultados += 1
            if forca(c) == FORCA["NUM_PRECATORIO"]:
                break                                   # o DJEN do candidato cita este precatório: não há dúvida

    # 4. decide: primeiro a pessoa, depois o originário dela
    melhor = {k: max(forca(c) for c in cs) for k, cs in pessoas.items()}
    r["candidatos"] = [{"cnj": formatar_cnj(c["numero"]), "credor": c["credor"]["nome"],
                        "documento": c["credor"]["documento"], "classe": c["proc"]["classe"],
                        "sistema": c["proc"]["sistema"], "casamento": c["q"], "fonte": c["fonte"],
                        "evidencia": sorted(c["ev"]), "consultado_djen": c["consultado"]}
                       for k in sorted(pessoas, key=lambda k: -melhor[k]) for c in ordem_dos_candidatos(pessoas[k])][:20]
    topo = max(melhor.values())
    no_topo = [k for k, v in melhor.items() if v == topo]
    if topo >= FORCA_MINIMA and len(no_topo) == 1:
        escolhida = no_topo[0]
        regra = "UNICO_COM_EVIDENCIA" if len(pessoas) == 1 else "DESEMPATE_POR_EVIDENCIA"
    elif len(pessoas) == 1:
        escolhida, regra = next(iter(pessoas)), "UNICO_SEM_EVIDENCIA"
    else:
        lista = "; ".join(f"{cs[0]['credor']['nome']} ({formatar_cnj(ordem_dos_candidatos(cs)[0]['numero'])})"
                          for cs in pessoas.values())
        r.update(status="SUCESSO_ANALISAR",
                 motivo=f"CANDIDATOS_POR_INICIAIS: {len(pessoas)} pessoas com as iniciais "
                        f"{'/'.join(sorted(alvos_ini))} nos processos do advogado"
                        f"{' (empatadas na evidência)' if topo >= FORCA_MINIMA else ''}: {lista}"[:1500])
        return r
    cs = ordem_dos_candidatos(pessoas[escolhida])
    forca_topo = max(forca(c) for c in cs)
    originarios_topo = [c for c in cs if forca(c) == forca_topo]
    c = originarios_topo[0]
    credor = c["credor"]
    r.update(regra=regra, evidencia="+".join(sorted(c["ev"])),
             credor={"nome": credor["nome"], "documento": credor["documento"] or None,
                     "papel_bruto": credor["papel_bruto"], "nascimento": credor["nascimento"]})
    desc = (f"cnj={formatar_cnj(c['numero'])} regra={regra} evidencia={r['evidencia'] or 'nenhuma'} "
            f"casamento={'NOME' if c['q'] == 3 else 'INICIAIS' if c['q'] == 2 else 'INICIAIS_D_A_MAIS'} "
            f"fonte={c['fonte']} credor={credor['nome']}")
    if RE_ORGAO_PUBLICO.search(chave_nome(credor["nome"])):
        r.update(status="FALHA", credor=None, motivo=f"REQTE_ORGAO_PUBLICO: {desc}")
        return r
    honorarios = " (honorários: sociedade de advogados)" if RE_SOCIEDADE_ADV.search(chave_nome(credor["nome"])) else ""
    credor_certo = forca_topo >= FORCA_MINIMA or c["q"] == 3      # evidência no originário, ou o nome inteiro
    if credor_certo and len(originarios_topo) == 1:
        # data de ajuizamento: a mais antiga do número (processo migrado para o PJe traz a data da migração)
        tempo()
        datas = sorted(p["data"] for p in consulta.por_numero(c["numero"]) if p["data"])
        proc = dict(c["proc"], data=datas[0]) if datas else c["proc"]
        r.update(originario=c["numero"], via="ORIGINARIO", capa={"proc": proc, "credor": credor})
        if credor["documento"]:
            r.update(status="SUCESSO_PROCESSO_ORIGINARIO" if forca_topo >= FORCA_MINIMA else "SUCESSO_PARTES_SEM_VALOR",
                     motivo=desc + honorarios)
        else:
            r.update(status="SUCESSO_INCOMPLETO", motivo=f"SUCESSO_SEM_CPF: {desc}{honorarios}")
    elif credor_certo:
        # o credor é certo, mas ele tem mais de um processo igualmente provável: liga só o credor, sem originário
        lista = ", ".join(formatar_cnj(x["numero"]) for x in originarios_topo)
        r.update(status="SUCESSO_ANALISAR", via="NOME", liga_credor=bool(credor["documento"]),
                 motivo=f"CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO: {len(originarios_topo)} processos do credor "
                        f"com a mesma evidência ({lista}); {desc}{honorarios}"[:1500])
        r["candidato_analisar"] = {**r["credor"], "originario": lista}
    else:
        # regra do usuário (01/10/2026): só pelas iniciais, sem evidência no originário -> analisar, sem ligar
        r.update(status="SUCESSO_ANALISAR", via="NOME", motivo=f"CREDOR_SO_POR_INICIAIS: {desc}{honorarios}")
        r["candidato_analisar"] = {**r["credor"], "originario": formatar_cnj(c["numero"])}
        r["credor"] = None
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
                    f.write("-- Desfaz as mudanças do fetch_TJMT.py nas tabelas antigas "
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
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJMT",
                                "Credor do TJMT: DJEN (iniciais e advogado) + consulta processual pública "
                                "(TJMT/fetch_TJMT.py)", raspa_credor=True, criar=criar)


def filas_antigas(cur):
    """Filas mensais do legado (de FILA_ANTIGA_DESDE em diante) em que o usuário pode gravar."""
    return banco.filas_antigas(cur, FILA_ANTIGA_DESDE)[0]


def recorte(r, coletiva):
    """Capa recortada do originário para o banco: só o credor confirmado, o polo passivo (como REU: o recálculo
    leria 'EXEQUENTE' no passivo de um recurso como credor) e os advogados do credor que também estão no
    precatório. Gravar todos os autores faria o recálculo ligar cada um deles ao precatório como credor; e no
    processo coletivo vai sem advogados (o recálculo liga advogado da capa a todos os créditos do processo)."""
    proc, credor = r["capa"]["proc"], r["capa"]["credor"]
    advs_precatorio = set() if coletiva else {chave_nome(a.split(" (")[0]) for a in r["advogados_djen"]}
    partes = [{"nome": credor["nome"], "documento": credor["documento"], "polo": "ATIVO",
               "papel": papel_ativo(credor["papel_bruto"]), "papel_bruto": credor["papel_bruto"] or "AUTOR"}]
    partes += [{"nome": x["nome"], "documento": x["documento"], "polo": "PASSIVO", "papel": "REU",
                "papel_bruto": x["papel_bruto"] or "REU"} for x in proc["passivo"]]
    advogados = [{"nome": a["nome"], "oab_uf": a["oab_uf"], "oab_numero": a["oab_numero"], "polo": "ATIVO",
                  "cpf": a["cpf"]}
                 for a in credor["advogados"] if a["oab_numero"] and chave_nome(a["nome"]) in advs_precatorio]
    capa = {"classe_judicial": proc["classe"] or None, "classe_codigo": proc.get("classe_codigo"),
            "orgao_julgador": proc["orgao"] or None, "jurisdicao": proc["jurisdicao"] or None, "assunto": None,
            "data_autuacao": proc["data"] or None, "grau": proc.get("grau") or "G1",
            "segredo_justica": proc.get("segredo", False)}
    return {"partes": partes, "advogados": advogados, "capa": capa, "sistema": proc["sistema"], "proc": proc}


def partes_para_banco(cur, dados, existentes):
    """(partes, advogados) para registrar_capa, SÓ ACRESCENTANDO: o que o banco já tem do processo (reenviado como
    está) + as partes e advogados novos (o registrar_capa troca o conjunto inteiro; mandar só o novo apagaria o resto).
    O CPF que faltar vem do banco (mesmo nome)."""
    do_banco = ([{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "papel": e["papel"],
                  "papel_bruto": RE_PREFIXO_POLO.sub("", e["papel_bruto"] or "")}
                 for e in existentes if e["papel"] != "ADVOGADO"],
                [{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "oab_uf": e["oab_uf"],
                  "oab_numero": e["oab_numero"], "papel_bruto": RE_PREFIXO_POLO.sub("", e["papel_bruto"] or "")}
                 for e in existentes if e["papel"] == "ADVOGADO"])
    doc_banco = {e["chave"]: e["documento"] for e in existentes if e["chave"] and e["documento"]}
    chaves = chave_texto_lote(cur, [p["nome"] for p in dados["partes"]])
    partes = [{"nome": p["nome"], "cpf_cnpj": p["documento"] if documento_valido(p["documento"]) else doc_banco.get(ch),
               "polo": p["polo"], "papel": p["papel"], "papel_bruto": p["papel_bruto"]}
              for p, ch in zip(dados["partes"], chaves)]
    advogados = [{"nome": a["nome"], "cpf_cnpj": a.get("cpf") or None, "polo": "ATIVO", "oab_uf": a["oab_uf"],
                  "oab_numero": a["oab_numero"], "papel_bruto": "ADVOGADO"} for a in dados["advogados"]]
    nomes_banco = {e["chave"] for e in existentes}
    oabs_banco = {(e["oab_uf"], e["oab_numero"]) for e in existentes if e["oab_numero"]}
    return (do_banco[0] + [p for p, ch in zip(partes, chaves) if ch not in nomes_banco],
            do_banco[1] + [a for a in advogados if (a["oab_uf"], a["oab_numero"]) not in oabs_banco])


def capa_para_banco(dados):
    """Capa do originário -> JSON do registrar_capa (colunas de creditos.processo: classe_nome, classe_codigo,
    orgao_julgador, grau, segredo_justica, sistema; o sistema só quando é o PJe: 1/2 na consulta)."""
    capa = dados["capa"]
    return {"orgao_julgador": capa["orgao_julgador"], "classe_judicial": capa["classe_judicial"],
            "classe_codigo": str(capa["classe_codigo"]) if capa.get("classe_codigo") else None,
            "grau": capa.get("grau") or "G1", "segredo_justica": bool(capa.get("segredo_justica")),
            "sistema": "PJE" if dados["sistema"] in (1, 2) else None}


def metadata_do_processo(cur, cnj, dados, bk):
    """O resto da capa em creditos.processo.metadata, no formato dos outros robôs (capa_pje: fonte, campos, classe,
    partes; dataAjuizamento): só acrescenta (capa_pje de outra fonte e dataAjuizamento que já existem ficam).
    Backup do metadata de antes no desfazer_legado_*.sql."""
    cur.execute("SELECT id, metadata FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s) FOR UPDATE",
                (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if not linha:
        return 0
    pid, antes = linha[0], linha[1] or {}
    proc = dados["proc"]
    novo = {}
    if (antes.get("capa_pje") or {}).get("fonte") in (None, "consulta_tjmt"):
        novo["capa_pje"] = {
            "fonte": "consulta_tjmt",
            "campos": {"numero_processo": formatar_cnj(cnj), "classe_judicial": proc["classe"],
                       "orgao_julgador": proc["orgao"], "jurisdicao": proc["jurisdicao"],
                       "data_da_distribuicao": proc["data"], "valor_causa": proc["valor_causa"],
                       "arquivado": proc["arquivado"], "sistema_tjmt": proc["sistema"]},
            "classe": proc["classe"],
            "partes": {"autores": [{"nome": p["nome"], "doc": p["documento"] or None, "papel": p["papel_bruto"]}
                                   for p in dados["partes"] if p["polo"] == "ATIVO"],
                       "reus": [{"nome": p["nome"], "doc": p["documento"] or None, "papel": p["papel_bruto"]}
                                for p in dados["partes"] if p["polo"] == "PASSIVO"]}}
    if proc["data"] and not antes.get("dataAjuizamento"):
        novo["dataAjuizamento"] = proc["data"].replace("-", "") + "000000"
    if not novo:
        return 0
    bk.update(cur, "creditos.processo", {"id": pid},
              {"metadata": json.dumps(antes, ensure_ascii=False) if linha[1] is not None else None})
    cur.execute("UPDATE creditos.processo SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb, "
                "updated_at = now() WHERE id = %s", (json.dumps(novo, ensure_ascii=False, default=str), pid))
    return 1


def nascimento_do_credor(cur, credor, bk):
    """Data de nascimento (da consulta) em creditos.pessoa, só para CPF e só quando o banco não tem."""
    if not (credor.get("nascimento") and len(credor.get("documento") or "") == 11):
        return 0
    cur.execute("""UPDATE creditos.pessoa SET data_nascimento = %s, updated_at = now()
                    WHERE documento = creditos.documento_normalizar(%s) AND data_nascimento IS NULL
                    RETURNING id""", (credor["nascimento"], credor["documento"]))
    linhas = cur.fetchall()
    for (pid,) in linhas:
        bk.update(cur, "creditos.pessoa", {"id": pid}, {"data_nascimento": None})
    return len(linhas)


def gravar_advogados_precatorio(cur, cid, advogados):
    """Advogados do precatório (DJEN) ligados ao crédito como ADVOGADO (pela OAB; com o CPF quando a consulta mostra):
    vale em todo lead processado, mesmo sem credor, porque saem do próprio precatório."""
    n = 0
    for a in advogados:
        cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'ADVOGADO', p_nome => %s,
                                                        p_documento => %s, p_oab_uf => %s, p_oab_numero => %s,
                                                        p_fonte => %s)""",
                    (cid, a["nome"], a["cpf"], a["oab_uf"], a["oab_numero"], SOFTWARE))
        n += 1
    return n


def travar_processo(cur, cnj):
    """O mesmo lock do registrar_capa, pego antes de ler as partes: duas instâncias no mesmo processo
    não se atropelam."""
    cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)", (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if linha:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('creditos.registrar_capa'), %s::int)", (linha[0],))


def gravar_capa_legado(cur, cnj, dados, relacionar, precatorio, bk):
    """Capa antiga do originário (originarios.*), como o RPA grava; só acrescenta partes e advogados (o recorte do
    credor vale também aqui: o espelho do legado recriaria o resto)."""
    cont, agora, hoje = Counter(), datetime.now(), date.today()
    capa = dados["capa"]
    cnpj = next((p["documento"] for p in dados["partes"] if p["polo"] == "PASSIVO" and len(p["documento"]) == 14), None)
    novos = {"classe_judicial": capa.get("classe_judicial"), "orgao_julgador": capa.get("orgao_julgador"),
             "jurisdicao": capa.get("jurisdicao"), "assunto": capa.get("assunto"),
             "data_autuacao": date.fromisoformat(capa["data_autuacao"]) if capa.get("data_autuacao") else None,
             "cnpj_entidade_devedora": cnpj, "origem": "TJMT", "tribunal_sigla": "TJMT"}
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
    cur.execute("SELECT * FROM originarios.partes_processuais WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    antigas = como_dicts(cur)
    cpf_antigo = {(a["polo"], normal(a["nome"])): a["cpf_cnpj"] for a in antigas if a["cpf_cnpj"]}
    ja = {(a["polo"], normal(a["nome"])) for a in antigas}
    novas = [(pid, p["polo"], p["nome"],
              p["documento"] if documento_valido(p["documento"]) else cpf_antigo.get((p["polo"], normal(p["nome"]))),
              PAPEL_LEGADO[p["polo"]], "TJMT", agora, p["papel_bruto"], POLO_BRUTO[p["polo"]])
             for p in dados["partes"] if (p["polo"], normal(p["nome"])) not in ja]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    ja_oab = {(a["oab_uf"], a["oab_numero"]) for a in como_dicts(cur)}
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJMT", agora,
                                            "ADVOGADO", "AUTOR")
            for a in dados["advogados"] if (a["oab_uf"], a["oab_numero"]) not in ja_oab}
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
    para o RPA antigo não reprocessar o que o robô já resolveu."""
    cont = Counter()
    status = status_legado[r["status"]]
    originario = [formatar_cnj(r["originario"])] if r["originario"] else None
    for fila in filas:
        cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead,
                               numero_originario, ultima_atualizacao
                          FROM {fila}
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJMT'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJMT"}
            bk.update(cur, fila, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                             "numero_originario", "ultima_atualizacao")})
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJMT'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Resumo da decisão em credito_fonte.metadata do software (a função troca o JSON inteiro: mescla aqui).
    Do DJEN só entram as iniciais/nome do credor, a contagem de publicações e os advogados (o texto não é guardado).
    No SUCESSO_ANALISAR o candidato (nome, documento, originário) fica só aqui, para a revisão."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "evidencia": r["evidencia"], "n_pessoas": r["n_pessoas"],
                     "candidatos": r["candidatos"],
                     "djen": {"credor": r["credor_djen"], "publicacoes": r["publicacoes"],
                              "advogados": r["advogados_djen"], "fonte_nome": r["fonte_nome"]}}
    meta["credor"] = ({"nome": r["credor"]["nome"], "papel": r["credor"]["papel_bruto"],
                       "data_nascimento": r["credor"]["nascimento"], "cpf_encontrado": bool(r["credor"]["documento"])}
                      if r["credor"] else None)
    meta["candidato_analisar"] = r.get("candidato_analisar")
    if r["capa"]:
        proc = r["capa"]["proc"]
        meta["capa_originario"] = {"classe": proc["classe"], "orgao": proc["orgao"], "jurisdicao": proc["jurisdicao"],
                                   "data": proc["data"], "valor_causa": proc["valor_causa"],
                                   "arquivado": proc["arquivado"], "sistema": proc["sistema"]}
    meta["lista"] = {"faixa": lead["faixa"], "situacao": lead["situacao"],
                     "envio": lead["envio"].isoformat() if lead["envio"] else None}
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJMT', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def corrigir_credor(cur, lead, credor, bk):
    """A fonte vence o banco: com o CPF do credor confirmado na fonte, apaga os vínculos de CREDOR do crédito que são
    a mesma pessoa (mesmo nome ou grafia próxima) com outro documento, e troca o CPF na capa antiga do precatório
    (senão o espelho do legado recriaria o vínculo errado). Herdeiro, cessionário, sucessor, advogado e credor com
    outro nome não são tocados. O banco audita cada DELETE e o desfazer_legado_*.sql guarda o INSERT que o recria."""
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
            corrigidos.append(f"capa antiga do precatório: {nome} -> CPF da fonte")
    return corrigidos


def outros_credores_no_banco(cur, originario, credor, credito_id):
    """Quantos credores (com pessoa) o processo já tem no banco além do credor confirmado, quando nenhum outro crédito
    está ligado a ele. Nesse caso o recálculo do banco (recalcular_credores_do_processo, dentro do registrar_capa)
    ligaria todos eles como credores deste crédito; com 2 ou mais créditos ligados ele não liga credor nenhum."""
    cur.execute("""SELECT count(*) FROM creditos.processo_parte pa JOIN creditos.processo pr ON pr.id = pa.processo_id
                    WHERE pr.numero_cnj = creditos.cnj_normalizar(%s) AND pa.papel_id = 1 AND pa.pessoa_id IS NOT NULL
                      AND pa.pessoa_id IS DISTINCT FROM (SELECT id FROM creditos.pessoa
                                                          WHERE documento = creditos.documento_normalizar(%s))
                      AND NOT EXISTS (SELECT 1 FROM creditos.credito_originario co
                                       WHERE co.processo_id = pr.id AND co.credito_id <> %s)""",
                (formatar_cnj(originario), (credor or {}).get("documento"), credito_id))
    return cur.fetchone()[0]


def gravar(cur, lead, r, filas, status_legado, bk):
    """Grava o resultado de um crédito (quem chama cuida do SAVEPOINT e do COMMIT). Devolve o resumo para o CSV."""
    cid = lead["credito_id"]
    id_do_software(cur, criar=True)                     # na simulação ele nasce e morre nesta transação
    antes = credores_do_credito(cur, cid)
    resumo, legado, proc, corrigidos = "", Counter(), None, []
    vincula = r["originario"] and r["status"] in STATUS_COM_VINCULO
    if vincula:
        outros = outros_credores_no_banco(cur, r["originario"], r["credor"], cid)
        if outros:
            # ação coletiva que já está no banco: ligar o originário faria o recálculo ligar os outros autores como
            # credores deste crédito; liga só o credor confirmado
            vincula = False
            r["liga_credor"] = True
            r["motivo"] = (f"ORIGINARIO_COLETIVO_NAO_LIGADO: {formatar_cnj(r['originario'])} já tem {outros} outro(s) "
                           f"credor(es) no banco; {r['motivo']}")[:2000]
            legado["coletivo_nao_ligado"] += 1
    if vincula:
        cur.execute("SELECT creditos.fila_credor_registrar_originario(%s, %s, %s)",
                    (cid, WORKER, [formatar_cnj(r["originario"])]))
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)",
                    (formatar_cnj(r["originario"]),))
        linha = cur.fetchone()
        proc = linha[0] if linha else None
    credor = r["credor"]
    if (vincula or r.get("liga_credor")) and credor and credor["documento"]:
        cur.execute("SELECT creditos.documento_de_parte(%s)", (credor["documento"],))
        # antes do registrar_capa: o vínculo fica com origem FONTE, que o recálculo das partes não apaga
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, credor["nome"], credor["documento"], proc, SOFTWARE))
            corrigidos = corrigir_credor(cur, lead, credor, bk)
            legado["nascimento"] += nascimento_do_credor(cur, credor, bk)
    legado["advogados_precatorio"] += gravar_advogados_precatorio(cur, cid, r["advogados_precatorio"])
    if vincula and r["capa"]:
        cnj = r["originario"]
        cur.execute("""SELECT count(DISTINCT co.credito_id) FROM creditos.credito_originario co
                         JOIN creditos.processo pr ON pr.id = co.processo_id
                        WHERE pr.numero_cnj = creditos.cnj_normalizar(%s)""", (formatar_cnj(cnj),))
        coletiva = cur.fetchone()[0] > 1 or \
            len({chave_nome(x["nome"]) for x in r["capa"]["proc"]["ativo"]}) > 1
        dados = recorte(r, coletiva)
        travar_processo(cur, cnj)
        partes, advogados = partes_para_banco(cur, dados, partes_do_banco(cur, formatar_cnj(cnj)))
        cur.execute("SELECT creditos.registrar_capa(%s, %s::jsonb, %s::jsonb, %s::jsonb)",
                    (formatar_cnj(cnj), json.dumps(partes, ensure_ascii=False),
                     json.dumps(advogados, ensure_ascii=False), json.dumps(capa_para_banco(dados), ensure_ascii=False)))
        res = cur.fetchone()[0]
        resumo = (f"{formatar_cnj(cnj)}: partes +{res['partes_inseridas']}/-{res['partes_removidas']} "
                  f"credores +{res['credores_inseridos']}/-{res['credores_removidos']}")
        legado["processo_metadata"] += metadata_do_processo(cur, cnj, dados, bk)
        legado += gravar_capa_legado(cur, cnj, dados, r["status"] == "SUCESSO_PROCESSO_ORIGINARIO",
                                     lead["precatorio"], bk)
    registrar_metadata(cur, lead, r)
    legado += atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "evidencia": r["evidencia"], "n_pessoas": r["n_pessoas"],
               "candidatos": r["candidatos"][:10], "credor_djen": r["credor_djen"],
               "credores_antes": fmt_credores(antes), "credores_depois": fmt_credores(depois)}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => %s, p_sistema => 'PJE',
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (cid, WORKER, r["status"], r["motivo"][:2000], r["via"],
                 formatar_cnj(r["originario"]) if vincula else None,
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
        cand = r.get("candidato_analisar") or r["credor"] or {}
        linha.update(resultado=r["status"], motivo=r["motivo"],
                     originario=formatar_cnj(r["originario"]) if r["originario"] else "", regra=r["regra"],
                     evidencia=r["evidencia"], credor=cand.get("nome", ""),
                     credor_documento=cand.get("documento") or "", n_pessoas=r["n_pessoas"],
                     candidatos=" | ".join(f"{c['cnj']}[{c['credor']} q{c['casamento']} "
                                           f"{'+'.join(c['evidencia']) or '-'}]" for c in r["candidatos"][:8]),
                     banco=g["banco"], legado=g["legado"], credor_corrigido=" | ".join(g["corrigidos"]),
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


def com_banco(rod, funcao, *args):
    """Chama funcao(rod.con, *args); se a conexão de escrita caiu, reconecta e tenta mais uma vez."""
    try:
        return funcao(rod.con, *args)
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        log.warning(f"conexão com o banco caiu ({e.__class__.__name__}): reconectando")
        rod.reconectar()
        return funcao(rod.con, *args)


def descarregar(rod, pendentes):
    """Grava o lote pendente, registra nos CSVs e esvazia a lista. Conexão caída no meio: o lote foi desfeito pelo
    banco; reconecta e grava de novo (os créditos continuam reservados para este worker)."""
    try:
        gravados = gravar_lote(rod, pendentes)
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        log.warning(f"lote não gravou, conexão caiu ({e.__class__.__name__}): reconectando e gravando de novo")
        rod.reconectar()
        rod.n_lote -= 1
        gravados = gravar_lote(rod, pendentes)
    registrar_lote(rod, pendentes, gravados)
    pendentes.clear()

# =============================================================================== fila: reserva e devolução


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
                    (credito_id, TRIBUNAL_TJMT, id_software, WORKER, lease))
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
    """Próximo crédito na ordem do robô que consegue reservar. Acabou a lista: refaz a ordem uma vez (entram os
    adiados que venceram); vazia de novo, a fila acabou (None)."""
    for tentativa in (1, 2):
        while rod.posicao < len(rod.ordem):
            credito_id = rod.ordem[rod.posicao]
            rod.posicao += 1
            antes = com_banco(rod, reservar, credito_id, rod.lease, rod.id_software)
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
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, DJEN, consulta do TJMT e contadores.
    Ao nascer prepara o banco (software, filas antigas, status do legado) e a ordem da fila (na simulação, a amostra
    sai dela e nada é reservado)."""

    def __init__(self, simulacao, limite, workers, usar_proxies, ritmo, creditos=()):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers = simulacao, limite, workers
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJMT{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = Backup(SAIDA / f"desfazer_legado_{self.rodada}{sufixo}.sql",
                         SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo)
        self.resultados, self.falhas_seguidas, self.n, self.n_lote = Counter(), 0, 0, 0
        self.parar, self.fila_vazia = threading.Event(), False
        # o 1º crédito do lote espera os outros LOTE-1, que andam `workers` por vez
        self.lease = f"{(LOTE // workers + 2) * TETO_CREDITO // 60 + 30} minutes"
        self.faixas = {}

        self.con = conectar(escrita=True)
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        self.djen = Djen(self.parar, usar_proxies)
        self.djen.carregar_presidencia()
        self.consulta = Consulta(self.parar, ritmo)
        self.local, self.conexoes, self.trava = threading.local(), [], threading.Lock()
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | lease {self.lease} | consulta TJMT {ritmo:.1f} req/s | DJEN: "
                 f"{', '.join(s.nome for s in self.djen.saidas)} | filas antigas: {', '.join(self.filas)}")
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
        """Ordem do robô (não pagos primeiro) e a faixa de cada crédito pegável."""
        con_l = conectar()
        try:
            ordem, self.faixas, contagem = ordenar_escopo(con_l, FILTRO_PEGAVEL)
        finally:
            con_l.close()
        log.info(f"ordem da fila: {len(ordem)} créditos | " + ", ".join(f"{s}: {contagem[s]}" for s in FAIXAS.values()))
        return ordem

    def reordenar(self):
        """Remonta a ordem da fila e volta ao começo dela (repete a cada REORDENAR_A_CADA)."""
        self.ordem, self.posicao, self.ultima_ordem = self.ordenar(), 0, time.time()

    def reconectar(self):
        """Conexão de escrita nova (a antiga caiu)."""
        try:
            self.con.close()
        except Exception:
            pass
        self.con = conectar(escrita=True)

    def conexao_da_thread(self):
        """Conexão de leitura da thread que chama; criada na 1ª vez (e de novo se a anterior caiu)."""
        if getattr(self.local, "con", None) is not None and self.local.con.closed:
            self.local.con = None
        if not getattr(self.local, "con", None):
            self.local.con = conectar()
            with self.trava:
                self.conexoes.append(self.local.con)
        return self.local.con

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
        log.info("fila do TJMT vazia." if not rod.simulacao else "amostra acabou.")
    return credito_id


def esperar_adiados(rod):
    """Fila vazia no modo real: se algum crédito adiado por erro passageiro (ADIAMENTO) vence em breve, espera por
    ele e volta a trabalhar (True). Sem nenhum, a carga acabou (False): só sobram os sem publicação, que voltam em
    ADIAMENTO_SEM_PUBLICACAO."""
    if rod.simulacao or rod.parar.is_set() or (rod.limite and rod.n >= rod.limite):
        return False

    def proximo(con):
        with con.cursor() as cur:
            cur.execute("""SELECT EXTRACT(EPOCH FROM min(cc.disponivel_em) - now())
                             FROM creditos.coleta_credor cc JOIN creditos.credito c ON c.id = cc.credito_id
                            WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id = 1
                              AND cc.software_id IN (%s, %s)
                              AND cc.disponivel_em <= now() + %s::interval + interval '15 minutes'""",
                        (TRIBUNAL_TJMT, SOFTWARE_RPA, rod.id_software, ADIAMENTO))
            segundos = cur.fetchone()[0]
        con.commit()
        return segundos
    segundos = com_banco(rod, proximo)
    if segundos is None:
        return False
    espera = max(5.0, float(segundos) + 5)
    log.info(f"fila vazia, mas há crédito adiado que vence em {espera / 60:.0f} min: esperando para continuar")
    if rod.parar.wait(espera):
        return False
    rod.fila_vazia = False
    rod.reordenar()
    return True


def processar_credito(rod, credito_id, n):
    """(Numa thread) Lê o crédito e decide credor e originário (DJEN + consulta), sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r} ou {tipo: ADIAR, credito_id, motivo, intervalo, linha, tecnico}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    try:
        with rod.conexao_da_thread().cursor() as cur:
            lead = ler_credito(cur, credito_id)
        linha.update(precatorio=lead["precatorio"], faixa=lead["faixa"], ente=lead["ente"],
                     ultimo_status=lead["ultimo_status"])
        r = processar(lead, rod.djen, rod.consulta, inicio)
    except Exception as e:                              # erro passageiro (ou inesperado): volta para a fila
        if isinstance(e, psycopg2.Error):
            rod.local.con = None                        # a próxima leitura desta thread abre conexão nova
        tecnico = isinstance(e, ErroTecnico)
        motivo = str(e) if tecnico else \
            f"ERRO_DESCONHECIDO: {e.__class__.__name__}: {(str(e).splitlines() or [''])[0][:200]}"
        if not tecnico:
            log.warning(f"[{n}] {credito_id}: erro inesperado", exc_info=True)
        linha.update(resultado="ADIADO", motivo=motivo, segundos=round(time.time() - inicio))
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": motivo, "intervalo": ADIAMENTO, "linha": linha,
                "tecnico": True}
    linha.update(segundos=round(time.time() - inicio), credor_djen=r["credor_djen"], publicacoes_djen=r["publicacoes"],
                 advogados_djen=" | ".join(r["advogados_djen"]))
    cand = r.get("candidato_analisar") or r["credor"] or {}
    log.info(f"[{n}] {credito_id} {lead['precatorio']} -> {r['status']} "
             f"{formatar_cnj(r['originario']) if r['originario'] else ''} "
             f"{('doc ' + cand['documento']) if cand.get('documento') else ''} "
             f"| {r['credor_djen'][:40]} | {r['regra'] or r['motivo'].split(':')[0]} | {linha['segundos']} s")
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
        com_banco(rod, devolver, item["credito_id"], item["motivo"], item["intervalo"])
    anexar_csv(rod.arq_credito, COLUNAS, [item["linha"]])
    rod.resultados[item["linha"]["resultado"]] += 1
    if not item["tecnico"]:
        rod.falhas_seguidas = 0
        return
    rod.falhas_seguidas += 1
    log.warning(f"{item['credito_id']} -> ADIADO: {item['motivo']}")
    if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (TJMT/DJEN fora ou bloqueando).")
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
    log.info(f"DJEN: {rod.djen.n_429} resposta(s) 429 | consulta TJMT: {rod.consulta.ritmo.freadas} freada(s), "
             f"{len(rod.consulta.cache)} advogado(s) em cache")
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --ritmo N, --creditos e --proxies."""
    ap = argparse.ArgumentParser(description="Credor do TJMT pela consulta pública (DJEN + consulta processual), "
                                             f"gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS})")
    ap.add_argument("--ritmo", type=float, default=RITMO_CONSULTA,
                    help=f"teto de req/s na consulta processual do TJMT (padrão {RITMO_CONSULTA})")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
    ap.add_argument("--proxies", action="store_true",
                    help="soma PROXY_01..05 do .env como saídas do DJEN (precisam sair pelo Brasil)")
    return ap.parse_args()


def executar(args, creditos):
    """Uma rodada: mantém `workers` créditos raspando, junta os prontos no lote e grava a cada LOTE; com a fila vazia,
    espera os adiados por erro passageiro que vencem logo. Devolve True se a carga acabou (ou --limite, ou Ctrl+C) e
    False se a rodada parou por falhas técnicas seguidas ou erro inesperado (main reinicia). Créditos em andamento
    voltam para a fila; o lote já processado é gravado ao encerrar."""
    inicio = time.time()
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), args.proxies, max(RITMO_MINIMO, args.ritmo),
                 creditos)
    pendentes, em_voo, terminou = [], {}, False
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjmt")
    try:
        while True:
            while len(em_voo) < rod.workers and (credito_id := proximo_credito(rod)):
                em_voo[executor.submit(processar_credito, rod, credito_id, rod.n)] = credito_id
            if not em_voo:
                if pendentes:
                    descarregar(rod, pendentes)         # antes de esperar: o que já foi processado vai para o banco
                if esperar_adiados(rod):
                    continue
                terminou = not rod.parar.is_set()
                break
            prontos, _ = wait(em_voo, return_when=FIRST_COMPLETED)
            for futuro in prontos:
                em_voo.pop(futuro)
                tratar(rod, futuro.result(), pendentes)
            if len(pendentes) >= LOTE:
                descarregar(rod, pendentes)
    except KeyboardInterrupt:
        log.warning("interrompido (Ctrl+C).")
        terminou = True
    except Exception:
        log.error("erro inesperado no laço principal", exc_info=True)
    finally:
        rod.parar.set()
        executor.shutdown(wait=True, cancel_futures=True)
        if em_voo and not rod.simulacao:
            for credito_id in em_voo.values():
                try:
                    com_banco(rod, devolver, credito_id)
                except Exception:
                    pass
            log.warning(f"{len(em_voo)} crédito(s) em andamento voltaram para a fila.")
        encerrar(rod, pendentes, inicio)
    return terminou


def main():
    """Roda até acabar a carga: se uma rodada para (TJMT/DJEN fora, banco caiu, erro inesperado), espera
    PAUSA_REINICIO e começa outra (até MAX_REINICIOS). Simulação e --limite rodam uma vez só."""
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    creditos = [int(x) for x in re.findall(r"\d+", args.creditos)]
    if creditos and not args.simulacao:
        raise SystemExit("--creditos só vale com --simulacao")
    for tentativa in range(1, MAX_REINICIOS + 1):
        try:
            if executar(args, creditos):
                log.info("carga do TJMT encerrada.")
                return
        except KeyboardInterrupt:
            return
        except Exception:
            log.error("a rodada não conseguiu começar", exc_info=True)
        if args.simulacao or args.limite:
            return
        log.warning(f"rodada parou: nova rodada em {PAUSA_REINICIO // 60} min ({tentativa}/{MAX_REINICIOS})")
        try:
            time.sleep(PAUSA_REINICIO)
        except KeyboardInterrupt:
            return
    log.error(f"{MAX_REINICIOS} rodadas pararam seguidas: desisti (ver o log).")


if __name__ == "__main__":
    main()
