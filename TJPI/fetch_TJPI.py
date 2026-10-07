"""
fetch_TJPI.py - credor dos precatórios do TJPI pela consulta pública, de ponta a ponta, sem navegador e sem login:
lê o banco, acha o credor no DJEN, tenta confirmar o originário (também no DJEN) e grava no banco em lotes de LOTE
créditos (constante abaixo). No lugar do modo credor do RPA_SISTEMAS (token A3 no PJe 2º grau), por decisão do
usuário (07/10/2026): só consulta pública; o CPF que a fonte não mostra fica para o CPF_API/completar_cpf.py.

No TJPI o número do crédito é o processo do PRÓPRIO precatório no PJe 2º grau (07xxxxx-xx.AAAA.8.18.0000). A lista
não traz nome, CPF nem originário, e a consulta pública do PJe (1º e 2º grau) manda para o login do PDPJ. Então:
1. Fila: sem credor antes de com credor, prioridade de campanha e maior valor. Cada crédito é reservado com lease
   (reservar), também nas filas mensais do RPA antigo (CREDOR_EM_ANDAMENTO com a marca do robô: o RPA não o pega), e
   só nessa hora passa do RPA para o software próprio (CONSULTA_PUBLICA_TJPI); o que o RPA tinha fica em
   desfazer_fila_*.sql.
2. DJEN pelo nº do precatório. O cabeçalho do texto é a capa do precatório: 'REQUERENTE: A, B, FUNDO … REQUERIDO:
   ESTADO DO PIAUI'. Os destinatários do polo A NÃO servem como fonte primária: as decisões em massa da Presidência
   intimam fundos, escritórios e pessoas de outros precatórios (ADAUTO FORTES, DOMUS OCTANTE, FLAVIA CEOLIN...).
   Sem cabeçalho, vale o polo A só se tiver uma única pessoa física. Sem nenhuma publicação ainda: volta para a fila
   em ADIAMENTO_SEM_PUBLICACAO.
3. Cada requerente do cabeçalho é classificado (mesmos critérios do corrigir_legado_TJPI.py): pessoa física, fundo
   (FIDC: cessionário), sociedade de advogados (honorários), ente público ou outra empresa. CPF/CNPJ só entra quando o
   texto o escreve logo depois do nome do próprio credor (decisão de pagamento: 'Beneficiário(a) … CPF …'); nome dentro
   de um nome maior ('… JUNIOR') não conta.
4. Decide o credor: 1 pessoa física (ou só uma empresa) -> credor; 2+ pessoas físicas, ou pessoa física com fundo ou
   empresa (cessão) -> SUCESSO_ANALISAR com os nomes só no metadata; só sociedade de advogados -> SUCESSO_INCOMPLETO
   'SUCESSO_SEM_CPF: … regra=HONORARIOS_ADVOGADO'; nenhum nome -> volta para a fila em ADIAMENTO_SEM_CREDOR e, na
   MAX_TENTATIVAS_SEM_CREDOR-ésima vez, FALHA SEM_CREDOR (contador '[sem_credor=N]' no motivo da fila).
5. Originário (só com credor único; decisão do usuário: ligar só quando confirmado). Candidatos: CNJs de 1º grau
   citados no DJEN do precatório e os processos do credor no DJEN pelo nome (TJPI 1º grau, credor no polo ativo, não
   mais novos que o precatório). Confirmado = no DJEN do candidato, o ente no polo passivo e (o credor no polo ativo
   com o CNJ citado no precatório ou o valor do precatório no texto) ou o candidato citando o nº do precatório. OAB
   em comum só reforça: sozinha ela não distingue o conhecimento do cumprimento da mesma pessoa contra o mesmo ente.
   1 confirmado -> liga (fila_credor_registrar_originario, sem capa: não há capa pública); ação coletiva que já tem
   outros credores no banco -> não liga; empate ou nenhum -> candidatos só no metadata.
6. Status: CPF do credor no texto -> SUCESSO_PROCESSO_ORIGINARIO (com originário) ou SUCESSO_PROCESSO_CREDITO, com o
   credor ligado (registrar_credor, corrigindo o CPF divergente do banco). Sem CPF no texto, o próprio robô consulta
   a API de CPF pelo nome (utils/cpf_robo.py, decisão do usuário de 07/10/2026; regra, gate e cache do
   CPF_API/completar_cpf.py): aceito -> SUCESSO_API_TERCEIRO com o credor ligado e as marcas do completar_cpf.py;
   não aceito (homônimo, fora da base, outra região) ou API fora -> SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF', com
   metadata.credor e metadata.cpf_api (o completar_cpf.py não consulta de novo o que já foi tentado).
7. Junta LOTE créditos processados e grava o lote numa transação só, cada crédito no seu SAVEPOINT (erro desfaz só
   ele): originário, credor, advogados do precatório (ADVOGADO pela OAB), metadata (registrar_credito), filas mensais
   (legado) e o status na fila (fila_credor_finalizar).

Rápido: WORKERS threads raspam ao mesmo tempo; só a thread principal pega da fila e grava. O DJEN tem um relógio por
saída (direta e, com --proxies, PROXY_01..05 do .env, que precisam sair pelo Brasil) e o limite dele é por IP,
dividido com os outros robôs da máquina.

Erro passageiro (DJEN fora, timeout) devolve o crédito para a fila (fila_credor_adiar) na hora, nunca vira FALHA.
Ctrl+C devolve os créditos em andamento e grava o lote que já estava processado. O texto das publicações do DJEN não é
gravado em lugar nenhum.

Saídas em TJPI/saida: fetch_TJPI.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql e desfazer_fila_*.sql.

Uso:
    python fetch_TJPI.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJPI.py --simulacao --creditos 411392,409643
    python fetch_TJPI.py                        # processa a fila do TJPI até acabar (Ctrl+C para parar)
    python fetch_TJPI.py --limite 30            # para depois de 30 créditos
    python fetch_TJPI.py --workers 4            # workers (threads) raspando ao mesmo tempo (padrão 3)
    python fetch_TJPI.py --proxies              # soma PROXY_01..05 como saídas extras do DJEN
    python fetch_TJPI.py --sem-cpf-api          # não consulta a API de CPF (fica para o completar_cpf.py)
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
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import psycopg2
import psycopg2.errors
import requests
from dotenv import load_dotenv

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils import banco, cpf_robo  # noqa: E402
from utils.arquivos import anexar_csv  # noqa: E402
from utils.banco import como_dicts  # noqa: E402
from utils.legado import (Backup, limpar_reservas_orfas, numero_do_credito, reserva_de_robo,  # noqa: E402
                          reservar_filas_mensais, soltar_filas_mensais)
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

# =============================================================================== configuração

SAIDA = AQUI / "saida"
load_dotenv(AQUI.parent / ".env")      # PG_* e (com --proxies) PROXY_01..05
log = logging.getLogger("fetch_TJPI")

TRIBUNAL_TJPI = 118
SOFTWARE = "CONSULTA_PUBLICA_TJPI"
GERAR_DESFAZER = False                 # True com --com-desfazer: escreve os arquivos de desfazer (o padrão é não escrever)
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJPI:consulta_publica:{os.getpid()}"
LOTE = 20                              # créditos por transação de gravação
FILAS_MENSAIS = []                     # filas do RPA antigo com permissão de UPDATE (a Rodada preenche): reserva e status
WORKERS = 3                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 5 * 60                  # s por crédito; passou disso, volta para a fila
ADIAMENTO = "30 minutes"               # erro passageiro
ADIAMENTO_SEM_PUBLICACAO = "15 days"   # precatório ainda sem publicação no DJEN
ADIAMENTO_SEM_CREDOR = "15 days"       # publicação sem nome de credor: tenta de novo depois
MAX_TENTATIVAS_SEM_CREDOR = 3          # na 3ª vez sem credor: FALHA SEM_CREDOR
RE_CONTADOR_SEM_CREDOR = re.compile(r"\[sem_credor=(\d+)\]")
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
MAX_FALHAS_SEGUIDAS = 8                # falhas técnicas seguidas que param a rodada
MAX_REINICIOS = 30                     # rodadas que param (fonte fora, banco caiu) antes de desistir de vez
PAUSA_REINICIO = 5 * 60                # s entre uma rodada que parou e a próxima
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"

DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
INTERVALO_DJEN = 1.2                   # s entre consultas por saída (sobe sozinho quando o DJEN devolve 429)
PAUSA_429 = 15                         # s que a saída fica parada depois de um 429 (o intervalo também sobe)
INTERVALO_DJEN_MAX = 6.0
FALHAS_PARA_DESLIGAR_PROXY = 3
MAX_PAGINAS_DJEN_PRECATORIO = 3        # 100 publicações por página
MAX_PAGINAS_DJEN_NOME = 3
MAX_CACHE_DJEN = 5000
MAX_ORIGINARIOS_DJEN = 6               # candidatos a originário conferidos no DJEN por crédito
# sigla do DJEN pelo segmento J.TR do número (a lista do TJPI tem uns poucos precatórios de outro tribunal)
SIGLA_TR = {"818": "TJPI", "810": "TJMA", "807": "TJDFT"}

CNJ_TEXTO = re.compile(r"\b(\d{7})-?(\d{2})\.?(\d{4})\.?8\.?18\.?(\d{4})\b")
# cabeçalho da publicação do precatório: 'REQUERENTE: A, B REQUERIDO: ENTE' (também CREDOR / EXEQUENTE)
RE_CABECALHO = re.compile(r"\b(?:REQUERENTES?|CREDOR(?:\(?[AE]S?\)?)?|EXEQUENTES?)\s*:\s*(?P<v>.{3,1500}?)\s+"
                          r"(?=REQUERID[OA]S?\s*:|EXECUTAD[OA]S?\s*:|DEVEDOR\s*:|Classe\s*:|REQUERENTES?\s*:|"
                          r"(?:ADVOGAD|Advogad)[oaOA]s?(?:\([aA]\))?(?:/Autoridade)?\s*(?::|\(|do\b|da\b|DO\b|DA\b))")
RE_REQUERIDO = re.compile(r"\b(?:REQUERID[OA]S?|EXECUTAD[OA]S?|DEVEDOR)\s*:\s*(?P<v>.{3,200}?)\s*"
                          r"(?=Classe\s*:|[A-ZÀ-Ú][a-zà-ú]|INTIMA|DECIS|DESPACHO|SENTEN|CERTID|ATO ORDINAT|$)")
RE_DOC = re.compile(r"\b(CPF|CNPJ)(?:/MF)?(?:\s*n[º°o]\.?)?\s*:?\s*(\d{3}\.?\d{3}\.?\d{3}-?\d{2}|"
                    r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})")
SUFIXOS_DE_NOME = {"JUNIOR", "JR", "FILHO", "FILHA", "NETO", "NETA", "SOBRINHO", "SOBRINHA", "SEGUNDO", "TERCEIRO"}

RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
RE_FUNDO = re.compile(r"DIREITOS CREDITORIOS|\bFIDC\b|FUNDO DE INVESTIMENTO")
RE_PUBLICO = re.compile(r"^(ESTADO D[OEA]S? |MUNICIPIO D|CAMARA MUNICIPAL|UNIAO FEDERAL|FAZENDA (PUBLICA|NACIONAL|ESTADUAL)"
                        r"|INSTITUTO NACIONAL DO SEGURO|PROCURADORIA|DEFENSORIA PUBLICA|TRIBUNAL D|ASSEMBLEIA LEGISLATIVA"
                        r"|SECRETARIA D|PREFEITURA|FUNDO (MUNICIPAL|ESTADUAL|NACIONAL|DE (SAUDE|PREVIDENCIA))|INSTITUTO D"
                        r"|FUNDACAO |UNIVERSIDADE ESTADUAL|DEPARTAMENTO ESTADUAL|AGENCIA )")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA")
RE_EMPRESA = re.compile(r"\b(?:LTDA|EIRELI|EPP|S A|S/A|CIA|COMPANHIA|ASSOCIACAO|SINDICATO|COOPERATIVA|CONSULTORIA|"
                        r"COMERCIO|COMERCIAL|SERVICOS|CONSTRUTORA|CONSTRUCOES|EMPREENDIMENTOS|PARTICIPACOES|"
                        r"INVESTIMENTOS|BANCO|IGREJA|CONDOMINIO|LIVROS|DISTRIBUIDORA|INDUSTRIA|ME)\b")
# classes de 1º grau que não geram precatório contra o ente
RE_CLASSE_FORA = re.compile(r"PENAL|CRIMIN|CARTA PRECATORIA|CARTA DE ORDEM|INQUERITO|INVENTARIO|ARROLAMENTO|DIVORCIO|"
                            r"ALIMENTOS|EXECUCAO FISCAL|PRECATORIO|PEQUENO VALOR|REQUISICAO")
RE_EXECUCAO = re.compile(r"CUMPRIMENTO|EXECU")
# palavras do nome do ente que não ajudam a reconhecê-lo no polo passivo
PALAVRAS_ENTE = {"MUNICIPIO", "ESTADO", "PREFEITURA", "MUNICIPAL", "DE", "DO", "DA", "DOS", "DAS", "E"}

SEMELHANCA_MESMA_PESSOA = 0.9          # nomes com grafia próxima (SOUSA x SOUZA) contam como a mesma pessoa
STATUS_COM_VINCULO = ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_INCOMPLETO")

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "ente", "ultimo_status", "resultado", "motivo",
           "originario", "regra", "evidencia", "credor", "credor_documento", "requerentes_djen", "fonte_nome",
           "publicacoes_djen", "advogados_djen", "candidatos", "credor_corrigido", "legado", "credores_antes",
           "credores_depois", "cpf_api", "lote", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
SEM_GRAVACAO = {"legado": "", "antes": set(), "depois": set(), "corrigidos": []}


class ErroTecnico(Exception):
    """Falha passageira (DJEN fora do ar, timeout): o crédito volta para a fila, não vira FALHA."""

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
    return bool(a and b) and (a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA)


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


def fmt_credores(itens):
    """{(papel, nome, documento, origem)} -> texto para o CSV."""
    return " | ".join(f"{nome} ({doc or 'sem doc'}, {papel}, {origem})" for papel, nome, doc, origem in sorted(itens))


def sigla_do_numero(numero20):
    """Sigla do tribunal no DJEN pelo J.TR do CNJ (None = consulta só pelo número)."""
    return SIGLA_TR.get(numero20[13:16]) if len(numero20) == 20 else None


def tipo_do_nome(nome):
    """PF (pessoa física), FUNDO (FIDC: cessionário), SOC_ADV (sociedade de advogados), PUBLICO ou EMPRESA."""
    n = normal(nome)
    if RE_FUNDO.search(n):
        return "FUNDO"
    if RE_PUBLICO.search(n):
        return "PUBLICO"
    if RE_SOCIEDADE_ADV.search(n):
        return "SOC_ADV"
    if RE_EMPRESA.search(n):
        return "EMPRESA"
    return "PF"


def chaves_ente(*nomes):
    """Trechos que reconhecem o ente devedor no polo passivo: 'ESTADO DO PIAUI' inteiro; município pela cidade;
    o resto pelas palavras que não são genéricas."""
    chaves = set()
    for nome in nomes:
        n = normal(re.split(r"\s+-\s+", nome or "")[0])
        if not n:
            continue
        m = re.match(r"(?:MUNICIPIO|PREFEITURA MUNICIPAL) D[EOA]S? (.+)", n)
        if m:
            chaves.add(m.group(1))
        elif n.startswith("ESTADO D"):
            chaves.add(n)
        else:
            palavras = [w for w in n.split() if w not in PALAVRAS_ENTE and len(w) > 3]
            if palavras:
                chaves.add(" ".join(palavras))
    return chaves


def ente_no_nome(nome, chaves):
    """O nome (parte do polo passivo) é o ente devedor? Todas as palavras de alguma chave estão no nome."""
    palavras = set(normal(nome).split())
    return any(set(c.split()) <= palavras for c in chaves)

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
    Cache só na memória e só das consultas que se repetem entre créditos (nome do credor, originário candidato)."""

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
        chave = json.dumps([com_texto, params], sort_keys=True, ensure_ascii=False)
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
        """Só o que o robô usa da publicação: número, classe, data, partes por polo, advogados com OAB e (se pedido) o
        texto, que fica só na memória."""
        advogados = []
        for a in item.get("destinatarioadvogados") or []:
            adv = a.get("advogado") or {}
            if adv.get("nome"):
                advogados.append({"nome": re.split(r"\s+REGISTRAD[OA]\(?A?\)?\s+CIVILMENTE", adv["nome"])[0].strip(),
                                  "oab": str(adv.get("numero_oab") or ""), "uf": (adv.get("uf_oab") or "").upper()})
        texto = H.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", item.get("texto") or ""))) if com_texto else ""
        return {"numero": so_digitos(item.get("numero_processo") or item.get("numeroprocessocommascara"))[:20],
                "classe": normal(item.get("nomeClasse")),
                "data": (item.get("data_disponibilizacao") or "")[:10],
                "partes": [[d.get("polo"), d.get("nome") or ""] for d in item.get("destinatarios") or []],
                "advogados": advogados, "texto": texto}

    def paginado(self, com_texto, usar_cache, max_paginas, **params):
        itens = []
        for pagina in range(1, max_paginas + 1):
            lote = self.buscar(com_texto, usar_cache, pagina=pagina, itensPorPagina=100, **params)
            itens += lote
            if len(lote) < 100:
                break
        return itens

    def do_processo(self, numero20, max_paginas=1, usar_cache=True):
        """Publicações de um processo, com o texto."""
        sigla = sigla_do_numero(numero20)
        return self.paginado(True, usar_cache, max_paginas, numeroProcesso=numero20,
                             **({"siglaTribunal": sigla} if sigla else {}))

    def por_nome(self, nome):
        """Publicações do TJPI com a parte pelo nome (sem texto)."""
        return self.paginado(False, True, MAX_PAGINAS_DJEN_NOME, nomeParte=nome, siglaTribunal="TJPI")

# =============================================================================== leitura das publicações


def cnjs_citados(texto):
    """CNJs do TJPI (20 dígitos) citados no texto."""
    return {a + b + c + "818" + d for a, b, c, d in CNJ_TEXTO.findall(texto or "")}


def eh_nome(nome):
    """Nome inteiro (2+ palavras, alguma com mais de uma letra): 'L. A. T.' (iniciais, como o DJEN do TJMA/TJDFT
    publica) não serve para achar a pessoa."""
    palavras = chave_nome(nome).split()
    return len(palavras) >= 2 and any(len(w) > 1 for w in palavras)


def separar_nomes(valor):
    """'A, B E OUTROS, FUNDO X' -> ['A', 'B', 'FUNDO X'] (vírgula ou ';' separam; aposto entre parênteses sai)."""
    nomes = []
    for parte in re.split(r"\s*[,;]\s*", valor or ""):
        parte = re.sub(r"\s*\([^)]*\)\s*", " ", parte)
        parte = re.sub(r"\s+e\s+outr[oa]s?\s*$", "", parte, flags=re.I)
        parte = re.sub(r"^\s*e\s+", "", parte, flags=re.I).strip(" .-")
        if re.search(r"\d", parte) or re.match(r"(?:CPF|CNPJ|RG|OAB|INSCRIT|PORTADOR|REP\b|REPRESENTAD)", normal(parte)):
            continue                                    # documento ou qualificação, não é nome
        if eh_nome(parte):
            nomes.append(" ".join(parte.split()))
    return nomes


def documento_do_nome(textos, nome):
    """CPF/CNPJ que o texto escreve logo depois do nome ('FULANO, CPF 123…'); o nome não pode ser pedaço de um nome
    maior ('FULANO JUNIOR'). Vários documentos diferentes para o mesmo nome: nenhum."""
    k = chave_nome(nome)
    achados = set()
    for t in textos:
        for m in RE_DOC.finditer(t):
            janela = normal(t[max(0, m.start() - 250): m.start()])
            i = janela.rfind(k)
            if i < 0:
                continue
            resto = janela[i + len(k):].split()
            if len(resto) > 6 or (resto and resto[0] in SUFIXOS_DE_NOME):
                continue
            doc = so_digitos(m.group(2))
            if documento_valido(doc):
                achados.add(doc)
    return next(iter(achados)) if len(achados) == 1 else None


def ler_publicacoes(itens):
    """Do DJEN do precatório: requerentes (cabeçalho; sem ele, o polo A), requerido, advogados com OAB das
    publicações que são só deste precatório e os CNJs de 1º grau citados."""
    requerentes, requeridos, textos, citados = [], [], [], set()
    for it in itens:
        t = it["texto"]
        textos.append(t)
        citados |= cnjs_citados(t)
        for m in RE_CABECALHO.finditer(t):
            for nome in separar_nomes(m.group("v")):
                if not any(chave_nome(nome) == chave_nome(x) for x in requerentes):
                    requerentes.append(nome)
        m = RE_REQUERIDO.search(t)
        if m and m.group("v").strip() and m.group("v").strip() not in requeridos:
            requeridos.append(" ".join(m.group("v").split()))
    fonte = "cabeçalho" if requerentes else ""
    if not requerentes:
        # sem cabeçalho: só o polo A com uma única pessoa física (decisões em massa trazem gente de outros precatórios)
        pf = {}
        for it in itens:
            advs = {chave_nome(a["nome"]) for a in it["advogados"]}
            for polo, nome in it["partes"]:
                if polo == "A" and eh_nome(nome) and chave_nome(nome) not in advs and tipo_do_nome(nome) == "PF":
                    pf.setdefault(chave_nome(nome), " ".join(nome.split()))
        if len(pf) == 1:
            requerentes, fonte = list(pf.values()), "polo ativo"
    # advogados: só das publicações em que o polo A não tem ninguém de fora do precatório
    alvo = {chave_nome(n) for n in requerentes}
    advogados = {}
    for it in itens:
        ativos = {chave_nome(n) for p, n in it["partes"] if p == "A"} - {chave_nome(a["nome"]) for a in it["advogados"]}
        de_fora = {n for n in ativos if n not in alvo and tipo_do_nome(n) != "SOC_ADV"}
        if de_fora or not alvo:
            continue
        for a in it["advogados"]:
            numero = (re.match(r"\d+", a["oab"] or "") or [""])[0]
            if numero and len(a["uf"]) == 2:
                advogados.setdefault((a["uf"], numero), {"nome": a["nome"], "oab_uf": a["uf"], "oab_numero": numero,
                                                         "cpf": None})
    return {"requerentes": requerentes, "fonte": fonte, "requerido": " | ".join(requeridos[:2]),
            "advogados": list(advogados.values()), "citados": citados, "textos": textos}

# =============================================================================== fila


# o que o robô pode pegar: do RPA em PENDENTE, do RPA sem credor em qualquer status (menos EM_ANDAMENTO) e o que já é
# do robô em PENDENTE; sempre vencido o disponivel_em. O que já é do robô com status final (SUCESSO/FALHA) não volta.
FILTRO_PEGAVEL = f"""cc.disponivel_em <= now() AND (
       (cc.software_id = {SOFTWARE_RPA}
        AND (cc.status_id = 1 OR NOT EXISTS (SELECT 1 FROM creditos.credito_credor k
                                              WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)))
    OR (cc.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}') AND cc.status_id = 1))"""

SQL_ESCOPO = """
SELECT cc.credito_id, cc.prioridade, cc.valor_referencia,
       EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2) AS tem_credor
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c ON c.id = cc.credito_id
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2 AND {filtro}
"""


def ordenar_escopo(con, filtro):
    """(ids na ordem do robô, contagem). Ordem: sem credor antes de com credor, prioridade de campanha, maior valor."""
    with con.cursor() as cur:
        cur.execute(SQL_ESCOPO.format(filtro=filtro), (TRIBUNAL_TJPI,))
        linhas = como_dicts(cur)
    chave = {x["credito_id"]: (x["tem_credor"], x["prioridade"], -float(x["valor_referencia"] or 0), x["credito_id"])
             for x in linhas}
    return sorted(chave, key=chave.get), Counter("com credor" if x["tem_credor"] else "sem credor" for x in linhas)

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJPI", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, left(c.numero_norm, 20) AS precatorio20,
       tc.codigo AS tipo_credito, li.valor_lista, COALESCE(li.metadata, '{}'::jsonb) AS lista,
       coalesce(e.nome, '') AS ente_nome, cc.motivo_detalhe AS motivo_fila,
       (SELECT st.codigo FROM creditos.coleta_credor_tentativa t JOIN creditos.status_coleta st ON st.id = t.status_id
         WHERE t.credito_id = c.id ORDER BY t.id DESC LIMIT 1) AS ultimo_status
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc  ON tc.id = c.tipo_credito_id
  JOIN creditos.coleta_credor cc ON cc.credito_id = c.id
  LEFT JOIN creditos.ente_alias ea ON ea.id = c.ente_alias_id
  LEFT JOIN creditos.ente e        ON e.id = ea.ente_id
  LEFT JOIN LATERAL (SELECT x.valor_lista, x.metadata FROM creditos.lista_item x
                      WHERE x.credito_id = c.id ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE c.id = %s
"""


def ler_credito(cur, credito_id):
    """O crédito (lead) com o que a lista do TJPI traz: ente devedor e valor."""
    cur.execute(SQL_CREDITO, (credito_id,))
    linhas = como_dicts(cur)
    if not linhas:
        raise RuntimeError(f"crédito {credito_id} não existe")
    lead = linhas[0]
    lista = lead["lista"] or {}
    lead["ente"] = " ".join((lista.get("Ente Devedor") or lista.get("entidade_nome") or lead["ente_nome"] or "").split())
    lead["chaves"] = chaves_ente(lead["ente"], lead["ente_nome"])
    valores = {valor_numerico(lead["valor_lista"]), valor_numerico(lista.get("Valor"))} - {None, 0}
    lead["valores"] = sorted(valores)
    m = RE_CONTADOR_SEM_CREDOR.search(lead["motivo_fila"] or "")
    lead["sem_credor_antes"] = int(m.group(1)) if m else 0
    return lead


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())

# =============================================================================== decisão (só lê: DJEN)


def resultado_vazio():
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "PROCESSO_CREDITO", "originario": None, "regra": "",
            "evidencia": "", "credor": None, "candidato_analisar": None, "candidatos": [], "requerentes": [],
            "fonte_nome": "", "requerido": "", "publicacoes": 0, "advogados": []}


def candidatos_originario(djen, lead, credor, pub):
    """{cnj20: {fontes, classe}}: CNJs de 1º grau do TJPI citados no DJEN do precatório e os processos do credor no
    DJEN pelo nome (credor no polo ativo e não como advogado, não mais novos que o precatório)."""
    prec20, ano = lead["precatorio20"], lead["precatorio20"][9:13]
    cands = {n: {"fontes": {"CITADO_NO_PRECATORIO"}, "classe": ""} for n in pub["citados"]
             if n != prec20 and n[16:] != "0000"}
    alvo = chave_nome(credor)
    for it in djen.por_nome(credor):
        n = it["numero"]
        if len(n) != 20 or n[13:16] != "818" or n[16:] == "0000" or n == prec20 or (ano.isdigit() and n[9:13] > ano):
            continue
        if RE_CLASSE_FORA.search(it["classe"]):
            continue
        advs = {chave_nome(a["nome"]) for a in it["advogados"]}
        if not any(p == "A" and chave_nome(x) == alvo and alvo not in advs for p, x in it["partes"]):
            continue
        c = cands.setdefault(n, {"fontes": set(), "classe": ""})
        c["fontes"].add("NOME_NO_DJEN")
        c["classe"] = c["classe"] or it["classe"]
    return cands


def confirmar_originario(djen, lead, credor, pub, cands):
    """(cnj confirmado ou None, [candidatos com o que se viu], regra). Confere no DJEN de cada candidato (até
    MAX_ORIGINARIOS_DJEN): ente no polo passivo e evidência forte."""
    oabs_prec = {(a["oab_uf"], a["oab_numero"]) for a in pub["advogados"]}
    alvo = chave_nome(credor)
    prec_formatos = {lead["precatorio20"], formatar_cnj(lead["precatorio20"])}
    valores = set().union(*(formatos_valor(v) for v in lead["valores"])) if lead["valores"] else set()
    ordem = sorted(cands, key=lambda n: ("CITADO_NO_PRECATORIO" not in cands[n]["fontes"],
                                         not RE_EXECUCAO.search(cands[n]["classe"] or ""), -int(n[9:13]), n))
    vistos, confirmados = [], []
    for n in ordem[:MAX_ORIGINARIOS_DJEN]:
        itens = djen.do_processo(n)
        ativo = any(p == "A" and chave_nome(x) == alvo for it in itens for p, x in it["partes"]
                    if alvo not in {chave_nome(a["nome"]) for a in it["advogados"]})
        ente = not lead["chaves"] or any(p == "P" and ente_no_nome(x, lead["chaves"])
                                        for it in itens for p, x in it["partes"])
        oabs = {(a["uf"], (re.match(r"\d+", a["oab"] or "") or [""])[0]) for it in itens for a in it["advogados"]}
        textos = " ".join(it["texto"] for it in itens)
        evid = set(cands[n]["fontes"]) & {"CITADO_NO_PRECATORIO"}
        if oabs & oabs_prec:
            evid.add("OAB_EM_COMUM")
        if any(f in textos for f in prec_formatos):
            evid.add("CITA_PRECATORIO")
        if any(v in textos for v in valores):
            evid.add("VALOR")
        # OAB em comum sozinha não confirma: o mesmo advogado leva o conhecimento e o cumprimento da mesma pessoa
        # contra o mesmo ente (na simulação de 07/10 ligou o MS de 2014 no lugar do cumprimento de 2021)
        ok = ente and ("CITA_PRECATORIO" in evid or (ativo and bool(evid - {"OAB_EM_COMUM"})))
        vistos.append({"cnj": formatar_cnj(n), "fontes": sorted(cands[n]["fontes"]), "classe": cands[n]["classe"],
                       "publicacoes": len(itens), "credor_no_ativo": ativo, "ente_no_passivo": ente,
                       "evidencia": sorted(evid), "confirmado": ok})
        if ok:
            confirmados.append(n)
    if len(confirmados) == 1:
        c = next(v for v in vistos if v["cnj"] == formatar_cnj(confirmados[0]))
        return confirmados[0], vistos, "+".join(c["evidencia"])
    return None, vistos, (f"ORIGINARIO_EMPATADO:{len(confirmados)}" if confirmados else "")


def processar(lead, djen, cpf_api=None):
    """Decide credor e originário de um crédito só lendo (DJEN) e, com o credor sem CPF, consulta a API de CPF pelo
    nome. Devolve o resultado a gravar (status ADIAR = volta para a fila)."""
    r = resultado_vazio()
    itens = djen.do_processo(lead["precatorio20"], MAX_PAGINAS_DJEN_PRECATORIO, usar_cache=False)
    r["publicacoes"] = len(itens)
    if not itens:
        r.update(status="ADIAR", intervalo=ADIAMENTO_SEM_PUBLICACAO,
                 motivo="AINDA_SEM_PUBLICACAO: precatório sem publicação no DJEN")
        return r
    pub = ler_publicacoes(itens)
    r.update(requerentes=pub["requerentes"], fonte_nome=pub["fonte"], requerido=pub["requerido"],
             advogados=pub["advogados"])
    tipos = {}
    for nome in pub["requerentes"]:
        tipos.setdefault(tipo_do_nome(nome), []).append(nome)
    pf, fundos, empresas = tipos.get("PF", []), tipos.get("FUNDO", []), tipos.get("EMPRESA", [])
    sociedades = tipos.get("SOC_ADV", [])
    if not pf and not empresas:
        if sociedades:
            r.update(status="SUCESSO_INCOMPLETO", regra="HONORARIOS_ADVOGADO",
                     motivo=f"SUCESSO_SEM_CPF: honorários de {sociedades[0]} (DJEN do precatório) "
                            f"regra=HONORARIOS_ADVOGADO")
            return r
        n = lead["sem_credor_antes"] + 1
        base = (f"SEM_CREDOR: {len(itens)} publicação(ões) no DJEN sem nome de credor"
                + (f" (só {', '.join(tipos.get('PUBLICO', []) + fundos)[:200]})" if tipos else ""))
        if n >= MAX_TENTATIVAS_SEM_CREDOR:
            r.update(status="FALHA", motivo=f"{base} [sem_credor={n}]")
        else:
            r.update(status="ADIAR", intervalo=ADIAMENTO_SEM_CREDOR, motivo=f"{base} [sem_credor={n}]")
        return r
    if len(pf) > 1 or (pf and (fundos or empresas)):
        regra = "CESSAO" if (fundos or empresas) else "VARIOS_REQUERENTES"
        r.update(status="SUCESSO_ANALISAR", regra=regra,
                 candidato_analisar={"credores": pf or empresas, "cessionarios": fundos + (empresas if pf else [])},
                 motivo=f"{regra}: {', '.join(pf)}" + (f"; cessionário(s): {', '.join(fundos + empresas)}"
                                                       if fundos or empresas else ""))
        return r
    nome = pf[0] if pf else empresas[0]
    documento = documento_do_nome(pub["textos"], nome)
    if documento and (len(documento) == 11) != bool(pf):
        documento = None                                # CPF para empresa ou CNPJ para pessoa: não é dele
    credor = {"nome": nome, "documento": documento, "papel_bruto": "REQUERENTE", "nascimento": None}
    # originário: só confirmado
    cands = candidatos_originario(djen, lead, nome, pub)
    originario, vistos, regra_orig = confirmar_originario(djen, lead, nome, pub, cands) if cands else (None, [], "")
    r.update(credor=credor, candidatos=vistos, originario=originario, evidencia=regra_orig if originario else "")
    desc = f"credor {nome} no DJEN do precatório ({pub['fonte']})"
    sufixo = (f"; originário {formatar_cnj(originario)} ({regra_orig})" if originario
              else (f"; {regra_orig}" if regra_orig else ""))
    if documento:
        r.update(status="SUCESSO_PROCESSO_ORIGINARIO" if originario else "SUCESSO_PROCESSO_CREDITO",
                 via="ORIGINARIO" if originario else "PROCESSO_CREDITO", regra="DOCUMENTO_NO_TEXTO",
                 motivo=f"{desc}, documento no texto{sufixo}")
    else:
        r.update(status="SUCESSO_INCOMPLETO", via="ORIGINARIO" if originario else "PROCESSO_CREDITO",
                 regra="NOME_NO_DJEN", motivo=f"SUCESSO_SEM_CPF: {desc}, sem documento público{sufixo}")
        if pf and cpf_api:
            r["cpf_api"] = cpf_api.consultar(nome)     # a gravação liga o CPF se a regra aceitou (utils/cpf_robo.py)
    return r

# =============================================================================== banco: gravação de um crédito


class BackupTJPI(Backup):
    """Backup do utils/legado.py com o desfazer de DELETE (a correção do credor apaga vínculos)."""

    def delete(self, cur, tabela, linha):
        """Guarda o INSERT que recria a linha apagada."""
        if self._criada_aqui(tabela, linha["id"]):
            return
        colunas = list(linha)
        self.p_sql.append(cur.mogrify(f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES "
                                      f"({', '.join(['%s'] * len(colunas))}) ON CONFLICT DO NOTHING;",
                                      list(linha.values())).decode())
        self.p_csv.append(("DELETE", tabela, {"id": linha["id"]}, linha))


def id_do_software(cur, criar):
    """Id do software do robô; cria na 1ª vez (raspa_credor: o próprio robô busca o credor)."""
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJPI",
                                "Credor do TJPI: DJEN (cabeçalho do precatório) + originário confirmado no DJEN "
                                "(TJPI/fetch_TJPI.py)", raspa_credor=True, criar=criar)


def filas_antigas(cur):
    """Filas mensais do legado (de FILA_ANTIGA_DESDE em diante) em que o usuário pode gravar."""
    return banco.filas_antigas(cur, FILA_ANTIGA_DESDE)[0]


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
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJPI'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            reservada = reserva_de_robo(linha)
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO" and not reservada:
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJPI"}
            antes = {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead", "numero_originario",
                                           "ultima_atualizacao")}
            if reservada:
                antes.update(status_coleta_lead=None, motivo_coleta_lead=None)
            bk.update(cur, fila, chave, antes)
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJPI'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Resumo da decisão em credito_fonte.metadata do software (a função troca o JSON inteiro: mescla aqui).
    Do DJEN entram os requerentes, o requerido, a contagem de publicações e os advogados (o texto não é guardado).
    metadata.credor é o que o CPF_API/completar_cpf.py lê para completar o CPF pela API."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "evidencia": r["evidencia"], "candidatos": r["candidatos"][:10],
                     "djen": {"requerentes": r["requerentes"], "requerido": r["requerido"],
                              "publicacoes": r["publicacoes"], "fonte_nome": r["fonte_nome"],
                              "advogados": [f"{a['nome']} ({a['oab_numero']}/{a['oab_uf']})" for a in r["advogados"]]}}
    meta["credor"] = ({"nome": r["credor"]["nome"], "papel": r["credor"]["papel_bruto"],
                       "data_nascimento": None, "cpf_encontrado": bool(r["credor"]["documento"])}
                      if r["credor"] else None)
    meta["candidato_analisar"] = r.get("candidato_analisar")
    if r.get("cpf_api"):
        meta["cpf_api"] = cpf_robo.metadata_cpf_api(r["cpf_api"])
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJPI', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def corrigir_credor(cur, lead, credor, bk):
    """A fonte vence o banco: com o documento do credor confirmado na fonte, apaga os vínculos de CREDOR do crédito
    que são a mesma pessoa (mesmo nome ou grafia próxima) com outro documento, e troca o documento na capa antiga do
    precatório (senão o espelho do legado recriaria o vínculo errado). Herdeiro, cessionário, sucessor, advogado e
    credor com outro nome não são tocados. O desfazer_legado_*.sql guarda o INSERT que recria o que saiu."""
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
        corrigidos.append(f"{nome} ({'outro doc' if doc else 'sem doc'}, {linha['origem']}) -> documento da fonte")
    cur.execute("""SELECT x.id, x.nome, x.cpf_cnpj
                     FROM precatorios.partes_processuais x
                     JOIN precatorios.processos_precatorios pp ON pp.id = x.processo_id
                    WHERE regexp_replace(pp.numero_cnj::text, '\\D', '', 'g') = %s AND x.polo = 'ATIVO'
                      AND x.cpf_cnpj IS DISTINCT FROM %s
                      FOR UPDATE OF x""", (lead["precatorio20"], credor["documento"]))
    for pid, nome, doc in cur.fetchall():
        if mesma_pessoa(re.split(r"\s*\(REQUERENTE\)", nome or "")[0], credor["nome"]) \
                and so_digitos(doc) != credor["documento"]:
            bk.update(cur, "precatorios.partes_processuais", {"id": pid}, {"cpf_cnpj": doc})
            cur.execute("UPDATE precatorios.partes_processuais SET cpf_cnpj = %s WHERE id = %s",
                        (credor["documento"], pid))
            corrigidos.append(f"capa antiga do precatório: {nome} -> documento da fonte")
    return corrigidos


def outros_credores_no_banco(cur, originario, credor, credito_id):
    """Quantos credores (com pessoa) o processo já tem no banco além do credor, quando nenhum outro crédito está
    ligado a ele. Nesse caso o recálculo do banco (recalcular_credores_do_processo) ligaria todos eles como credores
    deste crédito; com 2 ou mais créditos ligados ele não liga credor nenhum."""
    cur.execute("""SELECT count(*) FROM creditos.processo_parte pa JOIN creditos.processo pr ON pr.id = pa.processo_id
                    WHERE pr.numero_cnj = creditos.cnj_normalizar(%s) AND pa.papel_id = 1 AND pa.pessoa_id IS NOT NULL
                      AND pa.pessoa_id IS DISTINCT FROM (SELECT id FROM creditos.pessoa
                                                          WHERE documento = creditos.documento_normalizar(%s))
                      AND NOT EXISTS (SELECT 1 FROM creditos.credito_originario co
                                       WHERE co.processo_id = pr.id AND co.credito_id <> %s)""",
                (formatar_cnj(originario), (credor or {}).get("documento"), credito_id))
    return cur.fetchone()[0]


def gravar_advogados_precatorio(cur, cid, advogados):
    """Advogados do precatório (DJEN) ligados ao crédito como ADVOGADO (pela OAB): vale em todo lead processado,
    mesmo sem credor, porque saem do próprio precatório."""
    for a in advogados:
        cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'ADVOGADO', p_nome => %s,
                                                        p_documento => %s, p_oab_uf => %s, p_oab_numero => %s,
                                                        p_fonte => %s)""",
                    (cid, a["nome"], a["cpf"], a["oab_uf"], a["oab_numero"], SOFTWARE))
    return len(advogados)


def gravar(cur, lead, r, filas, status_legado, bk):
    """Grava o resultado de um crédito (quem chama cuida do SAVEPOINT e do COMMIT). Devolve o resumo para o CSV."""
    cid = lead["credito_id"]
    id_do_software(cur, criar=True)                     # na simulação ele nasce e morre nesta transação
    antes = credores_do_credito(cur, cid)
    legado, proc, corrigidos = Counter(), None, []
    vincula = r["originario"] and r["status"] in STATUS_COM_VINCULO
    if vincula:
        outros = outros_credores_no_banco(cur, r["originario"], r["credor"], cid)
        if outros:
            # ação coletiva que já está no banco: ligar o originário faria o recálculo ligar os outros autores como
            # credores deste crédito
            vincula = False
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
    if credor and credor["documento"] and r["status"] in ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_PROCESSO_CREDITO"):
        cur.execute("SELECT creditos.documento_de_parte(%s)", (credor["documento"],))
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, credor["nome"], credor["documento"], proc, SOFTWARE))
            corrigidos = corrigir_credor(cur, lead, credor, bk)
    # CPF pela API de CPF (consultada no processar): liga o credor e o crédito vai para SUCESSO_API_TERCEIRO
    res_api, vinculo_api = r.get("cpf_api"), None
    if res_api and res_api["aceito"] and r["status"] == "SUCESSO_INCOMPLETO":
        vinculo_api = cpf_robo.ligar_credor(cur, cid, credor["nome"], res_api, proc)
    legado["advogados_precatorio"] += gravar_advogados_precatorio(cur, cid, r["advogados"])
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "evidencia": r["evidencia"],
               "requerentes": r["requerentes"], "fonte_nome": r["fonte_nome"],
               "candidatos": [c["cnj"] for c in r["candidatos"][:10]]}
    if vinculo_api:
        r["status"], r["motivo"], detalhe = cpf_robo.motivo_e_detalhe(
            res_api, formatar_cnj(r["originario"]) if vincula else None, r["motivo"], detalhe, SOFTWARE)
        r["via"] = "NOME"
    registrar_metadata(cur, lead, r)
    legado += atualizar_filas_mensais(cur, filas, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe.update(credores_antes=fmt_credores(antes), credores_depois=fmt_credores(depois))
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => %s, p_sistema => 'PJE',
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (cid, WORKER, r["status"], r["motivo"][:2000], r["via"],
                 formatar_cnj(r["originario"]) if vincula else None,
                 json.dumps(detalhe, ensure_ascii=False, default=str), socket.gethostname()))
    tentativa = cur.fetchone()[0]
    if vinculo_api:
        cpf_robo.marcar_vinculo(cur, vinculo_api, tentativa)
    return {"legado": ", ".join(f"{k}={v}" for k, v in sorted(legado.items())),
            "antes": antes, "depois": depois, "corrigidos": corrigidos}

# =============================================================================== banco: gravação em lote


def marcar_falha(cur, credito_id, erro):
    """FALHA na fila para o crédito que não gravou, dentro da transação do lote (num SAVEPOINT próprio: se até isso
    falhar, o lote segue)."""
    cur.execute("SAVEPOINT falha")
    try:
        soltar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJPI", WORKER)
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
        erro = f"PERSISTENCIA: {(str(e).splitlines() or [''])[0][:300]}"
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
        credor = r["credor"] or {}
        linha.update(resultado=r["status"], motivo=r["motivo"],
                     originario=formatar_cnj(r["originario"]) if r["originario"] else "", regra=r["regra"],
                     evidencia=r["evidencia"], credor=credor.get("nome", ""),
                     credor_documento=credor.get("documento") or "",
                     cpf_api=cpf_robo.texto_csv(r.get("cpf_api")),
                     candidatos=" | ".join(f"{c['cnj']}[{'+'.join(c['evidencia']) or '-'}"
                                           f"{' OK' if c['confirmado'] else ''}]" for c in r["candidatos"][:8]),
                     legado=g["legado"], credor_corrigido=" | ".join(g["corrigidos"]),
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
    momento. UPDATE atômico: se outra instância já o pegou, não afeta nenhuma linha. Na mesma transação reserva as
    linhas do precatório nas filas mensais do RPA antigo; se o RPA (token A3) está com uma delas, desfaz tudo e não
    pega. Devolve (software, status, disponivel_em) de antes, ou None."""
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
                    (credito_id, TRIBUNAL_TJPI, id_software, WORKER, lease))
        antes = cur.fetchone()
        if antes and not reservar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJPI",
                                                WORKER):
            con.rollback()
            log.info(f"{credito_id}: o RPA está processando o precatório (fila mensal em andamento); fica para depois")
            return None
    con.commit()
    return antes


def anotar_desfazer_fila(rod, credito_id, antes):
    """Lead que era do RPA: guarda o UPDATE que o devolve ao RPA com o status de antes (desfazer_fila_<rodada>.sql)."""
    software, status, disponivel = antes
    if software != SOFTWARE_RPA or not GERAR_DESFAZER:
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
        soltar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJPI", WORKER)
        if motivo:
            cur.execute("SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
                        (credito_id, WORKER, intervalo, motivo[:2000]))
        else:
            cur.execute("SELECT creditos.fila_credor_liberar(%s, %s)", (credito_id, WORKER))
    con.commit()

# =============================================================================== execução


class Rodada:
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, DJEN e contadores.
    Ao nascer prepara o banco (software, filas antigas, status do legado) e a ordem da fila (na simulação, a amostra
    sai dela e nada é reservado)."""

    def __init__(self, simulacao, limite, workers, usar_proxies, creditos=(), usar_cpf_api=True):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers = simulacao, limite, workers
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJPI{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = BackupTJPI(SAIDA / f"desfazer_legado_{self.rodada}{sufixo}.sql",
                             SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo, "fetch_TJPI.py")
        self.bk.gravar_arquivos = GERAR_DESFAZER
        self.resultados, self.falhas_seguidas, self.n, self.n_lote = Counter(), 0, 0, 0
        self.parar, self.fila_vazia = threading.Event(), False
        # o 1º crédito do lote espera os outros LOTE-1, que andam `workers` por vez
        self.lease = f"{(LOTE // workers + 2) * TETO_CREDITO // 60 + 30} minutes"

        self.con = conectar(escrita=True)
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        FILAS_MENSAIS[:] = self.filas
        self.djen = Djen(self.parar, usar_proxies)
        self.cpf_api = cpf_robo.CpfNoRobo("TJPI", "PI", ligar=usar_cpf_api)
        log.info(f"API de CPF {self.cpf_api.motivo}")
        self.local, self.conexoes, self.trava = threading.local(), [], threading.Lock()
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | lease {self.lease} | DJEN: {', '.join(s.nome for s in self.djen.saidas)} | "
                 f"filas antigas: {', '.join(self.filas)}")
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
        """Ordem do robô (sem credor primeiro)."""
        con_l = conectar()
        try:
            ordem, contagem = ordenar_escopo(con_l, FILTRO_PEGAVEL)
        finally:
            con_l.close()
        log.info(f"ordem da fila: {len(ordem)} créditos | " + ", ".join(f"{k}: {v}" for k, v in contagem.items()))
        return ordem

    def reordenar(self):
        """Remonta a ordem da fila e volta ao começo dela (repete a cada REORDENAR_A_CADA). Antes, solta nas filas
        mensais as reservas de robô que ficaram sem dono (robô que caiu)."""
        with self.con.cursor() as cur:
            soltas = limpar_reservas_orfas(cur, self.filas, "TJPI")
        self.con.commit()
        if soltas:
            log.info(f"filas mensais: {soltas} reserva(s) de robô sem dono soltas")
        self.ordem, self.posicao, self.ultima_ordem = self.ordenar(), 0, time.time()

    def reconectar(self):
        """Conexão de escrita nova (a antiga caiu)."""
        try:
            self.con.close()
        except Exception:
            pass
        self.con = conectar(escrita=True)

    def renovar_conexoes(self):
        """Depois de uma espera longa: o Postgres derruba sessão parada há mais de 15 min (idle_session_timeout).
        Abre a conexão de escrita de novo e fecha as de leitura das threads (cada uma reabre a sua na próxima leitura)."""
        self.reconectar()
        with self.trava:
            for con in self.conexoes:
                try:
                    con.close()
                except Exception:
                    pass
            self.conexoes.clear()

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
        log.info("fila do TJPI vazia." if not rod.simulacao else "amostra acabou.")
    return credito_id


def esperar_adiados(rod):
    """Fila vazia no modo real: se algum crédito adiado por erro passageiro (ADIAMENTO) vence em breve, espera por
    ele e volta a trabalhar (True). Sem nenhum, a carga acabou (False): só sobram os sem publicação ou sem credor,
    que voltam em ADIAMENTO_SEM_PUBLICACAO / ADIAMENTO_SEM_CREDOR."""
    if rod.simulacao or rod.parar.is_set() or (rod.limite and rod.n >= rod.limite):
        return False

    def proximo(con):
        with con.cursor() as cur:
            cur.execute("""SELECT EXTRACT(EPOCH FROM min(cc.disponivel_em) - now())
                             FROM creditos.coleta_credor cc JOIN creditos.credito c ON c.id = cc.credito_id
                            WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id = 1
                              AND cc.software_id IN (%s, %s)
                              AND cc.disponivel_em <= now() + %s::interval + interval '15 minutes'""",
                        (TRIBUNAL_TJPI, SOFTWARE_RPA, rod.id_software, ADIAMENTO))
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
    rod.renovar_conexoes()
    rod.fila_vazia = False
    rod.reordenar()
    return True


def processar_credito(rod, credito_id, n):
    """(Numa thread) Lê o crédito e decide credor e originário (DJEN), sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r} ou {tipo: ADIAR, credito_id, motivo, intervalo, linha, tecnico}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    try:
        try:
            with rod.conexao_da_thread().cursor() as cur:
                lead = ler_credito(cur, credito_id)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            rod.local.con = None                        # conexão de leitura parada demais e derrubada: abre outra
            with rod.conexao_da_thread().cursor() as cur:
                lead = ler_credito(cur, credito_id)
        linha.update(precatorio=lead["precatorio"], ente=lead["ente"], ultimo_status=lead["ultimo_status"])
        r = processar(lead, rod.djen, rod.cpf_api)
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
    linha.update(segundos=round(time.time() - inicio), requerentes_djen=" | ".join(r["requerentes"]),
                 fonte_nome=r["fonte_nome"], publicacoes_djen=r["publicacoes"],
                 advogados_djen=" | ".join(f"{a['nome']} ({a['oab_numero']}/{a['oab_uf']})" for a in r["advogados"]))
    credor = r["credor"] or {}
    log.info(f"[{n}] {credito_id} {lead['precatorio']} -> {r['status']} "
             f"{formatar_cnj(r['originario']) if r['originario'] else ''} "
             f"{'doc ' + credor['documento'][:3] + '…' if credor.get('documento') else ''} "
             f"| {(credor.get('nome') or ', '.join(r['requerentes']))[:50]} "
             f"| {r['regra'] or r['motivo'].split(':')[0]} | {linha['segundos']} s")
    if r["status"] == "ADIAR":
        linha.update(resultado="ADIADO_" + r["motivo"].split(":")[0], motivo=r["motivo"])
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": r["motivo"], "intervalo": r["intervalo"],
                "linha": linha, "tecnico": False}
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
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (DJEN fora ou bloqueando).")
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
    log.info(f"DJEN: {rod.djen.n_429} resposta(s) 429 | API de CPF: {rod.cpf_api.chamadas_api} chamada(s)")
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --creditos, --proxies, --sem-cpf-api, --com-desfazer e --sem-desfazer."""
    ap = argparse.ArgumentParser(description="Credor do TJPI pela consulta pública (DJEN), "
                                             f"gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS})")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
    ap.add_argument("--proxies", action="store_true",
                    help="soma PROXY_01..05 do .env como saídas do DJEN (precisam sair pelo Brasil)")
    ap.add_argument("--sem-cpf-api", action="store_true",
                    help="não consulta a API de CPF (o credor sem CPF fica para o CPF_API/completar_cpf.py)")
    ap.add_argument("--com-desfazer", action="store_true",
                    help="escreve os arquivos de desfazer (desfazer_*.sql e backup .csv); o padrão é não escrever")
    ap.add_argument("--sem-desfazer", action="store_true",
                    help="não escreve os arquivos de desfazer (já é o padrão; os ciclos do modo 5 do RPA passam)")
    args = ap.parse_args()
    global GERAR_DESFAZER
    GERAR_DESFAZER = args.com_desfazer and not args.sem_desfazer
    return args


def executar(args, creditos):
    """Uma rodada: mantém `workers` créditos raspando, junta os prontos no lote e grava a cada LOTE; com a fila vazia,
    espera os adiados por erro passageiro que vencem logo. Devolve True se a carga acabou (ou --limite, ou Ctrl+C) e
    False se a rodada parou por falhas técnicas seguidas ou erro inesperado (main reinicia). Créditos em andamento
    voltam para a fila; o lote já processado é gravado ao encerrar."""
    inicio = time.time()
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), args.proxies, creditos,
                 usar_cpf_api=not args.sem_cpf_api)
    pendentes, em_voo, terminou = [], {}, False
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjpi")
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
    """Roda até acabar a carga: se uma rodada para (DJEN fora, banco caiu, erro inesperado), espera PAUSA_REINICIO e
    começa outra (até MAX_REINICIOS). Simulação e --limite rodam uma vez só."""
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    creditos = [int(x) for x in re.findall(r"\d+", args.creditos)]
    if creditos and not args.simulacao:
        raise SystemExit("--creditos só vale com --simulacao")
    for tentativa in range(1, MAX_REINICIOS + 1):
        try:
            if executar(args, creditos):
                log.info("carga do TJPI encerrada.")
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
