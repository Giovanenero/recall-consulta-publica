"""
fetch_TJRR.py - credor dos precatórios do TJRR pelas fontes públicas, de ponta a ponta, sem login: lê o banco, acha o
credor (SGP + Murakî + Projudi + DJEN), confirma o originário e grava no banco em lotes de LOTE créditos
(constante abaixo). No lugar do modo credor do RPA_SISTEMAS, que entra no Projudi com SSO+2FA e não abre o
precatório (é sigiloso: 988 SEGREDO_DE_JUSTICA e 552 PROCESSO_NAO_ENCONTRADO nas tentativas do RPA). Cada crédito é
reservado com lease, também nas filas mensais do RPA antigo (CREDOR_EM_ANDAMENTO com a marca do robô: o RPA não o
pega), e só nessa hora passa do RPA para o software próprio.

No TJRR o número do crédito é o processo do PRÓPRIO precatório no Projudi (Núcleo de Precatórios, sigiloso), não o
originário. A lista traz o ofício (Autos), o ID SGP e a vara de origem; o originário não vem em lugar nenhum.

Fontes (TJRR/captcha_TJRR.py resolve os dois captchas Tencent num Chrome próprio, numa thread só dele):
- SGP (precatorios.tjrr.jus.br/rest, sem captcha): lista de pagos com o nome do credor (credorPrincipal); detalhe do
  precatório com juízo de origem e advogados (só nomes).
- Murakî (mesma API, captcha Tencent, token de 30 min): GET /precatorios/muraki/{processo} devolve o CPF/CNPJ de UM
  beneficiário e o valor. valor > 0 -> o credor; valor = 0 -> o beneficiário dos honorários (advogado ou sociedade).
  ?documento=<CPF> confirma um candidato (200 com valor > 0) ou não (404). Pagos, cancelados e indeferidos: 404.
- Projudi (consulta pública, captcha Tencent, token de ~50 chamadas ou ~8 min): busca por CPF/CNPJ ou nome da parte e
  o detalhe do processo (polos, advogados com OAB, vara, movimentações). Não mostra CPF; o precatório dá 404.
- DJEN: nome do credor na 'Lista de distribuição' do precatório (só 2025+) e o texto do originário (valor, nº do
  precatório) para desempatar originários.

Esteira por crédito (faixas na ordem da fila):
1. NAO_PAGO: Murakî. valor > 0 -> CPF do credor. valor = 0 -> o advogado fica como ADVOGADO e o credor sai de um
   candidato confirmado no Murakî (CONTATOS do banco; nome do DJEN -> pessoas do banco com esse nome) ou só pelo nome
   (DJEN). Projudi pelo CPF: o nome do credor (o que se repete nos processos achados pelo CPF, ou o do banco) e o
   originário (cumprimento contra o ente com o credor no polo ativo; evidência: vara de origem, 'EXPEDIR PRECATÓRIO'
   perto da apresentação, valor ou nº do precatório no DJEN do originário).
2. HONORARIOS (a lista só tem a linha 'Honorários Advocatícios'): decisão do usuário de 02/10/2026, como no TJRJ: o
   advogado/sociedade fica só como ADVOGADO (com o documento do Murakî), sem CREDOR; SUCESSO_INCOMPLETO
   'SUCESSO_SEM_CPF: ... regra=HONORARIOS_ADVOGADO'.
3. PAGO: nome do SGP; Projudi pelo nome (ou pelo CPF do CONTATOS com o mesmo nome, que o Projudi confirma) -> o
   originário. CPF achado só por nome no banco é só candidato (metadata), nunca ligado (decisão do usuário).
4. CANCELADO/INDEFERIDO: FALHA definitiva (decisão do usuário).

Status: CPF + originário -> SUCESSO_PROCESSO_ORIGINARIO; CPF sem originário confirmado -> SUCESSO_PROCESSO_CREDITO
(credor confirmado no próprio precatório); CPF com originário empatado -> SUCESSO_ANALISAR (credor ligado); só o nome
-> SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF'; sem credor nem candidato (CREDOR_NAO_IDENTIFICADO, FORA_DO_MURAKI) -> volta
para a fila em 60 dias e, na 3ª vez, FALHA SEM_CREDOR (decisão do usuário, 05/10/2026; contador '[sem_credor=N]' no
motivo da fila); cancelado -> FALHA.

Gravação (igual ao TJMT): lote numa transação, cada crédito no seu SAVEPOINT; credor com CPF via registrar_credor
(corrigindo o CPF divergente do banco, só com CPF de valor > 0), advogados com OAB do originário e o beneficiário dos
honorários como ADVOGADO, capa recortada do originário (só o credor, o polo passivo e os advogados; ação coletiva sem
advogados), metadata (registrar_credito), capa antiga e filas mensais (legado) e o status (fila_credor_finalizar).

Saídas em TJRR/saida: fetch_TJRR.csv (1 linha por crédito) e fetch_credores_trocados.csv; com --com-desfazer,
também fetch_legado_backup.csv, desfazer_legado_*.sql e desfazer_fila_*.sql (o padrão é não escrever o desfazer).

Uso:
    python fetch_TJRR.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJRR.py --simulacao --creditos 1230524,1231207
    python fetch_TJRR.py                        # processa a fila do TJRR até acabar (Ctrl+C para parar)
    python fetch_TJRR.py --limite 50            # para depois de 50 créditos
    python fetch_TJRR.py --workers 3            # workers (threads) raspando ao mesmo tempo (padrão 3)
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
if str(AQUI) not in sys.path:
    sys.path.insert(0, str(AQUI))

import captcha_TJRR as captcha  # noqa: E402
from utils import banco  # noqa: E402
from utils.arquivos import anexar_csv  # noqa: E402
from utils.banco import chave_texto_lote, como_dicts, partes_do_banco  # noqa: E402
from utils.legado import (limpar_reservas_orfas, numero_do_credito, reserva_de_robo,  # noqa: E402
                          reservar_filas_mensais, soltar_filas_mensais)
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

# =============================================================================== configuração

SAIDA = AQUI / "saida"
load_dotenv(AQUI.parent / ".env")      # PG_*
log = logging.getLogger("fetch_TJRR")

TRIBUNAL_TJRR = 123
SOFTWARE = "CONSULTA_PUBLICA_TJRR"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJRR:consulta_publica:{os.getpid()}"
GERAR_DESFAZER = False                 # True com --com-desfazer: escreve os arquivos de desfazer (o padrão é não escrever)
LOTE = 20                              # créditos por transação de gravação
FILAS_MENSAIS = []                     # filas do RPA antigo com permissão de UPDATE (a Rodada preenche): reserva e status
WORKERS = 3                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 8 * 60                  # s por crédito; passou disso, volta para a fila
ADIAMENTO = "30 minutes"               # erro passageiro
ADIAMENTO_SEM_CREDOR = "60 days"       # sem credor nem candidato para revisar: tenta de novo depois (decisão do usuário)
MAX_TENTATIVAS_SEM_CREDOR = 3          # na 3ª vez sem credor: FALHA SEM_CREDOR
RE_CONTADOR_SEM_CREDOR = re.compile(r"\[sem_credor=(\d+)\]")
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
MAX_FALHAS_SEGUIDAS = 8                # falhas técnicas seguidas que param a rodada
MAX_REINICIOS = 30                     # rodadas que param (fonte fora, banco caiu) antes de desistir de vez
PAUSA_REINICIO = 5 * 60                # s entre uma rodada que parou e a próxima
AMOSTRA_SIMULACAO = 20
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"
FAIXAS = {1: "NAO_PAGO", 2: "HONORARIOS", 3: "PAGO", 4: "OUTRA_SITUACAO", 5: "CANCELADO"}
MORTOS = {"CANCELADO", "INDEFERIDO"}
VIVOS = {"REQUISITADO", "AUTUADO"}         # o Murakî mostra; Suspenso, Aguardando Baixa, Pago Parcialmente: 404

SGP = "https://precatorios.tjrr.jus.br/rest"
PAGOS_POR_PAGINA = 500
DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
INTERVALO_DJEN = 1.2                   # s entre consultas (o limite do DJEN é por IP e dividido com os outros robôs)
PAUSA_429 = 15
INTERVALO_DJEN_MAX = 6.0
MAX_CACHE = 3000
TIMEOUT_HTTP = 60
RITMO_SGP = 4.0                        # req/s no SGP e no Murakî (mesmo servidor)
RITMO_PROJUDI = 2.0                    # req/s no Projudi (--ritmo)
RITMO_MINIMO = 0.3
PAUSA_ERRO = 60                        # s que todos os workers param depois de um erro de servidor
SUBIR_A_CADA = 30                      # respostas boas seguidas para subir o ritmo em 25% (até o teto): depois de uma
                                       # instabilidade do TJRR, volta de 0,3 a 2 req/s em ~300 respostas (~15 min)
FATOR_SUBIDA = 1.25
HTTP_FREIA = {403, 429, 502, 503, 504}
H_PROJUDI = {"Accept": "application/json, text/plain, */*",
             "Referer": "https://consultaprojudi.tjrr.jus.br/app/consulta"}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"
EXCEDEU = "EXCEDEU"                    # o Projudi recusou a busca: 'excedeu o limite de resultados'
TAMANHO_BUSCA = 50
MAX_DETALHES = 4                       # processos abertos no Projudi por busca
MAX_ORIGINARIOS_DJEN = 3               # candidatos a originário consultados no DJEN para desempatar
MAX_CANDIDATOS_BANCO = 3               # pessoas do banco com o nome do DJEN testadas no Murakî
# dias entre a apresentação do precatório e a movimentação de expedição no originário (apresentação - movimentação)
JANELA_EXPEDICAO = (-15, 180)
# busca binária nas movimentações só até este tamanho (10 por página): ação coletiva gigante (6.600 movimentações) custa
# ~15 chamadas por candidato e a expedição de um precatório se perde entre as dos outros autores
MAX_PAGINAS_MOVIMENTACOES = 100

CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
# classes que não geram precatório contra o ente, e as recursais
RE_CLASSE_FORA = re.compile(r"PENAL|CRIMIN|CARTA PRECATORIA|CARTA DE ORDEM|INQUERITO|ALVARA|INVENTARIO|ARROLAMENTO|"
                            r"DIVORCIO|ALIMENTOS|TERMO CIRCUNSTANCIADO|MEDIDAS PROTETIVAS|EXECUCAO FISCAL|"
                            r"BUSCA E APREENSAO|PRECATORIO|REQUISICAO|PEQUENO VALOR|APELACAO|AGRAVO|RECURSO|"
                            r"EMBARGOS DE DECLARACAO|REMESSA NECESSARIA|CONFLITO DE COMPETENCIA")
# fundos públicos (FREBOM, fundo da PM) são credores frequentes em RR; fundo de investimento (FIDC) não é público
RE_ORGAO_PUBLICO = re.compile(r"^(?:ESTADO D|MUNICIPIO D|UNIAO\b|DISTRITO FEDERAL)|PROCURADORIA|DEFENSORIA PUBLICA|"
                              r"MINISTERIO PUBLICO|FAZENDA PUBLICA|PREFEITURA|CAMARA MUNICIPAL|TRIBUNAL D|"
                              r"ADVOCACIA[- ]GERAL|"    # AGU: começa com ADVOCACIA, mas não é sociedade de advogados
                              r"INSTITUTO NACIONAL DO SEGURO SOCIAL|"
                              r"^FUNDO (?!.*INVESTIMENTO)(?:DE |MUNICIPAL|ESTADUAL|ESPECIAL|ROTATIVO)|"
                              r"CORPO DE BOMBEIROS|POLICIA MILITAR|POLICIA CIVIL")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA|ESCRITORIO DE ADVOCACIA")
RE_MOV_PRECATORIO = re.compile(r"PRECAT", re.I)
# advogado como o Projudi escreve: 'THALES GARRIDO PINHO FORTE - 776N-RR', 'Z SANDRO (SUB) BUENO - 325A-RR'
RE_ADV_PROJUDI = re.compile(r"^(.*\S)\s+-\s+0*(\d{1,7})[A-Z]?\s*-\s*([A-Z]{2})\s*$")
# força de cada evidência do originário
FORCA = {"NUM_PRECATORIO": 4, "VALOR": 3, "EXPEDICAO_PERTO": 2, "VARA_ORIGEM": 1}
FORCA_MINIMA = 2
SEMELHANCA_MESMA_PESSOA = 0.9
PAPEL_LEGADO = {"ATIVO": "REQUERENTE", "PASSIVO": "REQUERIDO"}    # como o RPA grava a capa antiga
POLO_BRUTO = {"ATIVO": "AUTOR", "PASSIVO": "REU"}
STATUS_COM_VINCULO = ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_INCOMPLETO")

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "faixa", "ente", "situacao", "ultimo_status",
           "resultado", "motivo", "originario", "regra", "evidencia", "credor", "credor_documento", "fonte_documento",
           "fonte_nome", "muraki_valor", "honorarios_documento", "candidatos", "credor_corrigido", "banco", "legado",
           "credores_antes", "credores_depois", "lote", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set(), "corrigidos": []}


class ErroTecnico(Exception):
    """Falha passageira (fonte fora do ar, captcha, timeout): o crédito volta para a fila, não vira FALHA."""


class Adiar(Exception):
    """O crédito volta para a fila daqui a `intervalo` sem contar como falha técnica (sem credor ainda)."""

    def __init__(self, motivo, intervalo):
        super().__init__(motivo)
        self.intervalo = intervalo

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


def eh_pessoa_comum(nome):
    """Nem ente público nem sociedade de advogados."""
    n = chave_nome(nome)
    return bool(n) and not RE_ORGAO_PUBLICO.search(n) and not RE_SOCIEDADE_ADV.search(n)


def termo_do_ente(ente):
    """Trecho que identifica o ente no polo passivo do Projudi: a cidade do município, 'SEGURO SOCIAL' para o INSS,
    o nome sem a sigla depois do ' - ' para o resto ('ESTADO DE RORAIMA')."""
    n = normal(ente)
    if re.search(r"\bINSS\b|SEGURIDADE SOCIAL|SEGURO SOCIAL", n):
        return "SEGURO SOCIAL"
    n = normal(re.split(r"\s+-\s+", ente)[0]) or n
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


def advogado_do_projudi(texto):
    """'THALES GARRIDO PINHO FORTE - 776N-RR' -> {nome, oab_uf: RR, oab_numero: 776, cpf: ''}; sem OAB, só o nome."""
    t = " ".join((texto or "").split())
    m = RE_ADV_PROJUDI.match(t)
    if not m:
        return {"nome": t, "oab_uf": "", "oab_numero": "", "cpf": ""}
    return {"nome": m.group(1).strip(), "oab_uf": m.group(3), "oab_numero": m.group(2), "cpf": ""}

# =============================================================================== ritmo e HTTP


class Ritmo:
    """Teto de requisições por segundo a um servidor, dividido entre todos os workers, que se ajusta sozinho:
    resposta de carga/bloqueio (403, 429, 502-504, timeout, conexão) corta o ritmo pela metade (até RITMO_MINIMO) e
    pausa todos os workers por PAUSA_ERRO; SUBIR_A_CADA respostas boas seguidas sobem 25% (até o teto)."""

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
                self.atual, self.boas = min(self.teto, self.atual * FATOR_SUBIDA), 0

    def freia(self, motivo):
        with self.trava:
            self.boas, self.freadas = 0, self.freadas + 1
            antes, self.atual = self.atual, max(RITMO_MINIMO, self.atual / 2)
            self.pausa_ate = max(self.pausa_ate, time.monotonic() + PAUSA_ERRO)
        log.warning(f"ritmo {self.nome}: {motivo} -> {antes:.1f} para {self.atual:.1f} req/s e pausa de {PAUSA_ERRO}s")


class Http:
    """Sessão requests por thread."""

    def __init__(self, headers):
        self.headers = {"User-Agent": UA, **headers}
        self.local = threading.local()

    def sessao(self):
        if not getattr(self.local, "sessao", None):
            self.local.sessao = requests.Session()
            self.local.sessao.headers.update(self.headers)
        return self.local.sessao

# =============================================================================== SGP e Murakî


class Sgp:
    """API pública do SGP (sem captcha): lista de pagos (na memória, carregada uma vez por rodada) e detalhe."""

    def __init__(self, parar):
        self.parar, self.ritmo, self.http = parar, Ritmo("SGP", RITMO_SGP), Http({"Accept": "application/json"})
        self.pagos_por_autos, self.pagos_por_id = {}, {}

    def _get(self, caminho, **params):
        erro = ""
        for tentativa in range(3):
            self.ritmo.esperar(self.parar)
            try:
                r = self.http.sessao().get(SGP + caminho, params=params, timeout=TIMEOUT_HTTP)
                if r.status_code == 200:
                    self.ritmo.ok()
                    return r.json()
                if r.status_code == 404:
                    return None
                erro = f"HTTP {r.status_code}"
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except (requests.RequestException, ValueError) as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(2 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o SGP do TJRR não respondeu ({erro})")

    def carregar_pagos(self):
        """Índices {autos: [pagos]} e {id: pago} com todos os precatórios pagos do SGP."""
        inicio, vistos = time.time(), {}
        total = None
        for pagina in range(1000):
            d = self._get("/precatorios/pagos", pageNumber=pagina, size=PAGOS_POR_PAGINA) or {}
            itens = d.get("list") or []
            total = d.get("fullListSize") or total
            for x in itens:
                vistos[x["id"]] = x
            if not itens or (total and len(vistos) >= total):
                break
        for x in vistos.values():
            self.pagos_por_id[str(x["id"])] = x
            self.pagos_por_autos.setdefault(so_digitos(x.get("autos")), []).append(x)
        log.info(f"SGP: {len(vistos)} precatório(s) pago(s) com o nome do credor ({time.time() - inicio:.0f} s)")

    def pagos_do(self, lead):
        """Precatórios pagos deste crédito (pelo número do processo e pelos IDs do SGP)."""
        achados = {str(x["id"]): x for x in self.pagos_por_autos.get(lead["numero_norm"][:20], [])}
        for i in lead["sgp_ids"]:
            x = self.pagos_por_id.get(so_digitos(i))
            if x and so_digitos(x.get("autos")) == lead["numero_norm"][:20]:
                achados[str(x["id"])] = x
        return list(achados.values())

    def detalhe(self, lead):
        """Detalhe do 1º ID SGP do crédito que responder ({} se nenhum)."""
        for i in lead["sgp_ids"]:
            d = self._get(f"/precatorios/{i}")
            if d:
                return d
        return {}


class Muraki:
    """Consulta do Murakî ('Tenho precatório a receber?') com o token do captcha (header X-Captcha-Token)."""

    def __init__(self, parar, servico, ritmo_sgp):
        self.parar, self.servico, self.ritmo = parar, servico, ritmo_sgp
        self.http = Http({"Accept": "application/json", "Origin": "https://muraki.tjrr.jus.br",
                          "Referer": "https://muraki.tjrr.jus.br/"})

    def consultar(self, processo, documento=None):
        """{cpf, valor, protocolooficiorequisitorio, ...} do beneficiário do processo (com documento: só se for ele);
        None se o Murakî não tem (pago, cancelado, indeferido ou outro beneficiário)."""
        url = f"{SGP}/precatorios/muraki/{processo}"
        params = {"documento": documento} if documento else None
        erro = ""
        for tentativa in range(4):
            try:
                token = self.servico.token("muraki")
            except captcha.ErroCaptcha as e:
                raise ErroTecnico(f"CAPTCHA_REATIVADO: {e}") from None
            self.ritmo.esperar(self.parar)
            try:
                r = self.http.sessao().get(url, params=params, headers={"X-Captcha-Token": token}, timeout=TIMEOUT_HTTP)
                if r.status_code == 200:
                    self.ritmo.ok()
                    d = r.json()
                    return d if isinstance(d, dict) and d.get("cpf") else None
                if r.status_code == 404:
                    self.ritmo.ok()
                    return None
                if r.status_code == 428:                # token vencido ou recusado
                    self.servico.descartar("muraki", token)
                    erro = "HTTP 428 (captcha)"
                    continue
                erro = f"HTTP {r.status_code}"
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except (requests.RequestException, ValueError) as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(2 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o Murakî não respondeu ({erro})")

    def confirma(self, processo, documento):
        """True se o documento é de um beneficiário com valor > 0 neste processo (o credor, não os honorários)."""
        d = self.consultar(processo, documento)
        return bool(d and so_digitos(d.get("cpf")) == so_digitos(documento) and float(d.get("valor") or 0) > 0)

# =============================================================================== Projudi e DJEN


def enxugar_projudi(d):
    """Detalhe do Projudi -> só o que o robô usa (fica em cache)."""
    def polo(lista):
        return [{"nome": " ".join((p.get("descricao") or "").split()),
                 "advogados": [advogado_do_projudi(a) for a in p.get("advogados") or []]}
                for p in lista or [] if (p.get("descricao") or "").strip()]
    movs = []
    for m in d.get("movimentacoes") or []:
        texto = m.get("descricao") or ""
        if RE_MOV_PRECATORIO.search(texto) and (dt := data_br(texto)):
            movs.append((dt.isoformat(), " ".join(texto.split())[:160]))
    ms = d.get("dataDistribuicao")
    return {"numero": so_digitos(d.get("numeroUnico")), "classe": " ".join((d.get("classe") or "").split()),
            "assunto": d.get("assunto") or "", "vara": d.get("vara") or "", "comarca": d.get("comarca") or "",
            "orgao": d.get("orgaoJulgador") or d.get("vara") or "", "recurso": bool(d.get("recurso")),
            "data": datetime.fromtimestamp(ms / 1000).date().isoformat() if isinstance(ms, (int, float)) else "",
            "ativo": polo(d.get("polosAtivos")), "passivo": polo(d.get("polosPassivos")), "movs": movs}


class Projudi:
    """Consulta pública do Projudi do TJRR (/consilium-api) com o Authorization do captcha. Detalhes em cache."""

    def __init__(self, parar, servico, ritmo):
        self.parar, self.servico, self.ritmo = parar, servico, Ritmo("Projudi", ritmo)
        self.http = Http(H_PROJUDI)
        self.cache, self.trava = OrderedDict(), threading.Lock()

    def _get(self, caminho, **params):
        erro = ""
        for tentativa in range(5):
            try:
                token = self.servico.token("projudi")
            except captcha.ErroCaptcha as e:
                raise ErroTecnico(f"CAPTCHA_REATIVADO: {e}") from None
            self.ritmo.esperar(self.parar)
            try:
                r = self.http.sessao().get(captcha.API_PROJUDI + caminho, params=params,
                                           headers={"Authorization": token}, timeout=TIMEOUT_HTTP)
                if r.status_code == 200:
                    self.ritmo.ok()
                    return r.json()
                if r.status_code == 404:
                    self.ritmo.ok()
                    return None
                if r.status_code == 401:                # 'Captcha expirado'
                    self.servico.descartar("projudi", token)
                    erro = "HTTP 401 (captcha expirado)"
                    continue
                if r.status_code == 400 and "excedeu" in r.text.lower():
                    self.ritmo.ok()
                    return EXCEDEU
                erro = f"HTTP {r.status_code}"
                if r.status_code in HTTP_FREIA:
                    self.ritmo.freia(erro)
            except (requests.RequestException, ValueError) as e:
                erro = e.__class__.__name__
                self.ritmo.freia(erro)
            if self.parar.wait(2 * (tentativa + 1)):
                raise ErroTecnico("INTERROMPIDO")
        raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: o Projudi não respondeu ({erro})")

    def buscar(self, **params):
        """Processos do 1º grau da busca (lista de {numeroUnico, classe, versusDescricao, ...}) ou EXCEDEU."""
        d = self._get("/processos", page=0, size=TAMANHO_BUSCA, **params)
        if d == EXCEDEU:
            return EXCEDEU
        return [x for x in (d or {}).get("data") or [] if not x.get("recurso")]

    def movimentacoes(self, numero, pagina):
        """(movimentações da página, total de páginas); 10 por página, da mais nova para a mais antiga."""
        d = self._get(f"/processos/{formatar_cnj(so_digitos(numero))}/movimentacoes", page=pagina, size=10) or {}
        return (d.get("data") or d.get("content") or []), int(d.get("totalPages") or 0)

    def expedicoes_perto(self, numero, alvo, prazo):
        """Movimentações de precatório perto da data `alvo` (apresentação do precatório). O detalhe só traz as 20
        mais novas: aqui uma busca binária pelas páginas acha a da data e lê as vizinhas (~log2(páginas) + 3)."""
        paginas, cache = None, {}

        def pagina(p):
            nonlocal paginas
            if time.time() > prazo:
                raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")
            if p not in cache:
                cache[p], total = self.movimentacoes(numero, p)
                paginas = total if paginas is None else paginas
            return cache[p]

        def mais_velha(p):
            datas = [data_br(m.get("descricao")) for m in pagina(p)]
            datas = [d for d in datas if d]
            return min(datas) if datas else None

        pagina(0)
        if not paginas or paginas > MAX_PAGINAS_MOVIMENTACOES:
            return []
        lo, hi = 0, paginas - 1
        limite = alvo + timedelta(days=-JANELA_EXPEDICAO[0])          # a página mais velha que isso fica atrás
        while lo < hi:
            meio = (lo + hi) // 2
            velha = mais_velha(meio)
            if velha is not None and velha > limite:
                lo = meio + 1
            else:
                hi = meio
        achadas = []
        for p in range(max(0, lo - 1), min(paginas, lo + 3)):
            for m in pagina(p):
                texto = m.get("descricao") or ""
                dt = data_br(texto)
                if dt and RE_MOV_PRECATORIO.search(texto) and \
                        JANELA_EXPEDICAO[0] <= (alvo - dt).days <= JANELA_EXPEDICAO[1]:
                    achadas.append(" ".join(texto.split())[:160])
        return achadas

    def detalhe(self, numero):
        """Processo enxuto pelo número (None se o Projudi não mostra)."""
        n = so_digitos(numero)
        with self.trava:
            if n in self.cache:
                self.cache.move_to_end(n)
                return self.cache[n]
        d = self._get(f"/processos/{formatar_cnj(n)}")
        e = enxugar_projudi(d) if isinstance(d, dict) else None
        with self.trava:
            self.cache[n] = e
            while len(self.cache) > MAX_CACHE:
                self.cache.popitem(last=False)
        return e


class Djen:
    """DJEN com um relógio só (o limite é por IP), nova tentativa e cache dos originários."""

    def __init__(self, parar):
        self.parar, self.http = parar, Http({})
        self.trava, self.proxima, self.intervalo, self.n_429 = threading.Lock(), 0.0, INTERVALO_DJEN, 0
        self.cache = OrderedDict()

    def _vez(self):
        with self.trava:
            vez = max(time.time(), self.proxima)
            self.proxima = vez + self.intervalo
        if (espera := vez - time.time()) > 0 and self.parar.wait(espera):
            raise ErroTecnico("INTERROMPIDO")

    def buscar(self, **params):
        for _ in range(6):
            self._vez()
            try:
                r = self.http.sessao().get(DJEN, params={"itensPorPagina": 100, **params}, timeout=TIMEOUT_HTTP)
            except requests.RequestException:
                time.sleep(3)
                continue
            if r.status_code == 200:
                with self.trava:
                    self.intervalo = max(INTERVALO_DJEN, self.intervalo * 0.95)
                return r.json().get("items") or []
            if r.status_code == 429:
                with self.trava:
                    self.n_429 += 1
                    self.intervalo = min(self.intervalo * 1.5, INTERVALO_DJEN_MAX)
                    self.proxima = time.time() + PAUSA_429
            else:
                time.sleep(5)
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: DJEN não respondeu")

    def nomes_do_precatorio(self, prec20):
        """(nomes do polo ativo, advogados {nome, oab_uf, oab_numero}) nas publicações do precatório; o TJRR publica
        o credor por inteiro na 'Lista de distribuição' (só de 2025 em diante) e 'SIGILOSO' no resto."""
        nomes, advs = [], {}
        for it in self.buscar(numeroProcesso=prec20, siglaTribunal="TJRR"):
            for d in it.get("destinatarios") or []:
                n = " ".join((d.get("nome") or "").split())
                if d.get("polo") == "A" and n and not n.upper().startswith("SIGILOSO") and n not in nomes:
                    nomes.append(n)
            for a in it.get("destinatarioadvogados") or []:
                adv = a.get("advogado") or {}
                if adv.get("nome") and adv.get("numero_oab") and (adv.get("uf_oab") or "").upper() == "RR":
                    advs[str(adv["numero_oab"])] = {"nome": adv["nome"].strip(), "oab_uf": "RR",
                                                    "oab_numero": so_digitos(str(adv["numero_oab"])), "cpf": ""}
        return nomes, list(advs.values())

    def texto_do_originario(self, numero20):
        """Texto de todas as publicações do processo (em cache; só na memória)."""
        with self.trava:
            if numero20 in self.cache:
                return self.cache[numero20]
        itens = self.buscar(numeroProcesso=numero20, siglaTribunal="TJRR")
        texto = " ".join(H.unescape(re.sub(r"<[^>]+>", " ", i.get("texto") or "")) for i in itens)
        texto = " ".join(texto.split())
        with self.trava:
            self.cache[numero20] = texto
            while len(self.cache) > MAX_CACHE:
                self.cache.popitem(last=False)
        return texto

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
       array_agg(li.situacao_texto) AS situacoes, array_agg(li.metadata->>'Motivo') AS motivos,
       min(li.ordem_cronologica) AS ordem, max(li.valor_lista) AS valor
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c ON c.id = cc.credito_id
  LEFT JOIN creditos.lista_item li ON li.credito_id = c.id AND li.removido_em IS NULL
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2 AND {filtro}
 GROUP BY cc.credito_id, cc.prioridade, cc.valor_referencia
"""


def faixa_do_credito(situacoes, motivos):
    """1 não pago (Requisitado, Autuado); 2 só honorários (a lista só tem a linha 'Honorários Advocatícios'); 3 pago;
    4 outra situação (Suspenso, Aguardando Baixa, Pago Parcialmente, sem situação: o Murakî não mostra);
    5 cancelado/indeferido."""
    s = [normal(x) for x in situacoes or [] if x]
    if any(x.startswith("PAGO POR") for x in s):
        return 3
    if s and all(x in MORTOS for x in s):
        return 5
    m = [normal(x) for x in motivos or [] if x]
    if m and all(x.startswith("HONORARIOS") for x in m):
        return 2
    return 1 if any(x in VIVOS for x in s) else 4


def ordenar_escopo(con, filtro):
    """(ids na ordem do robô, {credito_id: faixa}, contagem por faixa).
    Ordem: faixa, prioridade de campanha, ordem cronológica da lista (quem recebe antes), maior valor, id."""
    with con.cursor() as cur:
        cur.execute(SQL_ESCOPO.format(filtro=filtro), (TRIBUNAL_TJRR,))
        linhas = como_dicts(cur)
    faixas = {x["credito_id"]: faixa_do_credito(x["situacoes"], x["motivos"]) for x in linhas}
    chave = {x["credito_id"]: (faixas[x["credito_id"]], x["prioridade"], x["ordem"] or 10 ** 9,
                               -float(x["valor"] or x["valor_referencia"] or 0), x["credito_id"]) for x in linhas}
    ordem = sorted(chave, key=chave.get)
    return ordem, faixas, Counter(FAIXAS[f] for f in faixas.values())

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJRR", escrita=escrita, worker=WORKER)


SQL_CREDITO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, tc.codigo AS tipo_credito,
       COALESCE((SELECT json_agg(json_build_object('valor', x.valor_lista, 'situacao', x.situacao_texto,
                                                   'ordem', x.ordem_cronologica, 'apresentacao', x.data_apresentacao,
                                                   'meta', x.metadata) ORDER BY x.ordem_cronologica)
                   FROM creditos.lista_item x WHERE x.credito_id = c.id AND x.removido_em IS NULL), '[]') AS itens,
       COALESCE((SELECT json_agg(json_build_object('nome', p.nome, 'documento', p.documento::text, 'origem', k.origem))
                   FROM creditos.credito_credor k JOIN creditos.pessoa p ON p.id = k.pessoa_id
                  WHERE k.credito_id = c.id AND k.papel_id = 1), '[]') AS credores,
       (SELECT st.codigo FROM creditos.coleta_credor_tentativa t JOIN creditos.status_coleta st ON st.id = t.status_id
         WHERE t.credito_id = c.id ORDER BY t.id DESC LIMIT 1) AS ultimo_status,
       (SELECT cc.motivo_detalhe FROM creditos.coleta_credor cc WHERE cc.credito_id = c.id) AS motivo_fila
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc ON tc.id = c.tipo_credito_id
 WHERE c.id = %s
"""


def ler_credito(cur, credito_id):
    """O crédito (lead) com o que a lista do TJRR traz: itens (credor e honorários), ente, vara de origem, ofício,
    IDs do SGP, valores, data de apresentação e os credores que o banco já tem."""
    cur.execute(SQL_CREDITO, (credito_id,))
    linhas = como_dicts(cur)
    if not linhas:
        raise RuntimeError(f"crédito {credito_id} não existe")
    lead = linhas[0]
    itens = lead["itens"] or []
    metas = [i.get("meta") or {} for i in itens]
    primeiro = metas[0] if metas else {}
    lead["ente"] = " ".join((primeiro.get("Órgão Devedor") or primeiro.get("entidade_nome") or "").split())
    lead["termo_ente"] = termo_do_ente(lead["ente"]) if lead["ente"] else None
    lead["vara_origem"] = next((m.get("Vara De Origem") for m in metas if (m.get("Vara De Origem") or "-") != "-"), "")
    lead["oficios"] = sorted({m.get("Autos") for m in metas if m.get("Autos")})
    lead["sgp_ids"] = list(dict.fromkeys(m.get("ID SGP") for m in metas if m.get("ID SGP")))
    lead["situacoes"] = [i.get("situacao") for i in itens]
    lead["situacao"] = ", ".join(sorted({s for s in lead["situacoes"] if s}))
    lead["faixa"] = FAIXAS[faixa_do_credito(lead["situacoes"], [m.get("Motivo") for m in metas])]
    datas = sorted(i["apresentacao"] for i in itens if i.get("apresentacao"))
    lead["apresentacao"] = date.fromisoformat(datas[0][:10]) if datas else None
    valores = set()
    for i, m in zip(itens, metas):
        for v in (i.get("valor"), m.get("Valor Deferido SGP"), m.get("Valor Atualizado SGP")):
            v = valor_numerico(v)
            if v and v > 0:
                valores.add(round(float(v), 2))
    lead["valores"] = sorted(valores)
    lead["credores_banco"] = [c for c in lead["credores"] or [] if c.get("documento")]
    m = RE_CONTADOR_SEM_CREDOR.search(lead["motivo_fila"] or "")
    lead["sem_credor_n"] = int(m.group(1)) if m else 0     # vezes que já voltou para a fila sem credor
    return lead


def com_contador(motivo, lead):
    """Motivo de adiamento com o contador de 'sem credor' do lead (o fila_credor_adiar troca o motivo da fila: sem
    isso, um erro passageiro zeraria a contagem)."""
    n = (lead or {}).get("sem_credor_n") or 0
    if n and not RE_CONTADOR_SEM_CREDOR.search(motivo):
        return motivo[:1900] + f" [sem_credor={n}]"
    return motivo


def sem_credor(lead, r, detalhe):
    """Nem credor nem candidato para revisar: não fica em SUCESSO_ANALISAR (decisão do usuário, 05/10/2026). Volta
    para a fila em ADIAMENTO_SEM_CREDOR (a situação no SGP/Murakî e as publicações mudam) e, na
    MAX_TENTATIVAS_SEM_CREDOR-ésima vez, vira FALHA SEM_CREDOR. O contador fica no motivo da fila ('[sem_credor=N]')."""
    n = lead["sem_credor_n"] + 1
    if n >= MAX_TENTATIVAS_SEM_CREDOR:
        r.update(status="FALHA", motivo=f"SEM_CREDOR: {detalhe} (tentativa {n}/{MAX_TENTATIVAS_SEM_CREDOR})"[:2000])
        return r
    raise Adiar(f"{detalhe}"[:1900] + f" [sem_credor={n}]", ADIAMENTO_SEM_CREDOR)


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())


class Pessoas:
    """Leituras de creditos.pessoa feitas pelas threads (cada uma com a sua conexão readonly)."""

    def __init__(self, rod):
        self.rod = rod

    def _consulta(self, sql, args):
        with self.rod.conexao_da_thread().cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()

    def nomes_do_documento(self, documento):
        """Nomes que o banco tem para o CPF/CNPJ."""
        return [n for (n,) in self._consulta("SELECT nome FROM creditos.pessoa WHERE documento = "
                                             "creditos.documento_normalizar(%s) AND nome IS NOT NULL", (documento,))]

    def documentos_do_nome(self, nome, limite=MAX_CANDIDATOS_BANCO + 1):
        """CPFs/CNPJs das pessoas do banco com exatamente este nome."""
        return [d for (d,) in self._consulta("""SELECT documento::text FROM creditos.pessoa
                                                 WHERE nome_chave = creditos.chave_texto(%s) AND documento IS NOT NULL
                                                 LIMIT %s""", (nome, limite))]

    def candidato_rr(self, nome):
        """CPF do banco para o nome quando há UMA pessoa só com ele e ela tem processo em Roraima (TJRR .8.23 ou
        JFRR ...4200). Só candidato: vai para o metadata, nunca é ligado (decisão do usuário de 02/10/2026)."""
        linhas = self._consulta("""SELECT p.documento::text,
                                          EXISTS (SELECT 1 FROM creditos.processo_parte pa
                                                    JOIN creditos.processo pr ON pr.id = pa.processo_id
                                                   WHERE pa.pessoa_id = p.id
                                                     AND (pr.numero_cnj ~ '^[0-9]{13}823[0-9]{4}$'
                                                          OR pr.numero_cnj ~ '^[0-9]{13}401[0-9]{4}$'
                                                             AND pr.numero_cnj LIKE '%%4200'))
                                     FROM creditos.pessoa p
                                    WHERE p.nome_chave = creditos.chave_texto(%s) AND p.documento IS NOT NULL
                                    LIMIT 2""", (nome,))
        return linhas[0][0] if len(linhas) == 1 and linhas[0][1] else None

# =============================================================================== decisão (só lê as fontes)


class Fontes:
    """As fontes de uma rodada, para as threads."""

    def __init__(self, sgp, muraki, projudi, djen, pessoas):
        self.sgp, self.muraki, self.projudi, self.djen, self.pessoas = sgp, muraki, projudi, djen, pessoas


def resultado_vazio():
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "PRECATORIO", "originario": None, "regra": "", "evidencia": "",
            "credor": None, "capa": None, "candidatos": [], "fonte_documento": "", "fonte_nome": "", "muraki": None,
            "honorarios": None, "advogados_precatorio": [], "liga_credor": False, "candidato_analisar": None,
            "sgp": {}, "djen_nomes": []}


def ordenar_buscados(processos, termo_ente):
    """Processos da busca na ordem de abrir: contra o ente e de execução primeiro."""
    def chave(x):
        versus = normal(x.get("versusDescricao"))
        lado_passivo = versus.split(" X ", 1)[-1] if " X " in versus else ""
        return (0 if termo_ente and termo_ente in lado_passivo else 1,
                0 if re.search(r"CUMPRIMENTO|EXECU|JUIZADO", normal(x.get("classe"))) else 1)
    return sorted(processos, key=chave)


def abrir(projudi, processos, termo_ente, prazo):
    """Detalhes dos MAX_DETALHES primeiros processos (na ordem de ordenar_buscados)."""
    abertos = []
    for x in ordenar_buscados(processos, termo_ente)[:MAX_DETALHES]:
        if time.time() > prazo:
            raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")
        d = projudi.detalhe(x["numeroUnico"])
        if d:
            abertos.append(d)
    return abertos


def nome_do_documento(abertos, conhecidos):
    """(nome, como) do dono do CPF/CNPJ nos processos achados por ele: o nome que o banco/DJEN já liga ao documento
    e aparece num polo; senão o nome comum a todos os processos; senão o único autor pessoa de um processo só."""
    pessoas = [[p for p in d["ativo"] + d["passivo"]] for d in abertos]
    for c in conhecidos:
        for lista in pessoas:
            for p in lista:
                if mesma_pessoa(p["nome"], c):
                    return p["nome"], "BANCO"
    if len(abertos) >= 2:
        comuns = set.intersection(*({chave_nome(p["nome"]) for p in lista if eh_pessoa_comum(p["nome"])}
                                    for lista in pessoas))
        if len(comuns) == 1:
            alvo = comuns.pop()
            return next(p["nome"] for p in pessoas[0] if chave_nome(p["nome"]) == alvo), "COMUM_AOS_PROCESSOS"
    if len(abertos) == 1:
        pf = [p for p in abertos[0]["ativo"] if eh_pessoa_comum(p["nome"])]
        if len(pf) == 1:
            return pf[0]["nome"], "UNICO_AUTOR"
    return None, ""


def vara_bate(lead, d, juizo_sgp):
    """A vara do processo é a vara de origem que a lista ou o SGP dão para o precatório."""
    vara = normal(d["vara"])
    for alvo in (normal(lead["vara_origem"]), normal(juizo_sgp)):
        if len(alvo) >= 8 and len(vara) >= 8 and (alvo in vara or vara in alvo):
            return True
    return False


def candidatos_originario(lead, abertos, credor_nome, juizo_sgp):
    """Processos que podem ser o originário: o credor no polo ativo, o ente no passivo, classe que gera precatório.
    Cada um com as evidências do Projudi (vara de origem, expedição de precatório perto da apresentação)."""
    cands = []
    for d in abertos:
        if d["recurso"] or RE_CLASSE_FORA.search(normal(d["classe"])):
            continue
        if not any(mesma_pessoa(p["nome"], credor_nome) for p in d["ativo"]):
            continue
        if lead["termo_ente"] and not any(lead["termo_ente"] in normal(p["nome"]) for p in d["passivo"]):
            continue
        if lead["apresentacao"] and d["data"] and date.fromisoformat(d["data"]) > lead["apresentacao"]:
            continue                                    # distribuído depois do precatório: não é o originário
        ev = []
        if vara_bate(lead, d, juizo_sgp):
            ev.append("VARA_ORIGEM")
        if lead["apresentacao"] and any(
                JANELA_EXPEDICAO[0] <= (lead["apresentacao"] - date.fromisoformat(dt)).days <= JANELA_EXPEDICAO[1]
                for dt, _ in d["movs"]):
            ev.append("EXPEDICAO_PERTO")
        cands.append({"d": d, "ev": ev})
    return cands


def forca(c):
    return sum(FORCA[e] for e in c["ev"])


def desempatar_no_djen(lead, cands, djen, muraki_valor):
    """Valor do precatório ou o nº dele no texto do DJEN dos candidatos mais fortes (só quando há dúvida)."""
    valores = set(lead["valores"]) | ({round(muraki_valor, 2)} if muraki_valor else set())
    formatos = set().union(*(formatos_valor(v) for v in valores)) if valores else set()
    for c in sorted(cands, key=forca, reverse=True)[:MAX_ORIGINARIOS_DJEN]:
        texto = djen.texto_do_originario(c["d"]["numero"])
        if not texto:
            continue
        if lead["precatorio"] in texto or lead["numero_norm"][:20] in so_digitos(texto):
            c["ev"].append("NUM_PRECATORIO")
        elif any(f in texto for f in formatos):
            c["ev"].append("VALOR")


def em_duvida(cands):
    """Mais de um candidato e o mais forte não se destaca (fraco ou empatado)."""
    topo = max(forca(c) for c in cands)
    return len(cands) > 1 and (topo < FORCA_MINIMA or sum(forca(c) == topo for c in cands) > 1)


def escolher_originario(lead, cands, f, muraki_valor, prazo):
    """(candidato escolhido, empatados): o mais forte com FORCA_MINIMA (ou o único candidato); empate -> None.
    Na dúvida, procura a expedição do precatório nas movimentações antigas (o detalhe só traz as 20 mais novas) dos
    candidatos mais fortes e depois o valor/nº do precatório no DJEN deles."""
    if not cands:
        return None, []
    if em_duvida(cands) and lead["apresentacao"]:
        for c in sorted(cands, key=forca, reverse=True)[:MAX_ORIGINARIOS_DJEN]:
            if "EXPEDICAO_PERTO" not in c["ev"] and f.projudi.expedicoes_perto(c["d"]["numero"], lead["apresentacao"],
                                                                                prazo):
                c["ev"].append("EXPEDICAO_PERTO")
    if em_duvida(cands):
        desempatar_no_djen(lead, cands, f.djen, muraki_valor)
    topo = max(forca(c) for c in cands)
    tops = [c for c in cands if forca(c) == topo]
    if len(tops) == 1 and (topo >= FORCA_MINIMA or len(cands) == 1):
        return tops[0], []
    return None, tops


def capa_do(c, credor_nome, documento):
    """Capa para gravar: o processo e o credor como o Projudi escreve (com o documento confirmado)."""
    d = c["d"]
    parte = next(p for p in d["ativo"] if mesma_pessoa(p["nome"], credor_nome))
    return {"proc": d, "credor": {"nome": parte["nome"], "documento": documento or "", "papel_bruto": "POLO ATIVO",
                                  "advogados": parte["advogados"]}}


def advogados_com_oab(lead_advs_sgp, partes):
    """Advogados com OAB das partes do originário que também estão no precatório (pelo nome do SGP)."""
    alvo = {chave_nome(a) for a in lead_advs_sgp}
    vistos, saida = set(), []
    for p in partes:
        for a in p["advogados"]:
            if a["oab_numero"] and chave_nome(a["nome"]) in alvo and (a["oab_uf"], a["oab_numero"]) not in vistos:
                vistos.add((a["oab_uf"], a["oab_numero"]))
                saida.append(a)
    return saida


def resumo_candidatos(cands):
    return [{"cnj": formatar_cnj(c["d"]["numero"]), "classe": c["d"]["classe"][:40], "vara": c["d"]["vara"][:50],
             "evidencia": c["ev"]} for c in sorted(cands, key=forca, reverse=True)[:8]]


def originario_e_status(lead, f, r, abertos, credor_nome, documento, juizo_sgp, prazo):
    """Escolhe o originário entre os processos abertos e fecha o status (credor com ou sem documento confirmado)."""
    cands = candidatos_originario(lead, abertos, credor_nome, juizo_sgp)
    escolhido, empate = escolher_originario(lead, cands, f, (r["muraki"] or {}).get("valor"), prazo)
    r["candidatos"] = resumo_candidatos(cands)
    base = f"fonte_doc={r['fonte_documento'] or '-'} fonte_nome={r['fonte_nome'] or '-'}"
    if escolhido:
        r["capa"] = capa_do(escolhido, credor_nome, documento)
        r["credor"] = dict(r["capa"]["credor"])
        r.update(originario=escolhido["d"]["numero"], via="ORIGINARIO", evidencia="+".join(escolhido["ev"]) or "UNICO",
                 advogados_precatorio=r["advogados_precatorio"] + advogados_com_oab(
                     r["sgp"].get("advogados", []), [r["capa"]["credor"]]))
        desc = f"cnj={formatar_cnj(escolhido['d']['numero'])} evidencia={r['evidencia']} {base}"
        if documento:
            r.update(status="SUCESSO_PROCESSO_ORIGINARIO", motivo=desc)
        else:
            r.update(status="SUCESSO_INCOMPLETO", motivo=f"SUCESSO_SEM_CPF: {desc}")
        return r
    r["credor"] = {"nome": credor_nome, "documento": documento or "", "papel_bruto": "", "advogados": []}
    if len(empate) == 1:
        c = empate[0]
        r.update(status="SUCESSO_ANALISAR", liga_credor=bool(documento), via="NOME",
                 motivo=f"CREDOR_CONFIRMADO_ORIGINARIO_SEM_EVIDENCIA: o mais provável ({formatar_cnj(c['d']['numero'])}, "
                        f"{'+'.join(c['ev']) or 'nenhuma'}) entre {len(cands)} processos do credor contra o ente não tem "
                        f"evidência suficiente; {base}"[:2000])
    elif empate:
        lista = ", ".join(formatar_cnj(c["d"]["numero"]) for c in empate)
        r.update(status="SUCESSO_ANALISAR", liga_credor=bool(documento), via="NOME",
                 motivo=f"CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO: {len(empate)} processos com a mesma evidência "
                        f"({lista}); {base}"[:2000])
    elif documento:
        r.update(status="SUCESSO_PROCESSO_CREDITO", liga_credor=True,
                 motivo=f"CREDOR_SEM_ORIGINARIO: credor confirmado no precatório; nenhum processo do credor contra "
                        f"o ente no Projudi ({len(abertos)} aberto(s)); {base}")
    else:
        r.update(status="SUCESSO_INCOMPLETO",
                 motivo=f"SUCESSO_SEM_CPF: sem originário ({len(abertos)} processo(s) aberto(s)); {base}")
    return r


def com_documento(lead, f, r, documento, conhecidos, prazo):
    """Credor com CPF/CNPJ confirmado no Murakî: nome e originário pelo Projudi (busca pelo documento)."""
    juizo = r["sgp"].get("juizo", "")
    achados = f.projudi.buscar(cpfCnpj=documento)
    abertos = [] if achados == EXCEDEU else abrir(f.projudi, achados, lead["termo_ente"], prazo)
    conhecidos = list(dict.fromkeys(conhecidos + f.pessoas.nomes_do_documento(documento)))
    nome, como = nome_do_documento(abertos, conhecidos)
    if not nome and abertos:                            # nome da 'Lista de distribuição' do precatório (2025+)
        r["djen_nomes"] = r["djen_nomes"] or f.djen.nomes_do_precatorio(lead["numero_norm"][:20])[0]
        nome, como = nome_do_documento(abertos, [n for n in r["djen_nomes"] if eh_pessoa_comum(n)])
        como = "DJEN_" + como if nome else como
    if not nome and conhecidos:
        nome, como = conhecidos[0], "BANCO_SEM_PROCESSO"
    r["fonte_nome"] = como or r["fonte_nome"]
    if not nome:
        r.update(status="SUCESSO_ANALISAR", liga_credor=False,
                 candidato_analisar={"documento": documento, "processos": [formatar_cnj(d["numero"]) for d in abertos]},
                 motivo=f"CREDOR_SEM_NOME: CPF/CNPJ confirmado no Murakî, mas o nome não saiu do Projudi "
                        f"({'busca excedeu o limite' if achados == EXCEDEU else f'{len(abertos)} processo(s)'})")
        return r
    if not eh_pessoa_comum(nome) and RE_ORGAO_PUBLICO.search(chave_nome(nome)):
        r.update(status="FALHA", motivo=f"REQTE_ORGAO_PUBLICO: {nome}")
        return r
    return originario_e_status(lead, f, r, abertos, nome, documento, juizo, prazo)


def so_nome(lead, f, r, nome, documento_banco, prazo):
    """Credor só pelo nome (SGP dos pagos ou DJEN): originário pelo Projudi. Com o CPF do CONTATOS para esse nome,
    busca pelo CPF; se o Projudi mostra a pessoa com esse nome nesses processos, o CPF vale como confirmado."""
    juizo = r["sgp"].get("juizo", "")
    if documento_banco:
        achados = f.projudi.buscar(cpfCnpj=documento_banco)
        if achados != EXCEDEU and achados:
            abertos = abrir(f.projudi, achados, lead["termo_ente"], prazo)
            if any(mesma_pessoa(p["nome"], nome) for d in abertos for p in d["ativo"] + d["passivo"]):
                r.update(fonte_documento="BANCO_CONFIRMADO_PROJUDI")
                return originario_e_status(lead, f, r, abertos, nome, documento_banco, juizo, prazo)
    achados = f.projudi.buscar(nomeParte=nome)
    abertos = [] if achados == EXCEDEU else abrir(f.projudi, achados, lead["termo_ente"], prazo)
    cand = f.pessoas.candidato_rr(nome)
    if cand:
        r["candidato_analisar"] = {"nome": nome, "documento_banco": cand, "regra": "NOME_UNICO_NO_BANCO_LIGADO_A_RR"}
    return originario_e_status(lead, f, r, abertos, nome, "", juizo, prazo)


def fora_do_muraki(lead, f, r, prazo):
    """Não pago que o Murakî não mostra (Suspenso, Aguardando Baixa, Pago Parcialmente, sem situação) e fora da lista
    de pagos: só o credor do banco (CONTATOS/LEGADO), confirmado pelo Projudi na busca pelo CPF; senão analisar."""
    for c in lead["credores_banco"]:
        doc = so_digitos(c["documento"])
        if documento_valido(doc) and eh_pessoa_comum(c["nome"]):
            r["fonte_nome"] = f"BANCO_{c['origem']}"
            so_nome(lead, f, r, c["nome"], doc, prazo)
            if r["fonte_documento"] == "BANCO_CONFIRMADO_PROJUDI":
                return r
            r.update(resultado_vazio(), sgp=r["sgp"])
    return sem_credor(lead, r, f"FORA_DO_MURAKI: o Murakî e a lista de pagos não têm o precatório (situação "
                               f"{lead['situacao'] or '-'}); sem credor do banco confirmado no Projudi")


def honorarios_so(lead, f, r, documento, nome_pago):
    """Crédito só de honorários (decisão do usuário, como no TJRJ): o advogado/sociedade fica como ADVOGADO, sem
    CREDOR; SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF ... regra=HONORARIOS_ADVOGADO'."""
    nomes = ([nome_pago] if nome_pago else []) + (f.pessoas.nomes_do_documento(documento) if documento else [])
    advs = r["sgp"].get("advogados", [])
    if not nomes and len(advs) == 1 and len(documento or "") == 11:
        nomes = advs
    nome = nomes[0] if nomes else ""
    if documento or nome:
        r["honorarios"] = {"documento": documento or "", "nome": nome}
    r.update(status="SUCESSO_INCOMPLETO", regra="HONORARIOS_ADVOGADO", via="PRECATORIO",
             credor={"nome": nome, "documento": "", "papel_bruto": "HONORARIOS", "advogados": []},
             motivo=f"SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO beneficiario={nome or '-'} "
                    f"doc={'sim' if documento else 'nao'}")
    return r


def processar(lead, f, inicio):
    """Acha o credor e o originário. Devolve o resultado a gravar."""
    r = resultado_vazio()
    prazo = inicio + TETO_CREDITO - 15
    if lead["numero_norm"][13:16] != "823":
        r["motivo"] = f"CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO: {lead['precatorio']} não é do TJRR"
        return r
    det = f.sgp.detalhe(lead)
    r["sgp"] = {"juizo": det.get("juizoOrigem") or "", "advogados": det.get("advogados") or [],
                "situacao": det.get("situacaoAtual") or "", "oficio": det.get("oficioRequisitorio") or ""}
    pagos = f.sgp.pagos_do(lead)
    nome_pago = next((p["credorPrincipal"] for p in pagos if p.get("credorPrincipal")), "")

    if lead["faixa"] == "CANCELADO" and not pagos:
        tipo = "INDEFERIDO" if any(normal(s) == "INDEFERIDO" for s in lead["situacoes"] if s) else "CANCELADO"
        r["motivo"] = f"PRECATORIO_{tipo}: situação na lista = {lead['situacao']}"
        return r

    m = None if pagos else f.muraki.consultar(lead["precatorio"])
    if m:
        r["muraki"] = {"valor": float(m.get("valor") or 0), "ano": m.get("anoOrcamento"),
                       "regime": m.get("tipoOpcaoRegime"), "devedor": m.get("nomeOrgaoDevedor")}
    doc = so_digitos((m or {}).get("cpf"))
    if lead["faixa"] == "HONORARIOS":
        if not m and not nome_pago:
            r["motivo"] = f"PROCESSO_NAO_ENCONTRADO: o Murakî e o SGP não têm o precatório (situação {lead['situacao']})"
            return r
        return honorarios_so(lead, f, r, doc if documento_valido(doc) else "", nome_pago)

    if not m:
        if not nome_pago:
            return fora_do_muraki(lead, f, r, prazo)
        # pago: nome do SGP
        r.update(fonte_nome="SGP_PAGOS")
        if RE_ORGAO_PUBLICO.search(chave_nome(nome_pago)):
            r["motivo"] = f"REQTE_ORGAO_PUBLICO: {nome_pago}"
            return r
        if RE_SOCIEDADE_ADV.search(chave_nome(nome_pago)):
            return honorarios_so(lead, f, r, "", nome_pago)
        doc_banco = next((c["documento"] for c in lead["credores_banco"] if mesma_pessoa(c["nome"], nome_pago)), "")
        return so_nome(lead, f, r, nome_pago, doc_banco, prazo)

    valor = r["muraki"]["valor"]
    if valor > 0 and documento_valido(doc):
        r["fonte_documento"] = "MURAKI"
        return com_documento(lead, f, r, doc, [], prazo)

    # valor = 0: o Murakî devolveu o beneficiário dos honorários; o credor é outro
    r["honorarios"] = {"documento": doc if documento_valido(doc) else "", "nome": ""}
    if r["honorarios"]["documento"]:
        nomes_hon = f.pessoas.nomes_do_documento(doc)
        r["honorarios"]["nome"] = nomes_hon[0] if nomes_hon else ""
    for c in lead["credores_banco"]:                      # CONTATOS/LEGADO do banco, confirmados no Murakî
        cand = so_digitos(c["documento"])
        if cand != doc and documento_valido(cand) and f.muraki.confirma(lead["precatorio"], cand):
            r["fonte_documento"] = f"MURAKI_CONFIRMOU_{c['origem']}"
            return com_documento(lead, f, r, cand, [c["nome"]], prazo)
    nomes, advs_djen = f.djen.nomes_do_precatorio(lead["numero_norm"][:20])
    r["djen_nomes"] = nomes
    r["advogados_precatorio"] = advs_djen
    nomes = [n for n in nomes if eh_pessoa_comum(n)]
    for nome in nomes[:2]:
        for cand in f.pessoas.documentos_do_nome(nome)[:MAX_CANDIDATOS_BANCO]:
            cand = so_digitos(cand)
            if cand != doc and documento_valido(cand) and f.muraki.confirma(lead["precatorio"], cand):
                r.update(fonte_documento="MURAKI_CONFIRMOU_NOME_DJEN", fonte_nome="DJEN")
                return com_documento(lead, f, r, cand, [nome], prazo)
    if nomes:
        r["fonte_nome"] = "DJEN"
        return so_nome(lead, f, r, nomes[0], "", prazo)
    return sem_credor(lead, r, "CREDOR_NAO_IDENTIFICADO: o Murakî devolveu só o beneficiário dos honorários e não há "
                               "candidato (CONTATOS nem nome no DJEN)")

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
        manter=False (simulação) não lembra as linhas tocadas. Sem --com-desfazer não escreve nada."""
        for credito_id, sql, linhas in (self.l_creditos if GERAR_DESFAZER else []):
            novo = not self.arquivo_sql.exists()
            with open(self.arquivo_sql, "a", encoding="utf-8") as f:
                if novo:
                    f.write("-- Desfaz as mudanças do fetch_TJRR.py nas tabelas antigas "
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
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJRR",
                                "Credor do TJRR: SGP + Murakî + Projudi (captcha Tencent) + DJEN "
                                "(TJRR/fetch_TJRR.py)", raspa_credor=True, criar=criar)


def filas_antigas(cur):
    """Filas mensais do legado (de FILA_ANTIGA_DESDE em diante) em que o usuário pode gravar."""
    return banco.filas_antigas(cur, FILA_ANTIGA_DESDE)[0]


def recorte(r, coletiva):
    """Capa recortada do originário para o banco: só o credor confirmado, o polo passivo (como REU) e os advogados
    do credor que também estão no precatório. Gravar todos os autores faria o recálculo ligar cada um deles ao
    precatório como credor; e no processo coletivo vai sem advogados (o recálculo liga advogado da capa a todos os
    créditos do processo)."""
    proc, credor = r["capa"]["proc"], r["capa"]["credor"]
    partes = [{"nome": credor["nome"], "documento": credor["documento"], "polo": "ATIVO", "papel": "AUTOR",
               "papel_bruto": "AUTOR"}]
    partes += [{"nome": x["nome"], "documento": "", "polo": "PASSIVO", "papel": "REU", "papel_bruto": "REU"}
               for x in proc["passivo"]]
    oabs = {(a["oab_uf"], a["oab_numero"]) for a in r["advogados_precatorio"]}
    advogados = [] if coletiva else [a for a in credor["advogados"] if (a["oab_uf"], a["oab_numero"]) in oabs]
    capa = {"classe_judicial": proc["classe"] or None, "orgao_julgador": proc["vara"] or proc["orgao"] or None,
            "jurisdicao": proc["comarca"] or None, "assunto": proc["assunto"] or None,
            "data_autuacao": proc["data"] or None, "grau": "G1", "segredo_justica": False}
    return {"partes": partes, "advogados": advogados, "capa": capa, "proc": proc}


def partes_para_banco(cur, dados, existentes):
    """(partes, advogados) para registrar_capa, SÓ ACRESCENTANDO: o que o banco já tem do processo (reenviado como
    está) + as partes e advogados novos (o registrar_capa troca o conjunto inteiro; mandar só o novo apagaria o resto).
    O CPF que faltar vem do banco (mesmo nome)."""
    do_banco = ([{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "papel": e["papel"],
                  "papel_bruto": e["papel_bruto"] or ""}
                 for e in existentes if e["papel"] != "ADVOGADO"],
                [{"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"], "oab_uf": e["oab_uf"],
                  "oab_numero": e["oab_numero"], "papel_bruto": e["papel_bruto"] or ""}
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
    """Capa do originário -> JSON do registrar_capa."""
    capa = dados["capa"]
    return {"orgao_julgador": capa["orgao_julgador"], "classe_judicial": capa["classe_judicial"],
            "classe_codigo": None, "grau": "G1", "segredo_justica": False, "sistema": "PROJUDI"}


def metadata_do_processo(cur, cnj, dados, bk):
    """O resto da capa em creditos.processo.metadata (capa_pje com fonte projudi_tjrr; dataAjuizamento): só acrescenta.
    Backup do metadata de antes no desfazer_legado_*.sql."""
    cur.execute("SELECT id, metadata FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s) FOR UPDATE",
                (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if not linha:
        return 0
    pid, antes = linha[0], linha[1] or {}
    proc = dados["proc"]
    novo = {}
    if (antes.get("capa_pje") or {}).get("fonte") in (None, "projudi_tjrr"):
        novo["capa_pje"] = {
            "fonte": "projudi_tjrr",
            "campos": {"numero_processo": formatar_cnj(cnj), "classe_judicial": proc["classe"],
                       "vara": proc["vara"], "comarca": proc["comarca"], "assunto": proc["assunto"],
                       "data_da_distribuicao": proc["data"]},
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


def gravar_advogados(cur, cid, r):
    """Advogados do precatório (com OAB, do originário ou do DJEN) e o beneficiário dos honorários (com o documento
    do Murakî) ligados ao crédito como ADVOGADO: vale em todo lead processado, mesmo sem credor."""
    n = 0
    for a in r["advogados_precatorio"]:
        cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'ADVOGADO', p_nome => %s,
                                                        p_documento => %s, p_oab_uf => %s, p_oab_numero => %s,
                                                        p_fonte => %s)""",
                    (cid, a["nome"], a.get("cpf") or None, a["oab_uf"], a["oab_numero"], SOFTWARE))
        n += 1
    h = r.get("honorarios") or {}
    if h.get("documento") and h.get("nome"):
        cur.execute("SELECT creditos.documento_de_parte(%s)", (h["documento"],))
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'ADVOGADO', p_nome => %s,
                                                            p_documento => %s, p_fonte => %s)""",
                        (cid, h["nome"], h["documento"], SOFTWARE))
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
    novos = {"classe_judicial": capa.get("classe_judicial"), "orgao_julgador": capa.get("orgao_julgador"),
             "jurisdicao": capa.get("jurisdicao"), "assunto": capa.get("assunto"),
             "data_autuacao": date.fromisoformat(capa["data_autuacao"]) if capa.get("data_autuacao") else None,
             "origem": "TJRR", "tribunal_sigla": "TJRR"}
    prec = None
    if relacionar:                                      # índice único: não repete o precatório em outro originário
        cur.execute("SELECT 1 FROM originarios.processos_originarios WHERE precatorio_relacionado = %s::text[]",
                    ([precatorio],))
        prec = None if cur.fetchone() else [precatorio]
    cur.execute("""SELECT id, classe_judicial, orgao_julgador, jurisdicao, assunto, data_autuacao,
                          origem, tribunal_sigla, precatorio_relacionado, ultima_data_raspagem, data_raspagem
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
              PAPEL_LEGADO[p["polo"]], "TJRR", agora, p["papel_bruto"], POLO_BRUTO[p["polo"]])
             for p in dados["partes"] if (p["polo"], normal(p["nome"])) not in ja]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    ja_oab = {(a["oab_uf"], a["oab_numero"]) for a in como_dicts(cur)}
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJRR", agora,
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
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJRR'
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"), lead["numero_norm"]))
        for linha in como_dicts(cur):
            reservada = reserva_de_robo(linha)
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO" and not reservada:
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJRR"}
            antes = {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead", "numero_originario",
                                           "ultima_atualizacao")}
            if reservada:
                antes.update(status_coleta_lead=None, motivo_coleta_lead=None)
            bk.update(cur, fila, chave, antes)
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   numero_originario = COALESCE(%s::text[], numero_originario),
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJRR'
                               AND deleted = false""",
                        (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Resumo da decisão em credito_fonte.metadata do software (a função troca o JSON inteiro: mescla aqui).
    No SUCESSO_ANALISAR e nos pagos o candidato (nome, documento) fica só aqui, para a revisão."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "evidencia": r["evidencia"], "candidatos": r["candidatos"],
                     "fonte_documento": r["fonte_documento"], "fonte_nome": r["fonte_nome"],
                     "muraki": r["muraki"], "sgp": r["sgp"], "djen_nomes": r["djen_nomes"]}
    meta["credor"] = ({"nome": r["credor"]["nome"], "papel": r["credor"]["papel_bruto"],
                       "cpf_encontrado": bool(r["credor"]["documento"])} if r["credor"] else None)
    meta["honorarios"] = r.get("honorarios")
    meta["candidato_analisar"] = r.get("candidato_analisar")
    if r["capa"]:
        proc = r["capa"]["proc"]
        meta["capa_originario"] = {"classe": proc["classe"], "vara": proc["vara"], "comarca": proc["comarca"],
                                   "data": proc["data"]}
    meta["lista"] = {"faixa": lead["faixa"], "situacao": lead["situacao"], "oficios": lead["oficios"],
                     "apresentacao": lead["apresentacao"].isoformat() if lead["apresentacao"] else None}
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJRR', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE,
                 json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")


def corrigir_credor(cur, lead, credor, bk):
    """A fonte vence o banco: com o CPF do credor confirmado na fonte, apaga os vínculos de CREDOR do crédito que são
    a mesma pessoa (mesmo nome ou grafia próxima) com outro documento. Herdeiro, cessionário, sucessor, advogado e
    credor com outro nome não são tocados. O banco audita cada DELETE e o desfazer_legado_*.sql guarda o INSERT."""
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
                (formatar_cnj(originario), (credor or {}).get("documento") or None, credito_id))
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
    if (vincula or r.get("liga_credor")) and credor and credor["documento"] and credor["nome"]:
        cur.execute("SELECT creditos.documento_de_parte(%s)", (credor["documento"],))
        # antes do registrar_capa: o vínculo fica com origem FONTE, que o recálculo das partes não apaga
        if cur.fetchone()[0]:
            cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                            p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                        (cid, credor["nome"], credor["documento"], proc, SOFTWARE))
            if r["fonte_documento"].startswith("MURAKI"):
                corrigidos = corrigir_credor(cur, lead, credor, bk)
    legado["advogados"] += gravar_advogados(cur, cid, r)
    if vincula and r["capa"]:
        cnj = r["originario"]
        cur.execute("""SELECT count(DISTINCT co.credito_id) FROM creditos.credito_originario co
                         JOIN creditos.processo pr ON pr.id = co.processo_id
                        WHERE pr.numero_cnj = creditos.cnj_normalizar(%s)""", (formatar_cnj(cnj),))
        coletiva = cur.fetchone()[0] > 1 or \
            len({chave_nome(x["nome"]) for x in r["capa"]["proc"]["ativo"] if eh_pessoa_comum(x["nome"])}) > 1
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
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "evidencia": r["evidencia"],
               "candidatos": r["candidatos"][:10], "fonte_documento": r["fonte_documento"],
               "fonte_nome": r["fonte_nome"], "credores_antes": fmt_credores(antes),
               "credores_depois": fmt_credores(depois)}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => %s, p_sistema => 'PROJUDI',
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
        soltar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJRR", WORKER)
        cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s,
                          p_status => 'FALHA', p_motivo => %s, p_sistema => 'PROJUDI', p_host => %s)""",
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
        cand = r.get("credor") or {}
        linha.update(resultado=r["status"], motivo=r["motivo"],
                     originario=formatar_cnj(r["originario"]) if r["originario"] else "", regra=r["regra"],
                     evidencia=r["evidencia"], credor=cand.get("nome", ""),
                     credor_documento=cand.get("documento") or "", fonte_documento=r["fonte_documento"],
                     fonte_nome=r["fonte_nome"], muraki_valor=(r["muraki"] or {}).get("valor", ""),
                     honorarios_documento=(r.get("honorarios") or {}).get("documento", ""),
                     candidatos=" | ".join(f"{c['cnj']}[{'+'.join(c['evidencia']) or '-'}]" for c in r["candidatos"]),
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
    momento. UPDATE atômico: se outra instância já o pegou, não afeta nenhuma linha. Na mesma transação reserva as
    linhas do precatório nas filas mensais do RPA antigo; se o RPA (token) está com uma delas, desfaz tudo e não pega.
    Devolve (software, status, disponivel_em) de antes, ou None."""
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
                    (credito_id, TRIBUNAL_TJRR, id_software, WORKER, lease))
        antes = cur.fetchone()
        if antes and not reservar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJRR",
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
    """Volta para a fila daqui a `intervalo` (erro passageiro); sem motivo (Ctrl+C), volta já."""
    with con.cursor() as cur:
        soltar_filas_mensais(cur, FILAS_MENSAIS, numero_do_credito(cur, credito_id) or "", "TJRR", WORKER)
        if motivo:
            cur.execute("SELECT creditos.fila_credor_adiar(%s, %s, %s::interval, %s)",
                        (credito_id, WORKER, intervalo, motivo[:2000]))
        else:
            cur.execute("SELECT creditos.fila_credor_liberar(%s, %s)", (credito_id, WORKER))
    con.commit()

# =============================================================================== execução


class Rodada:
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, fontes (com o serviço de captcha) e
    contadores. Ao nascer prepara o banco, a lista de pagos do SGP e a ordem da fila (na simulação, a amostra sai
    dela e nada é reservado)."""

    def __init__(self, simulacao, limite, workers, ritmo, creditos=()):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers = simulacao, limite, workers
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJRR{sufixo}.csv"
        self.arq_trocas = SAIDA / f"fetch_credores_trocados{sufixo}.csv"
        self.bk = Backup(SAIDA / f"desfazer_legado_{self.rodada}{sufixo}.sql",
                         SAIDA / f"fetch_legado_backup{sufixo}.csv", self.rodada, self.modo)
        self.resultados, self.falhas_seguidas, self.n, self.n_lote = Counter(), 0, 0, 0
        self.parar, self.fila_vazia = threading.Event(), False
        self.lease = f"{(LOTE // workers + 2) * TETO_CREDITO // 60 + 30} minutes"
        self.faixas = {}
        self.local, self.conexoes, self.trava = threading.local(), [], threading.Lock()

        self.con = conectar(escrita=True)
        with self.con.cursor() as cur:
            self.id_software = id_do_software(cur, criar=not simulacao)
            self.filas = filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        FILAS_MENSAIS[:] = self.filas
        self.servico = captcha.ServicoCaptcha(self.parar)
        sgp = Sgp(self.parar)
        sgp.carregar_pagos()
        self.fontes = Fontes(sgp, Muraki(self.parar, self.servico, sgp.ritmo),
                             Projudi(self.parar, self.servico, ritmo), Djen(self.parar), Pessoas(self))
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | lease {self.lease} | Projudi {ritmo:.1f} req/s | "
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
        """Ordem do robô (não pagos primeiro) e a faixa de cada crédito pegável."""
        con_l = conectar()
        try:
            ordem, self.faixas, contagem = ordenar_escopo(con_l, FILTRO_PEGAVEL)
        finally:
            con_l.close()
        log.info(f"ordem da fila: {len(ordem)} créditos | " + ", ".join(f"{s}: {contagem[s]}" for s in FAIXAS.values()))
        return ordem

    def reordenar(self):
        """Remonta a ordem da fila e volta ao começo dela (repete a cada REORDENAR_A_CADA). Antes, solta nas filas
        mensais as reservas de robô que ficaram sem dono (robô que caiu)."""
        with self.con.cursor() as cur:
            soltas = limpar_reservas_orfas(cur, self.filas, "TJRR")
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
        """Fecha as conexões e os Chromes do captcha."""
        for con in [self.con] + self.conexoes:
            try:
                con.close()
            except Exception:
                pass
        self.servico.fechar()


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
        log.info("fila do TJRR vazia." if not rod.simulacao else "amostra acabou.")
    return credito_id


def esperar_adiados(rod):
    """Fila vazia no modo real: se algum crédito adiado por erro passageiro vence em breve, espera por ele e volta
    a trabalhar (True). Sem nenhum, a carga acabou (False)."""
    if rod.simulacao or rod.parar.is_set() or (rod.limite and rod.n >= rod.limite):
        return False

    def proximo(con):
        with con.cursor() as cur:
            cur.execute("""SELECT EXTRACT(EPOCH FROM min(cc.disponivel_em) - now())
                             FROM creditos.coleta_credor cc JOIN creditos.credito c ON c.id = cc.credito_id
                            WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id = 1
                              AND cc.software_id IN (%s, %s)
                              AND cc.disponivel_em <= now() + %s::interval + interval '15 minutes'""",
                        (TRIBUNAL_TJRR, SOFTWARE_RPA, rod.id_software, ADIAMENTO))
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
    """(Numa thread) Lê o crédito e decide credor e originário, sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r} ou {tipo: ADIAR, credito_id, motivo, intervalo, linha, tecnico}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    lead = None
    try:
        try:
            with rod.conexao_da_thread().cursor() as cur:
                lead = ler_credito(cur, credito_id)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            rod.local.con = None                        # conexão de leitura parada demais e derrubada: abre outra
            with rod.conexao_da_thread().cursor() as cur:
                lead = ler_credito(cur, credito_id)
        linha.update(precatorio=lead["precatorio"], faixa=lead["faixa"], ente=lead["ente"],
                     situacao=lead["situacao"], ultimo_status=lead["ultimo_status"])
        r = processar(lead, rod.fontes, inicio)
    except Adiar as e:                                  # sem credor ainda: volta daqui a dias, sem ser falha técnica
        motivo = com_contador(str(e), lead)
        linha.update(resultado="ADIADO", motivo=motivo, segundos=round(time.time() - inicio))
        log.info(f"[{n}] {credito_id} {linha.get('precatorio', '')} -> ADIADO {e.intervalo}: {motivo[:150]}")
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": motivo, "intervalo": e.intervalo, "linha": linha,
                "tecnico": False}
    except Exception as e:                              # erro passageiro (ou inesperado): volta para a fila
        if isinstance(e, psycopg2.Error):
            rod.local.con = None                        # a próxima leitura desta thread abre conexão nova
        tecnico = isinstance(e, ErroTecnico)
        motivo = com_contador(str(e) if tecnico else
                              f"ERRO_DESCONHECIDO: {e.__class__.__name__}: {(str(e).splitlines() or [''])[0][:200]}",
                              lead)
        if not tecnico:
            log.warning(f"[{n}] {credito_id}: erro inesperado", exc_info=True)
        linha.update(resultado="ADIADO", motivo=motivo, segundos=round(time.time() - inicio))
        return {"tipo": "ADIAR", "credito_id": credito_id, "motivo": motivo, "intervalo": ADIAMENTO, "linha": linha,
                "tecnico": True}
    linha.update(segundos=round(time.time() - inicio))
    cand = r["credor"] or {}
    log.info(f"[{n}] {credito_id} {lead['precatorio']} {lead['faixa']} -> {r['status']} "
             f"{formatar_cnj(r['originario']) if r['originario'] else ''} "
             f"{('doc ' + cand['documento']) if cand.get('documento') else ''} "
             f"| {cand.get('nome', '')[:40]} | {r['motivo'].split(':')[0]} | {linha['segundos']} s")
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
    if not item.get("tecnico", True):                   # sem credor ainda: não é falha da fonte
        return
    rod.falhas_seguidas += 1
    log.warning(f"{item['credito_id']} -> ADIADO: {item['motivo']}")
    if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (fonte fora, captcha ou bloqueio).")
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
    f = rod.fontes
    log.info(f"DJEN: {f.djen.n_429} resposta(s) 429 | Projudi: {f.projudi.ritmo.freadas} freada(s) | captchas: "
             f"Murakî {rod.servico.renovacoes['muraki']}, Projudi {rod.servico.renovacoes['projudi']}")
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --ritmo N, --creditos, --com-desfazer e --sem-desfazer."""
    ap = argparse.ArgumentParser(description="Credor do TJRR pelas fontes públicas (SGP, Murakî, Projudi, DJEN), "
                                             f"gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS})")
    ap.add_argument("--ritmo", type=float, default=RITMO_PROJUDI,
                    help=f"teto de req/s no Projudi (padrão {RITMO_PROJUDI})")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
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
    False se a rodada parou por falhas técnicas seguidas ou erro inesperado (main reinicia)."""
    inicio = time.time()
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), max(RITMO_MINIMO, args.ritmo), creditos)
    pendentes, em_voo, terminou = [], {}, False
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjrr")
    try:
        while True:
            while len(em_voo) < rod.workers and (credito_id := proximo_credito(rod)):
                em_voo[executor.submit(processar_credito, rod, credito_id, rod.n)] = credito_id
            if not em_voo:
                if pendentes:
                    descarregar(rod, pendentes)
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
    """Roda até acabar a carga: se uma rodada para (fonte fora, banco caiu, erro inesperado), espera PAUSA_REINICIO e
    começa outra (até MAX_REINICIOS). Simulação e --limite rodam uma vez só."""
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    creditos = [int(x) for x in re.findall(r"\d+", args.creditos)]
    if creditos and not args.simulacao:
        raise SystemExit("--creditos só vale com --simulacao")
    for tentativa in range(1, MAX_REINICIOS + 1):
        try:
            if executar(args, creditos):
                log.info("carga do TJRR encerrada.")
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
