"""
fetch_TJDF.py - credor dos precatórios do TJDFT pelas fontes públicas, de ponta a ponta, sem login: lê o banco, acha o
credor (DJe do TJDFT + DJEN + PJe consulta pública), confirma o originário e grava no banco em lotes de LOTE créditos
(LOTE_GRAVACAO_TJDF no .env). No lugar do modo credor do RPA_SISTEMAS, que precisa do token A3 do advogado.

No TJDFT o número do crédito é o processo do PRÓPRIO precatório no PJe 2º grau (COORPRE, sigiloso). A lista (tabela
antiga processos_unificados, cadastrada no schema creditos pelo TJDF/carregar_TJDF.py) só traz número, ano, prioridade,
ordem, devedor e data de apresentação: o credor e o originário não vêm em lugar nenhum.

Fontes (anônimas, sem captcha):
- DJe do TJDFT (pesquisadje.tjdft.jus.br/api/v1/buscador, até ~2024): as publicações do precatório até ~2021 trazem
  'N. <prec> - PRECATÓRIO - A: NOME. Adv(s).: DF8583 - ADVOGADO' e a certidão '<prec> NOME (CPF: ...); ...' com o
  CPF completo. De 2022 em diante, só os advogados. O DJe também acha, pelo nome, os processos do 1º grau em que a
  pessoa é autora (candidatos a originário) e, pelo advogado, os processos dele contra a Fazenda.
- DJEN (2023+): publicações da COORPRE (orgaoId 42759 e 51634) com o polo ativo em INICIAIS, os advogados com OAB e,
  no texto, às vezes o primeiro nome ('SABINA N. M.') ou o nome inteiro. Pré-carga mês a mês em saida/djen_coorpre.
  Por OAB: os processos do advogado (polo ativo por extenso) na janela da apresentação.
- PJe consulta pública (API REST pje-consultapublica-api / pje2i-consultapublica-api): pelo número abre até processo
  arquivado (as buscas por nome, CPF e advogado não mostram arquivados); polo ativo com CPF completo, advogados com
  OAB e CPF, polo passivo e o texto dos documentos. O precatório (sigiloso) nunca abre.

Esteira por crédito (faixas na ordem da fila: NOME = apresentado até 2021, INICIAIS = 2022 em diante):
1. Precatório: DJe pelo número ('"<prec>"' e '"<prec>" CPF') -> nomes, CPFs, advogados; DJEN -> iniciais,
   advogados, pistas do texto (primeiro nome, nome inteiro, 'autos de execução n. X').
2. Credor com nome (rota NOME): DJe pelo nome -> processos do 1º grau com ele no polo ativo -> PJe pelo número ->
   credor no polo ativo (mesmo CPF ou mesmo nome), devedor no passivo, distribuído antes do precatório; evidência:
   documento do originário que cita o número do precatório ('Precatório distribuído na COORPRE com o número ...').
   Sem candidato pelo nome, tenta pelos advogados (rota de baixo) com o nome inteiro.
3. Só iniciais (rota INICIAIS): processos dos advogados do precatório (DJEN por OAB e DJe pelo nome, janela de
   JANELA_ADVOGADO da apresentação) contra a Fazenda cujo polo ativo bate com as iniciais (e o primeiro nome, se o
   texto der) -> PJe -> polo ativo inteiro -> documento citando o precatório. Com o documento: credor confirmado (o
   CPF sai do PJe). Sem o documento: decisão do usuário (05/10/2026), como no TJMT: SUCESSO_ANALISAR
   'CREDOR_SO_POR_INICIAIS', candidato só no metadata, sem ligar.
4. Sem publicação no DJe nem no DJEN: volta para a fila em ADIAMENTO_SEM_PUBLICACAO.
5. Publicações sem credor identificável (nem candidato para revisar): decisão do usuário (05/10/2026), não é
   SUCESSO_ANALISAR: volta para a fila em ADIAMENTO_SEM_CREDOR (a COORPRE segue publicando) e, na
   MAX_TENTATIVAS_SEM_CREDOR-ésima vez, FALHA 'SEM_CREDOR'. O contador vai no motivo da fila ('[sem_credor=N]').

Status: CPF + originário -> SUCESSO_PROCESSO_ORIGINARIO; CPF do DJe sem originário -> SUCESSO_PROCESSO_CREDITO; só o
nome -> SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF'; originário empatado, várias pessoas, só iniciais -> SUCESSO_ANALISAR;
só sociedade/advogado no polo ativo -> SUCESSO_INCOMPLETO regra HONORARIOS_ADVOGADO; ninguém identificado -> volta em
60 dias, na 3ª vez FALHA SEM_CREDOR; órgão público -> FALHA.

Gravação (igual ao TJRR/TJMT): lote numa transação, cada crédito no seu SAVEPOINT; credor com CPF via registrar_credor
(corrigindo o CPF divergente do banco), advogados com OAB, capa recortada do originário (só o credor, o polo passivo e
os advogados; ação coletiva sem advogados), metadata (registrar_credito), capa antiga e a linha da tabela antiga
processos_unificados (legado, decisão do usuário) e o status (fila_credor_finalizar).

Saídas em TJDF/saida: fetch_TJDF.csv (1 linha por crédito), fetch_credores_trocados.csv, fetch_legado_backup.csv,
desfazer_legado_*.sql e desfazer_fila_*.sql.

Uso:
    python fetch_TJDF.py --simulacao            # 20 créditos: faz tudo e desfaz no banco (ROLLBACK); não mexe na fila
    python fetch_TJDF.py --simulacao --creditos 1230524,1231207
    python fetch_TJDF.py                        # processa a fila do TJDFT até acabar (Ctrl+C para parar)
    python fetch_TJDF.py --limite 50            # para depois de 50 créditos
    python fetch_TJDF.py --workers 4            # workers (threads) raspando ao mesmo tempo (padrão 4)
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
load_dotenv(AQUI.parent / ".env")      # PG_* e LOTE_GRAVACAO_TJDF
log = logging.getLogger("fetch_TJDF")


def lote_do_env():
    """Créditos por transação de gravação, de LOTE_GRAVACAO_TJDF no .env (inteiro maior que zero)."""
    valor = os.environ.get("LOTE_GRAVACAO_TJDF", "").strip()
    if not valor.isdigit() or int(valor) < 1:
        raise SystemExit(f"LOTE_GRAVACAO_TJDF no .env precisa ser um inteiro maior que zero (veio {valor!r}).")
    return int(valor)


TRIBUNAL_TJDFT = 107
SOFTWARE = "CONSULTA_PUBLICA_TJDFT"
SOFTWARE_RPA = 2                       # RPA_CREDOR_V1
WORKER = f"{socket.gethostname()}:TJDF:consulta_publica:{os.getpid()}"
LOTE = lote_do_env()                   # créditos por transação de gravação
WORKERS = 6                            # workers (threads deste processo) raspando ao mesmo tempo (--workers)
TETO_CREDITO = 8 * 60                  # s por crédito; passou disso, volta para a fila
ADIAMENTO = "30 minutes"               # erro passageiro
ADIAMENTO_SEM_PUBLICACAO = "15 days"   # precatório sem publicação no DJe nem no DJEN: tenta de novo depois
ADIAMENTO_SEM_CREDOR = "60 days"       # publicações sem credor identificável: tenta de novo depois (decisão do usuário)
MAX_TENTATIVAS_SEM_CREDOR = 3          # na 3ª vez sem credor: FALHA SEM_CREDOR
RE_CONTADOR_SEM_CREDOR = re.compile(r"\[sem_credor=(\d+)\]")
REORDENAR_A_CADA = 30 * 60             # s entre remontagens da ordem da fila (entram os adiados que venceram)
MAX_FALHAS_SEGUIDAS = 8                # falhas técnicas seguidas que param a rodada
MAX_REINICIOS = 300                    # rodadas que param (fonte fora, banco caiu) antes de desistir de vez (~25 h)
PAUSA_REINICIO = 5 * 60                # s entre uma rodada que parou e a próxima
AMOSTRA_SIMULACAO = 20
FAIXAS = {1: "NOME", 2: "INICIAIS"}    # apresentado até 2021 (DJe com nome e CPF) / de 2022 em diante (iniciais)
ANO_SO_INICIAIS = 2022

DJE = "https://pesquisadje.tjdft.jus.br/api/v1/buscador"
DJE_DESDE = "2007-01-01"
RITMO_DJE = 5.0                        # req/s no DJe
MAX_PAGINAS_DJE_PRECATORIO = 5         # 10 publicações por página
MAX_PAGINAS_DJE_NOME = 5
MAX_PAGINAS_DJE_ADVOGADO = 30
PJE = {1: "https://pje-consultapublica-api.tjdft.jus.br/v1", 2: "https://pje2i-consultapublica-api.tjdft.jus.br/v1"}
PJE_SITE = {1: "https://pje-consultapublica.tjdft.jus.br", 2: "https://pje2i-consultapublica.tjdft.jus.br"}
RITMO_PJE = 4.0                        # req/s no PJe (--ritmo); o servidor derruba conexões sob carga
MAX_PAGINAS_POLO = 60                  # 10 partes por página (ação coletiva grande)
MAX_PAGINAS_DOCUMENTOS = 30            # 10 documentos por página
MAX_DOCUMENTOS_LIDOS = 25              # textos lidos por processo atrás do número do precatório
JANELA_DOCUMENTOS = 60                 # dias: documentos de perto da apresentação primeiro; mais velhos não citam
MAX_CACHE_PROCESSOS = 20000            # processos e listas de documentos guardados (coletivos e irmãos reaproveitam)
MAX_CACHE = 3000
DJEN = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
ORGAOS_COORPRE = (42759, 51634)        # Gabinete e Secretaria da Coordenação de Conciliação de Precatórios
DJEN_DESDE = date(2023, 1, 1)          # antes disso o DJEN não tem publicação do TJDFT
PASTA_DJEN = SAIDA / "djen_coorpre"
TETO_DJEN_CONSULTA = 10000             # o DJEN não passa disso numa consulta: período acima disso é dividido
INTERVALO_DJEN = 1.2                   # s entre consultas (o limite do DJEN é por IP e dividido com os outros robôs)
PAUSA_429 = 15
INTERVALO_DJEN_MAX = 6.0
MAX_PAGINAS_DJEN_OAB = 10              # 100 publicações por página
TIMEOUT_HTTP = 60
RITMO_MINIMO = 0.3
PAUSA_ERRO = 60                        # s que todos os workers param depois de um erro de servidor
SUBIR_A_CADA = 30                      # respostas boas seguidas para subir o ritmo em 25% (até o teto)
FATOR_SUBIDA = 1.25
HTTP_FREIA = {403, 429, 502, 503, 504}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"
UM_DIA = timedelta(days=1)

JANELA_ADVOGADO = (540, 60)            # dias antes/depois da apresentação em que se lê o que o advogado publicou
MAX_ADVOGADOS = 4                      # advogados do precatório usados na rota das iniciais
MAX_CANDIDATOS_NOME = 8                # processos abertos no PJe na rota do nome
MAX_CANDIDATOS_INICIAIS = 15           # processos abertos no PJe na rota das iniciais (polo publicado já bate)
MAX_SEM_PISTA = 2                      # e os do advogado cuja publicação não mostrou ninguém com as iniciais
MAX_EVIDENCIA = 4                      # candidatos em que se procura o número do precatório nos documentos

CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.07\.\d{4}")
RE_NUM_PRECATORIO = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.07\.0000")
RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
# cabeçalho do DJe: 'N. 0722180-36.2019.8.07.0000 - PRECATÓRIO - A: NOME. Adv(s).: DF0038015A - ADV. R: DISTRITO FEDERAL'
RE_CAB = re.compile(r"N\. (\d{7}-\d{2}\.\d{4}\.8\.07\.\d{4}) - ([A-ZÇÃÁÉÍÓÚÊÔÂÀÕÜ /]+?) - (.{0,1500}?)"
                    r"(?=N\. \d{7}-\d{2}\.|Poder Judici|$)")
RE_POLO_A = re.compile(r"A: (.+?)\.(?=\s*Adv\(s\)|\s*A:|\s*R:|\s*$)")
RE_POLO_R = re.compile(r"R: (.+?)\.(?=\s*Adv\(s\)|\s*A:|\s*R:|\s*$)")
RE_ADV_DJE = re.compile(r"\b([A-Z]{2})0*(\d{1,7})[A-Z]? - ([A-ZÀ-Ü' ]+?)(?=,|\.|$)")
RE_ADV_CERTIDAO = re.compile(r"Advogado do\(a\) [A-ZÀ-Ü ]+?: ([A-ZÀ-Ü' ]+?) - ([A-Z]{2})0*(\d{1,7})")
RE_FIM_CERTIDAO = re.compile(r"C E R T I D|CERTID[ÃA]O|DECIS[ÃA]O|DESPACHO|N\. \d{7}-")
RE_NOME_CPF = re.compile(r"([A-ZÀ-Ü][A-ZÀ-Ü'.\-&/ ]+?) \((CPF|CNPJ): ?([\d./\-]+)\)")
# classes que não geram precatório contra o ente, e as recursais
RE_CLASSE_FORA = re.compile(r"PENAL|CRIMIN|CARTA PRECATORIA|CARTA DE ORDEM|INQUERITO|ALVARA|INVENTARIO|ARROLAMENTO|"
                            r"DIVORCIO|ALIMENTOS|TERMO CIRCUNSTANCIADO|MEDIDAS PROTETIVAS|EXECUCAO FISCAL|"
                            r"BUSCA E APREENSAO|PRECATORIO|REQUISICAO|PEQUENO VALOR|APELACAO|AGRAVO|RECURSO|"
                            r"EMBARGOS DE DECLARACAO|REMESSA NECESSARIA|CONFLITO DE COMPETENCIA")
RE_CLASSE_EXECUCAO = re.compile(r"CUMPRIMENTO|EXECU|JUIZADO ESPECIAL DA FAZENDA|LIQUIDACAO")
RE_ORGAO_PUBLICO = re.compile(r"^(?:ESTADO D|MUNICIPIO D|UNIAO\b|DISTRITO FEDERAL)|PROCURADORIA|DEFENSORIA PUBLICA|"
                              r"MINISTERIO PUBLICO|FAZENDA PUBLICA|PREFEITURA|CAMARA MUNICIPAL|TRIBUNAL D|"
                              r"INSTITUTO NACIONAL DO SEGURO SOCIAL|CORPO DE BOMBEIROS|POLICIA MILITAR|POLICIA CIVIL")
# devedores do TJDFT no polo passivo do originário (o DF e a administração indireta)
RE_DEVEDOR = re.compile(r"DISTRITO FEDERAL|SEGURO SOCIAL|\bINSS\b|INSTITUTO DE PREVIDENCIA|IPREV|INSTITUTO DE ASSISTENCIA|"
                        r"DEPARTAMENTO DE TRANSITO|DETRAN|DEPARTAMENTO DE ESTRADA|SERVICO DE LIMPEZA|CAESB|SANEAMENTO|"
                        r"NOVACAP|TERRACAP|COMPANHIA|FUNDACAO|SECRETARI|GOVERNADOR|PROCON|FAZENDA|UNIAO\b|MUNICIPIO|"
                        r"ESTADO D|AGENCIA|AUTARQUIA|UNIVERSIDADE|POLICIA|BOMBEIRO|INSTITUTO")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA|ESCRITORIO DE ADVOCACIA|"
                              r"\bADVOGADOS ASSOCIADOS\b|\bADV\b")
# pistas do texto da COORPRE: 'credor(a)(es) SABINA N. M.', 'CREDOR: DAILTON DAS GRAÇAS G. F.', '... MARIA L. F. CRUZ, no'
RE_CREDOR_TEXTO = re.compile(r"CREDOR(?:\(A\))?(?:\(ES\))?(?:\s*:)?\s+(?:CREDOR\s*:\s*)?([A-Z][A-Z. ]{3,90}?)"
                             r"(?=,|;|\(| NO MONTANTE| EM RAZAO| FORMUL| FAZ JUS| CPF| POR MEIO| SOBRE| PARA |$)")
RE_AUTOS_EXECUCAO = re.compile(r"AUTOS (?:DE|DA|DO) (?:EXECU[CÇ][AÃ]O|CUMPRIMENTO|PROCESSO)[^0-9]{0,40}"
                               r"(\d{7}-\d{2}\.\d{4}\.8\.07\.\d{4})", re.I)
# documento do originário que pode citar o número do precatório (os demais são intimação/disponibilização)
RE_DOC_UTIL = re.compile(r"CERTID|DECIS|DESPACH|SENTEN|OF[IÍ]CIO|ATO ORDIN|PLANILHA|C[AÁ]LCULO", re.I)
RE_DOC_INUTIL = re.compile(r"DISPONIBILIZA", re.I)
SEMELHANCA_MESMA_PESSOA = 0.9
PAPEL_LEGADO = {"ATIVO": "REQUERENTE", "PASSIVO": "REQUERIDO"}    # como o RPA grava a capa antiga
POLO_BRUTO = {"ATIVO": "AUTOR", "PASSIVO": "REU"}
STATUS_COM_VINCULO = ("SUCESSO_PROCESSO_ORIGINARIO", "SUCESSO_INCOMPLETO")
TABELA_LEGADO = "listas_primarias.processos_unificados"

COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "faixa", "ente", "apresentacao", "prioridade",
           "ultimo_status", "resultado", "motivo", "originario", "regra", "evidencia", "credor", "credor_documento",
           "fonte_documento", "fonte_nome", "iniciais", "advogados", "candidatos", "credor_corrigido", "banco",
           "legado", "credores_antes", "credores_depois", "lote", "segundos"]
COLUNAS_TROCAS = ["processado_em", "modo", "credito_id", "precatorio", "credores_antes", "credores_depois",
                  "saiu", "entrou"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
SEM_GRAVACAO = {"banco": "", "legado": "", "antes": set(), "depois": set(), "corrigidos": []}


class ErroTecnico(Exception):
    """Falha passageira (fonte fora do ar, timeout): o crédito volta para a fila, não vira FALHA."""


class Adiar(Exception):
    """O crédito volta para a fila daqui a `intervalo` sem contar como falha técnica (ex.: sem publicação ainda)."""

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


def nome_cortado(a, b):
    """Um nome é o outro cortado no fim ('ANA CLAUDIA R' / 'MANOEL RAIMUNDO N' x o nome inteiro): mesmas palavras,
    a última abreviada."""
    wa, wb = chave_nome(a).split(), chave_nome(b).split()
    curto, longo = (wa, wb) if len(" ".join(wa)) <= len(" ".join(wb)) else (wb, wa)
    if len(curto) < 2 or len(curto[-1]) > 2 or len(curto) > len(longo) or curto[:-1] != longo[:len(curto) - 1]:
        return False
    return longo[len(curto) - 1].startswith(curto[-1]) and curto != longo


def eh_sociedade(nome):
    return bool(RE_SOCIEDADE_ADV.search(chave_nome(nome)))


def eh_orgao(nome):
    return bool(RE_ORGAO_PUBLICO.search(chave_nome(nome)))


def eh_pessoa_comum(nome):
    """Nem ente público nem sociedade de advogados."""
    n = chave_nome(nome)
    return bool(n) and not RE_ORGAO_PUBLICO.search(n) and not RE_SOCIEDADE_ADV.search(n)


def iniciais(nome):
    """'JOSE ANTONIO DE OLIVEIRA' -> 'JADO'; 'J. A. D. O.' -> 'JADO' (as partículas contam, como no DJEN)."""
    return "".join(w[0] for w in re.findall(r"[A-Z0-9]+", sem_acento(re.split(r"\bREGISTRAD[OA]\b",
                                                                              sem_acento(nome or ""))[0])))


def so_iniciais(nome):
    """O nome é só iniciais ('J. A. D. O.', 'P. A. A. S. -. M.')."""
    return bool(re.fullmatch(r"(?:(?:[A-Z0-9]|-)\.+\s*)+", sem_acento(nome or "").strip()))


def termo_do_ente(ente):
    """Trecho que identifica o ente no polo passivo: 'DISTRITO FEDERAL', 'SEGURO SOCIAL' para o INSS, a cidade do
    município, o nome sem a sigla depois do ' - ' para o resto."""
    n = normal(ente)
    if re.search(r"\bINSS\b|SEGURIDADE SOCIAL|SEGURO SOCIAL", n):
        return "SEGURO SOCIAL"
    n = normal(re.split(r"\s+-\s+", ente)[0]) or n
    m = re.match(r"MUNICIPIO D[EOA]S? (.+)", n)
    return m.group(1) if m else n


def limpa_html(t):
    return " ".join(H.unescape(re.sub(r"<[^>]+>", " ", t or "")).split())


def data_br(t):
    """Primeira data dd/mm/aaaa do texto -> date; None se não tiver."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", t or "")
    try:
        return date(int(m[3]), int(m[2]), int(m[1])) if m else None
    except ValueError:
        return None


def fmt_credores(itens):
    """{(papel, nome, documento, origem)} -> texto para o CSV."""
    return " | ".join(f"{nome} ({doc or 'sem doc'}, {papel}, {origem})" for papel, nome, doc, origem in sorted(itens))


def advogado(nome, uf, numero, cpf=""):
    return {"nome": " ".join((nome or "").split()), "oab_uf": (uf or "").upper(), "oab_numero": so_digitos(numero),
            "cpf": so_digitos(cpf) if documento_valido(cpf) else ""}

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


class Cache:
    """Dicionário LRU com trava, para as threads."""

    def __init__(self, tamanho=MAX_CACHE):
        self.dados, self.tamanho, self.trava = OrderedDict(), tamanho, threading.Lock()

    def get(self, chave, padrao=None):
        with self.trava:
            if chave in self.dados:
                self.dados.move_to_end(chave)
                return self.dados[chave]
            return padrao

    def __contains__(self, chave):
        with self.trava:
            return chave in self.dados

    def put(self, chave, valor):
        with self.trava:
            self.dados[chave] = valor
            while len(self.dados) > self.tamanho:
                self.dados.popitem(last=False)
        return valor

# =============================================================================== DJe do TJDFT


class Dje:
    """Busca textual no DJe do TJDFT (datas obrigatórias, 10 publicações por página, termos entre aspas com E)."""

    def __init__(self, parar):
        self.parar, self.http = parar, Http({"Accept": "application/json"})
        self.ritmo = Ritmo("DJe", RITMO_DJE)
        self.cache = Cache(MAX_CACHE_PROCESSOS)

    def _pagina(self, query, ini, fim, pagina):
        for tentativa in range(5):
            self.ritmo.esperar(self.parar)
            try:
                r = self.http.sessao().get(DJE, params={"query": query, "pagina": pagina, "dataInicio": ini,
                                                        "dataFim": fim}, timeout=TIMEOUT_HTTP)
            except requests.RequestException as e:
                self.ritmo.freia(e.__class__.__name__)
                continue
            if r.status_code == 200:
                self.ritmo.ok()
                return r.json()
            if r.status_code in HTTP_FREIA or r.status_code >= 500:
                self.ritmo.freia(f"HTTP {r.status_code}")
                continue
            return {"documentos": [], "totalPaginas": 0}
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: DJe do TJDFT não respondeu")

    def buscar(self, query, ini=DJE_DESDE, fim=None, max_paginas=MAX_PAGINAS_DJE_PRECATORIO):
        """Prévias das publicações (texto limpo, data) da consulta; em cache."""
        fim = fim or date.today().isoformat()
        chave = (query, ini, fim, max_paginas)
        if chave in self.cache:
            return self.cache.get(chave)
        saida = []
        for pagina in range(max_paginas):
            j = self._pagina(query, ini, fim, pagina)
            for d in j.get("documentos") or []:
                saida.append({"data": d.get("dataDisponibilizacao") or "",
                              "texto": " ".join(re.sub(r"</?em>", "", " ".join(d.get("preview") or [])).split())})
            if pagina + 1 >= (j.get("totalPaginas") or 0):
                break
        return self.cache.put(chave, saida)

    @staticmethod
    def cabecalhos(pubs):
        """[(cnj20, classe, polo A, polo R, advogados, corpo, data)] dos cabeçalhos 'N. <cnj> - CLASSE - A: ...'."""
        saida = []
        for p in pubs:
            for m in RE_CAB.finditer(p["texto"]):
                corpo = m.group(3)
                advs = [advogado(a.group(3), a.group(1), a.group(2)) for a in RE_ADV_DJE.finditer(corpo)]
                saida.append((so_digitos(m.group(1)), normal(m.group(2)), [x.strip() for x in RE_POLO_A.findall(corpo)],
                              [x.strip() for x in RE_POLO_R.findall(corpo)], advs, corpo, p["data"]))
        return saida

    def do_precatorio(self, prec_fmt):
        """{nomes, cpfs [(nome, doc)], advogados, n}: o que o DJe publicou do precatório."""
        pubs = self.buscar(f'"{prec_fmt}"') + self.buscar(f'"{prec_fmt}" CPF')
        prec20 = so_digitos(prec_fmt)
        nomes, advs, cpfs = [], {}, []
        for num, _, polo_a, _, advs_cab, _, _ in self.cabecalhos(pubs):
            if num != prec20:
                continue
            nomes += [n for n in polo_a if n not in nomes]
            for a in advs_cab:
                advs.setdefault((a["oab_uf"], a["oab_numero"]), a)
        for p in pubs:
            for m in re.finditer(re.escape(prec_fmt) + r" ((?:[A-ZÀ-Ü][A-ZÀ-Ü'.\-&/ ]+ \((?:CPF|CNPJ): ?[\d./\-]+\);? ?)+)",
                                 p["texto"]):
                for n in RE_NOME_CPF.finditer(m.group(1)):
                    doc = so_digitos(n.group(3))
                    if documento_valido(doc) and (n.group(1).strip(), doc) not in cpfs:
                        cpfs.append((n.group(1).strip(), doc))
                # logo depois da lista: 'Advogado do(a) CREDOR: ROBERTO GOMES FERREIRA - DF11723-A' (quem é advogado)
                resto = RE_FIM_CERTIDAO.split(p["texto"][m.end():m.end() + 1500], 1)[0]
                for a in RE_ADV_CERTIDAO.finditer(resto):
                    adv = advogado(a.group(1), a.group(2), a.group(3))
                    advs.setdefault((adv["oab_uf"], adv["oab_numero"]), adv)
        return {"nomes": nomes, "cpfs": cpfs, "advogados": list(advs.values()), "n": len(pubs)}

    def processos_do_nome(self, nome):
        """{cnj20: processo} das publicações em que o nome está no polo ativo (fora o 2º grau de precatório)."""
        procs = {}
        for num, classe, polo_a, polo_r, advs, _, dt in self.cabecalhos(
                self.buscar(f'"{nome}"', max_paginas=MAX_PAGINAS_DJE_NOME)):
            if RE_CLASSE_FORA.search(classe) or not any(mesma_pessoa(a, nome) for a in polo_a):
                continue
            p = procs.setdefault(num, {"classe": classe, "ativo": set(), "passivo": set(), "fonte": "DJE_NOME"})
            p["ativo"].update(polo_a)
            p["passivo"].update(polo_r)
        return procs

    def processos_do_advogado(self, nome, ini, fim):
        """{cnj20: processo} das publicações do advogado contra a Fazenda na janela."""
        procs = {}
        for num, classe, polo_a, polo_r, _, _, _ in self.cabecalhos(
                self.buscar(f'"{nome}" "FAZENDA"', ini, fim, MAX_PAGINAS_DJE_ADVOGADO)):
            if RE_CLASSE_FORA.search(classe):
                continue
            p = procs.setdefault(num, {"classe": classe, "ativo": set(), "passivo": set(), "fonte": "DJE_ADVOGADO"})
            p["ativo"].update(polo_a)
            p["passivo"].update(polo_r)
        return procs

# =============================================================================== DJEN


def pistas_do_texto(texto, prec20):
    """Do texto de uma publicação da COORPRE: nomes do credor (inteiro ou 'PRIMEIRO N. M.') e processos citados
    ('autos de execução n. X', outros números CNJ que não o do precatório)."""
    t = sem_acento(texto)
    nomes = []
    for m in RE_CREDOR_TEXTO.finditer(t):
        n = " ".join(m.group(1).replace(" .", ".").split()).strip(" .")
        palavras = n.split()
        if len(palavras) >= 2 and palavras[0] not in ("DO", "DA", "DE", "NO", "E", "PARA", "QUE", "COM") \
                and not so_iniciais(n) and n not in nomes:
            nomes.append(n)
    citados = [so_digitos(c) for c in RE_AUTOS_EXECUCAO.findall(texto)]
    citados += [so_digitos(c) for c in CNJ.findall(texto) if so_digitos(c) != prec20 and so_digitos(c) not in citados]
    return {"nomes": nomes, "cnjs": list(dict.fromkeys(citados))}


class Djen:
    """DJEN com um relógio só (o limite é por IP e dividido com os outros robôs), nova tentativa, pré-carga da COORPRE
    e cache das consultas por OAB."""

    def __init__(self, parar):
        self.parar, self.http = parar, Http({})
        self.trava, self.proxima, self.intervalo, self.n_429 = threading.Lock(), 0.0, INTERVALO_DJEN, 0
        self.cache = Cache(MAX_CACHE_PROCESSOS)
        self.indice = {}

    def _vez(self):
        with self.trava:
            vez = max(time.time(), self.proxima)
            self.proxima = vez + self.intervalo
        if (espera := vez - time.time()) > 0 and self.parar.wait(espera):
            raise ErroTecnico("INTERROMPIDO")

    def buscar(self, **params):
        for _ in range(8):
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

    @staticmethod
    def enxuto(item):
        """Só o que o robô usa: número, data, classe, partes por polo, advogados com OAB e as pistas do texto."""
        numero = so_digitos(item.get("numero_processo") or item.get("numeroprocessocommascara"))[:20]
        advogados = []
        for a in item.get("destinatarioadvogados") or []:
            adv = a.get("advogado") or {}
            if adv.get("nome") and adv.get("numero_oab"):
                advogados.append(advogado(re.split(r"\s+REGISTRAD[OA]\(?A?\)?\s+CIVILMENTE", adv["nome"])[0],
                                          adv.get("uf_oab"), str(adv.get("numero_oab"))))
        return {"numero": numero, "data": (item.get("data_disponibilizacao") or "")[:10],
                "classe": normal(item.get("nomeClasse")),
                "partes": [[d.get("polo"), " ".join((d.get("nome") or "").split())]
                           for d in item.get("destinatarios") or []],
                "advogados": advogados, "pistas": pistas_do_texto(limpa_html(item.get("texto")), numero)}

    def _periodo(self, orgao, ini, fim):
        """Publicações do órgão entre ini e fim; período que satura o DJEN é dividido ao meio."""
        itens, pagina = [], 1
        while True:
            lote = self.buscar(siglaTribunal="TJDFT", orgaoId=orgao, pagina=pagina,
                               dataDisponibilizacaoInicio=ini.isoformat(), dataDisponibilizacaoFim=fim.isoformat())
            itens += [self.enxuto(i) for i in lote]
            if len(lote) < 100:
                return itens
            pagina += 1
            if pagina * 100 > TETO_DJEN_CONSULTA and fim > ini:
                meio = ini + (fim - ini) / 2
                return self._periodo(orgao, ini, meio) + self._periodo(orgao, meio + UM_DIA, fim)

    def carregar_coorpre(self):
        """Índice {precatório20: [publicações]} com tudo o que a COORPRE publicou desde DJEN_DESDE. Cada mês fica em
        PASTA_DJEN/AAAA-MM.json: mês fechado não é baixado de novo; o corrente e o anterior sim."""
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
                itens = []
                for orgao in ORGAOS_COORPRE:
                    itens += self._periodo(orgao, mes, min(prox - UM_DIA, hoje))
                arquivo.write_text(json.dumps(itens, ensure_ascii=False), encoding="utf-8")
                baixados += 1
            for it in itens:
                indice.setdefault(it["numero"], []).append(it)
            mes = prox
        self.indice = indice
        log.info(f"DJEN da COORPRE: {sum(len(v) for v in indice.values())} publicações de {len(indice)} precatórios "
                 f"desde {DJEN_DESDE:%m/%Y} ({baixados} mês(es) baixado(s), {time.time() - inicio:.0f} s)")

    def do_precatorio(self, prec20):
        """Publicações do precatório: do índice da COORPRE; fora dele, o DJEN direto."""
        if prec20 in self.indice:
            return self.indice[prec20]
        chave = ("prec", prec20)
        if chave in self.cache:
            return self.cache.get(chave)
        return self.cache.put(chave, [self.enxuto(i) for i in self.buscar(numeroProcesso=prec20,
                                                                            siglaTribunal="TJDFT")])

    def processos_da_oab(self, uf, numero, ini, fim):
        """{cnj20: processo} das publicações do advogado (pela OAB) na janela, fora o 2º grau de precatório."""
        fim = min(fim, date.today())
        if fim < DJEN_DESDE:
            return {}
        ini = max(ini, DJEN_DESDE)
        chave = ("oab", uf, numero, ini.isoformat()[:7], fim.isoformat()[:7])
        if chave in self.cache:
            return self.cache.get(chave)
        procs = {}
        for pagina in range(1, MAX_PAGINAS_DJEN_OAB + 1):
            lote = self.buscar(numeroOab=numero, ufOab=uf, siglaTribunal="TJDFT", pagina=pagina,
                               dataDisponibilizacaoInicio=ini.isoformat(), dataDisponibilizacaoFim=fim.isoformat())
            for it in lote:
                e = self.enxuto(it)
                if RE_CLASSE_FORA.search(e["classe"]):
                    continue
                p = procs.setdefault(e["numero"], {"classe": e["classe"], "ativo": set(), "passivo": set(),
                                                   "fonte": "DJEN_OAB"})
                for polo, nome in e["partes"]:
                    (p["ativo"] if polo == "A" else p["passivo"] if polo == "P" else set()).add(nome)
            if len(lote) < 100:
                break
        return self.cache.put(chave, procs)

# =============================================================================== PJe consulta pública


def parte_do_pje(x):
    """{'participante': 'NOME - OAB DF14848-A - CPF: 777.401.220-68 (ADVOGADO)', ...} -> parte."""
    texto = x.get("participante") or ""
    doc = re.search(r"(?:CPF|CNPJ): ([\d./\-]+)", texto)
    oab = re.search(r"OAB ([A-Z]{2})(\d+)", texto)
    return {"nome": " ".join((x.get("nome") or "").split()), "tipo": normal(x.get("tipo")),
            "documento": so_digitos(doc.group(1)) if doc and documento_valido(doc.group(1)) else "",
            "oab_uf": oab.group(1) if oab else "", "oab_numero": oab.group(2) if oab else "", "advogados": []}


def agrupar_polo(lista):
    """O PJe lista cada advogado logo depois da parte que ele representa: advogados entram na parte de antes."""
    partes = []
    for x in lista:
        p = parte_do_pje(x)
        if p["tipo"] == "ADVOGADO":
            if partes:
                a = advogado(p["nome"], p["oab_uf"], p["oab_numero"], p["documento"])
                if a["oab_numero"] and a not in partes[-1]["advogados"]:
                    partes[-1]["advogados"].append(a)
            continue
        partes.append(p)
    return partes


class Pje:
    """API REST da consulta pública do PJe do TJDFT (1º e 2º grau), com ritmo adaptativo e cache dos processos."""

    def __init__(self, parar, teto):
        self.parar = parar
        self.http = {g: Http({"Accept": "application/json", "Origin": PJE_SITE[g], "Referer": PJE_SITE[g] + "/"})
                     for g in (1, 2)}
        self.ritmo = Ritmo("PJe", teto)
        self.processos, self.evidencias, self.documentos = Cache(MAX_CACHE_PROCESSOS), Cache(), Cache(MAX_CACHE_PROCESSOS)
        self.chamadas, self.trava = Counter(), threading.Lock()

    def _get(self, grau, caminho, opcional=False, **params):
        """JSON da API; None se a API recusou (400/404) ou, em pedido opcional, se deu erro interno (500) 3 vezes;
        ErroTecnico se não respondeu. O 500 é defeito do próprio processo no servidor (o /dados de alguns processos
        volta 500 sempre, em 0,1 s), não carga: tenta de novo sem frear o ritmo de todos os workers."""
        tipo = "texto" if caminho.startswith("/documentos/") else (caminho.rsplit("/", 1)[-1] if "/" in caminho[1:]
                                                                   else "busca")
        with self.trava:
            self.chamadas[tipo] += 1
        internos = 0
        for tentativa in range(5):
            self.ritmo.esperar(self.parar)
            try:
                r = self.http[grau].sessao().get(PJE[grau] + caminho, params=params, timeout=TIMEOUT_HTTP)
            except requests.RequestException as e:
                self.ritmo.freia(e.__class__.__name__)
                continue
            if r.status_code == 200:
                self.ritmo.ok()
                try:
                    return r.json()
                except ValueError:
                    return None
            if r.status_code == 500:
                internos += 1
                if internos == 3:
                    break
                if self.parar.wait(2):
                    raise ErroTecnico("INTERROMPIDO")
                continue
            if r.status_code in HTTP_FREIA or r.status_code > 500:
                self.ritmo.freia(f"HTTP {r.status_code}")
                continue
            return None
        if internos == 3:
            with self.trava:
                self.chamadas[f"{tipo} com HTTP 500"] += 1
            if opcional:
                return None
            raise ErroTecnico(f"PESQUISA_SEM_RESPOSTA: PJe do TJDFT com erro interno (HTTP 500) em {tipo}")
        raise ErroTecnico("PESQUISA_SEM_RESPOSTA: PJe consulta pública do TJDFT não respondeu")

    def _paginas(self, grau, caminho, maximo):
        saida = []
        for pagina in range(maximo):
            j = self._get(grau, caminho, page=pagina) or {}
            itens = j.get("result") or []
            saida += itens
            if len(itens) < 10:
                break
        return saida

    def abrir(self, cnj20):
        """O processo pelo número (abre arquivado também): capa, polo ativo e passivo (com CPF e advogados com OAB).
        None se não abre (sigiloso, de outro sistema)."""
        cnj20 = so_digitos(cnj20)
        if cnj20 in self.processos:
            return self.processos.get(cnj20)
        grau = 2 if cnj20.endswith("0000") else 1
        j = self._get(grau, "/processos", numeroProcesso=formatar_cnj(cnj20)) or {}
        res = j.get("result") or []
        if not res:
            return self.processos.put(cnj20, None)
        pid = res[0]["idProcesso"]
        # /dados quebrado no servidor (HTTP 500 sempre, em alguns processos): segue com classe e assunto da busca, sem
        # vara, comarca e data (sem data, o candidato não é descartado por ter sido distribuído depois do precatório)
        dados = (self._get(grau, f"/processos/{pid}/dados", opcional=True) or {}).get("result") or {}
        proc = {"numero": cnj20, "grau": grau, "id": pid,
                "classe": re.sub(r"\s*\(\d+\)\s*$", "", dados.get("classeJudicial") or res[0].get("classe") or ""),
                "vara": dados.get("orgaoJulgador") or "", "comarca": dados.get("jurisdicao") or "",
                "assunto": (dados.get("assunto") or res[0].get("assunto") or "")[:300],
                "data": (dados.get("dataDistribuicao") or "")[:10],
                "ativo": agrupar_polo(self._paginas(grau, f"/processos/{pid}/poloAtivo", MAX_PAGINAS_POLO)),
                "passivo": agrupar_polo(self._paginas(grau, f"/processos/{pid}/poloPassivo", 5))}
        return self.processos.put(cnj20, proc)

    def memo_documentos(self, proc):
        """Cache dos documentos do processo, com uma trava própria (dois precatórios do mesmo originário em threads
        diferentes: o segundo espera e reaproveita o que o primeiro leu)."""
        with self.trava:
            memo = self.documentos.get(proc["numero"])
            if memo is None:
                memo = self.documentos.put(proc["numero"], {"lista": [], "pagina": 0, "fim": False, "numeros": {},
                                                            "trava": threading.Lock()})
        return memo

    def _lista_documentos(self, proc, desde):
        """Documentos do processo (do mais novo para o mais velho, como a API lista), até o primeiro de antes de
        `desde`: o que é mais velho que a apresentação do precatório não o cita. Em cache por processo, com os números
        de precatório achados em cada texto já lido (os precatórios irmãos do mesmo originário não releem nada)."""
        memo = self.memo_documentos(proc)
        while not memo["fim"] and memo["pagina"] < MAX_PAGINAS_DOCUMENTOS:
            ultimo = data_br(memo["lista"][-1].get("descricao")) if memo["lista"] else None
            if desde and ultimo and ultimo < desde:
                break
            j = self._get(proc["grau"], f"/processos/{proc['id']}/documentos", page=memo["pagina"]) or {}
            itens = j.get("result") or []
            memo["lista"] += itens
            memo["pagina"] += 1
            memo["fim"] = len(itens) < 10
        return memo

    def cita_precatorio(self, proc, prec_fmt, apresentacao):
        """Trecho do documento do processo que cita o número do precatório (None se nenhum lido cita). Lê primeiro o
        que tem o número na descrição; depois as certidões/decisões de perto da apresentação (a certidão 'precatório
        distribuído na COORPRE com o número ...' sai nos dias dela) e as mais novas, até MAX_DOCUMENTOS_LIDOS textos."""
        chave = (proc["numero"], prec_fmt)
        if chave in self.evidencias:
            return self.evidencias.get(chave)
        with self.memo_documentos(proc)["trava"]:
            return self.evidencias.put(chave, self._procurar(proc, prec_fmt, apresentacao))

    def _procurar(self, proc, prec_fmt, apresentacao):
        desde = apresentacao - timedelta(days=JANELA_DOCUMENTOS) if apresentacao else None
        memo = self._lista_documentos(proc, desde)
        prec20 = so_digitos(prec_fmt)
        for d in memo["lista"]:
            if prec_fmt in (d.get("descricao") or "") or prec20 in so_digitos(d.get("descricao")):
                return f"descrição do documento: {d['descricao'][:120]}"
        for doc_id, numeros in memo["numeros"].items():           # textos lidos para outro precatório
            if prec20 in numeros:
                d = next((x for x in memo["lista"] if x["id"] == doc_id), {"descricao": ""})
                return f"{d['descricao'][:60]}: cita o precatório (lido antes)"

        def data_doc(d):
            return data_br(d.get("descricao"))
        uteis = [d for d in memo["lista"] if not d.get("binario") and d["id"] not in memo["numeros"]
                 and RE_DOC_UTIL.search(d.get("descricao") or "") and not RE_DOC_INUTIL.search(d.get("descricao") or "")
                 and not (desde and data_doc(d) and data_doc(d) < desde)]
        if apresentacao:
            perto = [d for d in uteis if data_doc(d) and abs((data_doc(d) - apresentacao).days) <= JANELA_DOCUMENTOS]
            perto.sort(key=lambda d: abs((data_doc(d) - apresentacao).days))
            uteis = perto + [d for d in uteis if d not in perto]            # o resto, do mais novo para o mais velho
        for d in uteis[:MAX_DOCUMENTOS_LIDOS]:
            j = self._get(proc["grau"], f"/documentos/{d['id']}") or {}
            texto = limpa_html((j.get("result") or {}).get("documento"))
            memo["numeros"][d["id"]] = {so_digitos(n) for n in RE_NUM_PRECATORIO.findall(texto)}
            if prec_fmt in texto:
                i = texto.find(prec_fmt)
                return f"{d['descricao'][:60]}: ...{texto[max(0, i - 160):i + 30]}"
        return None

# =============================================================================== fila


# o que o robô pode pegar: do RPA em PENDENTE, do RPA sem credor em qualquer status (menos EM_ANDAMENTO) e o que já é
# do robô em PENDENTE; sempre vencido o disponivel_em. O que já é do robô com status final (SUCESSO/FALHA) não volta.
FILTRO_PEGAVEL = f"""cc.disponivel_em <= now() AND (
       (cc.software_id = {SOFTWARE_RPA}
        AND (cc.status_id = 1 OR NOT EXISTS (SELECT 1 FROM creditos.credito_credor k
                                              WHERE k.credito_id = cc.credito_id AND k.papel_id <> 2)))
    OR (cc.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}') AND cc.status_id = 1))"""

SQL_ESCOPO = f"""
SELECT cc.credito_id, cc.prioridade, f.metadata->'lista' AS lista
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c ON c.id = cc.credito_id
  LEFT JOIN creditos.credito_fonte f
         ON f.credito_id = c.id AND f.software_id = (SELECT id FROM creditos.software WHERE codigo = '{SOFTWARE}')
 WHERE cc.tribunal_id = %s AND c.saiu_da_lista_em IS NULL AND cc.status_id <> 2 AND {{filtro}}
"""


def faixa_da_lista(lista):
    """1 NOME (apresentado até 2021: o DJe publicou o nome e o CPF); 2 INICIAIS (2022 em diante)."""
    ano = str((lista or {}).get("apresentacao") or "")[:4]
    return 2 if ano.isdigit() and int(ano) >= ANO_SO_INICIAIS else 1


def ordenar_escopo(con, filtro, so_faixa=None):
    """(ids na ordem do robô, {credito_id: faixa}, contagem por faixa).
    Ordem: faixa, prioridade de campanha, superpreferência, ordem cronológica da lista, id."""
    with con.cursor() as cur:
        cur.execute(SQL_ESCOPO.format(filtro=filtro), (TRIBUNAL_TJDFT,))
        linhas = como_dicts(cur)
    faixas = {x["credito_id"]: faixa_da_lista(x["lista"]) for x in linhas}
    if so_faixa:                                        # --faixa: só os créditos de uma faixa
        linhas = [x for x in linhas if FAIXAS[faixas[x["credito_id"]]] == so_faixa]
        faixas = {x["credito_id"]: faixas[x["credito_id"]] for x in linhas}
    chave = {}
    for x in linhas:
        lista = x["lista"] or {}
        sup = 0 if normal(lista.get("prioridade")).startswith("SUPER") else 1
        chave[x["credito_id"]] = (faixas[x["credito_id"]], x["prioridade"], sup, lista.get("ordem") or 10 ** 9,
                                  x["credito_id"])
    ordem = sorted(chave, key=chave.get)
    return ordem, faixas, Counter(FAIXAS[f] for f in faixas.values())

# =============================================================================== banco: leitura


def conectar(escrita=False):
    """Leitura: sessão readonly sem transação aberta (nada fica preso enquanto se raspa). Escrita: transação manual."""
    return banco.conectar("fetch_TJDF", escrita=escrita, worker=WORKER)


SQL_CREDITO = f"""
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm, tc.codigo AS tipo_credito,
       (SELECT f.metadata->'lista' FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
         WHERE f.credito_id = c.id AND s.codigo = '{SOFTWARE}') AS lista,
       (SELECT a.texto_exemplo FROM creditos.ente_alias a WHERE a.id = c.ente_alias_id) AS ente_alias,
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
    """O crédito (lead) com o que a lista do TJDFT traz (credito_fonte.metadata.lista, gravado pelo carregar_TJDF.py)
    e os credores que o banco já tem."""
    cur.execute(SQL_CREDITO, (credito_id,))
    linhas = como_dicts(cur)
    if not linhas:
        raise RuntimeError(f"crédito {credito_id} não existe")
    lead = linhas[0]
    lista = lead["lista"] or {}
    lead["ente"] = " ".join(((lista.get("entes") or [None])[0] or lead["ente_alias"] or "").split())
    lead["termo_ente"] = termo_do_ente(lead["ente"]) if lead["ente"] else None
    try:
        lead["apresentacao"] = date.fromisoformat(str(lista.get("apresentacao"))[:10])
    except ValueError:
        lead["apresentacao"] = None
    lead["prioridade_lista"] = lista.get("prioridade") or ""
    lead["ordem"] = lista.get("ordem")
    lead["faixa"] = FAIXAS[faixa_da_lista(lista)]
    lead["credores_banco"] = [c for c in lead["credores"] or [] if c.get("documento")]
    lead["prec_fmt"] = formatar_cnj(lead["numero_norm"])
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


def credores_do_credito(cur, credito_id):
    """{(papel, nome, documento, origem)} ligados ao crédito (para comparar antes e depois da gravação)."""
    cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, ''), x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return set(cur.fetchall())


class Fontes:
    """As fontes de uma rodada, para as threads."""

    def __init__(self, dje, djen, pje, oabs=None):
        self.dje, self.djen, self.pje, self.oabs = dje, djen, pje, oabs


class OabsDoBanco:
    """Advogados que o banco já conhece (creditos.pessoa_oab), pelo nome: separa do credor o advogado que aparece no
    polo ativo do precatório sem nenhuma marca de advogado ali (honorários sucumbenciais). Conexão readonly por
    thread, cache por nome."""

    def __init__(self):
        self.local, self.cache = threading.local(), Cache(MAX_CACHE_PROCESSOS)

    def de(self, nome):
        """[(uf, numero, documento)] das OABs das pessoas do banco com esse nome."""
        chave = chave_nome(nome)
        if chave in self.cache:
            return self.cache.get(chave)
        if getattr(self.local, "con", None) is None or self.local.con.closed:
            self.local.con = conectar()
        try:
            with self.local.con.cursor() as cur:
                cur.execute("""SELECT o.uf::text, o.numero::text, COALESCE(p.documento::text, '')
                                 FROM creditos.pessoa p JOIN creditos.pessoa_oab o ON o.pessoa_id = p.id
                                WHERE p.nome_chave = creditos.chave_texto(%s)""", (nome,))
                linhas = cur.fetchall()
        except psycopg2.Error:
            self.local.con = None
            return []
        return self.cache.put(chave, linhas)

# =============================================================================== decisão


def resultado_vazio():
    """Resultado a gravar, antes de decidir."""
    return {"status": "FALHA", "motivo": "", "via": "PRECATORIO", "originario": None, "regra": "", "evidencia": "",
            "credor": None, "capa": None, "candidatos": [], "fonte_documento": "", "fonte_nome": "",
            "honorarios": None, "advogados_precatorio": [], "liga_credor": False, "candidato_analisar": None,
            "iniciais": [], "pistas": {}}


def dados_do_precatorio(lead, f):
    """O que as publicações dizem do precatório: pessoas (nome e CPF do DJe; nome inteiro do texto da COORPRE),
    iniciais do polo ativo (DJEN), primeiros nomes, advogados com OAB (e CPF), sociedades de advogados e os
    processos citados. Separa advogado, sociedade e órgão público do credor."""
    dje = f.dje.do_precatorio(lead["prec_fmt"])
    pubs = f.djen.do_precatorio(lead["numero_norm"])
    advs = {(a["oab_uf"], a["oab_numero"]): a for a in dje["advogados"] if a["oab_numero"]}
    ini_djen, pistas_nomes, citados = {}, [], []
    for p in pubs:
        for polo, nome in p["partes"]:
            if polo == "A" and nome:
                ini_djen.setdefault(iniciais(nome), nome)
        for a in p["advogados"]:
            advs.setdefault((a["oab_uf"], a["oab_numero"]), a)
        pistas_nomes += [n for n in p["pistas"]["nomes"] if n not in pistas_nomes]
        citados += [c for c in p["pistas"]["cnjs"] if c not in citados]
    nomes_adv = {chave_nome(a["nome"]) for a in advs.values()}
    ini_adv = {iniciais(a["nome"]) for a in advs.values()}

    def do_credor(nome):
        return eh_pessoa_comum(nome) and chave_nome(nome) not in nomes_adv

    # CPF de advogado na lista do DJe (honorários sucumbenciais): completa o advogado
    for nome, doc in dje["cpfs"]:
        for a in advs.values():
            if not a["cpf"] and mesma_pessoa(a["nome"], nome) and len(doc) == 11:
                a["cpf"] = doc
    pessoas, docs_iniciais = {}, {}                      # chave_nome -> {nome, documento}; CPF de quem veio em iniciais
    for nome, doc in dje["cpfs"]:
        if so_iniciais(nome):                           # 'M. G. B. D. S. (CPF: ...)': o CPF guia o polo do originário
            ini_djen.setdefault(iniciais(nome), nome)
            docs_iniciais[doc] = nome
        elif do_credor(nome):
            pessoas.setdefault(chave_nome(nome), {"nome": nome, "documento": doc})
    for nome in dje["nomes"]:
        if so_iniciais(nome):
            ini_djen.setdefault(iniciais(nome), nome)
        elif do_credor(nome) and not any(mesma_pessoa(nome, p["nome"]) or nome_cortado(nome, p["nome"])
                                         for p in pessoas.values()):
            pessoas.setdefault(chave_nome(nome), {"nome": nome, "documento": ""})
    primeiros = {}
    for n in pistas_nomes:                               # nome inteiro no texto, ou 'SABINA N. M.'
        k = iniciais(n)
        if "." in n:
            primeiros.setdefault(k, n)
        elif do_credor(n) and k in ini_djen and not any(mesma_pessoa(n, p["nome"]) or nome_cortado(n, p["nome"])
                                                          for p in pessoas.values()):
            pessoas.setdefault(chave_nome(n), {"nome": n, "documento": ""})
    for k, p in list(pessoas.items()):                  # nome cortado sem CPF de quem já veio inteiro
        if not p["documento"] and any(q is not p and nome_cortado(p["nome"], q["nome"])
                                      and len(q["nome"]) > len(p["nome"]) for q in pessoas.values()):
            pessoas.pop(k)
    if len(pessoas) > 1 and f.oabs:                     # advogado (honorários) no polo ativo sem marca de advogado
        for k, p in list(pessoas.items()):
            oabs = f.oabs.de(p["nome"])
            meu = [o for o in oabs if p["documento"] and o[2] == p["documento"]]
            df = [o for o in oabs if o[0] == "DF"]
            if meu or (not p["documento"] and df):
                uf, numero, doc = (meu or df)[0]
                a = advogado(p["nome"], uf, numero, p["documento"] or doc)
                advs.setdefault((a["oab_uf"], a["oab_numero"]), a)
                pessoas.pop(k)
    entidades = [n for n in dje["nomes"] + [d[0] for d in dje["cpfs"]] if eh_sociedade(n) or eh_orgao(n)]
    hon = next(({"nome": n, "documento": d} for n, d in dje["cpfs"] if eh_sociedade(n) and len(d) == 14), None)
    # iniciais do credor: as do DJEN tirando advogado e as de sociedade/órgão já vistas por extenso
    ini_fora = ini_adv | {iniciais(n) for n in entidades}
    alvo = {k: v for k, v in ini_djen.items() if k not in ini_fora and len(k) >= 2}
    # advogado do precatório que está no polo ativo (iniciais do DJEN ou nome no DJe): causa própria / honorários
    adv_no_polo = [a for a in advs.values() if iniciais(a["nome"]) in ini_djen
                   or any(mesma_pessoa(a["nome"], n) for n in dje["nomes"])]
    return {"pessoas": list(pessoas.values()), "iniciais": alvo, "primeiros": primeiros, "citados": citados,
            "docs_iniciais": docs_iniciais, "adv_no_polo": adv_no_polo,
            "advogados": list(advs.values()), "entidades": entidades, "honorarios": hon,
            "n_pubs": dje["n"] + len(pubs), "nomes_dje": dje["nomes"]}


def bate_iniciais(nome, alvo, primeiros):
    """O nome bate com as iniciais do DJEN (e com o primeiro nome, quando o texto o deu)."""
    if not eh_pessoa_comum(nome) or so_iniciais(nome):
        return False
    k = iniciais(nome)
    if k not in alvo:
        return False
    if k in primeiros:
        exigido = [w for w in normal(primeiros[k]).split() if len(w) > 1]
        return normal(nome).split()[:len(exigido)] == exigido
    return True


def contra_o_ente(lead, proc):
    """O devedor está no polo passivo do processo."""
    passivo = " | ".join(normal(p["nome"]) for p in proc["passivo"])
    return bool(passivo) and ((lead["termo_ente"] and lead["termo_ente"] in passivo) or bool(RE_DEVEDOR.search(passivo)))


def antes_do_precatorio(lead, proc):
    """Distribuído antes da apresentação do precatório (sem data, não descarta)."""
    try:
        return not (lead["apresentacao"] and proc["data"] and date.fromisoformat(proc["data"]) > lead["apresentacao"])
    except ValueError:
        return True


def ordem_de_abrir(lead, procs):
    """Candidatos na ordem de abrir: execução contra a Fazenda primeiro."""
    return sorted(procs, key=lambda n: (0 if RE_CLASSE_EXECUCAO.search(procs[n]["classe"] or "") else 1, n))


def abrir_candidatos(lead, f, numeros, prazo):
    abertos = []
    for n in numeros:
        if time.time() > prazo:
            raise ErroTecnico(f"TIMEOUT_PAGINA_PROCESSO: passou de {TETO_CREDITO // 60} min no crédito")
        if n == lead["numero_norm"]:
            continue
        proc = f.pje.abrir(n)
        if proc and not RE_CLASSE_FORA.search(normal(proc["classe"])):
            abertos.append(proc)
    return abertos


def resumo_candidatos(cands):
    return [{"cnj": formatar_cnj(c["proc"]["numero"]), "classe": c["proc"]["classe"][:40],
             "vara": c["proc"]["vara"][:50], "evidencia": c["ev"],
             "pessoas": [f"{p['nome']} {p['documento']}".strip() for p in c["pessoas"][:3]]} for c in cands[:8]]


def capa_do(proc, parte):
    """Capa para gravar: o processo e o credor como o PJe escreve (com o CPF do PJe)."""
    return {"proc": proc, "credor": {"nome": parte["nome"], "documento": parte["documento"],
                                     "papel_bruto": parte["tipo"] or "POLO ATIVO", "advogados": parte["advogados"]}}


def advogados_do_credor(r, parte):
    """Advogados com OAB do credor no originário que também estão no precatório (pela OAB)."""
    oabs = {(a["oab_uf"], a["oab_numero"]) for a in r["advogados_precatorio"]}
    extra = [a for a in parte["advogados"] if (a["oab_uf"], a["oab_numero"]) in oabs]
    por_oab = {(a["oab_uf"], a["oab_numero"]): a for a in r["advogados_precatorio"]}
    for a in extra:                                     # o PJe dá o CPF do advogado
        if a["cpf"] and not por_oab[(a["oab_uf"], a["oab_numero"])]["cpf"]:
            por_oab[(a["oab_uf"], a["oab_numero"])]["cpf"] = a["cpf"]
    return list(por_oab.values())


def processos_dos_advogados(lead, f, info):
    """{cnj20: processo} em que os advogados do precatório publicaram, na janela da apresentação."""
    if not lead["apresentacao"]:
        return {}
    # janela arredondada para trimestres: precatórios do mesmo advogado apresentados em datas próximas caem na mesma
    # janela e reaproveitam o cache (DJEN e DJe)
    ini = lead["apresentacao"] - timedelta(days=JANELA_ADVOGADO[0])
    fim = lead["apresentacao"] + timedelta(days=JANELA_ADVOGADO[1])
    ini = date(ini.year, 3 * ((ini.month - 1) // 3) + 1, 1)
    fim = (date(fim.year, 3 * ((fim.month - 1) // 3) + 1, 1) + timedelta(days=95)).replace(day=1) - UM_DIA
    procs = {}
    for a in info["advogados"][:MAX_ADVOGADOS]:
        fontes = [f.djen.processos_da_oab(a["oab_uf"], a["oab_numero"], ini, fim)]
        if ini.isoformat() < "2025-06-30":
            fontes.append(f.dje.processos_do_advogado(a["nome"], ini.isoformat(),
                                                      min(fim, date(2025, 6, 30)).isoformat()))
        for fonte in fontes:
            for n, p in fonte.items():
                q = procs.setdefault(n, {"classe": p["classe"], "ativo": set(), "passivo": set(), "fonte": p["fonte"]})
                q["ativo"] |= p["ativo"]
                q["passivo"] |= p["passivo"]
    return {n: p for n, p in procs.items() if n != lead["numero_norm"]
            and (not p["passivo"] or RE_DEVEDOR.search(" ".join(normal(x) for x in p["passivo"])))}


def avaliar(lead, f, abertos, achar, pular_se_unico=False):
    """Candidatos: processos contra o ente, distribuídos antes do precatório, com a(s) pessoa(s) que `achar` acha no
    polo ativo; evidência (documento citando o precatório) nos MAX_EVIDENCIA primeiros. pular_se_unico: com um
    candidato só (o credor achado pelo CPF), a evidência não muda a decisão e os documentos não são lidos."""
    cands = []
    for proc in abertos:
        if not contra_o_ente(lead, proc) or not antes_do_precatorio(lead, proc):
            continue
        pessoas = [p for p in proc["ativo"] if achar(p)]
        if pessoas:
            cands.append({"proc": proc, "pessoas": pessoas, "ev": []})
    if pular_se_unico and len(cands) == 1:
        return cands
    for c in cands[:MAX_EVIDENCIA]:
        trecho = f.pje.cita_precatorio(c["proc"], lead["prec_fmt"], lead["apresentacao"])
        if trecho:
            c["ev"].append("NUM_PRECATORIO")
            c["trecho"] = trecho
    return cands


def com_nome(lead, f, r, info, pessoa, prazo):
    """Credor com nome (e às vezes CPF) pelo DJe/DJEN: originário pelo DJe (nome) ou pelos advogados -> PJe."""
    nome, doc = pessoa["nome"], pessoa["documento"]

    def achar(p):
        if doc and p["documento"]:
            return p["documento"] == doc
        return mesma_pessoa(p["nome"], nome)

    procs = f.dje.processos_do_nome(nome)
    for c in info["citados"]:
        procs.setdefault(c, {"classe": "", "ativo": set(), "passivo": set(), "fonte": "CITADO_PELA_COORPRE"})
    via = "NOME"
    if not procs and info["advogados"]:
        procs = {n: p for n, p in processos_dos_advogados(lead, f, info).items()
                 if any(mesma_pessoa(a, nome) for a in p["ativo"])}
        via = "NOME_ADVOGADO"
    abertos = abrir_candidatos(lead, f, ordem_de_abrir(lead, procs)[:MAX_CANDIDATOS_NOME], prazo)
    cands = avaliar(lead, f, abertos, achar, pular_se_unico=bool(doc))
    r["candidatos"] = resumo_candidatos(cands)
    base = f"fonte_doc={r['fonte_documento'] or '-'} fonte_nome={r['fonte_nome'] or '-'}"
    fortes = [c for c in cands if c["ev"]]
    escolhido = fortes[0] if len(fortes) == 1 else (cands[0] if len(cands) == 1 and not fortes else None)
    if escolhido:
        parte = escolhido["pessoas"][0]
        if not doc and parte["documento"]:
            doc = parte["documento"]
            r["fonte_documento"] = "PJE_POLO_ORIGINARIO"
        r["capa"] = capa_do(escolhido["proc"], {**parte, "documento": doc})
        r["credor"] = dict(r["capa"]["credor"])
        r["advogados_precatorio"] = advogados_do_credor(r, parte)
        r.update(originario=escolhido["proc"]["numero"], via="ORIGINARIO",
                 evidencia="+".join(escolhido["ev"]) or "UNICO_PROCESSO")
        desc = (f"cnj={formatar_cnj(escolhido['proc']['numero'])} evidencia={r['evidencia']} via={via} {base}"
                f"{' | ' + escolhido['trecho'] if escolhido.get('trecho') else ''}")[:2000]
        if doc:
            r.update(status="SUCESSO_PROCESSO_ORIGINARIO", motivo=desc)
        else:
            r.update(status="SUCESSO_INCOMPLETO", motivo=f"SUCESSO_SEM_CPF: {desc}"[:2000])
        return r
    r["credor"] = {"nome": nome, "documento": doc, "papel_bruto": "", "advogados": []}
    if len(fortes) > 1 or (len(cands) > 1 and not fortes):
        lista = ", ".join(formatar_cnj(c["proc"]["numero"]) for c in (fortes or cands)[:6])
        r.update(status="SUCESSO_ANALISAR", liga_credor=bool(doc), via=via,
                 motivo=f"CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO: {len(fortes or cands)} processos do credor contra o "
                        f"ente ({lista}); {base}"[:2000])
    elif doc:
        r.update(status="SUCESSO_PROCESSO_CREDITO", liga_credor=True,
                 motivo=f"CREDOR_SEM_ORIGINARIO: nome e CPF do credor publicados no DJe do precatório; nenhum processo "
                        f"dele contra o ente achado ({len(procs)} candidato(s), {len(abertos)} aberto(s)); {base}")
    else:
        r.update(status="SUCESSO_INCOMPLETO",
                 motivo=f"SUCESSO_SEM_CPF: sem originário ({len(procs)} candidato(s), {len(abertos)} aberto(s)); {base}")
    return r


def so_iniciais_credor(lead, f, r, info, prazo):
    """Só as iniciais do credor (2022+): processos dos advogados do precatório -> polo ativo com as iniciais ->
    documento citando o precatório confirma (o CPF sai do PJe). Sem o documento: analisar (decisão do usuário)."""
    alvo, primeiros, docs = info["iniciais"], info["primeiros"], info["docs_iniciais"]

    def achar(p):
        if p["documento"] and p["documento"] in docs:
            return True
        return bate_iniciais(p["nome"], alvo, primeiros)

    procs = processos_dos_advogados(lead, f, info)
    for c in info["citados"]:
        procs.setdefault(c, {"classe": "", "ativo": set(), "passivo": set(), "fonte": "CITADO_PELA_COORPRE"})
    pri = [n for n in ordem_de_abrir(lead, procs) if any(bate_iniciais(a, alvo, primeiros) for a in procs[n]["ativo"])
           or procs[n]["fonte"] == "CITADO_PELA_COORPRE"]
    resto = [n for n in ordem_de_abrir(lead, procs) if n not in pri
             and RE_CLASSE_EXECUCAO.search(procs[n]["classe"] or "")]
    abertos = abrir_candidatos(lead, f, pri[:MAX_CANDIDATOS_INICIAIS] + resto[:MAX_SEM_PISTA], prazo)
    cands = avaliar(lead, f, abertos, achar)
    cands.sort(key=lambda c: (0 if c["ev"] else 1))
    r["candidatos"] = resumo_candidatos(cands)
    r["fonte_nome"] = "INICIAIS_DJEN"
    base = (f"iniciais={','.join(alvo.values())} advogados={len(info['advogados'])} processos_dos_advogados="
            f"{len(procs)} abertos={len(abertos)}")
    for c in cands:                                     # o CPF que o DJe publicou com as iniciais está no polo
        if any(p["documento"] in docs for p in c["pessoas"] if p["documento"]):
            c["ev"].append("CPF_DJE")
            c["pessoas"] = [p for p in c["pessoas"] if p["documento"] in docs]
    fortes = [c for c in cands if c["ev"]]
    pessoas = {}
    for c in (fortes or cands):
        for p in c["pessoas"]:
            pessoas.setdefault(p["documento"] or chave_nome(p["nome"]), (p, c))
    if fortes and len(pessoas) == 1:
        parte, c = next(iter(pessoas.values()))
        r["capa"] = capa_do(c["proc"], parte)
        r["credor"] = dict(r["capa"]["credor"])
        r["advogados_precatorio"] = advogados_do_credor(r, parte)
        r.update(originario=c["proc"]["numero"], via="ORIGINARIO", evidencia="+".join(c["ev"] + ["INICIAIS"]),
                 fonte_documento="PJE_POLO_ORIGINARIO" if parte["documento"] else "")
        desc = (f"cnj={formatar_cnj(c['proc']['numero'])} evidencia={r['evidencia']} {base}"
                f"{' | ' + c['trecho'] if c.get('trecho') else ''}")[:2000]
        if parte["documento"]:
            r.update(status="SUCESSO_PROCESSO_ORIGINARIO", motivo=desc)
        else:
            r.update(status="SUCESSO_INCOMPLETO", motivo=f"SUCESSO_SEM_CPF: {desc}"[:2000])
        return r
    candidatos = [{"nome": p["nome"], "documento": p["documento"], "originario": formatar_cnj(c["proc"]["numero"]),
                   "evidencia": "+".join(c["ev"]) or "-"} for p, c in pessoas.values()]
    if len(pessoas) == 1:
        r.update(status="SUCESSO_ANALISAR", candidato_analisar=candidatos[0],
                 motivo=f"CREDOR_SO_POR_INICIAIS: {candidatos[0]['nome']} ({candidatos[0]['originario']}) bate com as "
                        f"iniciais, mas nenhum documento do processo cita o precatório; {base}"[:2000])
    elif pessoas:
        r.update(status="SUCESSO_ANALISAR", candidato_analisar=candidatos[:10],
                 motivo=f"CANDIDATOS_POR_INICIAIS: {len(pessoas)} pessoas batem com as iniciais "
                        f"({', '.join(c['nome'] for c in candidatos[:4])}); {base}"[:2000])
    else:
        return sem_credor(lead, r, f"ninguém com as iniciais nos processos dos advogados; {base}")
    return r


def sem_credor(lead, r, detalhe):
    """Nada identificado (nem candidato para revisar): não é SUCESSO_ANALISAR (decisão do usuário, 05/10/2026). Volta
    para a fila em ADIAMENTO_SEM_CREDOR (a COORPRE segue publicando no DJEN: o nome pode aparecer); na
    MAX_TENTATIVAS_SEM_CREDOR-ésima vez vira FALHA SEM_CREDOR. O contador fica no motivo da fila ('[sem_credor=N]')."""
    n = lead["sem_credor_n"] + 1
    if n >= MAX_TENTATIVAS_SEM_CREDOR:
        r.update(status="FALHA", motivo=f"SEM_CREDOR: {detalhe} (tentativa {n}/{MAX_TENTATIVAS_SEM_CREDOR})"[:2000])
        return r
    raise Adiar(f"CREDOR_NAO_IDENTIFICADO: {detalhe}"[:1900] + f" [sem_credor={n}]", ADIAMENTO_SEM_CREDOR)


def honorarios_so(r, info):
    """Só sociedade de advogados/advogado no polo ativo do precatório (honorários): o beneficiário fica como ADVOGADO,
    sem CREDOR; SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF ... regra=HONORARIOS_ADVOGADO' (como no TJRJ e no TJRR)."""
    adv = (info.get("adv_no_polo") or [{}])[0]
    hon = info["honorarios"] or ({"nome": adv["nome"], "documento": adv.get("cpf", "")} if adv else {})
    nome = hon.get("nome") or next((n for n in info["entidades"] if eh_sociedade(n)), "")
    r["honorarios"] = {"nome": nome, "documento": hon.get("documento", "")} if nome else None
    r.update(status="SUCESSO_INCOMPLETO", regra="HONORARIOS_ADVOGADO", via="PRECATORIO",
             credor={"nome": nome, "documento": "", "papel_bruto": "HONORARIOS", "advogados": []},
             motivo=f"SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO beneficiario={nome or '-'} "
                    f"doc={'sim' if hon.get('documento') else 'nao'}")
    return r


def processar(lead, f, inicio):
    """Acha o credor e o originário. Devolve o resultado a gravar."""
    r = resultado_vazio()
    prazo = inicio + TETO_CREDITO - 15
    if lead["numero_norm"][13:16] != "807":
        r["motivo"] = f"CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO: {lead['precatorio']} não é do TJDFT"
        return r
    info = dados_do_precatorio(lead, f)
    r["advogados_precatorio"] = info["advogados"]
    r["iniciais"] = list(info["iniciais"].values())
    r["pistas"] = {"primeiros": list(info["primeiros"].values()), "citados": [formatar_cnj(c) for c in info["citados"]],
                   "nomes_dje": info["nomes_dje"][:10]}
    if info["honorarios"]:
        r["honorarios"] = dict(info["honorarios"])
    if not info["n_pubs"]:
        raise Adiar("SEM_PUBLICACAO: o precatório não tem publicação no DJe nem no DJEN", ADIAMENTO_SEM_PUBLICACAO)
    pessoas = info["pessoas"]
    orgaos = [n for n in info["entidades"] if eh_orgao(n)]
    if not pessoas and orgaos and not info["iniciais"]:
        r["motivo"] = f"REQTE_ORGAO_PUBLICO: {orgaos[0]}"
        return r
    if len(pessoas) == 1:
        r.update(fonte_nome="DJE" if pessoas[0]["nome"] in info["nomes_dje"] or pessoas[0]["documento"] else "DJEN_TEXTO",
                 fonte_documento="DJE_CERTIDAO" if pessoas[0]["documento"] else "")
        return com_nome(lead, f, r, info, pessoas[0], prazo)
    if len(pessoas) > 1:
        lista = [{"nome": p["nome"], "documento": p["documento"]} for p in pessoas[:10]]
        r.update(status="SUCESSO_ANALISAR", candidato_analisar=lista, fonte_nome="DJE",
                 motivo=f"VARIOS_CREDORES_NO_PRECATORIO: {len(pessoas)} pessoas no polo ativo "
                        f"({', '.join(p['nome'] for p in pessoas[:4])})"[:2000])
        return r
    if info["iniciais"] and (info["advogados"] or info["citados"]):
        return so_iniciais_credor(lead, f, r, info, prazo)
    if (info["entidades"] and any(eh_sociedade(n) for n in info["entidades"])) or info["adv_no_polo"]:
        return honorarios_so(r, info)
    return sem_credor(lead, r, f"publicações sem nome nem iniciais utilizáveis (iniciais={len(info['iniciais'])}, "
                               f"advogados={len(info['advogados'])})")

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
                    f.write("-- Desfaz as mudanças do fetch_TJDF.py nas tabelas antigas "
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
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJDFT",
                                "Credor do TJDFT: DJe do TJDFT + DJEN + PJe consulta pública (TJDF/fetch_TJDF.py)",
                                raspa_credor=True, criar=criar)


def recorte(r, coletiva):
    """Capa recortada do originário para o banco: só o credor confirmado, o polo passivo (como REU) e os advogados
    do credor que também estão no precatório. Gravar todos os autores faria o recálculo ligar cada um deles ao
    precatório como credor; e no processo coletivo vai sem advogados (o recálculo liga advogado da capa a todos os
    créditos do processo)."""
    proc, credor = r["capa"]["proc"], r["capa"]["credor"]
    partes = [{"nome": credor["nome"], "documento": credor["documento"], "polo": "ATIVO", "papel": "AUTOR",
               "papel_bruto": "AUTOR"}]
    partes += [{"nome": x["nome"], "documento": x["documento"] if len(x["documento"]) == 14 else "",
                "polo": "PASSIVO", "papel": "REU", "papel_bruto": "REU"} for x in proc["passivo"]]
    oabs = {(a["oab_uf"], a["oab_numero"]) for a in r["advogados_precatorio"]}
    advogados = [] if coletiva else [a for a in credor["advogados"] if (a["oab_uf"], a["oab_numero"]) in oabs]
    grau = "G2" if proc["grau"] == 2 else "G1"
    capa = {"classe_judicial": proc["classe"] or None, "orgao_julgador": proc["vara"] or None,
            "jurisdicao": proc["comarca"] or None, "assunto": proc["assunto"] or None,
            "data_autuacao": proc["data"] or None, "grau": grau, "segredo_justica": False}
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
            "classe_codigo": None, "grau": capa["grau"], "segredo_justica": False, "sistema": "PJE"}


def metadata_do_processo(cur, cnj, dados, bk):
    """O resto da capa em creditos.processo.metadata (capa_pje com fonte pje_consultapublica_tjdft; dataAjuizamento):
    só acrescenta. Backup do metadata de antes no desfazer_legado_*.sql."""
    cur.execute("SELECT id, metadata FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s) FOR UPDATE",
                (formatar_cnj(cnj),))
    linha = cur.fetchone()
    if not linha:
        return 0
    pid, antes = linha[0], linha[1] or {}
    proc = dados["proc"]
    novo = {}
    if (antes.get("capa_pje") or {}).get("fonte") in (None, "pje_consultapublica_tjdft"):
        novo["capa_pje"] = {
            "fonte": "pje_consultapublica_tjdft",
            "campos": {"numero_processo": formatar_cnj(cnj), "classe_judicial": proc["classe"],
                       "orgao_julgador": proc["vara"], "jurisdicao": proc["comarca"], "assunto": proc["assunto"],
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
    """Advogados do precatório (com OAB, do DJe/DJEN; CPF do DJe ou do PJe) e o beneficiário dos honorários (sociedade
    com CNPJ do DJe) ligados ao crédito como ADVOGADO: vale em todo lead processado, mesmo sem credor."""
    n = 0
    for a in r["advogados_precatorio"]:
        if not (a["oab_uf"] and a["oab_numero"]):
            continue
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
             "origem": "TJDFT", "tribunal_sigla": "TJDFT"}
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
              PAPEL_LEGADO[p["polo"]], "TJDFT", agora, p["papel_bruto"], POLO_BRUTO[p["polo"]])
             for p in dados["partes"] if (p["polo"], normal(p["nome"])) not in ja]
    cur.execute("SELECT * FROM originarios.advogados WHERE processo_id = %s ORDER BY id FOR UPDATE", (pid,))
    ja_oab = {(a["oab_uf"], a["oab_numero"]) for a in como_dicts(cur)}
    advs = {(a["oab_uf"], a["oab_numero"]): (pid, "ATIVO", a["nome"], a["oab_numero"], a["oab_uf"], "TJDFT", agora,
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


def atualizar_legado(cur, lead, r, status_legado, bk):
    """Status, motivo e originário nas linhas do precatório na tabela antiga processos_unificados (decisão do
    usuário); pula o que o RPA está processando."""
    cont = Counter()
    status = status_legado[r["status"]]
    originario = [formatar_cnj(r["originario"])] if r["originario"] else None
    cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead,
                           numero_originario, ultima_atualizacao
                      FROM {TABELA_LEGADO}
                     WHERE regexp_replace(numero_precatorio, '\\D', '', 'g') = %s
                       AND tribunal_origem = 'TJDFT' AND deleted IS NOT TRUE
                       FOR UPDATE""", (lead["numero_norm"],))
    for linha in como_dicts(cur):
        if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
            cont["legado_pulado"] += 1
            continue
        chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                 "tribunal_origem": "TJDFT"}
        bk.update(cur, TABELA_LEGADO, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                                    "numero_originario", "ultima_atualizacao")})
        cur.execute(f"""UPDATE {TABELA_LEGADO}
                           SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                               numero_originario = COALESCE(%s::text[], numero_originario),
                               ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                         WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJDFT'""",
                    (status, r["motivo"][:500], originario, linha["id_processo"], linha["numero_precatorio"]))
        cont["legado_atualizado"] += 1
    return cont


def registrar_metadata(cur, lead, r):
    """Resumo da decisão em credito_fonte.metadata do software (a função troca o JSON inteiro: mescla aqui e mantém a
    'lista' do carregar_TJDF.py). No SUCESSO_ANALISAR o candidato (nome, documento, originário) fica só aqui."""
    cur.execute("""SELECT f.metadata FROM creditos.credito_fonte f JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s""", (lead["credito_id"], SOFTWARE))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["motor"] = {"processado_em": datetime.now().isoformat(timespec="seconds"), "resultado": r["status"],
                     "motivo": r["motivo"], "originario": formatar_cnj(r["originario"]) if r["originario"] else None,
                     "regra": r["regra"], "evidencia": r["evidencia"], "candidatos": r["candidatos"],
                     "fonte_documento": r["fonte_documento"], "fonte_nome": r["fonte_nome"],
                     "iniciais": r["iniciais"], "pistas": r["pistas"]}
    meta["credor"] = ({"nome": r["credor"]["nome"], "papel": r["credor"]["papel_bruto"],
                       "cpf_encontrado": bool(r["credor"]["documento"])} if r["credor"] else None)
    meta["honorarios"] = r.get("honorarios")
    meta["candidato_analisar"] = r.get("candidato_analisar")
    if r["capa"]:
        proc = r["capa"]["proc"]
        meta["capa_originario"] = {"classe": proc["classe"], "vara": proc["vara"], "comarca": proc["comarca"],
                                   "data": proc["data"]}
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJDFT', p_numero => %s,
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


def gravar(cur, lead, r, status_legado, bk):
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
    legado += atualizar_legado(cur, lead, r, status_legado, bk)
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": SOFTWARE, "regra": r["regra"], "evidencia": r["evidencia"],
               "candidatos": r["candidatos"][:10], "fonte_documento": r["fonte_documento"],
               "fonte_nome": r["fonte_nome"], "credores_antes": fmt_credores(antes),
               "credores_depois": fmt_credores(depois)}
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
        item["gravado"] = gravar(cur, lead, r, rod.status_legado, rod.bk)
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
                     fonte_nome=r["fonte_nome"], iniciais=" | ".join(r["iniciais"]),
                     advogados=" | ".join(f"{a['nome']} {a['oab_uf']}{a['oab_numero']}"
                                          for a in r["advogados_precatorio"][:6]),
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
    momento. UPDATE atômico: se outra instância já o pegou, não afeta nenhuma linha.
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
                    (credito_id, TRIBUNAL_TJDFT, id_software, WORKER, lease))
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
    """Estado de uma execução: modo, conexões, arquivos de saída, backup, fontes (com a pré-carga do DJEN da COORPRE)
    e contadores. Na simulação, a amostra sai da ordem da fila e nada é reservado."""

    def __init__(self, simulacao, limite, workers, ritmo, creditos=(), faixa=None):
        SAIDA.mkdir(exist_ok=True)
        self.simulacao, self.limite, self.workers, self.so_faixa = simulacao, limite, workers, faixa
        self.modo, sufixo = ("SIMULACAO", "_simulacao") if simulacao else ("REAL", "")
        self.rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arq_credito = SAIDA / f"fetch_TJDF{sufixo}.csv"
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
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        self.con.commit()
        djen = Djen(self.parar)
        djen.carregar_coorpre()
        self.fontes = Fontes(Dje(self.parar), djen, Pje(self.parar, ritmo), OabsDoBanco())
        log.info(f"{self.modo} | worker {WORKER} | software {SOFTWARE} (id {self.id_software}) | lote de {LOTE} | "
                 f"{workers} workers | lease {self.lease} | PJe {ritmo:.1f} req/s | DJe {RITMO_DJE:.1f} req/s")
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
        """Ordem do robô (faixa NOME primeiro) e a faixa de cada crédito pegável."""
        con_l = conectar()
        try:
            ordem, self.faixas, contagem = ordenar_escopo(con_l, FILTRO_PEGAVEL, self.so_faixa)
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
        """Fecha as conexões."""
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
        log.info("fila do TJDFT vazia." if not rod.simulacao else "amostra acabou.")
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
                        (TRIBUNAL_TJDFT, SOFTWARE_RPA, rod.id_software, ADIAMENTO))
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
    """(Numa thread) Lê o crédito e decide credor e originário, sem gravar nada.
    Devolve {tipo: LOTE, linha, lead, r} ou {tipo: ADIAR, credito_id, motivo, intervalo, linha, tecnico}."""
    inicio = time.time()
    linha = {"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": rod.modo, "credito_id": credito_id}
    lead = None
    try:
        with rod.conexao_da_thread().cursor() as cur:
            lead = ler_credito(cur, credito_id)
        linha.update(precatorio=lead["precatorio"], faixa=lead["faixa"], ente=lead["ente"],
                     apresentacao=lead["apresentacao"].isoformat() if lead["apresentacao"] else "",
                     prioridade=lead["prioridade_lista"], ultimo_status=lead["ultimo_status"])
        r = processar(lead, rod.fontes, inicio)
    except Adiar as e:                                  # sem publicação/sem credor: volta daqui a dias, sem ser falha
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
    if not item["tecnico"]:
        return
    rod.falhas_seguidas += 1
    log.warning(f"{item['credito_id']} -> ADIADO: {item['motivo']}")
    if rod.falhas_seguidas >= MAX_FALHAS_SEGUIDAS:
        log.error(f"{MAX_FALHAS_SEGUIDAS} falhas técnicas seguidas: robô parado (fonte fora ou bloqueio).")
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
    log.info(f"DJEN: {f.djen.n_429} resposta(s) 429 | DJe: {f.dje.ritmo.freadas} freada(s) | "
             f"PJe: {f.pje.ritmo.freadas} freada(s), ritmo final {f.pje.ritmo.atual:.1f} req/s | chamadas ao PJe: "
             f"{sum(f.pje.chamadas.values())} ({sum(f.pje.chamadas.values()) / max(rod.n, 1):.0f}/crédito: "
             + ", ".join(f"{k} {v}" for k, v in f.pje.chamadas.most_common()) + ")")
    log.info(f"{rod.modo}: {rod.n} crédito(s) em {rod.n_lote} lote(s), {minutos:.1f} min "
             f"({rod.n / minutos:.1f}/min) | " + ", ".join(f"{k}: {v}" for k, v in rod.resultados.most_common()))
    log.info(f"CSV: {rod.arq_credito}")


def ler_argumentos():
    """--simulacao, --limite N, --workers N, --ritmo N e --creditos."""
    ap = argparse.ArgumentParser(description="Credor do TJDFT pelas fontes públicas (DJe, DJEN, PJe consulta pública), "
                                             f"gravado no banco em lotes de {LOTE}.")
    ap.add_argument("--simulacao", action="store_true",
                    help=f"{AMOSTRA_SIMULACAO} créditos (ou --limite), faz tudo e desfaz no banco (ROLLBACK)")
    ap.add_argument("--limite", type=int, default=None, help="para depois de N créditos (padrão: até a fila acabar)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"workers raspando ao mesmo tempo, como threads deste processo (padrão {WORKERS})")
    ap.add_argument("--ritmo", type=float, default=RITMO_PJE, help=f"teto de req/s no PJe (padrão {RITMO_PJE})")
    ap.add_argument("--creditos", default="",
                    help="só na simulação: ids de crédito separados por vírgula (no lugar dos primeiros da fila)")
    ap.add_argument("--faixa", choices=list(FAIXAS.values()), default=None,
                    help="só uma faixa: NOME (apresentados até 2021) ou INICIAIS (2022 em diante); padrão: as duas")
    return ap.parse_args()


def executar(args, creditos):
    """Uma rodada: mantém `workers` créditos raspando, junta os prontos no lote e grava a cada LOTE; com a fila vazia,
    espera os adiados por erro passageiro que vencem logo. Devolve True se a carga acabou (ou --limite, ou Ctrl+C) e
    False se a rodada parou por falhas técnicas seguidas ou erro inesperado (main reinicia)."""
    inicio = time.time()
    rod = Rodada(args.simulacao, args.limite, max(1, args.workers), max(RITMO_MINIMO, args.ritmo), creditos,
                 args.faixa)
    pendentes, em_voo, terminou = [], {}, False
    executor = ThreadPoolExecutor(max_workers=rod.workers, thread_name_prefix="tjdf")
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
                log.info("carga do TJDFT encerrada.")
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
