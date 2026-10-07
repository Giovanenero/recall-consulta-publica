"""
fetch_TJAL.py - enriquece os leads do TJAL pela consulta pública (substitui o modo credor do RPA_SISTEMAS).

1. Lê do banco (schema creditos) os precatórios do TJAL que estão na lista.
2. Consulta cada um na API do SAPRE e no e-SAJ (precatório e originário), sem login.
3. Atualiza só leads que já existem (nenhum crédito é criado), com a API como prioridade:
     - schema creditos: situação e originários, partes e advogados (com OAB) e status do credor;
     - filas do robô de credor (listas_primarias.processos_unificados_AAAA_MM, de 2026_08 em diante): credor e status;
     - capa antiga do precatório (precatorios.*): CPF do credor, OAB e CNPJ do devedor.
4. Mostra o resumo no log. Os CSVs de TJAL/saida/AAAAMMDD_HHMM_<modo>/ são apagados no fim da execução
   (fica só o desfazer_tabelas_antigas.sql da gravação).

Uso:
    python fetch_TJAL.py --simulacao                # todos os leads; desfaz no banco (ROLLBACK): só mostra o resumo
    python fetch_TJAL.py                            # todos os leads; GRAVA no banco
    python fetch_TJAL.py --fila --limite 500        # fila (modo 5 do RPA_SISTEMAS): grava até 500 leads da fila
    python fetch_TJAL.py --fila --limite 20 --simulacao

--fila: só os leads em FALHA/PENDENTE (os únicos cujo status o robô melhora) que ele não raspou nos últimos DIAS_FILA
dias, com número CNJ do TJAL que bate com o crédito, do mais prioritário e de maior valor para o menor. Gravando, cada
lead é reservado antes da raspagem (coleta_credor em EM_ANDAMENTO com lease deste processo, sem trocar o software, e
as linhas do precatório nas filas mensais em CREDOR_EM_ANDAMENTO com a marca do robô): o RPA com token A3 e outra
máquina não o pegam. No fim (ou no Ctrl+C) o lead que não virou SUCESSO volta ao status de antes. Se o robô cair, o
lease vence em LEASE_FILA e o banco devolve o lead como PENDENTE.

Se a raspagem for interrompida, os arquivos ficam e rodar o mesmo comando de novo continua de onde parou (a fila tem
pasta própria, *_fila-<modo>: nunca retoma nem apaga a de uma execução com todos os leads).
O valor do precatório é ignorado.

Log: terminal e TJAL/saida/logs/fetch_TJAL_AAAAMMDD.log, no formato padrão (utils/log.py).
"""
import argparse
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
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils.arquivos import gravar_csv  # noqa: E402
from utils.banco import chave_texto_lote, conectar, filas_antigas, id_do_software, partes_do_banco  # noqa: E402
from utils.legado import MARCA_RESERVA, reservar_filas_mensais, soltar_filas_mensais  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

# =============================================================================== configuração

SAIDA = AQUI / "saida"
log = logging.getLogger("fetch_TJAL")

TRIBUNAL_TJAL = 102
SOFTWARE = "CONSULTA_PUBLICA_TJAL"
GERAR_DESFAZER = False                 # True com --com-desfazer: escreve os arquivos de desfazer (o padrão é não escrever)
WORKER = "consulta_publica_tjal"                      # com --fila ganha a máquina e o processo (é o dono do lease)
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"    # filas do robô atualizadas: esta e as mais novas
DIAS_FILA = 7                                         # --fila: lead raspado há menos que isso não volta
LEASE_FILA = "3 hours"                                # --fila: reserva de cada lead (raspagem + gravação da rodada)
STATUS_LEGADO = {"SUCESSO_PROCESSO_CREDITO": "CREDOR_SUCESSO_atraves_precatorio",
                 "SUCESSO_PROCESSO_ORIGINARIO": "CREDOR_SUCESSO_atraves_originario",
                 "SUCESSO_API_TERCEIRO": "CREDOR_SUCESSO_API_TERCEIRO"}
WORKERS = 4

SAPRE = "https://precatorios.tjal.jus.br/api/sapre/precatorios"
ESAJ = "https://www2.tjal.jus.br"
TIMEOUT = 75
# O 1º grau do e-SAJ (cpopg) bloqueia o IP por ~1 min ("multiplas consultas simultâneas") se as consultas vêm a
# menos de ~1,5 s uma da outra, mesmo em sequência. O 2º grau (cposg5) não tem esse limite.
INTERVALO = {"cpopg": 1.6, "cposg5": 0}
ESPERA_BLOQUEIO = 45
BLOQUEIO = re.compile(r"multiplas consultas simult", re.I)
CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}")

# Rótulos de polo e órgãos públicos copiados do RPA_SISTEMAS (consulta_esaj.py e esaj.py), com as formas
# por extenso que aparecem no TJAL.
ROTULOS_POLO_ATIVO = ("reqte", "requerente", "credor", "favorecido", "exequente", "exeqte", "autor",
                      "apte", "apelante", "embargte", "cedente", "cessionari", "impetrante", "litsativ")
ROTULOS_POLO_PASSIVO = ("devedor", "reqdo", "requerid", "reu", "reo", "executad", "exectdo", "apdo",
                        "apelad", "embargdo", "impetrad", "litspassiv")
ORGAO_PUBLICO_CONTEM = ("procuradoria", "defensoria publica", "ministerio publico", "fazenda publica",
                        "prefeitura", "camara municipal", "assembleia legislativa", "tribunal de justica",
                        "tribunal de contas")
ORGAO_PUBLICO_PREFIXO = ("estado d", "municipio d", "uniao federal", "uniao (", "df -", "distrito federal")
PAPEIS_DE_CREDOR = ("credor", "requerente", "reqte", "autor", "exequente", "exeqte", "favorecido", "impetrante")

PAGAMENTO = re.compile(
    r"intima[cç][aã]o de pagamento|pagamento (efetuado|realizado|total)|libera[cç][aã]o dos pagamentos|"
    r"alvar[aá] (expedido|de levantamento)|levantamento de valores|quita[cç][aã]o", re.I)
LIMINAR = re.compile(r"(liminar|tutela antecipada|antecipa[cç][aã]o de tutela)\s+(deferid|concedid)", re.I)

COLUNAS_PROCESSO = [
    "credito_id", "numero_cnj", "grau", "tribunal_sigla", "origem", "classe_judicial", "assunto", "competencia",
    "orgao_julgador", "juiz_relator", "municipio", "jurisdicao", "data_autuacao", "prioridade", "segredo_justica",
    "justica_gratuita", "juizo_digital", "tutela_liminar", "cnpj_entidade_devedora", "precatorio_pago",
    "integra_disponivel", "motivo_integra_indisponivel", "precatorio_relacionado", "situacao", "etiquetas",
    "ultima_movimentacao", "qtd_movimentacoes", "ultima_data_raspagem", "url", "erro",
]
COLUNAS_PARTE = ["credito_id", "numero_cnj", "grau", "polo", "nome", "cpf_cnpj", "papel", "papel_esaj",
                 "fonte_documento"]
COLUNAS_ADVOGADO = ["credito_id", "numero_cnj", "grau", "polo", "nome", "cpf_cnpj", "oab_numero", "oab_uf", "papel"]
CAMPOS_METADATA = ("classe_judicial", "assunto", "competencia", "orgao_julgador", "juiz_relator", "municipio",
                   "jurisdicao", "data_autuacao", "prioridade", "segredo_justica", "justica_gratuita",
                   "juizo_digital", "tutela_liminar", "precatorio_pago", "integra_disponivel",
                   "motivo_integra_indisponivel", "situacao", "etiquetas", "ultima_movimentacao", "url")
COLUNAS_PARTE_ANTIGA = ("id", "processo_id", "polo", "nome", "cpf_cnpj", "papel", "origem", "data_raspagem",
                        "papel_bruto", "polo_bruto", "status")
PAPEIS_REQUERENTE = ("REQUERENTE", "CREDOR")     # papel (em maiúsculas) do credor na capa antiga


# =============================================================================== utilitários

def numero_base(numero):
    """Número CNJ sem sufixo de incidente (/01) e sem texto extra, ou None se não for CNJ."""
    m = CNJ.search(numero or "")
    return m.group(0) if m else None


def normal(t):
    """Minúsculas, sem acento."""
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().lower().strip()


def chave(t):
    """Maiúsculas, sem acento e só letras/números separados por espaço (para casar nomes de entidades)."""
    return re.sub(r"[^A-Z0-9]+", " ", normal(t).upper()).strip()


def nome_chave(nome):
    """Palavras do nome, sem parênteses e sem sufixos de empresa (SOC, LTDA, ME, EPP, SA)."""
    partes = re.sub(r"[^A-Z ]", " ", normal(re.sub(r"\(.*?\)", "", nome or "")).upper()).split()
    while partes and partes[-1] in ("SOC", "LTDA", "ME", "EPP", "SA"):
        partes.pop()
    return partes


def mesmo_nome(a, b):
    """Mesmo primeiro nome e mesmo último nome (ou nomes muito parecidos)."""
    a, b = nome_chave(a), nome_chave(b)
    if not a or not b:
        return False
    return a[0] == b[0] and (a[-1] == b[-1] or SequenceMatcher(None, " ".join(a), " ".join(b)).ratio() >= 0.85)


def mesma_pessoa_grafia(a, b):
    """Mesma pessoa escrita de outro jeito (ex.: 'DALMO PEIXOTO S. A.' x 'Dalmo Peixoto S/A - Indústria')."""
    ka, kb = nome_chave(a), nome_chave(b)
    if not ka or not kb:
        return False
    return mesmo_nome(a, b) or ka[0] == kb[0] and SequenceMatcher(None, " ".join(ka), " ".join(kb)).ratio() >= 0.6


def nome_compacto_igual(a, b):
    """Mesmo nome ignorando espaços, pontuação, 'ESPÓLIO DE' e sobras como 'REPRESENTA: ...'."""
    def compacto(s):
        s = re.split(r"REPRESENTA|ADVOGAD[OA]:|PROCURADOR", normal(s).upper())[0]
        s = re.sub(r"^ESPOLIO( DE)? ", "", s)
        return re.sub(r"[^A-Z]", "", s)
    ca, cb = compacto(a), compacto(b)
    return bool(ca and cb) and (ca in cb or cb in ca or SequenceMatcher(None, ca, cb).ratio() >= 0.85)


def orgao_publico(nome):
    """Nome de órgão público (procuradoria, estado, município...): não é credor de precatório."""
    n = normal(nome)
    return bool(n) and (any(k in n for k in ORGAO_PUBLICO_CONTEM) or n.startswith(ORGAO_PUBLICO_PREFIXO))


def sim_nao(v):
    return "SIM" if v else "NAO"


# =============================================================================== HTTP

_local = threading.local()


def requisitar(metodo, url, **kw):
    """Requisição com até 4 tentativas: os servidores do TJAL às vezes derrubam a conexão ou devolvem 5xx."""
    if not hasattr(_local, "sessao"):
        _local.sessao = requests.Session()
        _local.sessao.headers["User-Agent"] = "Mozilla/5.0"
    ultimo = None
    for tentativa in range(4):
        try:
            r = _local.sessao.request(metodo, url, timeout=TIMEOUT, **kw)
            if r.status_code >= 500 or r.status_code == 429:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            ultimo = e
            time.sleep(3 * (tentativa + 1))
    raise RuntimeError(f"indisponível ({ultimo.__class__.__name__}): {url}")


# =============================================================================== banco

# Uma linha por crédito do TJAL que está na lista (mesmo filtro da fila do credor, sem reservar nada).
SQL_LEADS = """
select
    c.id                                                        as credito_id,
    c.numero_exibicao,
    c.numero_norm,
    c.ano_orcamentario,
    sc.codigo                                                   as status_coleta,
    mc.codigo                                                   as motivo_coleta,
    cc.motivo_detalhe,
    e.nome                                                      as ente,
    (select string_agg(distinct a2.texto_exemplo, ' | ')
       from creditos.lista_item li
       join creditos.ente_alias a2 on a2.id = li.ente_alias_id
      where li.credito_id = c.id and li.removido_em is null)    as devedor_lista,
    (select string_agg(distinct pr.numero_cnj, ' | ')
       from creditos.credito_originario o
       join creditos.processo pr on pr.id = o.processo_id
      where o.credito_id = c.id)                                as originarios,
    (select string_agg(distinct p.nome, ' | ')
       from creditos.credito_credor x
       join creditos.pessoa p on p.id = x.pessoa_id
       join creditos.papel_parte pp on pp.id = x.papel_id
      where x.credito_id = c.id and pp.codigo in ('CREDOR', 'CESSIONARIO'))   as credor_nome,
    (select string_agg(distinct p.documento, ' | ')
       from creditos.credito_credor x
       join creditos.pessoa p on p.id = x.pessoa_id
       join creditos.papel_parte pp on pp.id = x.papel_id
      where x.credito_id = c.id and pp.codigo in ('CREDOR', 'CESSIONARIO')
        and p.documento is not null)                            as credor_documento,
    proc.classe_nome,
    proc.orgao_julgador,
    (select count(*) from creditos.processo_parte pa
      where pa.processo_id = c.processo_id
        and (pa.papel_bruto ilike '%%advogad%%' or pa.papel_id = 2))          as qtd_advogados,
    (select count(*) from creditos.processo_parte pa
      where pa.processo_id = c.processo_id
        and (pa.papel_bruto ilike '%%advogad%%' or pa.papel_id = 2)
        and exists (select 1 from creditos.pessoa_oab o where o.pessoa_id = pa.pessoa_id)) as qtd_advogados_com_oab,
    (select count(*) from creditos.processo_parte pa
       join creditos.pessoa p on p.id = pa.pessoa_id
      where pa.processo_id = c.processo_id and pa.polo = 'PASSIVO'
        and length(p.documento) = 14)                           as qtd_devedor_com_cnpj
from creditos.credito c
join creditos.coleta_credor cc      on cc.credito_id = c.id
join creditos.status_coleta sc      on sc.id = cc.status_id
left join creditos.motivo_coleta mc on mc.id = cc.motivo_id
left join creditos.ente_alias a     on a.id = c.ente_alias_id
left join creditos.ente e           on e.id = a.ente_id
left join creditos.processo proc    on proc.id = c.processo_id
where c.tribunal_id = %(tribunal)s
  and c.saiu_da_lista_em is null
order by c.id
"""

# --fila (modo 5 do RPA_SISTEMAS): FALHA/PENDENTE (os únicos que melhorar_status muda) fora de lease, que o robô não
# raspou nos últimos DIAS_FILA dias (credito_fonte.metadata.raspado_em, gravado por registrar_fonte). Fora também o
# que nunca ganharia raspado_em: número que não é CNJ do TJAL (processar_lead não abre capa) ou que não bate com o
# crédito (registrar_fonte pula) — senão a fila nunca esvazia. A contagem do modo 5 (main.py, _SQL_MODO5["TJAL"] no
# RPA_SISTEMAS) usa o MESMO filtro.
SQL_LEADS_FILA = SQL_LEADS.replace("order by c.id", """  and sc.codigo in ('PENDENTE', 'FALHA')
  and cc.disponivel_em <= now()
  and c.tipo_credito_id = 1
  and c.numero_exibicao ~ '[0-9]{7}-[0-9]{2}[.][0-9]{4}[.][0-9][.][0-9]{2}[.][0-9]{4}'
  and substr(c.numero_norm, 14, 3) = '802'
  and c.numero_norm = coalesce(creditos.cnj_normalizar(c.numero_exibicao), creditos.so_digitos(c.numero_exibicao))
  and not exists (select 1 from creditos.credito_fonte f
                   where f.credito_id = c.id
                     and f.software_id = (select id from creditos.software where codigo = %(software)s)
                     and f.metadata->>'raspado_em' >= to_char((now() at time zone 'America/Sao_Paulo')
                                                              - %(dias)s * interval '1 day', 'YYYY-MM-DD HH24:MI:SS'))
order by cc.prioridade, cc.valor_referencia desc nulls last, c.id
limit %(limite)s""")
assert SQL_LEADS_FILA != SQL_LEADS


def carregar_leads(fila=False, limite=None):
    """Leads do TJAL na lista (todos, ou só os da fila, até `limite`), como lista de dicts."""
    con = conectar("fetch_TJAL")
    try:
        with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL_LEADS_FILA if fila else SQL_LEADS,
                        {"tribunal": TRIBUNAL_TJAL, "software": SOFTWARE, "dias": DIAS_FILA, "limite": limite or None})
            return [dict(r) for r in cur.fetchall()]
    finally:
        con.close()


def reservar_leads(leads):
    """--fila gravando: reserva cada lead antes da raspagem (coleta_credor em EM_ANDAMENTO com lease deste processo,
    sem trocar o software; e as linhas do precatório nas filas mensais). Lead que outro processo pegou, ou que o RPA
    está processando na fila mensal, fica de fora. Devolve (leads reservados, {credito_id: status_id de antes}, filas)."""
    con = conectar("fetch_TJAL", escrita=True)
    reservados, originais, pulados = [], {}, Counter()
    try:
        with con.cursor() as cur:
            filas = filas_antigas(cur, FILA_ANTIGA_DESDE)[0]
            for lead in leads:
                cur.execute("SAVEPOINT reserva")
                cur.execute("""WITH alvo AS (
                                 SELECT cc.credito_id, cc.status_id
                                   FROM creditos.coleta_credor cc
                                   JOIN creditos.status_coleta sc ON sc.id = cc.status_id
                                  WHERE cc.credito_id = %s AND sc.codigo IN ('PENDENTE', 'FALHA')
                                    AND cc.lease_worker IS NULL AND cc.disponivel_em <= now()
                                    FOR UPDATE OF cc SKIP LOCKED)
                               UPDATE creditos.coleta_credor cc
                                  SET status_id = 2, lease_worker = %s, lease_ate = now() + %s::interval,
                                      reservado_em = now(), updated_at = now()
                                 FROM alvo
                                WHERE cc.credito_id = alvo.credito_id
                               RETURNING alvo.status_id""", (lead["credito_id"], WORKER, LEASE_FILA))
                linha = cur.fetchone()
                if not linha:
                    cur.execute("RELEASE SAVEPOINT reserva")
                    pulados["outro processo"] += 1
                    continue
                if not reservar_filas_mensais(cur, filas, lead["numero_norm"] or "", "TJAL", WORKER):
                    cur.execute("ROLLBACK TO SAVEPOINT reserva")
                    pulados["RPA processando na fila mensal"] += 1
                    continue
                cur.execute("RELEASE SAVEPOINT reserva")
                originais[lead["credito_id"]] = linha[0]
                reservados.append(lead)
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()
    log.info(f"fila: {len(reservados)} lead(s) reservados por {WORKER} (lease {LEASE_FILA})"
             + (" | fora: " + ", ".join(f"{k}: {v}" for k, v in pulados.items()) if pulados else ""))
    return reservados, originais, filas


def soltar_leads(leads, originais, filas):
    """Fim da rodada --fila (também no Ctrl+C ou erro): o lead que não virou SUCESSO (o lease ainda é deste processo)
    volta ao status de antes, FALHA ou PENDENTE, e as linhas dele nas filas mensais voltam a vazio."""
    if not originais:
        return
    con = conectar("fetch_TJAL", escrita=True)
    try:
        with con.cursor() as cur:
            cur.execute("""UPDATE creditos.coleta_credor cc
                              SET status_id = v.status_id, lease_worker = NULL, lease_ate = NULL,
                                  reservado_em = NULL, updated_at = now()
                             FROM unnest(%s::bigint[], %s::smallint[]) AS v(credito_id, status_id)
                            WHERE cc.credito_id = v.credito_id AND cc.status_id = 2 AND cc.lease_worker = %s""",
                        (list(originais), list(originais.values()), WORKER))
            devolvidos = cur.rowcount
            soltas = sum(soltar_filas_mensais(cur, filas, lead["numero_norm"] or "", "TJAL", WORKER)
                         for lead in leads if lead["credito_id"] in originais)
        con.commit()
    finally:
        con.close()
    log.info(f"fila: {devolvidos} lead(s) voltaram ao status de antes (sem SUCESSO nesta rodada); "
             f"{soltas} linha(s) das filas mensais soltas")


# =============================================================================== SAPRE (API pública)

def sapre(metodo, caminho, **kw):
    r = requisitar(metodo, f"{SAPRE}/{caminho}", **kw)
    r.raise_for_status()
    return r.json()


class MapaEntidades:
    """Nome do devedor (como está na lista do banco) -> ids de entidade principal na API, em ordem de chance."""

    def __init__(self):
        entidades = sapre("GET", "entidades")
        with ThreadPoolExecutor(WORKERS) as ex:
            devedoras = list(ex.map(lambda e: sapre("GET", f"entidades-devedoras/{e['id']}"), entidades))
        self.principais = [(chave(e["nome"]), e["id"]) for e in entidades]
        self.devedoras = [(chave(d["nome"]), e["id"]) for e, ds in zip(entidades, devedoras) for d in ds]
        self._cache = {}

    def candidatos(self, *nomes):
        variantes = []
        for nome in nomes:
            variantes.append(nome)
            # "FUNPREMA - Fundo de Previdência..." -> também tenta só a sigla antes do traço
            partes = re.split(r"\s+[-–\x96]\s+", nome or "")
            if len(partes) > 1:
                variantes.append(partes[0])
        ids = []
        for nome in variantes:
            k = chave(nome).replace("MUNICIPIO DE ", "")
            if not k:
                continue
            if k not in self._cache:
                self._cache[k] = self._procurar(k)
            ids += [i for i in self._cache[k] if i not in ids]
        return ids

    def _procurar(self, k):
        todas = self.principais + self.devedoras
        # 1) nome igual; 2) o texto aparece como palavra inteira; 3) nome parecido ("Passo de" x "PASSO DO")
        achados = [i for kk, i in todas if kk == k]
        if not achados:
            achados = [i for kk, i in todas if re.search(rf"\b{re.escape(k)}\b", kk)]
        if not achados:
            notas = sorted(((SequenceMatcher(None, k, kk).ratio(), i) for kk, i in self.principais), reverse=True)
            achados = [i for nota, i in notas[:2] if nota >= 0.8]
        return list(dict.fromkeys(achados))[:4]


def buscar_precatorio(mapa, numero, devedor_lista, ente, credor_nome):
    """Registro do precatório no SAPRE e como foi achado (pela entidade devedora ou pelo nome do credor)."""
    def mesmo_numero(reg):
        return so_digitos(reg.get("nuprocessosaj"))[:20] == so_digitos(numero)[:20]

    for id_ent in mapa.candidatos(*(devedor_lista or "").split(" | "), ente):
        j = sapre("POST", "por-entidade?page=1",
                  json={"idEntidadePrincipal": id_ent, "idEntidadeDevedora": None, "busca": numero, "cpf": None})
        for reg in j.get("data", []):
            if mesmo_numero(reg):
                return reg, f"entidade {id_ent}"
    # último recurso: nome do credor que está no banco, conferindo o número
    for nome in (credor_nome or "").split(" | ")[:2]:
        nome = re.sub(r"\s+SOC\.?$", "", nome.strip(), flags=re.I)
        if len(nome) < 8:
            continue
        j = sapre("GET", "por-nome-requerente", params={"nome_requerente": nome})
        for reg in j.get("data", []):
            if mesmo_numero(reg):
                return reg, "nome do credor"
    return None, "nao encontrado"


def resumo_sapre(reg):
    """Campos do SAPRE usados no enriquecimento (sem o valor)."""
    if not reg:
        return {}
    dev = reg.get("entidade_devedora") or {}
    return {
        "id_sapre": reg.get("id"),
        "credor_nome": (reg.get("nomerequerente") or "").strip(),
        "credor_cpf_cnpj": so_digitos(reg.get("cpfcnpjrequerente")),
        "devedor_nome": (dev.get("nome") or "").strip(),
        "devedor_cnpj": so_digitos(dev.get("cnpj")),
        "processo_conhecimento": (reg.get("numprocessoconhecimento") or "").strip(),
        "processo_execucao": (reg.get("numprocessoexecucao") or "").strip(),
        "data_cadastro": (reg.get("datacadastro") or "")[:10],
        "natureza": {0: "ALIMENTAR", 1: "COMUM"}.get(reg.get("naturezaprecatorio")),
        "comarca": (reg.get("comarca") or {}).get("nome"),
    }


# =============================================================================== e-SAJ (consulta pública)

class Ritmo:
    """Intervalo mínimo entre consultas de cada sistema do e-SAJ, compartilhado por todas as threads."""

    def __init__(self):
        self.lock = threading.Lock()
        self.proximo = {}
        self.bloqueios = 0

    def aguardar(self, sistema):
        with self.lock:
            agora = time.monotonic()
            vez = max(agora, self.proximo.get(sistema, 0))
            self.proximo[sistema] = vez + INTERVALO.get(sistema, 0)
        if vez > agora:
            time.sleep(vez - agora)

    def penalizar(self, sistema, segundos):
        with self.lock:
            self.bloqueios += 1
            self.proximo[sistema] = max(self.proximo.get(sistema, 0), time.monotonic() + segundos)


ritmo = Ritmo()


def consultar(sistema, caminho, params):
    """GET no e-SAJ respeitando o ritmo; se ele acusar consultas simultâneas, espera e repete."""
    for _ in range(6):
        ritmo.aguardar(sistema)
        r = requisitar("GET", f"{ESAJ}/{sistema}/{caminho}", params=params)
        if not BLOQUEIO.search(r.text):
            return r
        log.warning(f"e-SAJ {sistema} bloqueou por consultas simultâneas: pausa de {ESPERA_BLOQUEIO} s")
        ritmo.penalizar(sistema, ESPERA_BLOQUEIO)
    raise RuntimeError(f"e-SAJ {sistema} continua bloqueando por consultas simultâneas")


def texto(el):
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip() if el else ""


def abrir(grau, numero):
    """Abre a capa pelo número: (soup, url, None) ou (None, None, motivo)."""
    base = numero_base(numero)
    if not base:
        return None, None, "NUMERO_NAO_CNJ"
    sistema = "cposg5" if grau == 2 else "cpopg"
    if grau == 2:
        params = {"conversationId": "", "paginaConsulta": "1", "cbPesquisa": "NUMPROC", "tipoNuProcesso": "UNIFICADO",
                  "numeroDigitoAnoUnificado": base[:15], "foroNumeroUnificado": base[-4:],
                  "dePesquisaNuUnificado": base, "dePesquisa": "", "uuidCaptcha": ""}
    else:
        params = {"conversationId": "", "cbPesquisa": "NUMPROC", "numeroDigitoAnoUnificado": base[:15],
                  "foroNumeroUnificado": base[-4:], "dadosConsulta.valorConsultaNuUnificado": base,
                  "dadosConsulta.valorConsulta": "", "dadosConsulta.tipoNuProcesso": "UNIFICADO"}
    r = consultar(sistema, "search.do", params)
    sp = BeautifulSoup(r.text, "html.parser")
    if sp.find(id="numeroProcesso") is None:
        # tela "Selecione o processo" (o número tem incidentes): o primeiro da lista é o principal
        escolha = sp.find("input", attrs={"name": "processoSelecionado"})
        link = sp.select_one("a.linkProcesso, a[href*='show.do']")
        codigo = escolha["value"] if escolha is not None else None
        if codigo is None and link is not None:
            m = re.search(r"processo\.codigo=([^&]+)", link["href"])
            codigo = m.group(1) if m else None
        if codigo:
            r = consultar(sistema, "show.do", {"processo.codigo": codigo, "processo.foro": int(base[-4:])})
            sp = BeautifulSoup(r.text, "html.parser")
    if sp.find(id="numeroProcesso") is None:
        if re.search(r"segredo de justi|sigilo|senha do processo", r.text, re.I):
            return None, None, "SEGREDO_DE_JUSTICA"
        return None, None, "PROCESSO_NAO_ENCONTRADO"
    return sp, r.url, None


def rotulos(sp):
    """Campos da capa pelo rótulo visível (Origem, Outros números...)."""
    campos = {}
    for lbl in sp.select(".unj-label"):
        valor = lbl.find_next_sibling()
        if valor is not None:
            campos[texto(lbl)] = texto(valor)
    return campos


def numeros_primeira_instancia(sp):
    """Números CNJ das tabelas da seção 'Números de 1ª Instância' (até o próximo título)."""
    numeros = []
    for cab in sp.find_all(string=re.compile(r"Números de 1ª\s*[Ii]nstância")):
        for el in cab.find_parent().find_all_next(["h2", "table"]):
            if el.name == "h2":
                break
            numeros += CNJ.findall(texto(el))
    return list(dict.fromkeys(numeros))


def polo_do_rotulo(rotulo, classes_tr=""):
    if "poloAtivo" in classes_tr:
        return "ATIVO"
    if "poloPassivo" in classes_tr:
        return "PASSIVO"
    palavra = (normal(rotulo).replace(":", "").split() or [""])[0]
    if palavra.startswith(ROTULOS_POLO_ATIVO):
        return "ATIVO"
    if palavra.startswith(ROTULOS_POLO_PASSIVO) or palavra == "re":
        return "PASSIVO"
    return "OUTRO"


def partes_e_advogados(sp):
    """Todas as partes e seus representantes. O 2º grau traz a OAB num <input hidden value="6717AL">."""
    tabela = sp.find(id="tableTodasPartes") or sp.find(id="tablePartesPrincipais")
    partes, advogados = [], []
    if not tabela:
        return partes, advogados
    for tr in tabela.find_all("tr"):
        td = tr.find(class_="nomeParteEAdvogado")
        if not td:
            continue
        papel = texto(tr.find(class_="tipoDeParticipacao")).rstrip(":").strip()
        polo = polo_do_rotulo(papel, " ".join(tr.get("class", [])))
        nome, atual = [], None
        for node in td.children:
            tag = getattr(node, "name", None)
            if tag == "span" and "mensagemExibindo" in node.get("class", []):
                atual = {"polo": polo, "papel": texto(node).rstrip(":").strip(), "nome": "", "oab": ""}
                advogados.append(atual)
            elif tag == "input" and atual is not None:
                atual["oab"] = (node.get("value") or "").strip()
            elif tag in (None, "a"):
                t = (node if isinstance(node, str) else node.get_text(" ")).strip()
                if t and atual is None:
                    nome.append(t)
                elif t:
                    atual["nome"] = f"{atual['nome']} {t}".strip()
        partes.append({"polo": polo, "papel": papel, "nome": re.sub(r"\s+", " ", " ".join(nome)).strip()})
    for a in advogados:
        a["nome"] = re.sub(r"\s+", " ", a["nome"]).strip()
        m = re.fullmatch(r"0*(\d+[A-Z]?)([A-Z]{2})", a.pop("oab"))
        a["oab_numero"], a["oab_uf"] = (m.group(1), m.group(2)) if m else ("", "")
    return partes, advogados


def movimentacoes(sp):
    tab = sp.find(id="tabelaTodasMovimentacoes")
    return [texto(tr) for tr in tab.find_all("tr")] if tab else []


def extrair_capa(sp, grau, url):
    """Campos da capa com os nomes do modo credor, mais situação, etiquetas e se o precatório foi pago."""
    def g(i):
        return texto(sp.find(id=i))

    rot = rotulos(sp)
    tags = [texto(t) for t in sp.select(".unj-tag")]
    tags_n = normal(" | ".join(tags))
    movs = movimentacoes(sp)
    situacao = g("situacaoProcesso") or g("labelSituacaoProcesso") or "Em andamento"
    origem = rot.get("Origem", "")
    pago = ""
    if grau == 2:
        teve_pagamento = any(PAGAMENTO.search(m) for m in movs)
        encerrado = re.search(r"arquiv|baixad|extint", normal(situacao) + " " + normal(movs[0] if movs else ""))
        pago = "SIM" if teve_pagamento and encerrado else ("PARCIAL" if teve_pagamento else "NAO")
    distribuicao = g("dataHoraDistribuicaoProcesso")
    return {
        "numero_cnj": g("numeroProcesso").split(" ")[0],
        "grau": f"{grau}º grau",
        "tribunal_sigla": "TJAL",
        "origem": origem,
        "classe_judicial": g("classeProcesso"),
        "assunto": g("assuntoProcesso"),
        "competencia": " / ".join(x for x in (g("areaProcesso"), g("assuntoProcesso")) if x),
        "orgao_julgador": g("varaProcesso") or g("orgaoJulgadorProcesso"),
        "juiz_relator": g("juizProcesso") or g("relatorProcesso"),
        "municipio": g("foroProcesso") or (origem.split(" / ")[1] if origem.count(" / ") >= 1 else ""),
        "jurisdicao": origem.split(" / ")[0] if origem else g("foroProcesso"),
        "data_autuacao": distribuicao.split(" ")[0] if distribuicao else (movs[-1][:10] if movs else ""),
        "situacao": situacao,
        "etiquetas": " | ".join(tags),
        "prioridade": "SIM" if re.search(r"priorit|idoso|preferenc", tags_n) else "NAO",
        "segredo_justica": "NAO",
        "justica_gratuita": "SIM" if "gratuit" in tags_n else "NAO",
        "juizo_digital": "SIM" if "100%" in tags_n or "juizo digital" in tags_n else "NAO",
        "tutela_liminar": "SIM" if "liminar" in tags_n or any(LIMINAR.search(m) for m in movs) else "NAO",
        "precatorio_pago": pago,
        "integra_disponivel": "NAO",      # a Pasta Digital exige login ou senha do processo
        "motivo_integra_indisponivel": ("exige login ou senha do processo"
                                        if sp.find(id=re.compile("linkPasta|pbVisualizarAutos"))
                                        else "sem link de autos"),
        "numeros_1a_instancia": " | ".join(numeros_primeira_instancia(sp)) if grau == 2 else "",
        "ultima_movimentacao": movs[0] if movs else "",
        "qtd_movimentacoes": len(movs),
        "url": url,
    }


def raspar_capa(grau, numero):
    """Capa + partes + advogados de um processo; dict com 'erro' quando não abre."""
    sp, url, motivo = abrir(grau, numero)
    if sp is None:
        return {"erro": motivo, "numero_cnj": numero_base(numero) or numero, "grau": f"{grau}º grau",
                "segredo_justica": "SIM" if motivo == "SEGREDO_DE_JUSTICA" else ""}
    c = extrair_capa(sp, grau, url)
    c["partes"], c["advogados"] = partes_e_advogados(sp)
    return c


# =============================================================================== raspagem dos leads

def aplicar_prioridade_api(partes, nome_api, documento_api):
    """A API do SAPRE decide o credor do precatório (aplicar de novo não muda nada):
    - a parte do polo ativo que é o credor da API fica com o nome e o documento dela;
    - outro credor que o e-SAJ mostra continua como parte, mas com papel OUTRO;
    - credor da API que a capa não mostra entra como parte do polo ativo."""
    if not nome_api:
        return partes
    alvo = next((p for p in partes if p["polo"] == "ATIVO" and (
        (documento_api and p.get("cpf_cnpj") == documento_api) or mesma_pessoa_grafia(p["nome"], nome_api))), None)
    for p in partes:
        if p is alvo:
            p.update(nome=nome_api, cpf_cnpj=documento_api or p.get("cpf_cnpj", ""),
                     fonte_documento="SAPRE (prioridade)")
        elif (p["polo"] == "ATIVO" and normal(p.get("papel")).startswith(PAPEIS_DE_CREDOR)
              and p.get("fonte_documento") != "SAPRE (prioridade)"):
            # papel "OUTRO" puro: creditos.papel_do_texto lê o texto, e "Credor" em qualquer lugar voltaria a
            # fazer da pessoa credora. O rótulo original do e-SAJ fica em papel_esaj.
            p.update(papel_esaj=p["papel"], papel="OUTRO", cpf_cnpj="", fonte_documento="credor no e-SAJ, não na API")
    if alvo is None:
        base = partes[0] if partes else {}
        partes.append({**{k: base.get(k) for k in ("credito_id", "numero_cnj", "grau")}, "polo": "ATIVO",
                       "nome": nome_api, "cpf_cnpj": documento_api or "", "papel": "Credor",
                       "fonte_documento": "SAPRE (credor só na API)"})
    return partes


class CacheOriginarios:
    """Vários precatórios saem do mesmo originário (ações coletivas): cada um é raspado uma vez só."""

    def __init__(self):
        self.lock = threading.Lock()
        self.dados, self.travas = {}, {}

    def obter(self, numero):
        """(capa, veio_do_cache)."""
        with self.lock:
            if numero in self.dados:
                return self.dados[numero], True
            trava = self.travas.setdefault(numero, threading.Lock())
        with trava:
            with self.lock:
                if numero in self.dados:
                    return self.dados[numero], True
            valor = raspar_capa(1, numero)
            with self.lock:
                self.dados[numero] = valor
            return valor, False


def credor_da_capa(cap):
    """Primeira parte do polo ativo de uma capa que abriu."""
    if not cap or cap.get("erro"):
        return None
    return next((p for p in cap.get("partes", []) if p["polo"] == "ATIVO"), None)


def status_sugerido(sap, prec, orig):
    """(status, motivo) nos códigos de creditos.status_coleta / motivo_coleta; a API tem prioridade."""
    credor_prec, credor_orig = credor_da_capa(prec), credor_da_capa(orig)
    credor = credor_prec or credor_orig
    nome = sap.get("credor_nome") or (credor["nome"] if credor else "")
    if prec and prec.get("erro") == "SEGREDO_DE_JUSTICA" and not sap.get("credor_cpf_cnpj"):
        return "FALHA", "SEGREDO_DE_JUSTICA"
    if not credor and not sap.get("credor_nome"):
        return "FALHA", "PROCESSO_NAO_ENCONTRADO"
    if orgao_publico(nome):
        return "FALHA", "REQTE_ORGAO_PUBLICO"
    if not sap.get("credor_cpf_cnpj"):
        return "FALHA", "SEM_CPF_CREDOR"
    if credor_prec:
        return "SUCESSO_PROCESSO_CREDITO", None
    if credor_orig:
        return "SUCESSO_PROCESSO_ORIGINARIO", None
    return "SUCESSO_API_TERCEIRO", None        # credor e CPF só pela API (a capa não abriu no e-SAJ)


def linhas_das_capas(cid, sap, prec, orig):
    """Linhas de processos, partes e advogados (o precatório sempre vem primeiro)."""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processos, partes, advogados = [], [], []
    for cap, relacionado in ((prec, orig and orig.get("numero_cnj")), (orig, prec and prec.get("numero_cnj"))):
        if not cap:
            continue
        abriu = not cap.get("erro")
        linha = {k: cap.get(k, "") for k in COLUNAS_PROCESSO}
        linha.update(credito_id=cid, tribunal_sigla="TJAL", precatorio_relacionado=relacionado or "",
                     cnpj_entidade_devedora=sap.get("devedor_cnpj", ""), ultima_data_raspagem=agora,
                     erro=cap.get("erro", ""))
        if cap is prec and abriu:
            linha["municipio"] = linha["municipio"] or (f"Comarca de {sap['comarca']}" if sap.get("comarca") else "")
            linha["jurisdicao"] = linha["jurisdicao"] or linha["municipio"]
            linha["data_autuacao"] = linha["data_autuacao"] or sap.get("data_cadastro", "")
        processos.append(linha)

        partes_cap = []
        for p in cap.get("partes", []):
            # CPF do credor e CNPJ do devedor vêm do SAPRE
            doc = ""
            if p["polo"] == "ATIVO" and sap.get("credor_cpf_cnpj") and mesmo_nome(p["nome"], sap.get("credor_nome")):
                doc = sap["credor_cpf_cnpj"]
            elif p["polo"] == "PASSIVO" and sap.get("devedor_cnpj") and (
                    mesmo_nome(p["nome"], sap.get("devedor_nome")) or orgao_publico(p["nome"])):
                doc = sap["devedor_cnpj"]
            partes_cap.append({"credito_id": cid, "numero_cnj": cap["numero_cnj"], "grau": cap["grau"],
                               "polo": p["polo"], "nome": p["nome"], "cpf_cnpj": doc, "papel": p["papel"],
                               "fonte_documento": "SAPRE" if doc else ""})
        if cap is prec and abriu:
            partes_cap = aplicar_prioridade_api(partes_cap, sap.get("credor_nome"), sap.get("credor_cpf_cnpj"))
        partes += partes_cap
        advogados += [{"credito_id": cid, "numero_cnj": cap["numero_cnj"], "grau": cap["grau"], "polo": a["polo"],
                       "nome": a["nome"], "cpf_cnpj": "", "oab_numero": a["oab_numero"], "oab_uf": a["oab_uf"],
                       "papel": a["papel"]} for a in cap.get("advogados", [])]
    return processos, partes, advogados


def processar_lead(lead, mapa, cache):
    """Consulta SAPRE + e-SAJ para um lead e compara com o banco."""
    cid, numero = lead["credito_id"], lead["numero_exibicao"]
    t = {"t_sapre_s": 0.0, "t_esaj_precatorio_s": 0.0, "t_esaj_originario_s": 0.0}
    sap, prec, orig = {}, None, None
    orig_do_cache = precatorio_no_1g = False

    base = numero_base(numero)
    if not base:
        status, motivo = "FALHA", "NUMERO_NAO_CNJ"
    elif (lead["numero_norm"] or "")[13:16] != "802":          # J.TR do CNJ: 8.02 = TJAL
        status, motivo = "FALHA", "CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO"
    else:
        t0 = time.perf_counter()
        registro, como = buscar_precatorio(mapa, numero, lead["devedor_lista"], lead["ente"], lead["credor_nome"])
        sap = {**resumo_sapre(registro), "como": como}
        t["t_sapre_s"] = round(time.perf_counter() - t0, 2)

        # capa do precatório no 2º grau; se não existir lá, no 1º grau (precatório aberto no foro de origem)
        t0 = time.perf_counter()
        prec = raspar_capa(2, numero)
        if prec.get("erro") == "PROCESSO_NAO_ENCONTRADO":
            precatorio_no_1g = True
            prec = raspar_capa(1, numero)
        t["t_esaj_precatorio_s"] = round(time.perf_counter() - t0, 2)

        # originário: SAPRE > "Números de 1ª Instância" > banco
        candidatos = [sap.get("processo_conhecimento"), sap.get("processo_execucao"),
                      *(prec.get("numeros_1a_instancia") or "").split(" | "),
                      *[formatar_cnj(o) for o in (lead["originarios"] or "").split(" | ")]]
        numero_orig = next((numero_base(c) for c in candidatos if numero_base(c) and numero_base(c) != base), None)
        if numero_orig:
            t0 = time.perf_counter()
            orig, orig_do_cache = cache.obter(numero_orig)
            t["t_esaj_originario_s"] = round(time.perf_counter() - t0, 2)
        status, motivo = status_sugerido(sap, prec, orig)

    processos, partes, advogados = linhas_das_capas(cid, sap, prec, orig)
    credor = credor_da_capa(prec) or credor_da_capa(orig)
    nome_credor = sap.get("credor_nome") or (credor["nome"] if credor else "")

    docs_banco = [d for d in (lead["credor_documento"] or "").split(" | ") if d]
    cpf_fonte = sap.get("credor_cpf_cnpj", "")
    orig_banco = {so_digitos(o) for o in (lead["originarios"] or "").split(" | ") if o}
    orig_fonte = so_digitos(orig.get("numero_cnj")) if orig and not orig.get("erro") else ""
    oab_fonte = sum(1 for a in advogados if a["oab_numero"])
    prec_abriu = bool(prec and not prec.get("erro"))

    linha = {
        "credito_id": cid,
        "numero_precatorio": numero,
        "ente": lead["ente"],
        "devedor_lista": lead["devedor_lista"],
        "ano_orcamentario": lead["ano_orcamentario"],
        # como está no banco
        "banco_status_coleta": lead["status_coleta"],
        "banco_motivo": lead["motivo_coleta"] or "",
        "banco_credor_nome": lead["credor_nome"] or "",
        "banco_credor_documento": lead["credor_documento"] or "",
        "banco_originarios": " | ".join(formatar_cnj(o) for o in sorted(orig_banco)),
        "banco_classe": lead["classe_nome"] or "",
        "banco_orgao_julgador": lead["orgao_julgador"] or "",
        "banco_qtd_advogados": lead["qtd_advogados"],
        "banco_qtd_advogados_com_oab": lead["qtd_advogados_com_oab"],
        "banco_devedor_com_cnpj": sim_nao(lead["qtd_devedor_com_cnpj"]),
        # o que as fontes trouxeram
        "sapre_encontrado": sap.get("como", ""),
        "id_sapre": sap.get("id_sapre", ""),
        "sapre_data_cadastro": sap.get("data_cadastro", ""),
        "sapre_natureza": sap.get("natureza", ""),
        "fonte_credor_nome_sapre": sap.get("credor_nome", ""),
        "fonte_credor_cpf_cnpj": cpf_fonte,
        "fonte_credor_nome_esaj": credor["nome"] if credor else "",
        # credor que vale (prioridade da API)
        "regra_credor": "API" if sap.get("credor_nome") else ("e-SAJ" if credor else ""),
        "credor_final_nome": nome_credor,
        "credor_final_documento": cpf_fonte,
        "sapre_processo_conhecimento": sap.get("processo_conhecimento", ""),
        "sapre_processo_execucao": sap.get("processo_execucao", ""),
        "fonte_devedor": sap.get("devedor_nome", ""),
        "fonte_devedor_cnpj": sap.get("devedor_cnpj", ""),
        "fonte_originario": formatar_cnj(orig_fonte) if orig_fonte else "",
        "grau_precatorio": prec.get("grau", "") if prec_abriu else "",
        "fonte_classe": prec.get("classe_judicial", "") if prec_abriu else "",
        "fonte_orgao_julgador": prec.get("orgao_julgador", "") if prec_abriu else "",
        "fonte_situacao": prec.get("situacao", "") if prec_abriu else "",
        "precatorio_pago": prec.get("precatorio_pago", "") if prec_abriu else "",
        "prioridade": sim_nao(any(p.get("prioridade") == "SIM" for p in processos)),
        "fonte_qtd_partes": len(partes),
        "fonte_qtd_advogados": len(advogados),
        "fonte_qtd_advogados_com_oab": oab_fonte,
        # o que a raspagem acrescenta ao banco
        "preenche_cpf_credor": sim_nao(cpf_fonte and not docs_banco),
        "diverge_cpf_credor": sim_nao(cpf_fonte and docs_banco and cpf_fonte not in docs_banco),
        "diverge_nome_credor": sim_nao(sap.get("credor_nome") and lead["credor_nome"]
                                       and not any(mesmo_nome(sap["credor_nome"], n)
                                                   for n in lead["credor_nome"].split(" | "))),
        "preenche_originario": sim_nao(orig_fonte and not orig_banco),
        "diverge_originario": sim_nao(orig_fonte and orig_banco and orig_fonte not in orig_banco),
        "preenche_oab": max(0, oab_fonte - (lead["qtd_advogados_com_oab"] or 0)),
        "preenche_cnpj_devedor": sim_nao(sap.get("devedor_cnpj") and not lead["qtd_devedor_com_cnpj"]),
        "preenche_situacao": sim_nao(prec_abriu),
        "preenche_capa": sim_nao(prec_abriu and not lead["classe_nome"]),
        "status_sugerido": status,
        "motivo_sugerido": motivo or "",
        "erro": "",
        "originario_do_cache": sim_nao(orig_do_cache),
        "precatorio_buscado_no_1g": sim_nao(precatorio_no_1g),
        **t,
        "t_total_s": round(sum(t.values()), 2),
    }
    return {"credito_id": cid, "lead": linha, "processos": processos, "partes": partes, "advogados": advogados}


def processar_com_erro(lead, mapa, cache):
    """Um lead com erro não derruba a execução: vira linha de falha (e é raspado de novo na retomada)."""
    try:
        return processar_lead(lead, mapa, cache)
    except Exception as e:  # noqa: BLE001
        log.warning(f"lead {lead['credito_id']} ({lead['numero_exibicao']}) falhou: {e.__class__.__name__}: {e}",
                    exc_info=not isinstance(e, RuntimeError))    # erro inesperado leva o traceback para o log
        return {"credito_id": lead["credito_id"], "falhou": True,
                "lead": {"credito_id": lead["credito_id"], "numero_precatorio": lead["numero_exibicao"],
                         "status_sugerido": "FALHA", "motivo_sugerido": "ERRO_DESCONHECIDO",
                         "erro": f"{e.__class__.__name__}: {e}"[:300]},
                "processos": [], "partes": [], "advogados": []}


def ler_progresso(arquivo):
    """Último resultado de cada crédito no checkpoint (linha cortada por queda é ignorada)."""
    feitos = {}
    if arquivo.exists():
        for linha in arquivo.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(linha)
            except ValueError:
                continue
            feitos[r["credito_id"]] = r
    return feitos


def resumo_raspagem(leads, segundos):
    """Métricas da raspagem: status sugerido, lacunas que a raspagem preenche e tempo."""
    def contar(coluna, valor="SIM"):
        return sum(1 for lead in leads if lead.get(coluna) == valor)

    resumo = [("leads", len(leads)), ("tempo_raspagem_min", round(segundos / 60, 1))]
    resumo += sorted(Counter(f"status::{lead.get('status_sugerido')}"
                             + (f" / {lead['motivo_sugerido']}" if lead.get("motivo_sugerido") else "")
                             for lead in leads).items())
    for col in ("preenche_cpf_credor", "diverge_cpf_credor", "diverge_nome_credor", "preenche_originario",
                "diverge_originario", "preenche_cnpj_devedor", "preenche_situacao", "preenche_capa"):
        resumo.append((col, contar(col)))
    resumo += [
        ("preenche_oab (advogados)", sum(int(lead.get("preenche_oab") or 0) for lead in leads)),
        ("precatorio_pago=SIM", contar("precatorio_pago")),
        ("precatorio_pago=PARCIAL", contar("precatorio_pago", "PARCIAL")),
        ("originarios_reaproveitados_do_cache", contar("originario_do_cache")),
        ("bloqueios_esaj_1g (consultas simultaneas)", ritmo.bloqueios),
    ]
    return resumo


def raspar_leads(pasta, leads):
    """Raspa os leads em paralelo com checkpoint em _progresso.jsonl (retoma o que já está lá).
    Devolve (resultados na ordem dos leads, resumo, completou)."""
    pasta.mkdir(parents=True, exist_ok=True)
    progresso = pasta / "_progresso.jsonl"
    feitos = ler_progresso(progresso)
    pendentes = [lead for lead in leads
                 if lead["credito_id"] not in feitos or feitos[lead["credito_id"]].get("falhou")]
    log.info(f"{len(pendentes)} leads a raspar ({len(leads) - len(pendentes)} já feitos) -> {pasta}")

    inicio = time.time()
    completou = True
    if pendentes:
        mapa, cache = MapaEntidades(), CacheOriginarios()
        ex = ThreadPoolExecutor(WORKERS)
        try:
            with progresso.open("a", encoding="utf-8") as f:
                futuros = {ex.submit(processar_com_erro, lead, mapa, cache) for lead in pendentes}
                n = 0
                while futuros:
                    # espera com timeout: no Windows, a espera sem timeout não atende o Ctrl+C
                    prontos, futuros = wait(futuros, timeout=2, return_when=FIRST_COMPLETED)
                    for fut in prontos:
                        r = fut.result()
                        feitos[r["credito_id"]] = r
                        f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                        n += 1
                        if n % 100 == 0 or n == len(pendentes):
                            por_hora = n / (time.time() - inicio) * 3600
                            falta = (len(pendentes) - n) / por_hora * 60
                            log.info(f"raspagem {n}/{len(pendentes)} | {por_hora:.0f} leads/h | "
                                     f"faltam ~{falta:.0f} min")
                    f.flush()
        except KeyboardInterrupt:
            completou = False
        finally:
            ex.shutdown(wait=True, cancel_futures=True)

    resultados = [feitos[lead["credito_id"]] for lead in leads if lead["credito_id"] in feitos]
    if not completou:
        return resultados, [], False
    linhas = [r["lead"] for r in resultados]
    gravar_csv(pasta / "processos.csv", [p for r in resultados for p in r["processos"]], COLUNAS_PROCESSO)
    gravar_csv(pasta / "partes.csv", [p for r in resultados for p in r["partes"]], COLUNAS_PARTE)
    gravar_csv(pasta / "advogados.csv", [a for r in resultados for a in r["advogados"]], COLUNAS_ADVOGADO)
    gravar_csv(pasta / "leads_enriquecimento.csv", linhas)      # por último: marca a raspagem como completa
    return resultados, resumo_raspagem(linhas, time.time() - inicio), True


# =============================================================================== atualização do banco

def doc_api(lead):
    """CPF/CNPJ do credor pela API, só se o dígito verificador estiver certo (inválido conta como ausente)."""
    d = lead.get("fonte_credor_cpf_cnpj") or ""
    return d if documento_valido(d) else ""


def credor_orgao_publico(lead):
    return lead.get("motivo_sugerido") == "REQTE_ORGAO_PUBLICO"


def texto_situacao(lead):
    """Texto gravado em credito_fonte.situacao (vira credito_situacao pelo de-para do software)."""
    pago = lead.get("precatorio_pago")
    situacao = (lead.get("fonte_situacao") or "").lower()
    if pago == "SIM":
        return "PAGO"
    if pago == "PARCIAL":
        return "PAGO_PARCIAL"
    if re.search(r"arquiv|baixad|extint", situacao):
        return "ARQUIVADO"
    if "suspens" in situacao:
        return "SUSPENSO"
    return "EM_ANDAMENTO" if situacao else None


def precatorio_cnj(r):
    """A 1ª capa da raspagem é sempre a do precatório."""
    return r["processos"][0]["numero_cnj"] if r["processos"] else None


def metadata(r):
    """Dados da capa sem coluna própria no schema, guardados em credito_fonte.metadata."""
    lead = r["lead"]
    capas = {}
    for p in r["processos"]:
        if not p.get("erro"):
            capas["precatorio" if p["numero_cnj"] == precatorio_cnj(r) else "originario"] = {
                "numero_cnj": p["numero_cnj"], "grau": p["grau"], **{k: p.get(k) for k in CAMPOS_METADATA if p.get(k)}}
    return {
        "fonte": "SAPRE + e-SAJ (consulta pública, sem login)",
        "raspado_em": next((p.get("ultima_data_raspagem") for p in r["processos"] if p.get("ultima_data_raspagem")),
                           None),
        "id_sapre": lead.get("id_sapre") or None,
        "sapre_data_cadastro": lead.get("sapre_data_cadastro") or None,
        "sapre_natureza": lead.get("sapre_natureza") or None,
        "status_sugerido": lead.get("status_sugerido"),
        "motivo_sugerido": lead.get("motivo_sugerido") or None,
        **capas,
    }


def garantir_software(cur):
    """Cadastra o software CONSULTA_PUBLICA_TJAL se ainda não existir (raspa_credor=false: não cria fila própria)."""
    id_do_software(cur, SOFTWARE, "Consulta pública do TJAL",
                   "Enriquecimento pela API do SAPRE e pelo e-SAJ público (TJAL/fetch_TJAL.py)", raspa_credor=False)


def registrar_fonte(cur, r):
    """credito_fonte (situação + metadata) e originários, sem nunca criar crédito."""
    lead = r["lead"]
    cid, numero = lead["credito_id"], lead["numero_precatorio"]
    cur.execute("""SELECT 1 FROM creditos.credito c
                    WHERE c.id = %s AND c.tipo_credito_id = 1 AND c.tribunal_id = %s
                      AND c.numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (cid, TRIBUNAL_TJAL, numero, numero))
    if not cur.fetchone():
        return "PULADO", "número não bate com o crédito do banco (a função criaria outro crédito)"
    # prioridade da API: conhecimento e execução do SAPRE, mais o originário que a raspagem abriu
    candidatos = (lead.get("sapre_processo_conhecimento"), lead.get("sapre_processo_execucao"),
                  lead.get("fonte_originario"))
    originarios = list(dict.fromkeys(b for b in map(numero_base, candidatos) if b and b != numero_base(numero)))
    cur.execute("""SELECT creditos.registrar_credito(
                     p_tipo_credito => 'PRECATORIO', p_tribunal => 'TJAL', p_numero => %s, p_origem => 'RASPAGEM',
                     p_originarios => %s, p_software => %s, p_etapa => %s, p_situacao => %s, p_metadata => %s)""",
                (numero, originarios or None, SOFTWARE, lead.get("status_sugerido"), texto_situacao(lead),
                 json.dumps(metadata(r), ensure_ascii=False, default=str)))
    devolvido = cur.fetchone()[0]
    if devolvido != cid:
        raise RuntimeError(f"registrar_credito devolveu o crédito {devolvido}, esperado {cid}: lead desfeito")
    return "FEITO", f"situacao={texto_situacao(lead)} originarios={' | '.join(originarios) or '-'}"


def juntar_com_banco(cur, numero_cnj, partes, advogados, credor_api):
    """registrar_capa substitui o conjunto de partes, então a lista final não pode perder o que o banco tem:
    - CPF/OAB faltantes vêm do banco (mesmo nome no mesmo processo);
    - parte do banco que a raspagem não trouxe volta, exceto o credor que a API desmente (só no precatório)."""
    existentes = partes_do_banco(cur, numero_cnj)
    doc_por_nome = {e["chave"]: e["documento"] for e in existentes if e["chave"] and e["documento"]}
    oab_por_nome = {e["chave"]: (e["oab_uf"], e["oab_numero"]) for e in existentes
                    if e["chave"] and e["oab_uf"] and e["oab_numero"]}
    chaves = chave_texto_lote(cur, [x["nome"] for x in partes + advogados])
    completados = 0
    for item, ch in zip(partes + advogados, chaves):
        if not item.get("cpf_cnpj") and ch in doc_por_nome and item.get("papel") != "OUTRO":
            item["cpf_cnpj"] = doc_por_nome[ch]
            completados += 1
        if "oab_numero" in item and not item.get("oab_numero") and ch in oab_por_nome:
            item["oab_uf"], item["oab_numero"] = oab_por_nome[ch]
            completados += 1

    nossas, mantidas = set(chaves), 0
    for e in existentes:
        if not e["chave"] or e["chave"] in nossas:
            continue
        if e["papel"] == "ADVOGADO":
            advogados.append({"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"],
                              "oab_uf": e["oab_uf"], "oab_numero": e["oab_numero"], "papel_bruto": e["papel_bruto"]})
        elif credor_api and e["polo"] == "ATIVO" and e["papel"] == "CREDOR":
            # no precatório a API decide o credor: o do banco já está na lista com o nome da API ou foi desmentido
            continue
        else:
            partes.append({"nome": e["nome"], "cpf_cnpj": e["documento"], "polo": e["polo"],
                           "papel": e["papel"], "papel_bruto": e["papel_bruto"]})
        nossas.add(e["chave"])
        mantidas += 1
    return partes, advogados, completados, mantidas


def registrar_capa(cur, r, cap, eh_precatorio):
    """Partes e advogados (com OAB) de uma capa via creditos.registrar_capa."""
    lead = r["lead"]
    cnj = cap["numero_cnj"]
    if eh_precatorio and credor_orgao_publico(lead):
        return "PULADO", "credor é órgão público: credor do precatório não é alterado"
    linhas = [dict(p) for p in r["partes"] if p["numero_cnj"] == cnj and p.get("nome")]
    credor_api = lead.get("fonte_credor_nome_sapre") if eh_precatorio else None
    if eh_precatorio:
        # documento inválido da API não entra: a parte fica sem documento (ou com o que o banco já tem)
        linhas = aplicar_prioridade_api(linhas, credor_api, doc_api(lead))
        if not doc_api(lead):
            for p in linhas:
                if p.get("cpf_cnpj") and not documento_valido(p["cpf_cnpj"]):
                    p["cpf_cnpj"] = ""
    # papel vai puro para o schema decidir o papel_id; o rótulo original do e-SAJ vai em papel_bruto
    partes = [{"nome": p["nome"], "cpf_cnpj": p.get("cpf_cnpj") or None, "polo": p["polo"],
               "papel": p["papel"], "papel_bruto": p.get("papel_esaj") or p["papel"]} for p in linhas]
    # só advogados do lado do credor: o procurador do ente viraria "advogado do lead" nas campanhas
    advogados = [{"nome": a["nome"], "cpf_cnpj": None, "polo": a["polo"], "oab_uf": a.get("oab_uf") or None,
                  "oab_numero": a.get("oab_numero") or None, "papel_bruto": a.get("papel")}
                 for a in r["advogados"] if a["numero_cnj"] == cnj and a.get("nome") and a["polo"] != "PASSIVO"]
    if not partes:
        return "PULADO", "capa sem partes"
    partes, advogados, completados, mantidas = juntar_com_banco(cur, cnj, partes, advogados, credor_api)
    dados_capa = {"orgao_julgador": cap.get("orgao_julgador") or None,
                  "classe_judicial": cap.get("classe_judicial") or None,
                  "segredo_justica": cap.get("segredo_justica") == "SIM",
                  "grau": "G2" if cap.get("grau", "").startswith("2") else "G1", "sistema": "ESAJ"}
    cur.execute("SELECT creditos.registrar_capa(%s, %s::jsonb, %s::jsonb, %s::jsonb)",
                (cnj, json.dumps(partes, ensure_ascii=False), json.dumps(advogados, ensure_ascii=False),
                 json.dumps(dados_capa, ensure_ascii=False)))
    res = cur.fetchone()[0]
    return "FEITO", (f"partes +{res['partes_inseridas']}/-{res['partes_removidas']} "
                     f"credores +{res['credores_inseridos']}/-{res['credores_removidos']} "
                     f"completados_do_banco={completados} mantidas_do_banco={mantidas}")


def melhorar_status(cur, r):
    """Status do credor no schema novo: só FALHA/PENDENTE -> SUCESSO, e só com documento válido da API."""
    lead = r["lead"]
    if lead.get("banco_status_coleta") not in ("FALHA", "PENDENTE"):
        return "PULADO", f"banco já está {lead.get('banco_status_coleta')}"
    if not (lead.get("status_sugerido") or "").startswith("SUCESSO"):
        return "PULADO", "raspagem não achou o credor com CPF"
    if not doc_api(lead):
        return "PULADO", "CPF/CNPJ da API inválido"
    via = {"SUCESSO_PROCESSO_ORIGINARIO": "ORIGINARIO",
           "SUCESSO_API_TERCEIRO": "CONSULTA_PUBLICA"}.get(lead["status_sugerido"], "PROCESSO_CREDITO")
    cnj = lead.get("fonte_originario") if via == "ORIGINARIO" else precatorio_cnj(r)
    cur.execute("""SELECT creditos.fila_credor_finalizar(
                     p_credito_id => %s, p_worker => %s, p_status => %s, p_via => %s, p_sistema => 'ESAJ',
                     p_processo_cnj => %s, p_detalhe => %s)""",
                (lead["credito_id"], WORKER, lead["status_sugerido"], via, cnj,
                 json.dumps({"software": SOFTWARE, "id_sapre": lead.get("id_sapre") or None})))
    return "FEITO", f"{lead['banco_status_coleta']} -> {lead['status_sugerido']}"


def atualizar_fila_antiga(cur, r, fila):
    """Credor da API e status nas linhas do precatório numa fila do robô. (resultado, detalhe, backup)"""
    lead = r["lead"]
    credor_api = lead.get("fonte_credor_nome_sapre")
    if not credor_api:
        return "PULADO", "API sem credor para este precatório", []
    if credor_orgao_publico(lead):
        return "PULADO", "credor é órgão público", []
    status_novo = STATUS_LEGADO.get(lead.get("status_sugerido") or "") if doc_api(lead) else None
    # 20 dígitos do CNJ pelo índice pu_AAAA_MM_numprec_lpad_idx: acha também as linhas com sufixo ("…9003/2")
    digitos = so_digitos(lead["numero_precatorio"])[:20].rjust(20, "0")
    cur.execute(f"""SELECT id_processo, numero_precatorio, requerentes::text, status_coleta_lead, motivo_coleta_lead,
                           ultima_atualizacao
                      FROM {fila}
                     WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                       AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJAL'
                     FOR UPDATE""", (digitos,))
    linhas = cur.fetchall()
    if not linhas:
        return "PULADO", "precatório não está nesta fila", []
    requerentes_api = json.dumps([{"nome": credor_api, "tipo": "polo_ativo"}], ensure_ascii=False)
    backup, em_andamento = [], 0
    for id_processo, numero_linha, req_antes, status_antes, motivo_antes, atualizado_antes in linhas:
        reservada = status_antes == "CREDOR_EM_ANDAMENTO" and (motivo_antes or "").startswith(MARCA_RESERVA)
        if status_antes == "CREDOR_EM_ANDAMENTO" and not reservada:   # o RPA está trabalhando nesta linha
            em_andamento += 1
            continue
        if reservada:              # reserva deste robô (--fila): antes dela a linha estava vazia, e é isso que vale
            status_antes = motivo_antes = None
        status_depois, motivo_depois = status_antes, motivo_antes
        if status_novo and (status_antes is None or status_antes.startswith("CREDOR_FALHA")):
            status_depois, motivo_depois = status_novo, f"{WORKER} cnj={precatorio_cnj(r)}"
        # o credor só é regravado se estava vazio ou era OUTRA pessoa (a mesma pessoa com outra grafia fica)
        try:
            nomes_antes = [x.get("nome") for x in (json.loads(req_antes) or []) if isinstance(x, dict)]
        except (TypeError, ValueError):
            nomes_antes = []
        mesma_pessoa = any(mesma_pessoa_grafia(n, credor_api) for n in nomes_antes if n)
        requerentes_depois = req_antes if mesma_pessoa else requerentes_api
        if requerentes_depois == req_antes and status_depois == status_antes:
            continue
        cur.execute(f"""UPDATE {fila}
                           SET requerentes = %s::jsonb, status_coleta_lead = %s, motivo_coleta_lead = %s,
                               ultima_atualizacao = now()
                         WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJAL'""",
                    (requerentes_depois, status_depois, motivo_depois, id_processo, numero_linha))
        backup.append({"tabela": fila, "id_processo": id_processo, "credito_id": lead["credito_id"],
                       "numero_precatorio": numero_linha, "requerentes_antes": req_antes,
                       "status_antes": status_antes, "motivo_antes": motivo_antes,
                       "ultima_atualizacao_antes": atualizado_antes, "requerentes_depois": requerentes_depois,
                       "status_depois": status_depois, "motivo_depois": motivo_depois})
    detalhe = f"{len(backup)} de {len(linhas)} linhas"
    if em_andamento:
        detalhe += f" ({em_andamento} em andamento no robô, não tocadas)"
    return ("FEITO" if backup else "PULADO"), detalhe, backup


def atualizar_capa_antiga(cur, r):
    """Credor (nome + CPF da API), OAB e CNPJ do devedor na capa antiga do precatório. (resultado, detalhe, backup)
    Sem isso, o espelho das capas antigas traria o credor errado de volta ao schema creditos."""
    lead = r["lead"]
    credor_api, doc = lead.get("fonte_credor_nome_sapre"), doc_api(lead)
    if not credor_api or not lead.get("fonte_credor_cpf_cnpj"):
        return "PULADO", "API sem credor com CPF/CNPJ", []
    if not doc:
        return "PULADO", "CPF/CNPJ da API inválido", []
    if credor_orgao_publico(lead):
        return "PULADO", "credor é órgão público", []
    numero = lead["numero_precatorio"]
    cur.execute("""SELECT id, cnpj_entidade_devedora FROM precatorios.processos_precatorios
                    WHERE numero_cnj IN (%s, %s) FOR UPDATE""", (numero, so_digitos(numero)))
    linha = cur.fetchone()
    if not linha:
        return "PULADO", "precatório sem capa na base antiga (não é criada)", []
    pid, cnpj_antes = linha
    ops = []

    def registrar(op, tabela, linha_id, antes, depois=None):
        ops.append({"op": op, "tabela": tabela, "id": linha_id, "credito_id": lead["credito_id"],
                    "numero_precatorio": numero, "antes": json.dumps(antes, ensure_ascii=False, default=str),
                    "depois": json.dumps(depois, ensure_ascii=False, default=str)})

    # 1) credor
    cur.execute(f"SELECT {', '.join(COLUNAS_PARTE_ANTIGA)} FROM precatorios.partes_processuais "
                "WHERE processo_id = %s AND polo = 'ATIVO' AND upper(papel) = ANY(%s) ORDER BY id FOR UPDATE",
                (pid, list(PAPEIS_REQUERENTE)))
    requerentes = [dict(zip(COLUNAS_PARTE_ANTIGA, x)) for x in cur.fetchall()]
    alvo = (next((q for q in requerentes if so_digitos(q["cpf_cnpj"]) == doc), None)
            or next((q for q in requerentes if mesma_pessoa_grafia(q["nome"], credor_api)), None)
            or (requerentes[0] if requerentes else None))
    credor_msg = "credor mantido"
    if alvo is None:
        cur.execute("""INSERT INTO precatorios.partes_processuais
                         (processo_id, polo, nome, cpf_cnpj, papel, origem, data_raspagem, papel_bruto, polo_bruto)
                       VALUES (%s, 'ATIVO', %s, %s, 'REQUERENTE', 'TJAL', now(), 'REQUERENTE', 'AUTOR') RETURNING id""",
                    (pid, credor_api, doc))
        registrar("INSERT", "precatorios.partes_processuais", cur.fetchone()[0], None,
                  {"nome": credor_api, "cpf_cnpj": doc})
        credor_msg = "credor da API incluído"
    else:
        # só muda o que está errado: CPF diferente/vazio, e o nome só quando é outra pessoa (grafia não conta)
        nome_depois = alvo["nome"] if mesma_pessoa_grafia(alvo["nome"], credor_api) else credor_api
        if (nome_depois, doc) != (alvo["nome"], so_digitos(alvo["cpf_cnpj"])):
            cur.execute("UPDATE precatorios.partes_processuais SET nome = %s, cpf_cnpj = %s, data_raspagem = now() "
                        "WHERE id = %s", (nome_depois, doc, alvo["id"]))
            registrar("UPDATE", "precatorios.partes_processuais", alvo["id"],
                      {"nome": alvo["nome"], "cpf_cnpj": alvo["cpf_cnpj"], "data_raspagem": alvo["data_raspagem"]},
                      {"nome": nome_depois, "cpf_cnpj": doc})
            credor_msg = f"credor: {alvo['nome']} ({alvo['cpf_cnpj'] or 'sem CPF'}) -> {nome_depois} ({doc})"
    removidos = [q for q in requerentes if alvo is not None and q["id"] != alvo["id"]]
    for q in removidos:                                   # outro "requerente" que a API desmente
        cur.execute("DELETE FROM precatorios.partes_processuais WHERE id = %s", (q["id"],))
        registrar("DELETE", "precatorios.partes_processuais", q["id"], q)

    # 2) OAB dos advogados (só onde está vazia, pelo nome)
    oabs = {normal(a["nome"]): (a["oab_numero"], a["oab_uf"]) for a in r["advogados"]
            if a.get("oab_numero") and a["grau"].startswith("2")}
    n_oab = 0
    if oabs:
        cur.execute("""SELECT id, nome, oab_numero, oab_uf FROM precatorios.advogados
                        WHERE processo_id = %s AND COALESCE(oab_numero, '') = '' FOR UPDATE""", (pid,))
        for adv_id, nome, oab_antes, uf_antes in cur.fetchall():
            achado = oabs.get(normal(nome))
            if achado:
                cur.execute("UPDATE precatorios.advogados SET oab_numero = %s, oab_uf = %s WHERE id = %s",
                            (achado[0], achado[1], adv_id))
                registrar("UPDATE", "precatorios.advogados", adv_id, {"oab_numero": oab_antes, "oab_uf": uf_antes},
                          {"oab_numero": achado[0], "oab_uf": achado[1]})
                n_oab += 1

    # 3) CNPJ do devedor (só se vazio)
    cnpj = lead.get("fonte_devedor_cnpj")
    if cnpj and not cnpj_antes:
        cur.execute("UPDATE precatorios.processos_precatorios SET cnpj_entidade_devedora = %s WHERE id = %s",
                    (cnpj, pid))
        registrar("UPDATE", "precatorios.processos_precatorios", pid, {"cnpj_entidade_devedora": cnpj_antes},
                  {"cnpj_entidade_devedora": cnpj})

    detalhe = (f"{credor_msg}; requerentes removidos={len(removidos)}; OAB={n_oab}; "
               f"cnpj_devedor={sim_nao(cnpj and not cnpj_antes)}")
    return ("FEITO" if ops else "PULADO"), detalhe, ops


def sql_para_desfazer(backup_fila, backup_capa):
    """SQL que devolve as tabelas antigas ao estado anterior (na ordem inversa)."""
    def lit(v):
        return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"

    linhas = ["-- Desfaz as alterações do fetch_TJAL.py nas tabelas antigas.", "BEGIN;"]
    for b in reversed(backup_capa):
        antes = json.loads(b["antes"])
        if b["op"] == "INSERT":
            linhas.append(f"DELETE FROM {b['tabela']} WHERE id = {b['id']};")
        elif b["op"] == "UPDATE":
            sets = ", ".join(f"{k} = {lit(v)}" for k, v in antes.items())
            linhas.append(f"UPDATE {b['tabela']} SET {sets} WHERE id = {b['id']};")
        elif b["op"] == "DELETE":
            linhas.append(f"INSERT INTO {b['tabela']} ({', '.join(antes)}) "
                          f"VALUES ({', '.join(lit(v) for v in antes.values())});")
    for b in backup_fila:
        linhas.append(
            f"UPDATE {b['tabela']} SET requerentes = {lit(b['requerentes_antes'])}::jsonb, "
            f"status_coleta_lead = {lit(b['status_antes'])}, motivo_coleta_lead = {lit(b['motivo_antes'])}, "
            f"ultima_atualizacao = {lit(b['ultima_atualizacao_antes'])}::timestamp "
            f"WHERE id_processo = {b['id_processo']} AND numero_precatorio = {lit(b['numero_precatorio'])} "
            f"AND tribunal_origem = 'TJAL';")
    linhas.append("COMMIT;")
    return "\n".join(linhas) + "\n"


def atualizar_lead(con, r, gravar, filas, capa_antiga):
    """Todos os passos de um lead numa transação: COMMIT se gravar, senão ROLLBACK.
    Devolve (acoes, trocas, backup_fila, backup_capa, revisao)."""
    lead = r["lead"]
    cid, numero = lead["credito_id"], lead.get("numero_precatorio")
    acoes, trocas, backup_fila, backup_capa, revisao = [], [], [], [], []
    cur = None

    def revisar(tipo, detalhe=""):
        revisao.append({"tipo": tipo, "credito_id": cid, "numero_precatorio": numero, "detalhe": detalhe,
                        "credor_da_api": lead.get("fonte_credor_nome_sapre"),
                        "cpf_cnpj_da_api": lead.get("fonte_credor_cpf_cnpj"),
                        "credor_no_esaj": lead.get("fonte_credor_nome_esaj"),
                        "credor_no_banco": lead.get("banco_credor_nome"),
                        "documento_no_banco": lead.get("banco_credor_documento")})

    def acao(nome, resultado, detalhe=""):
        if resultado == "FEITO" and not gravar:
            resultado = "SIMULADO"
        acoes.append({"credito_id": cid, "numero_precatorio": numero, "acao": nome, "resultado": resultado,
                      "detalhe": detalhe})
        if resultado == "ERRO":
            revisar("ERRO", f"{nome}: {detalhe}")

    def passo(nome, funcao, *args, se_erro="ERRO"):
        """Roda um passo num savepoint (erro do Postgres desfaz só ele). Devolve o backup do passo, se houver."""
        cur.execute("SAVEPOINT passo")
        try:
            resultado, detalhe, *backup = funcao(cur, *args)
        except psycopg2.Error as e:
            cur.execute("ROLLBACK TO SAVEPOINT passo")
            acao(nome, se_erro, str(e).split("\n")[0][:200])
            return []
        cur.execute("RELEASE SAVEPOINT passo")
        acao(nome, resultado, detalhe)
        return backup[0] if backup else []

    def credores():
        cur.execute("""SELECT pp.codigo, p.nome, COALESCE(p.documento::text, '')
                         FROM creditos.credito_credor x
                         JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                         JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                        WHERE x.credito_id = %s AND pp.codigo IN ('CREDOR', 'CESSIONARIO')""", (cid,))
        return set(cur.fetchall())

    def comparar_credores(antes, depois):
        def fmt(s):
            return " | ".join(f"{nome} ({doc or 'sem doc'}, {papel})" for papel, nome, doc in sorted(s))
        sairam, entraram, ficaram = antes - depois, depois - antes, antes & depois
        if entraram and ficaram:
            # entrou o credor da API, mas outro continua (vínculo da compra de contatos, que o schema protege)
            revisar("DOIS_CREDORES", fmt(ficaram))
        if sairam or entraram:
            trocas.append({"credito_id": cid, "numero_precatorio": numero, "credores_que_saem": fmt(sairam),
                           "credores_que_entram": fmt(entraram), "credores_que_ficam": fmt(ficaram),
                           "credor_da_api": lead.get("fonte_credor_nome_sapre"),
                           "cpf_cnpj_da_api": lead.get("fonte_credor_cpf_cnpj"),
                           "credor_no_esaj": lead.get("fonte_credor_nome_esaj"),
                           "gravado": "SIM" if gravar else "SIMULADO"})

    if r.get("falhou") or not r["processos"]:
        acao("lead", "PULADO", f"sem dados da raspagem ({lead.get('motivo_sugerido') or lead.get('erro')})")
        return acoes, trocas, backup_fila, backup_capa, revisao

    # casos para conferência (não impedem a atualização)
    api, esaj, doc = lead.get("fonte_credor_nome_sapre"), lead.get("fonte_credor_nome_esaj"), doc_api(lead)
    if credor_orgao_publico(lead):
        revisar("CREDOR_ORGAO_PUBLICO", "credor do precatório não alterado")
    if lead.get("fonte_credor_cpf_cnpj") and not doc:
        revisar("DOCUMENTO_INVALIDO_API", "CPF/CNPJ da API com dígito verificador errado: não gravado")
    if api and esaj and not mesma_pessoa_grafia(api, esaj):
        revisar("API_E_ESAJ_DISCORDAM", "vale a API (prioridade)")

    cur = con.cursor()
    try:
        if doc and not credor_orgao_publico(lead):
            cur.execute("SELECT nome FROM creditos.pessoa WHERE documento = %s", (doc,))
            cadastro = cur.fetchone()
            if cadastro and cadastro[0] and not (mesma_pessoa_grafia(cadastro[0], api)
                                                 or nome_compacto_igual(cadastro[0], api)):
                revisar("CONFLITO_NOME_CPF", f"o CPF/CNPJ da API está cadastrado no banco novo como "
                                             f"'{cadastro[0]}' (o nome não é trocado)")
        antes = credores()
        garantir_software(cur)
        acao("situacao_e_originario", *registrar_fonte(cur, r))
        prec = r["processos"][0]
        for cap in r["processos"]:
            nome = "partes_precatorio" if cap is prec else "partes_originario"
            if cap.get("erro"):
                acao(nome, "PULADO", cap["erro"])
            else:
                passo(nome, registrar_capa, r, cap, cap is prec)
        passo("status", melhorar_status, r, se_erro="PULADO")
        comparar_credores(antes, credores())
        for fila in filas:
            backup_fila += passo(f"fila_antiga_{fila[-7:]}", atualizar_fila_antiga, r, fila)
        if capa_antiga:
            backup_capa += passo("capa_antiga_precatorio", atualizar_capa_antiga, r)
        if gravar:
            con.commit()
        else:
            con.rollback()
    except Exception as e:  # noqa: BLE001 - o lead inteiro é desfeito e registrado
        con.rollback()
        for a in acoes:
            if a["resultado"] in ("FEITO", "SIMULADO"):
                a["resultado"] = "DESFEITO"
        trocas.clear()
        backup_fila.clear()
        backup_capa.clear()
        acao("lead", "ERRO", f"{e.__class__.__name__}: {str(e).splitlines()[0][:200]}")
    finally:
        cur.close()
    return acoes, trocas, backup_fila, backup_capa, revisao


def atualizar_banco(pasta, resultados, gravar):
    """Atualiza (ou simula) lead a lead; grava acoes, credores_trocados, revisao, backups e o SQL de desfazer."""
    con = conectar("fetch_TJAL", escrita=True)
    with con.cursor() as cur:
        filas, filas_sem_permissao = filas_antigas(cur, FILA_ANTIGA_DESDE)
        cur.execute("SELECT has_schema_privilege(current_user, 'precatorios', 'USAGE')")
        capa_antiga = cur.fetchone()[0]
    con.rollback()
    log.info(f"{'GRAVANDO' if gravar else 'SIMULANDO'} a atualização de {len(resultados)} leads: schema creditos | "
             f"filas do robô: {', '.join(filas) or 'nenhuma'} | "
             f"capa antiga (precatorios.*): {'sim' if capa_antiga else 'NÃO (sem permissão)'}")
    if filas_sem_permissao:
        log.warning(f"filas sem permissão de UPDATE (puladas): {', '.join(filas_sem_permissao)}")

    acoes, trocas, backup_fila, backup_capa, revisao = [], [], [], [], []
    inicio = time.time()
    try:
        for i, r in enumerate(resultados, 1):
            a, t, bf, bc, rv = atualizar_lead(con, r, gravar, filas, capa_antiga)
            acoes += a
            trocas += t
            backup_fila += bf
            backup_capa += bc
            revisao += rv
            if i % 250 == 0 or i == len(resultados):
                log.info(f"atualização {i}/{len(resultados)} | {time.time() - inicio:.0f} s")
    except KeyboardInterrupt:
        con.rollback()
        log.warning("atualização interrompida: o lead em andamento foi desfeito; os anteriores ficam como estão")
    finally:
        con.close()

    gravar_csv(pasta / "acoes.csv", acoes)
    gravar_csv(pasta / "credores_trocados.csv", trocas)
    gravar_csv(pasta / "revisao.csv", sorted(revisao, key=lambda x: (x["tipo"], x["numero_precatorio"] or "")))
    if GERAR_DESFAZER:
        gravar_csv(pasta / "backup_fila_antiga.csv", backup_fila)
    if capa_antiga and GERAR_DESFAZER:
        gravar_csv(pasta / "backup_capa_antiga.csv", backup_capa)
    if gravar and GERAR_DESFAZER and (backup_fila or backup_capa):
        (pasta / "desfazer_tabelas_antigas.sql").write_text(sql_para_desfazer(backup_fila, backup_capa),
                                                            encoding="utf-8")

    resumo = [("tempo_atualizacao_min", round((time.time() - inicio) / 60, 1)),
              ("leads_com_credor_trocado", len(trocas))]
    resumo += sorted(Counter(f"{a['acao']} / {a['resultado']}" for a in acoes).items())
    resumo += sorted(Counter(f"revisao::{x['tipo']}" for x in revisao).items())
    for fila in filas:
        linhas = [b for b in backup_fila if b["tabela"] == fila]
        resumo += [(f"{fila}: linhas alteradas", len(linhas)),
                   (f"{fila}: status virou sucesso", sum(1 for b in linhas if b["status_depois"] != b["status_antes"]))]
    if filas_sem_permissao:
        resumo.append(("filas sem permissão (puladas)", ", ".join(filas_sem_permissao)))
    if capa_antiga:
        resumo += sorted(Counter(f"capa_antiga {b['op']} {b['tabela']}" for b in backup_capa).items())
    else:
        resumo.append(("capa_antiga", "PULADA: sem permissão no schema precatorios"))
    return resumo


# =============================================================================== execução

def pasta_da_execucao(modo):
    """Retoma a pasta mais recente do modo se a raspagem dela não terminou; senão, uma pasta nova."""
    pastas = sorted(p for p in SAIDA.glob(f"*_{modo}") if p.is_dir())
    if pastas and (pastas[-1] / "_progresso.jsonl").exists() and not (pastas[-1] / "leads_enriquecimento.csv").exists():
        return pastas[-1]
    return SAIDA / f"{datetime.now():%Y%m%d_%H%M}_{modo}"


def limpar_pasta(pasta):
    """Apaga os CSVs e o checkpoint da execução (o SQL de desfazer fica); a pasta vazia sai junto."""
    for arquivo in [*pasta.glob("*.csv"), pasta / "_progresso.jsonl"]:
        arquivo.unlink(missing_ok=True)
    if not any(pasta.iterdir()):
        pasta.rmdir()


def executar(leads, gravar, pasta):
    """Raspa os leads e atualiza o banco. Devolve False se a raspagem foi interrompida (banco não é tocado)."""
    resultados, resumo_r, completou = raspar_leads(pasta, leads)
    if not completou:
        return False
    resumo = [("modo", "GRAVACAO" if gravar else "SIMULACAO")] + resumo_r + atualizar_banco(pasta, resultados, gravar)
    log.info("Resumo:")
    for k, v in resumo:
        log.info(f"  {k}: {v}")
    limpar_pasta(pasta)
    if pasta.exists():
        log.info(f"CSVs apagados; o SQL para desfazer as tabelas antigas ficou em {pasta}")
    if not gravar:
        log.info("Simulação: nada foi gravado no banco. Para gravar, rode sem --simulacao.")
    return True


def ler_argumentos():
    """--simulacao: faz tudo, mas desfaz a atualização do banco. --fila [--limite N]: só os leads da fila."""
    ap = argparse.ArgumentParser(description="Enriquece os leads do TJAL pela consulta pública e atualiza o banco.")
    ap.add_argument("--simulacao", action="store_true",
                    help="faz tudo, mas desfaz a atualização do banco (ROLLBACK): só mostra o resumo")
    ap.add_argument("--fila", action="store_true",
                    help=f"só FALHA/PENDENTE não raspados há {DIAS_FILA} dias, reservados durante a rodada "
                         "(modo 5 do RPA_SISTEMAS)")
    ap.add_argument("--limite", type=int, default=None,
                    help="com --fila: no máximo N leads nesta rodada (0 = a fila inteira)")
    ap.add_argument("--com-desfazer", action="store_true",
                    help="escreve os arquivos de desfazer (desfazer_*.sql e backup .csv); o padrão é não escrever")
    ap.add_argument("--sem-desfazer", action="store_true",
                    help="não escreve os arquivos de desfazer (já é o padrão; os ciclos do modo 5 do RPA passam)")
    args = ap.parse_args()
    global GERAR_DESFAZER
    GERAR_DESFAZER = args.com_desfazer and not args.sem_desfazer
    if args.limite is not None and not args.fila:
        ap.error("--limite só vale com --fila")
    if args.limite is not None and args.limite < 0:
        ap.error("--limite precisa ser 0 ou mais")
    return args


def main():
    """Carrega os leads, raspa (retomando a execução interrompida do mesmo modo) e atualiza o banco."""
    global WORKER
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    gravar = not args.simulacao
    if args.fila:
        WORKER = f"consulta_publica_tjal:{socket.gethostname()}:{os.getpid()}"   # dono do lease desta rodada
    pasta = pasta_da_execucao(("fila-" if args.fila else "") + ("gravacao" if gravar else "simulacao"))

    if gravar:
        log.warning("ATENÇÃO: esta execução vai GRAVAR no banco (schema creditos, filas do robô e capa antiga). "
                    "Ctrl+C nos próximos 10 s para cancelar (use --simulacao para só gerar os CSVs).")
        time.sleep(10)
    if (pasta / "_progresso.jsonl").exists():
        log.info(f"Retomando a raspagem interrompida em {pasta.name}")

    t0 = time.time()
    leads = carregar_leads(args.fila, args.limite)
    if args.fila:
        log.info(f"{len(leads)} leads do TJAL na fila (FALHA/PENDENTE não raspados há {DIAS_FILA} dias"
                 f"{f', até {args.limite}' if args.limite else ''}; lidos em {time.time() - t0:.1f}s)")
        if not leads:
            log.info("Fila vazia: nada a fazer.")
            return
    else:
        log.info(f"{len(leads)} leads do TJAL na lista (lidos em {time.time() - t0:.1f}s)")
    originais, filas = {}, []
    try:
        if args.fila and gravar:
            leads, originais, filas = reservar_leads(leads)
            if not leads:
                log.info("Nenhum lead da fila pôde ser reservado agora: nada a fazer.")
                return
        if not executar(leads, gravar, pasta):
            log.warning("Raspagem interrompida: o banco NÃO foi atualizado. "
                        "Rode o mesmo comando de novo para continuar de onde parou.")
    finally:
        soltar_leads(leads, originais, filas)


if __name__ == "__main__":
    main()
