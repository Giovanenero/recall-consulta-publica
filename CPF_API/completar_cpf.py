"""
completar_cpf.py - CPF do credor pelo NOME (API de CPF, utils/cpf_api.py) nos créditos em que o robô do tribunal achou
o credor na fonte pública, mas sem o CPF.

Não é robô de tribunal: lê o que os robôs já gravaram (TJRJ, TJDFT, TJMT, TJRR, TJPI: SUCESSO_INCOMPLETO
'SUCESSO_SEM_CPF'; TJBA, TJMA: FALHA 'SEM_CPF_CREDOR'), consulta a API uma vez por nome e, quando a regra aceita, liga o credor com o CPF
e põe o crédito em SUCESSO_API_TERCEIRO (decisões do usuário de 06/10/2026). Não cria crédito e não mexe em
processo_parte nem nas capas antigas: CPF da API não é dado do processo.

A regra (utils/cpf_api.py, REGRA_VERSAO): nome idêntico, um só CPF no país, o 9º dígito do CPF na região fiscal do
tribunal (Adaptador.uf) e, havendo na fonte, a mesma data de nascimento. Com o CPF tarjado da fonte
(metadata.credor.cpf_mascarado, ex. 123.***.***-45), a busca já filtra pelos dígitos visíveis.

Quem foi consultado e não aceito (ou pulado pelo nome) ganha só metadata.cpf_api {aceito: false, regra_versao}: não
volta na próxima rodada da mesma versão da regra (--refazer força). É por isso que o modo 5 do RPA_SISTEMAS pode rodar
este script depois de cada robô sem consultar os mesmos nomes de novo.

Três modos:
    python CPF_API/completar_cpf.py --medir 500 [--tribunal TJMT,TJRJ] [--semente 1]
        só leitura + API: aplica a regra aos créditos que JÁ têm o CPF confirmado na fonte e mede acerto e cobertura.
    python CPF_API/completar_cpf.py --simulacao --tribunal TJMT [--limite 30] [--creditos 1,2]
        faz tudo, inclusive as escritas, e dá ROLLBACK em cada lote.
    python CPF_API/completar_cpf.py --tribunal TJMT [--limite 2000]
        grava (COMMIT). Só roda para tribunal liberado no CPF_API/liberacao.json (escrito pelo --medir): mesma versão
        da regra, MIN_ACEITOS_LIBERAR aceitos e CPF errado até MAX_TAXA_ERRO (ver liberado()); senão sai com rc 3.
Outras opções: --lote 50, --cache-dias 30, --renovar-cache, --so-cache (não chama a API), --refazer, --sem-espera,
--forcar.

Por crédito gravado, numa transação por lote (um SAVEPOINT por crédito):
  1. confere na fila (FOR UPDATE NOWAIT) que nada mudou desde a leitura e que o crédito continua sem CREDOR;
  2. creditos.registrar_credor(CREDOR, nome da fonte, CPF, originário ligado);
  3. credito_fonte.metadata.cpf_api do software do robô (regra, versão, quando);
  4. status CREDOR_SUCESSO_API_TERCEIRO nas filas do RPA antigo (mensais; TJDFT: tabela antiga), sem rebaixar linha
     que outro robô já melhorou;
  5. creditos.fila_credor_finalizar(SUCESSO_API_TERCEIRO, 'CPF_API: <regra> ...', via NOME), com o detalhe
     software=CPF_API_NOME;
  6. credito_credor.tentativa_id = a tentativa do passo 5 (a marca de que o CPF veio da API).
Tudo entra no desfazer_<rodada>.sql. CPF nunca vai para log, CSV, motivo ou detalhe (no CSV, só mascarado); fica no
banco e no cache da API (saida/cache_cpf_api.jsonl, fora do git).
"""
import argparse
import json
import random
import re
import socket
import sys
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import psycopg2
import psycopg2.errors

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from utils.arquivos import anexar_csv  # noqa: E402
from utils.banco import como_dicts, conectar, filas_antigas  # noqa: E402
from utils.cpf_api import (REGRA_VERSAO, Cache, Cliente, ErroApi, ErroToken, Resultado, decidir,  # noqa: E402
                           mascara_normal, mascarar, normalizar, regioes_da_uf)
from utils.legado import Backup, atualizar_filas_mensais, atualizar_tabela_antiga  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402
from utils.workers import ParadaSuave, travar_worker  # noqa: E402

PASTA = Path(__file__).resolve().parent
SAIDA = PASTA / "saida"
SCRIPT = "completar_cpf.py"
MARCA = "CPF_API_NOME"                      # detalhe.software da tentativa: separa este script de outros SUCESSO_API_TERCEIRO
STATUS_NOVO = "SUCESSO_API_TERCEIRO"
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"
LOTE_PADRAO = 50
# Liberação da gravação por tribunal: a medição (--medir) grava ARQ_LIBERACAO, que vai junto com o código (o modo 5
# do RPA_SISTEMAS lê daqui). Decisão do usuário (06/10/2026): regra nome + região fiscal, aceitando CPF errado até
# MAX_TAXA_ERRO na medição da versão atual da regra (medido em 06/10: ~0,8% no geral).
ARQ_LIBERACAO = PASTA / "liberacao.json"
MIN_ACEITOS_LIBERAR = 200
MAX_TAXA_ERRO = {"TJRJ": 0.025}             # por tribunal. TJRJ: 2,3% medido em 06/10; teto liberado pelo usuário
MAX_TAXA_ERRO_PADRAO = 0.01
ERROS_API_PARA_PAUSAR = 5
PAUSA_API = 300                             # s
PAUSAS_PARA_PARAR = 3
SEMELHANCA_MESMA_PESSOA = 0.9

log = None


@dataclass(frozen=True)
class Adaptador:
    sigla: str
    software: str
    tipo: str              # SEM_CPF: metadata.credor do robô (SUCESSO_INCOMPLETO) | PARTES: partes da tentativa (FALHA)
    legado: str            # MENSAL (processos_unificados_AAAA_MM) | ANTIGA (processos_unificados)
    uf: str                # região fiscal aceita para o CPF (9º dígito): a do tribunal
    gabarito: str = "ROBO"  # medição: ROBO (CPF que o próprio robô achou na fonte) | LEGADO (ver SQL_GABARITO_LEGADO)


ADAPTADORES = {a.sigla: a for a in (
    Adaptador("TJMT", "CONSULTA_PUBLICA_TJMT", "SEM_CPF", "MENSAL", "MT"),
    Adaptador("TJRR", "CONSULTA_PUBLICA_TJRR", "SEM_CPF", "MENSAL", "RR"),
    Adaptador("TJMA", "CONSULTA_PUBLICA_TJMA", "PARTES", "MENSAL", "MA"),
    Adaptador("TJDFT", "CONSULTA_PUBLICA_TJDFT", "SEM_CPF", "ANTIGA", "DF"),
    Adaptador("TJBA", "CONSULTA_PUBLICA_TJBA", "PARTES", "MENSAL", "BA"),
    Adaptador("TJRJ", "CONSULTA_PUBLICA_TJRJ", "SEM_CPF", "MENSAL", "RJ"),
    # o robô do TJPI quase nunca vê CPF na fonte (DJEN): a medição usa o credor que o RPA (A3, PJe) e a correção do
    # legado de 05/10/2026 deixaram com CPF (conferido no PDPJ 13/13)
    Adaptador("TJPI", "CONSULTA_PUBLICA_TJPI", "SEM_CPF", "MENSAL", "PI", gabarito="LEGADO"),
)}

# =============================================================================== nomes

RE_ESPOLIO = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?", re.I)
RE_ESPOLIO_REP = re.compile(r"^\s*esp[oó]lio\s+(?:de\s+)?(?P<falecido>.+?)\s+(?:rep\.?|representad[oa])\s+"
                            r"(?:por\s+)?(?P<rep>.+?)\s*$", re.I)
RE_REP = re.compile(r"\s+(?:rep\.?|representad[oa])\s+(?:por\s+)?.*$", re.I)
RE_E_OUTROS = re.compile(r"\s+e\s+outr[oa]s?\s*$", re.I)
RE_APOSTO = re.compile(r"\s*\([^)]*\)\s*$")
# filtros sobre o nome normalizado (sem acento, só A-Z e espaço)
PULOS = (
    ("ESPOLIO", re.compile(r"\bESPOLIO\b")),
    ("NOME_SOCIAL", re.compile(r"\bREGISTRAD[OA] CIVILMENTE\b|\bNOME SOCIAL\b")),
    ("REPRESENTADO", re.compile(r"\bREP\b|\bREPRESENTAD[OA]S?\b|\bASSISTID[OA]\b|\bCURADOR")),
    ("E_OUTROS", re.compile(r"\bE OUTR[OA]S?\b|\bOUTROS\b")),
    ("SOCIEDADE_ADV", re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|\bADV\b|ESCRITORIO DE ADVOCACIA")),
    ("ORGAO_PUBLICO", re.compile(
        r"^(?:ESTADO D|MUNICIPIO D|PREFEITURA|UNIAO\b|DISTRITO FEDERAL|INSS\b|INSTITUTO NACIONAL|DETRAN)|PROCURADORIA|"
        r"DEFENSORIA PUBLICA|MINISTERIO PUBLICO|FAZENDA PUBLICA|CAMARA MUNICIPAL|TRIBUNAL D|SECRETARIA D")),
    ("PJ_SEM_CNPJ", re.compile(
        r"\b(?:LTDA|EIRELI|EPP|CIA|COMPANHIA|ASSOCIACAO|ASSOC|SINDICATO|SIND|COOPERATIVA|COOP|FUNDACAO|INSTITUTO|"
        r"CONDOMINIO|IGREJA|BANCO|CLUBE|COMERCIO|COMERCIAL|SERVICOS|CONSTRUTORA|CONSTRUCOES|EMPREENDIMENTOS|"
        r"INDUSTRIA|DISTRIBUIDORA|TRANSPORTES|EMPRESA|FEDERACAO|CONFEDERACAO|COLEGIO|ESCOLA|HOSPITAL|CLINICA|"
        r"LABORATORIO|FARMACIA|PARTICIPACOES|INCORPORADORA|ENGENHARIA|PARTIDO|SOCIEDADE|ASSOCIADOS|AUTARQUIA|"
        r"DEPARTAMENTO|UNIVERSIDADE|SEGURADORA|SEGUROS|IMOBILIARIA|HOTEL|EDITORA)\b|\bS A$|\bME$")),
)
# papel do credor na fonte que não é a própria pessoa credora (ou é espólio)
RE_PAPEL_PULO = re.compile(r"ESPOLIO|ADMINISTRADOR|INVENTARIANTE|CURADOR|TUTOR|REPRESENTANTE|CONSORCIO|MASSA FALIDA")
PARTICULAS = {"DE", "DA", "DO", "DAS", "DOS", "E", "D"}
ABREVIACOES = {"JR", "FO", "FLH", "NT", "SOB"}


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples (igual ao normal dos robôs TJBA/TJMA)."""
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", t)).strip()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE' e sem o 'REGISTRADO(A) CIVILMENTE COMO ...' (como nos robôs)."""
    return re.split(r"\bREGISTRAD[OA]\b", normal(RE_ESPOLIO.sub("", nome or "")))[0].strip()


def nomes_para_buscar(beneficiario):
    """Nome da lista -> nomes da pessoa (espólio com inventariante: os dois). Cópia do TJBA/fetch_TJBA.py."""
    b = (beneficiario or "").strip()
    m = RE_ESPOLIO_REP.match(b)
    if m:
        nomes = [m["falecido"], m["rep"]]
    else:
        for regex in (RE_APOSTO, RE_REP, RE_ESPOLIO, RE_E_OUTROS):
            b = regex.sub("", b)
        nomes = [b]
    return [n for n in (x.strip(" -,;.") for x in nomes) if n]


def mesma_pessoa(a, b):
    """Mesmo nome, ou grafia próxima o bastante (SOUSA x SOUZA): o critério dos robôs (corrigir_credor)."""
    a, b = chave_nome(a), chave_nome(b)
    return a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA


def limpar_nome(nome):
    """Tira o aposto do fim ('FULANO (MENOR)') e devolve o nome como vai para a API."""
    return RE_APOSTO.sub("", (nome or "").strip()).strip()


def pulo_do_nome(nome):
    """Motivo para não consultar este nome, ou None. Antes de tirar o aposto, para não perder 'ESPÓLIO' etc."""
    n = normalizar(nome)
    for motivo, regex in PULOS:
        if regex.search(n):
            return motivo
    palavras = normalizar(limpar_nome(nome)).split()
    if len([p for p in palavras if p not in PARTICULAS]) < 2:
        return "NOME_CURTO"
    if any(len(p) == 1 and p not in PARTICULAS for p in palavras) or palavras[-1] in ABREVIACOES:
        return "NOME_ABREVIADO"
    return None

# =============================================================================== banco: leitura


SQL_ESTOQUE = """
SELECT cc.credito_id, c.numero_norm, c.numero_exibicao AS precatorio, tc.codigo AS tipo_credito,
       cc.status_id, st.codigo AS status, cc.motivo_detalhe, cc.software_id, cc.ultima_tentativa_id,
       cc.valor_referencia, f.metadata->'credor' AS credor, f.metadata->'motor'->>'originario' AS originario_motor,
       {extra}
       ARRAY(SELECT pr.numero_cnj FROM creditos.credito_originario co
               JOIN creditos.processo pr ON pr.id = co.processo_id WHERE co.credito_id = c.id) AS ligados
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id AND s.codigo = %(software)s
  JOIN creditos.status_coleta st ON st.id = cc.status_id
  JOIN creditos.credito c        ON c.id = cc.credito_id AND c.saiu_da_lista_em IS NULL
  JOIN creditos.tipo_credito tc  ON tc.id = c.tipo_credito_id
  LEFT JOIN creditos.credito_fonte f ON f.credito_id = cc.credito_id AND f.software_id = cc.software_id
  {join_tentativa}
 WHERE cc.lease_worker IS NULL
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id = 1)
   AND cc.motivo_detalhe !~ 'HONORARIOS'
   AND {filtro}
   AND (%(creditos)s::bigint[] IS NULL OR cc.credito_id = ANY(%(creditos)s::bigint[]))
   AND (%(refazer)s OR coalesce(f.metadata->'cpf_api'->>'regra_versao', '') <> %(versao)s)
 ORDER BY cc.valor_referencia DESC NULLS LAST, cc.credito_id
"""
FILTRO = {
    "SEM_CPF": ("st.codigo = 'SUCESSO_INCOMPLETO' AND cc.motivo_detalhe LIKE 'SUCESSO_SEM_CPF:%%' "
                "AND nullif(btrim(f.metadata->'credor'->>'nome'), '') IS NOT NULL"),
    "PARTES": "st.codigo = 'FALHA' AND cc.motivo_detalhe LIKE 'SEM_CPF_CREDOR: cnj=%%'",
}
EXTRA_PARTES = """t.detalhe->'partes' AS partes, f.metadata->'motor'->'djen'->>'credor' AS credor_djen,
       f.metadata->'motor'->'candidatos' AS candidatos_motor,
       ARRAY(SELECT DISTINCT btrim(v.x) FROM creditos.lista_item l2,
                    LATERAL (VALUES (l2.beneficiario_nome), (l2.metadata->>'de_beneficiario')) v(x)
              WHERE l2.credito_id = c.id AND l2.removido_em IS NULL
                AND nullif(btrim(v.x), '') IS NOT NULL) AS beneficiarios,"""
JOIN_TENTATIVA = "LEFT JOIN creditos.coleta_credor_tentativa t ON t.id = cc.ultima_tentativa_id"

# gabarito da medição: crédito cujo CREDOR com CPF veio da fonte, com o nome como a fonte escreveu
SQL_GABARITO_SEM_CPF = """
SELECT DISTINCT ON (cc.credito_id) cc.credito_id, f.metadata->'credor'->>'nome' AS nome,
       f.metadata->'credor'->>'data_nascimento' AS nascimento, p.documento::text AS cpf_gabarito,
       f.metadata->'motor'->>'originario' AS originario
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id AND s.codigo = %(software)s
  JOIN creditos.status_coleta st ON st.id = cc.status_id AND st.grupo = 'SUCESSO'
  JOIN creditos.credito_fonte f  ON f.credito_id = cc.credito_id AND f.software_id = cc.software_id
  JOIN creditos.credito_credor k ON k.credito_id = cc.credito_id AND k.papel_id = 1 AND k.origem = 'FONTE'
  JOIN creditos.pessoa p         ON p.id = k.pessoa_id AND length(p.documento) = 11
 WHERE (f.metadata->'credor'->>'cpf_encontrado') = 'true'
   AND creditos.chave_texto(f.metadata->'credor'->>'nome') = creditos.chave_texto(p.nome)
   AND NOT EXISTS (SELECT 1 FROM creditos.coleta_credor_tentativa t2
                    WHERE t2.id = k.tentativa_id AND t2.detalhe->>'software' = %(marca)s)
 ORDER BY cc.credito_id
"""
SQL_GABARITO_PARTES = """
SELECT DISTINCT ON (cc.credito_id) cc.credito_id, pa->>'nome' AS nome, NULL AS nascimento,
       p.documento::text AS cpf_gabarito, f.metadata->'motor'->>'originario' AS originario
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id AND s.codigo = %(software)s
  JOIN creditos.status_coleta st ON st.id = cc.status_id AND st.grupo = 'SUCESSO'
  JOIN creditos.credito_fonte f  ON f.credito_id = cc.credito_id AND f.software_id = cc.software_id
  JOIN creditos.coleta_credor_tentativa t ON t.id = cc.ultima_tentativa_id
  JOIN creditos.credito_credor k ON k.credito_id = cc.credito_id AND k.papel_id = 1 AND k.origem = 'FONTE'
  JOIN creditos.pessoa p         ON p.id = k.pessoa_id AND length(p.documento) = 11
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(t.detalhe->'partes') = 'array'
                                               THEN t.detalhe->'partes' ELSE '[]'::jsonb END) pa
 WHERE pa->>'polo' = 'ATIVO'
   AND regexp_replace(coalesce(pa->>'documento', ''), '\\D', '', 'g') = p.documento
   AND NOT EXISTS (SELECT 1 FROM creditos.coleta_credor_tentativa t2
                    WHERE t2.id = k.tentativa_id AND t2.detalhe->>'software' = %(marca)s)
 ORDER BY cc.credito_id
"""
# gabarito LEGADO (tribunal cujo robô quase nunca vê o CPF na fonte): crédito ativo do tribunal do software com um só
# CREDOR com CPF, ligado pela fonte do RPA (FONTE/LEGADO, não CONTATOS) e não pela própria API; o nome é o da pessoa
SQL_GABARITO_LEGADO = """
SELECT c.id AS credito_id, min(p.nome) AS nome, NULL AS nascimento, min(p.documento::text) AS cpf_gabarito,
       NULL AS originario
  FROM creditos.credito c
  JOIN creditos.tribunal tr      ON tr.id = c.tribunal_id
  JOIN creditos.credito_credor k ON k.credito_id = c.id AND k.papel_id = 1 AND k.origem IN ('FONTE', 'LEGADO')
  JOIN creditos.pessoa p         ON p.id = k.pessoa_id AND length(p.documento) = 11
 WHERE tr.sigla = %(sigla)s AND c.saiu_da_lista_em IS NULL
   AND NOT EXISTS (SELECT 1 FROM creditos.coleta_credor_tentativa t2
                    WHERE t2.id = k.tentativa_id AND t2.detalhe->>'software' = %(marca)s)
 GROUP BY c.id
HAVING count(DISTINCT p.id) = 1
 ORDER BY c.id
"""
SQL_GABARITO = {("SEM_CPF", "ROBO"): SQL_GABARITO_SEM_CPF, ("PARTES", "ROBO"): SQL_GABARITO_PARTES,
                ("SEM_CPF", "LEGADO"): SQL_GABARITO_LEGADO}


def ler_estoque(con, ad, creditos, refazer=False):
    """Créditos do tribunal com o credor achado na fonte e sem CPF, do maior valor para o menor. Fora os já tentados
    nesta versão da regra (metadata.cpf_api.regra_versao), a não ser com refazer."""
    sql = SQL_ESTOQUE.format(filtro=FILTRO[ad.tipo], extra=EXTRA_PARTES if ad.tipo == "PARTES" else "",
                             join_tentativa=JOIN_TENTATIVA if ad.tipo == "PARTES" else "")
    with con.cursor() as cur:
        cur.execute(sql, {"software": ad.software, "creditos": creditos, "refazer": refazer, "versao": REGRA_VERSAO})
        return como_dicts(cur)


def originario_de(linha):
    """CNJ (20 dígitos) do originário que o robô escolheu, se ele está ligado ao crédito; senão None."""
    cnj = so_digitos(linha.get("originario_motor"))
    if not cnj:
        m = re.search(r"cnj=(\S+)", linha.get("motivo_detalhe") or "")
        cnj = so_digitos(m.group(1)) if m else ""
    return cnj if len(cnj) == 20 and cnj in {so_digitos(x) for x in linha["ligados"] or []} else None


def nome_das_partes(linha, ad):
    """(nome, None) da única parte ATIVA sem CPF que é o credor que o robô confirmou; ou (None, motivo do pulo)."""
    if ad.sigla == "TJBA":
        fontes = linha["beneficiarios"] or []
    else:
        fontes = [n for n in (linha.get("credor_djen") or "").split(" | ") if n.strip()]
        orig = originario_de(linha)
        cand = next((c for c in linha.get("candidatos_motor") or [] if so_digitos(c.get("cnj")) == orig), None)
        if not cand or cand.get("credor_por") != "NOME":
            return None, "CREDOR_NAO_POR_NOME"
    if any(RE_ESPOLIO.match(f) or "ESPOLIO" in normalizar(f) for f in fontes):
        return None, "ESPOLIO"                        # o credor é o espólio, mesmo que a parte do PJe não diga
    alvo = {chave_nome(n) for b in fontes for n in nomes_para_buscar(b)} - {""}
    achadas = {}
    for p in linha.get("partes") or []:
        if p.get("polo") == "ATIVO" and chave_nome(p.get("nome")) in alvo and not documento_valido(p.get("documento")):
            achadas.setdefault(chave_nome(p.get("nome")), p.get("nome"))
    if len(achadas) != 1:
        return None, "NOME_NAO_CONFIRMADO" if not achadas else "VARIOS_NOMES"
    return next(iter(achadas.values())), None


def item_do_estoque(linha, ad):
    """O que a consulta e a gravação precisam de cada crédito (ou o motivo para pular)."""
    item = {k: linha[k] for k in ("credito_id", "numero_norm", "precatorio", "tipo_credito", "status_id", "status",
                                  "motivo_detalhe", "software_id", "ultima_tentativa_id")}
    item.update(originario=originario_de(linha), nascimento=None, nome=None, pulo=None, mascara=None)
    if ad.tipo == "SEM_CPF":
        credor = linha["credor"] or {}
        item["nome"], item["nascimento"] = credor.get("nome"), credor.get("data_nascimento") or None
        item["mascara"] = credor.get("cpf_mascarado") or None       # CPF tarjado que o robô viu na fonte
        papel = normalizar(credor.get("papel"))
        if RE_PAPEL_PULO.search(papel):
            item["pulo"] = "ESPOLIO" if "ESPOLIO" in papel else "PAPEL_REPRESENTANTE"
    else:
        item["nome"], item["pulo"] = nome_das_partes(linha, ad)
    if item["nome"] and not item["pulo"]:
        item["pulo"] = pulo_do_nome(item["nome"])
    return item

# =============================================================================== API


class Parar(Exception):
    """A rodada não deve continuar (token recusado, API fora por muito tempo)."""


class Api:
    """Cliente da API com a política de erro da rodada: erro de API nunca vira resultado; 5 erros seguidos pausam 5
    min; 3 pausas ou token recusado param a rodada."""

    def __init__(self, cache_dias, renovar, so_cache, parada):
        cache = Cache(SAIDA / "cache_cpf_api.jsonl", dias=cache_dias)
        self.cliente = _ClienteSoCache(cache) if so_cache else Cliente(cache=cache)   # sem .env: ErroToken
        self.renovar, self.so_cache, self.parada = renovar, so_cache, parada
        self.renovados, self.erros_seguidos, self.pausas = set(), 0, 0

    @property
    def chamadas(self):
        return getattr(self.cliente, "chamadas", 0)

    def consultar(self, nome, nascimento, mascara=None, regioes=None):
        """Resultado da regra; None = nome fora do cache (--so-cache); 'ERRO_API' = a API falhou para este nome."""
        chave = normalizar(limpar_nome(nome))
        renovar = self.renovar and (chave, mascara) not in self.renovados
        try:
            r = self.cliente.cpf_unico(chave, nascimento, renovar=renovar, so_cache=self.so_cache, mascara=mascara,
                                       regioes=regioes)
            self.renovados.add((chave, mascara))
            self.erros_seguidos = 0
            return r
        except ErroToken as e:
            raise Parar(f"token: {e}") from None
        except ErroApi as e:
            self.erros_seguidos += 1
            log.warning(f"API de CPF falhou ({e}); {self.erros_seguidos} seguida(s)")
            if self.erros_seguidos >= ERROS_API_PARA_PAUSAR:
                self.pausas += 1
                if self.pausas >= PAUSAS_PARA_PARAR:
                    raise Parar(f"API de CPF fora: {PAUSAS_PARA_PARAR} pausas seguidas") from None
                log.warning(f"pausa de {PAUSA_API // 60} min por erro da API ({self.pausas}/{PAUSAS_PARA_PARAR})")
                fim = time.monotonic() + PAUSA_API
                while time.monotonic() < fim and not self.parada:
                    time.sleep(1)
                self.erros_seguidos = 0
            return "ERRO_API"


class _ClienteSoCache:
    """--so-cache: só o que já está no cache, sem credenciais e sem chamar a API."""

    chamadas = 0

    def __init__(self, cache):
        self.cache = cache

    def cpf_unico(self, nome, nascimento=None, renovar=False, so_cache=True, mascara=None, regioes=None):
        if len(nome.split()) < 2:
            return Resultado("NOME_INVALIDO")
        m = mascara_normal(mascara) if mascara else None
        if mascara and not m:
            return Resultado("MASCARA_INVALIDA")
        registro = self.cache.ler(f"{nome}|{m}" if m else nome)
        if registro is None:
            return None
        return Resultado(**{**decidir(registro, nascimento, regioes).__dict__, "do_cache": True})

# =============================================================================== banco: gravação


class Pulo(Exception):
    """O crédito não é gravado por um motivo esperado (mudou na fila, já tem credor, conflito...)."""


SQL_APAGAR_PESSOA = """DELETE FROM creditos.pessoa p WHERE p.id = %s
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.processo_parte x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_oab x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_telefone x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_email x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_endereco x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_enriquecimento x WHERE x.pessoa_id = p.id)
   AND NOT EXISTS (SELECT 1 FROM creditos.compra_dados_item x WHERE x.pessoa_id = p.id);"""
COLUNAS_FILA = ("status_id", "motivo_id", "motivo_detalhe", "tentativas", "ultima_tentativa_id", "sucesso_em",
                "updated_at")


def conflito(cur, cpf, nome, nasc_api):
    """O CPF já está no banco com outra pessoa (outro nome ou outra data de nascimento)? Motivo ou None."""
    cur.execute("SELECT nome, data_nascimento FROM creditos.pessoa WHERE documento = creditos.documento_normalizar(%s)",
                (cpf,))
    linha = cur.fetchone()
    if not linha:
        return None
    nome_banco, nasc_banco = linha
    if nome_banco and nome_banco != "(SEM NOME)" and not mesma_pessoa(nome_banco, nome):
        return "CONFLITO_NOME_CPF"
    if nasc_banco and nasc_api and nasc_banco != nasc_api:
        return "CONFLITO_NASCIMENTO_CPF"
    return None


def credores_do_credito(cur, credito_id):
    """Credores ligados, sem o documento (só se tem ou não): para o detalhe e o CSV."""
    cur.execute("""SELECT pp.codigo, p.nome, p.documento IS NOT NULL, x.origem
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p       ON p.id = x.pessoa_id
                     JOIN creditos.papel_parte pp ON pp.id = x.papel_id
                    WHERE x.credito_id = %s""", (credito_id,))
    return " | ".join(f"{nome} ({'com doc' if doc else 'sem doc'}, {papel}, {origem})"
                      for papel, nome, doc, origem in sorted(cur.fetchall()))


def gravar(cur, rod, item):
    """Grava um crédito aceito (quem chama cuida do SAVEPOINT). Devolve o resumo para o CSV."""
    ad, r, cid = rod.ad, item["res"], item["credito_id"]
    cur.execute(f"SELECT lease_worker, software_id, {', '.join(COLUNAS_FILA)} FROM creditos.coleta_credor "
                "WHERE credito_id = %s FOR UPDATE NOWAIT", (cid,))
    fila = (como_dicts(cur) or [None])[0]
    if (not fila or fila["lease_worker"] or fila["status_id"] != item["status_id"]
            or fila["software_id"] != item["software_id"] or fila["ultima_tentativa_id"] != item["ultima_tentativa_id"]):
        raise Pulo("MUDOU_NA_FILA")
    cur.execute("SELECT 1 FROM creditos.credito_credor WHERE credito_id = %s AND papel_id = 1 LIMIT 1", (cid,))
    if cur.fetchone():
        raise Pulo("JA_TEM_CREDOR")
    motivo_conflito = conflito(cur, r.cpf, item["nome"], r.nascimento_api)
    if motivo_conflito:
        raise Pulo(motivo_conflito)
    cur.execute("SELECT creditos.documento_de_parte(%s)", (r.cpf,))
    documento = cur.fetchone()[0]
    if not documento:
        raise Pulo("CPF_INVALIDO")
    antes = credores_do_credito(cur, cid)
    processo_id = None
    if item["originario"]:
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = %s", (item["originario"],))
        processo_id = (cur.fetchone() or [None])[0]

    # 1. credor com o CPF
    cur.execute("SELECT id FROM creditos.pessoa WHERE documento = %s", (documento,))
    pessoa_existia = cur.fetchone() is not None
    cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                    p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                (cid, limpar_nome(item["nome"]), documento, processo_id, MARCA))
    vinculo = cur.fetchone()[0]
    if not pessoa_existia:
        cur.execute("SELECT id FROM creditos.pessoa WHERE documento = %s", (documento,))
        pessoa_id = cur.fetchone()[0]
        rod.bk.sql(cur, "creditos.pessoa", {"id": pessoa_id}, SQL_APAGAR_PESSOA, [pessoa_id])
    rod.bk.insert(cur, "creditos.credito_credor", vinculo)

    # 2. metadata do software do robô (só a chave cpf_api; nada mais do JSON muda)
    gravar_cpf_api(cur, rod, item, {"regra": r.regra, "aceito": True, "n_candidatos": r.n_candidatos,
                                    "nascimento_conferido": bool(item.get("nascimento")),
                                    "mascara_conferida": bool(item.get("mascara"))})

    # 3. filas do RPA antigo
    motivo = f"CPF_API: {r.regra} cnj={formatar_cnj(item['originario']) if item['originario'] else '-'}"
    cont = Counter()
    so_de = {None, rod.status_legado.get(item["status"])}
    status_legado = rod.status_legado[STATUS_NOVO]
    if ad.legado == "MENSAL":
        atualizar_filas_mensais(cur, rod.filas, item["numero_norm"], ad.sigla, status_legado, motivo, rod.bk, cont, so_de)
    else:
        atualizar_tabela_antiga(cur, item["numero_norm"], ad.sigla, status_legado, motivo, rod.bk, cont, so_de)

    # 4. status na fila e a marca no vínculo
    depois = credores_do_credito(cur, cid)
    detalhe = {"software": MARCA, "robo": ad.software, "regra": r.regra, "regra_versao": REGRA_VERSAO,
               "n_candidatos": r.n_candidatos, "n_linhas_api": r.n_brutos, "status_anterior": item["status"],
               "motivo_anterior": (item["motivo_detalhe"] or "")[:500], "credores_antes": antes,
               "credores_depois": depois}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => 'NOME', p_sistema => NULL,
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (cid, rod.worker, STATUS_NOVO, motivo, formatar_cnj(item["originario"]) if item["originario"] else None,
                 json.dumps(detalhe, ensure_ascii=False, default=str), socket.gethostname()))
    tentativa = cur.fetchone()[0]
    rod.bk.insert(cur, "creditos.coleta_credor_tentativa", tentativa)
    rod.bk.update(cur, "creditos.coleta_credor", {"credito_id": cid}, {k: fila[k] for k in COLUNAS_FILA})
    cur.execute("UPDATE creditos.credito_credor SET tentativa_id = %s WHERE id = %s", (tentativa, vinculo))
    return {"legado": ", ".join(f"{k}={v}" for k, v in sorted(cont.items())), "credores_antes": antes,
            "credores_depois": depois}


def gravar_cpf_api(cur, rod, item, dados):
    """credito_fonte.metadata.cpf_api do software do robô (só essa chave muda), com o desfazer. É também a marca de
    "já tentado nesta versão da regra": ler_estoque e a contagem do modo 5 pulam quem a tem."""
    cid = item["credito_id"]
    cpf_api = {**dados, "regra_versao": REGRA_VERSAO, "consultado_em": datetime.now().isoformat("T", "seconds"),
               "nome_consultado": normalizar(limpar_nome(item.get("nome") or ""))}
    cur.execute("""SELECT metadata->'cpf_api' FROM creditos.credito_fonte WHERE credito_id = %s AND software_id = %s
                   FOR UPDATE""", (cid, item["software_id"]))
    meta = cur.fetchone()
    if meta is None:
        return
    if meta[0] is None:
        rod.bk.sql(cur, "creditos.credito_fonte", {"credito_id": cid},
                   "UPDATE creditos.credito_fonte SET metadata = metadata - 'cpf_api' "
                   "WHERE credito_id = %s AND software_id = %s;", [cid, item["software_id"]])
    else:
        rod.bk.sql(cur, "creditos.credito_fonte", {"credito_id": cid},
                   "UPDATE creditos.credito_fonte SET metadata = jsonb_set(metadata, '{cpf_api}', %s::jsonb) "
                   "WHERE credito_id = %s AND software_id = %s;",
                   [json.dumps(meta[0], ensure_ascii=False), cid, item["software_id"]])
    cur.execute("""UPDATE creditos.credito_fonte
                      SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('cpf_api', %s::jsonb)
                    WHERE credito_id = %s AND software_id = %s""",
                (json.dumps(cpf_api, ensure_ascii=False), cid, item["software_id"]))


# pulo na gravação que não muda a fila: anota como tentado (senão volta a cada rodada)
PULOS_ANOTADOS = ("CONFLITO_NOME_CPF", "CONFLITO_NASCIMENTO_CPF", "CPF_INVALIDO")


def anotar(cur, rod, item):
    """Crédito consultado (ou pulado pelo nome) sem CPF aceito: só a marca de tentado no metadata, no SAVEPOINT dele."""
    r = item.get("res")
    regra = item.get("motivo") if item["resultado"] == "PULADO" else getattr(r, "regra", item.get("motivo"))
    cur.execute("SAVEPOINT anota")
    try:
        gravar_cpf_api(cur, rod, item, {"regra": regra, "aceito": False,
                                        "n_candidatos": getattr(r, "n_candidatos", None)})
        cur.execute("RELEASE SAVEPOINT anota")
        rod.bk.fechar_credito(item["credito_id"])
    except psycopg2.Error as e:
        cur.execute("ROLLBACK TO SAVEPOINT anota")
        rod.bk.descartar()
        log.warning(f"{item['credito_id']} não anotou a tentativa: {(str(e).splitlines() or [''])[0][:200]}")


def gravar_lote(rod, lote):
    """Grava os aceitos numa transação (um SAVEPOINT por crédito): COMMIT, ou ROLLBACK na simulação; depois o
    desfazer. Um crédito que não grava fica como estava (nunca vira FALHA: o resultado do robô continua valendo)."""
    if not lote:
        return
    try:
        with rod.con.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            for item in lote:
                if item.get("anotar"):                  # rejeitado ou pulado pelo nome: só a marca de tentado
                    anotar(cur, rod, item)
                    continue
                cur.execute("SAVEPOINT credito")
                try:
                    item["gravado"] = gravar(cur, rod, item)
                    cur.execute("RELEASE SAVEPOINT credito")
                    rod.bk.fechar_credito(item["credito_id"])
                    item["resultado"] = "SIMULADO" if rod.simulacao else "GRAVADO"
                except Pulo as p:
                    cur.execute("ROLLBACK TO SAVEPOINT credito")
                    rod.bk.descartar()
                    item.update(resultado="PULADO", motivo=str(p))
                    if str(p) in PULOS_ANOTADOS:
                        anotar(cur, rod, item)
                except psycopg2.errors.LockNotAvailable:
                    cur.execute("ROLLBACK TO SAVEPOINT credito")
                    rod.bk.descartar()
                    item.update(resultado="PULADO", motivo="OCUPADO")
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT credito")
                    rod.bk.descartar()
                    erro = (str(e).splitlines() or [""])[0][:300]
                    log.warning(f"{item['credito_id']} não gravou: {erro}")
                    item.update(resultado="ERRO_GRAVACAO", motivo=erro)
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
    for item in lote:
        registrar_item(rod, item)
    if any(not i.get("anotar") for i in lote) and all(i["resultado"] == "ERRO_GRAVACAO" for i in lote
                                                          if not i.get("anotar")):
        raise Parar("nenhum crédito do lote gravou: o banco está com problema")

# =============================================================================== rodada (simulação e gravação)


COLUNAS = ["rodada", "modo", "tribunal", "credito_id", "precatorio", "status_antes", "nome", "tem_nascimento",
           "tem_mascara",
           "originario", "resultado", "motivo", "regra", "n_candidatos", "n_linhas_api", "cpf_mascarado", "do_cache",
           "legado", "credores_antes", "credores_depois"]


class Rodada:
    def __init__(self, ad, simulacao, sem_desfazer=False):
        self.ad, self.simulacao = ad, simulacao
        self.modo = "simulacao" if simulacao else "gravacao"
        self.rodada = f"{datetime.now():%Y%m%d_%H%M%S}"
        self.worker = f"cpf_api_{ad.sigla.lower()}"
        self.arq_csv = SAIDA / f"completar_cpf{'_simulacao' if simulacao else ''}.csv"
        self.bk = Backup(SAIDA / f"desfazer_{self.rodada}_{ad.sigla}.sql", SAIDA / f"backup_{self.rodada}_{ad.sigla}.csv",
                         self.rodada, self.modo, SCRIPT)
        self.bk.gravar_arquivos = not sem_desfazer
        self.con = conectar("completar_cpf", escrita=True, worker=ad.sigla)
        self.leitura = conectar("completar_cpf", worker=ad.sigla)
        with self.con.cursor() as cur:
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            self.status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
            self.filas, sem_permissao = filas_antigas(cur, FILA_ANTIGA_DESDE)
        self.con.rollback()
        if sem_permissao:
            log.warning(f"filas mensais sem permissão de UPDATE (puladas): {', '.join(sem_permissao)}")
        self.contagem = Counter()

    def fechar(self):
        for c in (self.con, self.leitura):
            try:
                c.close()
            except Exception:
                pass


def registrar_item(rod, item):
    """Linha do CSV e contagem de um crédito já decidido."""
    r = item.get("res")
    g = item.get("gravado") or {}
    rod.contagem[item["resultado"] if item["resultado"] != "PULADO" else f"PULADO:{item.get('motivo')}"] += 1
    anexar_csv(rod.arq_csv, COLUNAS, [{
        "rodada": rod.rodada, "modo": rod.modo, "tribunal": rod.ad.sigla, "credito_id": item["credito_id"],
        "precatorio": item["precatorio"], "status_antes": item["status"], "nome": item.get("nome") or "",
        "tem_nascimento": "sim" if item.get("nascimento") else "nao",
        "tem_mascara": "sim" if item.get("mascara") else "nao",
        "originario": formatar_cnj(item["originario"]) if item.get("originario") else "",
        "resultado": item["resultado"], "motivo": item.get("motivo") or "",
        "regra": r.regra if hasattr(r, "regra") else "", "n_candidatos": getattr(r, "n_candidatos", ""),
        "n_linhas_api": getattr(r, "n_brutos", ""), "cpf_mascarado": mascarar(getattr(r, "cpf", None)),
        "do_cache": "sim" if getattr(r, "do_cache", False) else "", "legado": g.get("legado", ""),
        "credores_antes": g.get("credores_antes", ""), "credores_depois": g.get("credores_depois", "")}])
    log.info(f"{item['credito_id']} {item['precatorio']} -> {item['resultado']} "
             f"{item.get('motivo') or (r.regra if hasattr(r, 'regra') else '')}"
             + (f" | {r.n_candidatos} cand." if hasattr(r, "n_candidatos") else "")
             + (" | cache" if getattr(r, "do_cache", False) else ""))


def completar(ad, args, parada):
    """Simulação ou gravação de um tribunal."""
    rod = Rodada(ad, args.simulacao, args.sem_desfazer)
    api = Api(args.cache_dias, args.renovar_cache, args.so_cache, parada)
    regioes = regioes_da_uf(ad.uf)
    try:
        linhas = ler_estoque(rod.leitura, ad, args.creditos, refazer=args.refazer)
        itens = [item_do_estoque(x, ad) for x in linhas]
        log.info(f"{rod.modo} | {ad.sigla}: {len(itens)} créditos sem CPF a tentar; "
                 f"{sum(1 for i in itens if not i['pulo'])} com nome consultável "
                 f"({sum(1 for i in itens if not i['pulo'] and i['mascara'])} com CPF tarjado) | regra {REGRA_VERSAO}, "
                 f"região fiscal {ad.uf} ({''.join(sorted(regioes))})")
        lote, consultados = [], 0
        for item in itens:
            if parada:
                break
            if len(lote) >= args.lote:
                gravar_lote(rod, lote)
                lote = []
            if item["pulo"]:
                item.update(resultado="PULADO", motivo=item["pulo"], anotar=True)
                lote.append(item)
                continue
            if args.limite and consultados >= args.limite:
                break
            consultados += 1
            res = api.consultar(item["nome"], item["nascimento"], item["mascara"], regioes)
            if res is None or res == "ERRO_API":
                item.update(resultado="SEM_CACHE" if res is None else "ERRO_API")
                registrar_item(rod, item)
                continue
            item["res"] = res
            if not res.aceito:
                item.update(resultado="REJEITADO", motivo=res.regra, anotar=True)
            lote.append(item)
        gravar_lote(rod, lote)
    except Parar as e:
        log.error(f"rodada parada: {e}")
    finally:
        rod.fechar()
    log.info(f"{ad.sigla} {rod.modo}: " + ", ".join(f"{k}={v}" for k, v in sorted(rod.contagem.items()))
             + f" | chamadas à API: {api.chamadas}")
    if not rod.simulacao and rod.bk.arquivo_sql.exists():
        log.info(f"desfazer: {rod.bk.arquivo_sql}")

# =============================================================================== medição


def ano_do_cnj(cnj):
    d = so_digitos(cnj)
    return int(d[9:13]) if len(d) == 20 else None


def faixa_ano(ano):
    if not ano:
        return "sem_originario"
    return "<2000" if ano < 2000 else "2000-2009" if ano < 2010 else "2010-2019" if ano < 2020 else "2020+"


def medir(ads, n, semente, args, parada):
    """Aplica a regra a uma amostra do gabarito de cada tribunal (só leitura + API) e grava o resumo que libera a
    gravação daquele tribunal."""
    rodada = f"{datetime.now():%Y%m%d_%H%M%S}"
    leitura = conectar("completar_cpf_medir")
    api = Api(args.cache_dias, args.renovar_cache, args.so_cache, parada)
    arquivo_resumo = SAIDA / "medicao_resumo.json"
    resumo = json.loads(arquivo_resumo.read_text(encoding="utf-8")) if arquivo_resumo.exists() else {}
    colunas = ["rodada", "tribunal", "credito_id", "nome", "palavras", "faixa_ano_originario", "tem_nascimento",
               "resultado", "regra", "n_candidatos", "n_linhas_api", "cpf_certo_entre_candidatos", "do_cache"]
    try:
        for ad in ads:
            if parada:
                break
            sql = SQL_GABARITO[(ad.tipo, ad.gabarito)]
            with leitura.cursor() as cur:
                cur.execute(sql, {"software": ad.software, "marca": MARCA, "sigla": ad.sigla})
                gabarito = como_dicts(cur)
            por_nome = {}
            for g in gabarito:
                if g["nome"] and not pulo_do_nome(g["nome"]):
                    por_nome.setdefault(normalizar(limpar_nome(g["nome"])), g)
            elegiveis = sorted(por_nome.values(), key=lambda g: g["credito_id"])
            amostra = random.Random(semente).sample(elegiveis, min(n, len(elegiveis)))
            log.info(f"medição {ad.sigla}: gabarito {len(gabarito)} créditos, {len(elegiveis)} nomes elegíveis, "
                     f"amostra {len(amostra)}")
            cont, estratos, chamadas_antes = Counter(), Counter(), api.chamadas
            for g in amostra:
                if parada:
                    break
                res = api.consultar(g["nome"], g["nascimento"], None, regioes_da_uf(ad.uf))
                palavras = len(normalizar(limpar_nome(g["nome"])).split())
                faixa = faixa_ano(ano_do_cnj(g["originario"]))
                if res is None or res == "ERRO_API":
                    resultado, regra, certo = ("SEM_CACHE" if res is None else "API_ERRO"), "", ""
                else:
                    regra = res.regra
                    certo = "sim" if g["cpf_gabarito"] in res.candidatos else "nao"
                    resultado = (("ACERTO" if res.cpf == g["cpf_gabarito"] else "ERRO") if res.aceito else "REJEITADO")
                cont[resultado] += 1
                if resultado == "REJEITADO":
                    cont[f"rejeitado:{regra}"] += 1
                estratos[(f"palavras={min(palavras, 5)}", resultado)] += 1
                estratos[(f"ano={faixa}", resultado)] += 1
                if resultado == "ERRO":
                    log.warning(f"medição {ad.sigla}: CPF ERRADO no crédito {g['credito_id']} (regra {regra})")
                anexar_csv(SAIDA / f"medicao_{rodada}.csv", colunas, [{
                    "rodada": rodada, "tribunal": ad.sigla, "credito_id": g["credito_id"], "nome": g["nome"],
                    "palavras": palavras, "faixa_ano_originario": faixa,
                    "tem_nascimento": "sim" if g["nascimento"] else "nao", "resultado": resultado, "regra": regra,
                    "n_candidatos": getattr(res, "n_candidatos", ""), "n_linhas_api": getattr(res, "n_brutos", ""),
                    "cpf_certo_entre_candidatos": certo, "do_cache": "sim" if getattr(res, "do_cache", False) else ""}])
            aceitos = cont["ACERTO"] + cont["ERRO"]
            validos = aceitos + cont["REJEITADO"]
            m = {"regra_versao": REGRA_VERSAO, "em": datetime.now().isoformat(timespec="seconds"), "semente": semente,
                 "gabarito_creditos": len(gabarito), "nomes_elegiveis": len(elegiveis), "amostra": len(amostra),
                 "validos": validos, "aceitos": aceitos, "acertos": cont["ACERTO"], "erros": cont["ERRO"],
                 "api_erro": cont["API_ERRO"], "sem_cache": cont["SEM_CACHE"],
                 "cobertura": round(aceitos / validos, 4) if validos else None,
                 "precisao": round(cont["ACERTO"] / aceitos, 4) if aceitos else None,
                 "erro_max_95": round(3 / aceitos, 4) if aceitos and not cont["ERRO"] else None,
                 "rejeitados": {k.split(":", 1)[1]: v for k, v in cont.items() if k.startswith("rejeitado:")},
                 "gabarito_completo": len(amostra) == len(elegiveis) and validos == len(amostra),
                 "chamadas": api.chamadas - chamadas_antes}
            m["taxa_erro"] = round(cont["ERRO"] / aceitos, 4) if aceitos else None
            if not args.so_cache:
                resumo[ad.sigla] = m
                SAIDA.mkdir(parents=True, exist_ok=True)
                arquivo_resumo.write_text(json.dumps(resumo, ensure_ascii=False, indent=2), encoding="utf-8")
                lib = json.loads(ARQ_LIBERACAO.read_text(encoding="utf-8")) if ARQ_LIBERACAO.exists() else {}
                lib[ad.sigla] = {k: m[k] for k in ("regra_versao", "em", "amostra", "aceitos", "acertos", "erros",
                                                   "taxa_erro", "cobertura")}
                ARQ_LIBERACAO.write_text(json.dumps(lib, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            log.info(f"medição {ad.sigla}: aceitos {aceitos}/{validos} (cobertura {m['cobertura']}), acertos "
                     f"{m['acertos']}, ERROS {m['erros']}, api_erro {m['api_erro']}, chamadas {m['chamadas']} | "
                     f"rejeitados {m['rejeitados']} | liberado: {'sim' if liberado(ad.sigla, m) else 'não'}")
            for (estrato, resultado), qtd in sorted(estratos.items()):
                log.info(f"  {ad.sigla} {estrato:<22} {resultado:<10} {qtd}")
    except Parar as e:
        log.error(f"medição parada: {e}")
    finally:
        leitura.close()


def liberado(sigla, m):
    """A gravação do tribunal está liberada pela medição (ARQ_LIBERACAO)? Mesma versão da regra, aceitos suficientes e
    taxa de CPF errado até o teto do tribunal."""
    if not m or m.get("regra_versao") != REGRA_VERSAO or m.get("taxa_erro") is None:
        return False
    return (m.get("aceitos", 0) >= MIN_ACEITOS_LIBERAR
            and m["taxa_erro"] <= MAX_TAXA_ERRO.get(sigla, MAX_TAXA_ERRO_PADRAO))

# =============================================================================== execução


def ler_argumentos():
    ap = argparse.ArgumentParser(description="CPF do credor pelo nome (API de CPF) nos créditos sem CPF dos robôs.")
    ap.add_argument("--tribunal", help=f"siglas separadas por vírgula ({', '.join(ADAPTADORES)}); "
                                       "obrigatório fora da medição")
    ap.add_argument("--medir", type=int, metavar="N", help="mede a regra em N nomes do gabarito de cada tribunal")
    ap.add_argument("--semente", type=int, default=1)
    ap.add_argument("--simulacao", action="store_true", help="faz tudo e dá ROLLBACK")
    ap.add_argument("--limite", type=int, default=0, help="créditos consultados nesta rodada (0 = todos)")
    ap.add_argument("--creditos", help="só estes credito_id (separados por vírgula)")
    ap.add_argument("--lote", type=int, default=LOTE_PADRAO)
    ap.add_argument("--cache-dias", type=int, default=30)
    ap.add_argument("--renovar-cache", action="store_true", help="consulta de novo nomes que já estão no cache")
    ap.add_argument("--so-cache", action="store_true", help="não chama a API (só o que já está no cache)")
    ap.add_argument("--forcar", action="store_true", help="grava mesmo sem a medição liberar (decisão do usuário)")
    ap.add_argument("--refazer", action="store_true", help="tenta de novo quem já foi tentado nesta versão da regra")
    ap.add_argument("--sem-espera", action="store_true", help="grava sem os 10 s de aviso (uso pelo modo 5)")
    ap.add_argument("--sem-desfazer", action="store_true",
                    help="não escreve o desfazer_*.sql/backup_*.csv (como os outros tribunais do modo 5 do RPA)")
    a = ap.parse_args()
    siglas = [s.strip().upper() for s in (a.tribunal or "").split(",") if s.strip()]
    if any(s not in ADAPTADORES for s in siglas):
        ap.error(f"tribunal desconhecido: use {', '.join(ADAPTADORES)}")
    if a.medir is None and len(siglas) != 1:
        ap.error("--tribunal com UMA sigla é obrigatório para simular ou gravar")
    a.ads = [ADAPTADORES[s] for s in siglas] if siglas else list(ADAPTADORES.values())
    a.creditos = [int(x) for x in (a.creditos or "").split(",") if x.strip().isdigit()] or None
    if a.lote < 1:
        ap.error("--lote precisa ser maior que zero")
    return a


def main():
    global log
    args = ler_argumentos()
    log = configurar_log(SCRIPT, SAIDA / "logs")
    trava = travar_worker(SAIDA, "cpf_api")      # uma instância por máquina: o limite da API é por token
    try:
        if not args.so_cache:
            try:
                Cliente()
            except ErroToken as e:
                log.error(f"API de CPF: {e}")
                sys.exit(2)
        with ParadaSuave(log) as parada:
            if args.medir is not None:
                medir(args.ads, args.medir, args.semente, args, parada)
                return
            ad = args.ads[0]
            if not args.simulacao and not args.forcar:
                lib = json.loads(ARQ_LIBERACAO.read_text(encoding="utf-8")) if ARQ_LIBERACAO.exists() else {}
                m = lib.get(ad.sigla)
                if not liberado(ad.sigla, m):
                    log.error(f"{ad.sigla}: gravação não liberada pela medição ({m or 'sem medição'}; precisa da regra "
                              f"{REGRA_VERSAO}, {MIN_ACEITOS_LIBERAR} aceitos e CPF errado até "
                              f"{MAX_TAXA_ERRO.get(ad.sigla, MAX_TAXA_ERRO_PADRAO):.1%}). Rode --medir; --forcar só por "
                              f"decisão do usuário.")
                    sys.exit(3)
            if not args.simulacao and not args.sem_espera:
                log.warning(f"{ad.sigla}: modo GRAVAÇÃO (COMMIT) em 10 s; Ctrl+C cancela")
                for _ in range(10):
                    time.sleep(1)
                    if parada:
                        return
            completar(ad, args, parada)
    finally:
        trava.close()


if __name__ == "__main__":
    main()
