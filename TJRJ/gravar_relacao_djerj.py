"""
gravar_relacao_djerj.py - grava no banco o credor (com CPF/CNPJ) da "relação de precatórios" do DJERJ
(TJRJ/saida/relacao_djerj.csv, que o TJRJ/relacao_djerj.py monta).

Cada linha da relação é o beneficiário oficial do precatório, com o documento completo e o originário. Regras (as do
fetch_TJRJ.py e a do usuário "a fonte corrige o banco"):

- Só liga quando o originário da relação é um originário do crédito no banco (o da lista, um credito_originario ou o
  metadata.motor do robô). Originário diferente ou crédito sem originário: não liga; vai para o metadata
  (relacao_djerj.acao = ORIGINARIO_DIFERENTE / SEM_ORIGINARIO_NO_BANCO), para análise.
- CPF ou CNPJ de pessoa (credor): liga como CREDOR com o documento (registrar_credor com p_fonte do robô: origem
  FONTE, que o recálculo das partes não apaga) e finaliza com o status do robô para credor com CPF e originário
  confirmado: SUCESSO_PROCESSO_ORIGINARIO, motivo 'cnj=<originário> regra=RELACAO_DJERJ fontes=DJERJ;
  fonte=DJERJ relação <edição> p.<página> grupo=<grupo>'. Grupos (só para contar):
    COMPLETOU_CPF   o robô já tinha o nome (metadata.credor), mesma pessoa, sem CPF;
    COLETIVA        o robô deixou em analisar (polo ativo com vários autores): liga só o credor da relação, sem
                    registrar_capa (o recálculo ligaria todo o polo ativo; ver a nota recalculo-credores-coletivo);
    HONORARIOS      o beneficiário é o advogado pessoa física (o mesmo nome do advogado da linha ou a OAB ao lado
                    do nome): CREDOR com o CPF e a OAB (a regra do TJMA/TJDFT de 07-08/10 para honorários de PF);
    NOME_DIFERENTE  o robô tinha outro nome, sem CPF (a relação é a fonte oficial do precatório: ela vence);
    PRECATORIO_MENOR o robô deixou em analisar por ser o menor do originário (POSSIVEL_HONORARIOS,
                    PRECATORIO_MENOR_DO_PROCESSO, PRECATORIOS_SEM_VALOR_PARA_COMPARAR) e o beneficiário não é advogado;
    EMPRESA         CNPJ que não é escritório de advocacia nem órgão público;
    SEM_NOME_ANTES  o resto (FALHA, pendente, sem credor).
- O banco já tem CREDOR com o mesmo documento: só o metadata (JA_TINHA) ou, se o status não era o de credor com
  CPF da fonte, também o status (CONFIRMOU).
- O banco tem CREDOR da mesma pessoa (regra de nome do corrigir_credor) com outro documento: liga o da relação e
  apaga o antigo (CORRIGIU, corrigir_credor do robô, com desfazer).
- O banco tem CREDOR de outra pessoa com documento, ou o documento da relação já é de outra pessoa no banco: não
  troca; vai para revisão (TJRJ/saida/relacao_djerj_revisao.csv) e para o metadata (REVISAO_*).
- CNPJ de escritório de advocacia (honorários de sociedade): o fetch_TJRJ.py não tem regra para sociedade com CNPJ
  (ele a tira dos candidatos a credor); aqui só marca no metadata (SOCIEDADE_ADVOGADOS) e conta (CNPJ com
  ADVOGADOS/ADVOCACIA/ADVOGADO/ASSESSORIA ou CONSULTORIA JURÍDICA no nome). Órgão público
  (o ente, fundo de previdência, centro de estudos da PGE...) também só no metadata (ORGAO_PUBLICO).
- Precatório com duas linhas de documentos diferentes nas edições: revisão (RELACAO_CONFLITANTE).

Gravação: metadata.relacao_djerj {edição, caderno, página, ordem, nome, tipo do documento, OAB, advogado,
originário, grupo, ação} no credito_fonte do robô (CONSULTA_PUBLICA_TJRJ), sem o CPF e mesclado: o resto do
metadata (motor, credor, originario_provavel...) fica como está. Nos ligados também: coleta_credor passa para o
software do robô e é finalizada (fila_credor_finalizar), as filas mensais do RPA antigo recebem o status e o
originário (atualizar_filas_mensais do robô) e a capa antiga do originário (originarios.*) recebe o credor com o
documento (gravar_capa_legado do robô; o advogado só fora de coletiva; parte do mesmo nome sem documento ganha o
documento). Sem registrar_capa.

LOTE créditos por transação, cada um no seu SAVEPOINT. Pula crédito com lease (outro robô com ele) e crédito cujo
status/motivo mudou depois da leitura. Antes do COMMIT confere, dentro da transação, que só os créditos do lote
mudaram e que cada ligado tem o documento da relação como CREDOR; senão ROLLBACK e para. --simulacao faz tudo e
dá ROLLBACK. Desfazer (só com --com-desfazer): TJRJ/saida/desfazer_relacao_djerj_<rodada>.sql.

Uso:
    python TJRJ/gravar_relacao_djerj.py --medir
    python TJRJ/gravar_relacao_djerj.py --simulacao
    python TJRJ/gravar_relacao_djerj.py --com-desfazer
    python TJRJ/gravar_relacao_djerj.py --precatorios 2023.00210-3,2021.03833-0 --simulacao
Saída: TJRJ/saida/gravar_relacao_djerj_<rodada>.csv (1 linha por crédito) e relacao_djerj_revisao.csv.
"""
import argparse
import csv
import json
import logging
import os
import socket
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import re

import psycopg2
import psycopg2.errors

AQUI = Path(__file__).resolve().parent
RAIZ = AQUI.parent
for caminho in (str(RAIZ), str(AQUI)):
    if caminho not in sys.path:
        sys.path.insert(0, caminho)

import fetch_TJRJ as robo  # noqa: E402
from utils.arquivos import anexar_csv, gravar_csv  # noqa: E402
from utils.banco import como_dicts, conectar  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

SOFTWARE = robo.SOFTWARE
TRIBUNAL = robo.TRIBUNAL_TJRJ
LOTE = 100                              # créditos por transação de gravação
SAIDA = AQUI / "saida"
ARQ_RELACAO = SAIDA / "relacao_djerj.csv"
ARQ_REVISAO = SAIDA / "relacao_djerj_revisao.csv"
STATUS = "SUCESSO_PROCESSO_ORIGINARIO"
REGRA = "RELACAO_DJERJ"
UFS_OAB = robo.UFS
# além do RE_SOCIEDADE_ADV do robô: 'Sociedade Individual de Advogado' (singular) e assessoria jurídica
RE_SOCIEDADE_MAIS = re.compile(r"\bADVOGAD[OA]S?\b|\bADVOCATICI|ASSESSORIA JURIDICA|CONSULTORIA JURIDICA")
COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "pago", "acao", "grupo", "resultado", "nome_relacao",
           "tipo_documento", "documento", "originario_relacao", "originario_banco", "edicao", "pagina",
           "status_antes", "motivo_antes", "nome_robo", "credores_antes", "credores_depois", "corrigidos"]
COLUNAS_REVISAO = ["credito_id", "precatorio", "pago", "acao", "nome_relacao", "tipo_documento", "documento",
                   "originario_relacao", "credores_banco", "nome_robo", "status_antes", "edicao", "pagina", "detalhe"]
log = logging.getLogger("gravar_relacao_djerj")

LIGA = {"LIGAR", "CORRIGIR", "CONFIRMAR"}
SO_METADATA = {"JA_TINHA", "ORIGINARIO_DIFERENTE", "SEM_ORIGINARIO_NO_BANCO", "SOCIEDADE_ADVOGADOS", "ORGAO_PUBLICO",
               "REVISAO_OUTRO_CREDOR", "REVISAO_DOCUMENTO_DE_OUTRO_NOME", "REVISAO_RELACAO_CONFLITANTE"}

SQL_MEDIDA = r"""
WITH c AS MATERIALIZED (SELECT id FROM creditos.credito WHERE tribunal_id = %s),
li AS MATERIALIZED (SELECT DISTINCT ON (x.credito_id) x.credito_id, x.metadata->>'Pago' AS pago
                      FROM creditos.lista_item x JOIN c ON c.id = x.credito_id
                     ORDER BY x.credito_id, x.removido_em NULLS FIRST, x.updated_at DESC),
k AS MATERIALIZED (SELECT DISTINCT cc.credito_id FROM creditos.credito_credor cc JOIN c ON c.id = cc.credito_id
                     JOIN creditos.pessoa p ON p.id = cc.pessoa_id
                    WHERE cc.papel_id = 1 AND p.documento IS NOT NULL)
SELECT count(*), count(k.credito_id), count(*) FILTER (WHERE li.pago = 'False'),
       count(k.credito_id) FILTER (WHERE li.pago = 'False')
  FROM c LEFT JOIN li ON li.credito_id = c.id LEFT JOIN k ON k.credito_id = c.id
"""


def medir(rotulo):
    con = conectar("gravar_relacao_djerj")
    with con.cursor() as cur:
        cur.execute(SQL_MEDIDA, (TRIBUNAL,))
        n, k, np_, kp = cur.fetchone()
    con.close()
    log.info(f"[{rotulo}] TJRJ: {k} de {n} créditos com CREDOR com CPF/CNPJ ({100 * k / n:.2f}%); não pagos: {kp} de "
             f"{np_} ({100 * kp / max(np_, 1):.2f}%)")
    return k, kp


# =============================================================================== leitura


def ler_relacao(filtro):
    """{precatório: [linhas da relação]} (só com documento de dígito válido ou que precisam de revisão)."""
    por = defaultdict(list)
    with open(ARQ_RELACAO, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f, delimiter=";"):
            if filtro and r["precatorio"] not in filtro:
                continue
            por[r["precatorio"]].append(r)
    return por


def escolher_linha(linhas):
    """A linha da relação que vale para o precatório (a edição mais recente) e se há conflito de documento."""
    validas = [x for x in linhas if x["documento_valido"] == "True"]
    docs = {x["documento"] for x in validas}
    if not validas:
        return max(linhas, key=lambda x: x["edicao"]), "DOC_INVALIDO"
    if len(docs) > 1:
        return max(validas, key=lambda x: x["edicao"]), "CONFLITO"
    return max(validas, key=lambda x: x["edicao"]), ""


SQL_LEADS = r"""
SELECT c.id AS credito_id, c.numero_exibicao, c.numero_norm, tc.codigo AS tipo_credito,
       COALESCE(li.metadata, '{}'::jsonb) AS lista, cc.software_id, cc.status_id, st.codigo AS status,
       cc.motivo_detalhe, cc.lease_worker,
       (SELECT f.metadata FROM creditos.credito_fonte f WHERE f.credito_id = c.id AND f.software_id = %(sw)s) AS meta,
       ARRAY(SELECT regexp_replace(pr.numero_cnj::text, '\D', '', 'g') FROM creditos.credito_originario co
               JOIN creditos.processo pr ON pr.id = co.processo_id WHERE co.credito_id = c.id) AS originarios,
       ARRAY(SELECT json_build_object('nome', p.nome, 'doc', p.documento::text, 'origem', k.origem)
               FROM creditos.credito_credor k JOIN creditos.pessoa p ON p.id = k.pessoa_id
              WHERE k.credito_id = c.id AND k.papel_id = 1) AS credores
  FROM creditos.credito c
  JOIN creditos.tipo_credito tc ON tc.id = c.tipo_credito_id
  LEFT JOIN creditos.coleta_credor cc ON cc.credito_id = c.id
  LEFT JOIN creditos.status_coleta st ON st.id = cc.status_id
  LEFT JOIN LATERAL (SELECT x.metadata FROM creditos.lista_item x WHERE x.credito_id = c.id
                      ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
 WHERE c.id = ANY(%(ids)s)
"""


def ler_leads(cur, precatorios, id_sw):
    """{precatório: [leads]} dos créditos do TJRJ com esses números (numero_exibicao; nos de número malformado da
    carga legada, o NumeroPrecatorio da lista)."""
    cur.execute("SELECT id, numero_exibicao FROM creditos.credito WHERE tribunal_id = %s AND numero_exibicao = ANY(%s)",
                (TRIBUNAL, precatorios))
    ids = {i: n for i, n in cur.fetchall()}
    cur.execute(r"""SELECT c.id, li.metadata->>'NumeroPrecatorio' FROM creditos.credito c
                      JOIN LATERAL (SELECT x.metadata FROM creditos.lista_item x WHERE x.credito_id = c.id
                                     ORDER BY x.removido_em NULLS FIRST, x.updated_at DESC LIMIT 1) li ON true
                     WHERE c.tribunal_id = %s AND c.numero_exibicao !~ '^\d{4}\.\d{5}-\d$'
                       AND li.metadata->>'NumeroPrecatorio' = ANY(%s)""", (TRIBUNAL, precatorios))
    ids.update({i: n for i, n in cur.fetchall()})
    por = defaultdict(list)
    lista_ids = list(ids)
    for i in range(0, len(lista_ids), 2000):
        cur.execute(SQL_LEADS, {"sw": id_sw, "ids": lista_ids[i:i + 2000]})
        for lead in como_dicts(cur):
            lead["precatorio"] = ids[lead["credito_id"]]
            orig = (lead["lista"].get("NumeroProcOriginario") or "").strip()
            lead["originario"] = so_digitos(orig) if robo.RE_ORIGINARIO.match(orig) else ""
            por[lead["precatorio"]].append(lead)
    # créditos ligados ao mesmo originário (mais de 1 = coletiva)
    origs = sorted({x["originario"] for v in por.values() for x in v if x["originario"]})
    n_por = {}
    for i in range(0, len(origs), 2000):
        cur.execute(r"""SELECT regexp_replace(pr.numero_cnj::text, '\D', '', 'g'), count(*)
                          FROM creditos.processo pr JOIN creditos.credito_originario co ON co.processo_id = pr.id
                         WHERE pr.numero_cnj = ANY(ARRAY(SELECT creditos.cnj_normalizar(x) FROM unnest(%s::text[]) x))
                         GROUP BY 1""", ([formatar_cnj(o) for o in origs[i:i + 2000]],))
        n_por.update(dict(cur.fetchall()))
    for v in por.values():
        for x in v:
            x["n_creditos_originario"] = n_por.get(x["originario"], 0)
    return por


def nomes_dos_documentos(cur, docs):
    out = {}
    docs = sorted(docs)
    for i in range(0, len(docs), 5000):
        cur.execute("""SELECT documento::text, nome FROM creditos.pessoa
                        WHERE documento = ANY(ARRAY(SELECT creditos.documento_normalizar(x) FROM unnest(%s::text[]) x))""",
                    (docs[i:i + 5000],))
        out.update(dict(cur.fetchall()))
    return out


# =============================================================================== decisão


def eh_advogado_credor(rel):
    """O beneficiário é o próprio advogado (honorários de advogado pessoa física): OAB ao lado do nome ou o mesmo nome
    do advogado da linha."""
    return bool(rel["nome_com_oab"]) or robo.mesma_pessoa(rel["nome"], rel["advogado"])


def eh_sociedade(nome_n):
    return bool(robo.RE_SOCIEDADE_ADV.search(nome_n) or RE_SOCIEDADE_MAIS.search(nome_n))


def decidir(rel, aviso, lead, nome_do_doc):
    """(ação, grupo, detalhe) do crédito."""
    doc = so_digitos(rel["documento"])
    nome_robo = ((lead["meta"] or {}).get("credor") or {}).get("nome") or ""
    if aviso == "CONFLITO":
        return "REVISAO_RELACAO_CONFLITANTE", "", "o precatório tem documentos diferentes nas edições"
    if aviso == "DOC_INVALIDO":
        return "DOC_INVALIDO", "", f"documento {rel['tipo_documento'] or '-'} sem dígito válido"
    if lead["status_id"] is None:
        return "FORA_DA_FILA", "", "crédito sem linha em coleta_credor"
    orig = so_digitos(rel["originario"])
    do_banco = {o for o in [lead["originario"], *(lead["originarios"] or []),
                            so_digitos(((lead["meta"] or {}).get("motor") or {}).get("originario") or "")] if o}
    if not do_banco:
        return "SEM_ORIGINARIO_NO_BANCO", "", ""
    if orig not in do_banco:
        return "ORIGINARIO_DIFERENTE", "", f"banco={','.join(formatar_cnj(o) for o in sorted(do_banco))}"
    nome_n = robo.normal(rel["nome"])
    if len(doc) == 14 and eh_sociedade(nome_n):
        return "SOCIEDADE_ADVOGADOS", "", ""
    lead_ente = {"chaves_ente": robo.chaves_ente(lead["lista"].get("EntidadeDevedora") or "")}
    if len(doc) == 14 and (robo.eh_orgao_publico(rel["nome"], lead_ente) or robo.RE_ORGAO_PUBLICO.search(nome_n)):
        return "ORGAO_PUBLICO", "", ""
    dono = nome_do_doc.get(doc)
    if dono and dono != "(SEM NOME)" and not robo.mesma_pessoa(dono, rel["nome"]):
        return "REVISAO_DOCUMENTO_DE_OUTRO_NOME", "", f"no banco o documento é de outro nome"
    credores = lead["credores"] or []
    mesmo_doc = [c for c in credores if so_digitos(c["doc"]) == doc]
    outros = [c for c in credores if so_digitos(c["doc"]) != doc]
    mesma_pessoa = [c for c in outros if robo.mesma_pessoa(c["nome"], rel["nome"])]
    estranhos = [c for c in outros if not robo.mesma_pessoa(c["nome"], rel["nome"])]
    if mesmo_doc:
        if lead["status"] == STATUS and not mesma_pessoa:
            return "JA_TINHA", "", f"outros credores: {len(estranhos)}" if estranhos else ""
        return ("CORRIGIR" if mesma_pessoa else "CONFIRMAR"), "", ""
    if estranhos:
        return "REVISAO_OUTRO_CREDOR", "", f"{len(estranhos)} credor(es) de outro nome com documento"
    if mesma_pessoa:
        return "CORRIGIR", "", ""
    motivo = lead["motivo_detalhe"] or ""
    if len(doc) == 11 and eh_advogado_credor(rel):
        grupo = "HONORARIOS"
    elif len(doc) == 14:
        grupo = "EMPRESA"
    elif nome_robo and robo.mesma_pessoa(nome_robo, rel["nome"]):
        grupo = "COMPLETOU_CPF"
    elif nome_robo:
        grupo = "NOME_DIFERENTE"
    elif motivo.startswith(("POSSIVEL_HONORARIOS", "PRECATORIO_MENOR", "PRECATORIOS_SEM_VALOR")):
        grupo = "PRECATORIO_MENOR"
    elif motivo.startswith("CREDOR_POLO_ATIVO_SEM_VINCULO") or lead["n_creditos_originario"] > 1:
        grupo = "COLETIVA"
    else:
        grupo = "SEM_NOME_ANTES"
    return "LIGAR", grupo, ""


# =============================================================================== gravação


class MudouDepois(Exception):
    """Outro processo mexeu no crédito entre a leitura e a gravação."""


def oab_do_credor(rel):
    """(uf, número) da OAB do próprio beneficiário (advogado credor de honorários), ou (None, None)."""
    if rel["nome_com_oab"]:
        uf, num = rel["nome_com_oab"][:2], rel["nome_com_oab"][2:]
    elif rel["oab"] and robo.mesma_pessoa(rel["nome"], rel["advogado"]):
        uf, num = rel["oab"].split(";")[0][:2], rel["oab"].split(";")[0][2:]
    else:
        return None, None
    return (uf, num) if uf in UFS_OAB else (None, None)


def mesclar_metadata(cur, lead, info):
    """metadata.relacao_djerj no credito_fonte do robô (registrar_credito troca o JSON inteiro: mescla aqui)."""
    cur.execute("SELECT metadata FROM creditos.credito_fonte WHERE credito_id = %s AND software_id = %s",
                (lead["credito_id"], lead["_id_sw"]))
    linha = cur.fetchone()
    meta = dict(linha[0] or {}) if linha else {}
    meta["relacao_djerj"] = info
    cur.execute("""SELECT 1 FROM creditos.credito WHERE id = %s
                      AND numero_norm = COALESCE(creditos.cnj_normalizar(%s), creditos.so_digitos(%s))""",
                (lead["credito_id"], lead["numero_norm"], lead["numero_norm"]))
    if not cur.fetchone():
        raise RuntimeError("número não bate com o crédito (registrar_credito criaria outro crédito)")
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => %s, p_tribunal => 'TJRJ', p_numero => %s,
                                                     p_origem => 'RASPAGEM', p_software => %s,
                                                     p_metadata => %s::jsonb)""",
                (lead["tipo_credito"], lead["numero_norm"], SOFTWARE, json.dumps(meta, ensure_ascii=False, default=str)))
    if cur.fetchone()[0] != lead["credito_id"]:
        raise RuntimeError("registrar_credito devolveu outro crédito")
    return linha is not None


def capa_legado(cur, lead, rel, coletiva, bk):
    """Capa antiga do originário: o credor (com o documento) e, fora de coletiva, o advogado; a parte do mesmo nome
    que já estava lá sem documento ganha o documento."""
    cnj = so_digitos(rel["originario"])
    advs = []
    if not coletiva and rel["oab"]:
        uf, num = rel["oab"].split(";")[0][:2], rel["oab"].split(";")[0][2:]
        if uf in UFS_OAB:
            advs = [{"nome": rel["advogado"], "polo": "ATIVO", "oab_uf": uf, "oab_numero": num}]
    dados = {"capa": {}, "partes": [{"nome": rel["nome"], "documento": so_digitos(rel["documento"]), "polo": "ATIVO",
                                     "papel": "CREDOR", "papel_bruto": "BENEFICIARIO (RELACAO DJERJ)"}],
             "advogados": advs}
    cont = robo.gravar_capa_legado(cur, cnj, dados, True, lead["precatorio"], bk)
    cur.execute(r"""SELECT pp.id, pp.nome FROM originarios.partes_processuais pp
                      JOIN originarios.processos_originarios po ON po.id = pp.processo_id
                     WHERE regexp_replace(po.numero_cnj::text, '\D', '', 'g') = %s AND pp.polo = 'ATIVO'
                       AND pp.cpf_cnpj IS NULL FOR UPDATE OF pp""", (cnj,))
    for pid, nome in cur.fetchall():
        if robo.normal(nome) == robo.normal(rel["nome"]):
            bk.update(cur, "originarios.partes_processuais", {"id": pid}, {"cpf_cnpj": None})
            cur.execute("UPDATE originarios.partes_processuais SET cpf_cnpj = %s WHERE id = %s",
                        (so_digitos(rel["documento"]), pid))
            cont["documento_preenchido"] += 1
    return cont


def gravar_credito(cur, item, rod):
    """Grava um crédito (quem chama cuida do SAVEPOINT). Devolve (antes, depois, corrigidos)."""
    lead, rel, acao, grupo = item["lead"], item["rel"], item["acao"], item["grupo"]
    cid = lead["credito_id"]
    cur.execute("""SELECT lease_worker, status_id, motivo_detalhe, software_id, motivo_id, tentativas,
                          ultima_tentativa_id, sucesso_em, updated_at
                     FROM creditos.coleta_credor WHERE credito_id = %s FOR UPDATE NOWAIT""", (cid,))
    fila = como_dicts(cur)[0]
    if fila["lease_worker"]:
        raise psycopg2.errors.LockNotAvailable("lease")
    if fila["status_id"] != lead["status_id"] or fila["motivo_detalhe"] != lead["motivo_detalhe"]:
        raise MudouDepois()
    bk = rod["bk"]
    antes = robo.credores_do_credito(cur, cid)
    cur.execute("SELECT id FROM creditos.credito_credor WHERE credito_id = %s", (cid,))
    ids_antes = {i for (i,) in cur.fetchall()}
    cur.execute("SELECT metadata FROM creditos.credito_fonte WHERE credito_id = %s AND software_id = %s",
                (cid, rod["id_sw"]))
    linha_meta = cur.fetchone()
    cnj = formatar_cnj(so_digitos(rel["originario"]))
    info = {"edicao": rel["edicao"], "caderno": rel["caderno"], "pagina": int(rel["pagina"]), "ordem": rel["ordem"],
            "nome": rel["nome"], "tipo_documento": rel["tipo_documento"], "oab": rel["oab"] or None,
            "advogado": rel["advogado"] or None, "originario": rel["originario"] or None,
            "e_outros": "e_outros" in (rel["aviso"] or ""), "acao": acao, "grupo": grupo or None, "gravado_em": datetime.now().isoformat(timespec="seconds")}
    corrigidos, cont = [], Counter()
    if acao in LIGA:
        coletiva = grupo == "COLETIVA" or lead["n_creditos_originario"] > 1
        cur.execute("SELECT id FROM creditos.processo WHERE numero_cnj = creditos.cnj_normalizar(%s)", (cnj,))
        p = cur.fetchone()
        uf, num = oab_do_credor(rel) if grupo == "HONORARIOS" else (None, None)
        cur.execute("SELECT creditos.documento_de_parte(%s)", (rel["documento"],))
        documento = cur.fetchone()[0]
        if not documento:
            raise RuntimeError("documento recusado pelo banco")
        cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                        p_documento => %s, p_oab_uf => %s, p_oab_numero => %s,
                                                        p_processo_id => %s, p_fonte => %s)""",
                    (cid, rel["nome"], documento, uf, num, p[0] if p else None, SOFTWARE))
        if acao == "CORRIGIR":
            corrigidos = robo.corrigir_credor(cur, lead, {"nome": rel["nome"], "documento": documento}, bk)
        cont += capa_legado(cur, lead, rel, coletiva, bk)
    mesclar_metadata(cur, {**lead, "_id_sw": rod["id_sw"]}, info)
    if acao in LIGA:
        motivo = f"cnj={cnj} regra={REGRA} fontes=DJERJ; fonte=DJERJ relação {rel['edicao']} p.{rel['pagina']}" + (
            f" grupo={grupo}" if grupo else f" acao={acao}")
        r = {"status": STATUS, "motivo": motivo, "originario": so_digitos(rel["originario"])}
        cont += robo.atualizar_filas_mensais(cur, rod["filas"], lead, r, rod["status_legado"], bk)
        if fila["software_id"] != rod["id_sw"]:
            cur.execute("UPDATE creditos.coleta_credor SET software_id = %s WHERE credito_id = %s", (rod["id_sw"], cid))
        depois = robo.credores_do_credito(cur, cid)
        detalhe = {"software": SOFTWARE, "regra": REGRA, "fontes": "DJERJ", "edicao": rel["edicao"],
                   "pagina": int(rel["pagina"]), "acao": acao, "grupo": grupo or None,
                   "credores_antes": robo.fmt_credores(antes), "credores_depois": robo.fmt_credores(depois)}
        cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                             p_motivo => %s, p_via => 'ORIGINARIO', p_sistema => NULL,
                                                             p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                    (cid, robo.WORKER, STATUS, motivo[:2000], cnj, json.dumps(detalhe, ensure_ascii=False, default=str),
                     socket.gethostname()))
        tentativa = cur.fetchone()[0]
        # desfazer (roda ao contrário): vínculos novos, tentativa, fila
        bk.insert(cur, "creditos.coleta_credor_tentativa", tentativa)
        fila_antes = {k: fila[k] for k in ("status_id", "motivo_id", "motivo_detalhe", "tentativas",
                                            "ultima_tentativa_id", "sucesso_em", "updated_at", "software_id")}
        bk.update(cur, "creditos.coleta_credor", {"credito_id": cid}, fila_antes)
        cur.execute("SELECT id FROM creditos.credito_credor WHERE credito_id = %s", (cid,))
        for i in sorted({i for (i,) in cur.fetchall()} - ids_antes):
            bk.insert(cur, "creditos.credito_credor", i)
    else:
        depois = antes
    if linha_meta:
        bk.update(cur, "creditos.credito_fonte", {"credito_id": cid, "software_id": rod["id_sw"]},
                  {"metadata": json.dumps(linha_meta[0], ensure_ascii=False) if linha_meta[0] is not None else None})
    else:
        bk.p_sql.append(cur.mogrify("DELETE FROM creditos.credito_fonte WHERE credito_id = %s AND software_id = %s;",
                                    (cid, rod["id_sw"])).decode())
        bk.p_csv.append(("INSERT", "creditos.credito_fonte", {"credito_id": cid, "software_id": rod["id_sw"]}, {}))
    return antes, depois, corrigidos, cont


def conferir(cur, ids_lote, ligados):
    """Dentro da transação: só os créditos do lote mudaram e cada ligado tem o documento da relação como CREDOR."""
    fora = set()
    for sql in ("SELECT DISTINCT credito_id FROM creditos.credito_credor WHERE created_at = now() OR updated_at = now()",
                "SELECT credito_id FROM creditos.coleta_credor WHERE updated_at = now()",
                "SELECT DISTINCT credito_id FROM creditos.coleta_credor_tentativa WHERE finalizada_em = now()",
                "SELECT credito_id FROM creditos.credito_fonte WHERE ultimo_em = now()"):
        cur.execute(sql)
        fora |= {i for (i,) in cur.fetchall()} - set(ids_lote)
    if fora:
        raise RuntimeError(f"conferência: mudaram créditos fora do lote: {sorted(fora)[:10]}")
    if not ligados:
        return
    cur.execute("""SELECT k.credito_id, array_agg(p.documento::text)
                     FROM creditos.credito_credor k JOIN creditos.pessoa p ON p.id = k.pessoa_id
                    WHERE k.papel_id = 1 AND k.credito_id = ANY(%s) GROUP BY 1""", (list(ligados),))
    por = dict(cur.fetchall())
    errados = [cid for cid, doc in ligados.items() if doc not in (por.get(cid) or [])]
    if errados:
        raise RuntimeError(f"conferência: créditos sem o documento da relação como CREDOR: {errados[:10]}")


def gravar_lote(args, lote, rod, total):
    con = conectar("gravar_relacao_djerj", escrita=True, worker=f"pid {os.getpid()}")
    linhas, ligados, ids = [], {}, []
    try:
        with con.cursor() as cur:
            for item in lote:
                lead, rel = item["lead"], item["rel"]
                cid, acao = lead["credito_id"], item["acao"]
                antes_txt = depois_txt = corr = ""
                resultado = acao
                if acao in LIGA or acao in SO_METADATA:
                    cur.execute("SAVEPOINT credito")
                    try:
                        antes, depois, corrigidos, _ = gravar_credito(cur, item, rod)
                        cur.execute("RELEASE SAVEPOINT credito")
                        rod["bk"].fechar_credito(cid)
                        ids.append(cid)
                        if acao in LIGA:
                            ligados[cid] = so_digitos(rel["documento"])
                        resultado = "GRAVADO"
                        antes_txt, depois_txt = robo.fmt_credores(antes), robo.fmt_credores(depois)
                        corr = " | ".join(corrigidos)
                    except psycopg2.errors.LockNotAvailable:
                        cur.execute("ROLLBACK TO SAVEPOINT credito")
                        rod["bk"].descartar()
                        resultado = "PULADO_LEASE"
                    except MudouDepois:
                        cur.execute("ROLLBACK TO SAVEPOINT credito")
                        rod["bk"].descartar()
                        resultado = "PULADO_MUDOU_DEPOIS_DA_LEITURA"
                    except (psycopg2.Error, RuntimeError) as e:
                        cur.execute("ROLLBACK TO SAVEPOINT credito")
                        rod["bk"].descartar()
                        resultado = f"ERRO: {(str(e).splitlines() or [''])[0][:200]}"
                        log.warning(f"{cid}: {resultado}")
                total[(acao, item["grupo"], resultado.split(":")[0])] += 1
                linhas.append({"processado_em": datetime.now().isoformat(timespec="seconds"),
                               "modo": "SIMULACAO" if args.simulacao else "REAL", "credito_id": cid,
                               "precatorio": lead["precatorio"], "pago": lead["lista"].get("Pago"), "acao": acao,
                               "grupo": item["grupo"], "resultado": resultado, "nome_relacao": rel["nome"],
                               "tipo_documento": rel["tipo_documento"], "documento": rel["documento"],
                               "originario_relacao": rel["originario"],
                               "originario_banco": formatar_cnj(lead["originario"]) if lead["originario"] else "",
                               "edicao": rel["edicao"], "pagina": rel["pagina"], "status_antes": lead["status"],
                               "motivo_antes": lead["motivo_detalhe"], "nome_robo": item["nome_robo"],
                               "credores_antes": antes_txt, "credores_depois": depois_txt, "corrigidos": corr})
            conferir(cur, ids, ligados)
        if args.simulacao:
            con.rollback()
        else:
            con.commit()
    except BaseException:
        con.rollback()
        rod["bk"].descartar_lote()
        raise
    finally:
        con.close()
    rod["bk"].confirmar_lote(manter=not args.simulacao)
    return linhas


def ler_argumentos():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--simulacao", action="store_true", help="faz tudo e dá ROLLBACK em cada lote")
    p.add_argument("--com-desfazer", action="store_true", help="escreve o desfazer_relacao_djerj_*.sql")
    p.add_argument("--precatorios", default="", help="números separados por vírgula")
    p.add_argument("--limite", type=int, default=10 ** 9, help="créditos a gravar (os primeiros por número)")
    p.add_argument("--medir", action="store_true", help="só mede (créditos com CREDOR com CPF/CNPJ) e sai")
    return p.parse_args()


def main():
    args = ler_argumentos()
    configurar_log(__file__, SAIDA / "logs")
    if args.medir:
        medir("agora")
        return
    robo.GERAR_DESFAZER = args.com_desfazer          # na simulação vai para desfazer_relacao_djerj_<rodada>_simulacao.sql
    robo.WORKER = f"{socket.gethostname()}:TJRJ:relacao_djerj:{os.getpid()}"
    rodada = datetime.now().strftime("%Y%m%d_%H%M%S") + ("_simulacao" if args.simulacao else "")
    filtro = {x.strip() for x in args.precatorios.split(",") if x.strip()}
    relacao = ler_relacao(filtro)
    leitura = conectar("gravar_relacao_djerj")
    with leitura.cursor() as cur:
        id_sw = robo.id_do_software(cur, criar=False)
        filas = robo.filas_antigas(cur)
        cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
        status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
        leads = ler_leads(cur, sorted(relacao), id_sw)
        escolhas = {prec: escolher_linha(v) for prec, v in relacao.items()}
        nome_do_doc = nomes_dos_documentos(cur, {so_digitos(r["documento"]) for r, a in escolhas.values()
                                                 if r["documento_valido"] == "True"})
    leitura.close()
    robo.FILAS_MENSAIS[:] = filas
    medida_antes = medir("antes")
    itens, sem_credito = [], 0
    for prec in sorted(relacao):
        rel, aviso = escolhas[prec]
        if prec not in leads:
            sem_credito += 1
            continue
        for lead in leads[prec]:
            acao, grupo, det = decidir(rel, aviso, lead, nome_do_doc)
            nome_robo = ((lead["meta"] or {}).get("credor") or {}).get("nome") or ""
            itens.append({"lead": lead, "rel": rel, "acao": acao, "grupo": grupo, "detalhe": det,
                          "nome_robo": nome_robo})
    itens = itens[:args.limite]
    decisao = Counter((x["acao"], x["grupo"]) for x in itens)
    log.info(f"TJRJ relação DJERJ: {len(relacao)} precatórios na relação, {sem_credito} sem crédito no banco, "
             f"{len(itens)} créditos; {'SIMULAÇÃO (ROLLBACK)' if args.simulacao else 'GRAVAÇÃO REAL'}; desfazer "
             f"{'sim' if robo.GERAR_DESFAZER else 'não'}; software {SOFTWARE} (id {id_sw}); filas {', '.join(filas)}")
    for (a, g), n in sorted(decisao.items()):
        log.info(f"  decisão {a}{'/' + g if g else ''}: {n}")
    revisao = [{"credito_id": x["lead"]["credito_id"], "precatorio": x["lead"]["precatorio"],
                "pago": x["lead"]["lista"].get("Pago"), "acao": x["acao"], "nome_relacao": x["rel"]["nome"],
                "tipo_documento": x["rel"]["tipo_documento"], "documento": x["rel"]["documento"],
                "originario_relacao": x["rel"]["originario"],
                "credores_banco": " | ".join(f"{c['nome']} ({'doc' if c['doc'] else 'sem doc'}, {c['origem']})"
                                             for c in x["lead"]["credores"] or []),
                "nome_robo": x["nome_robo"], "status_antes": x["lead"]["status"], "edicao": x["rel"]["edicao"],
                "pagina": x["rel"]["pagina"], "detalhe": x["detalhe"]}
               for x in itens if x["acao"].startswith("REVISAO") or x["acao"] == "ORIGINARIO_DIFERENTE"]
    gravar_csv(ARQ_REVISAO if not args.simulacao else SAIDA / "relacao_djerj_revisao_simulacao.csv", revisao,
               COLUNAS_REVISAO)
    rod = {"id_sw": id_sw, "filas": filas, "status_legado": status_legado,
           "bk": robo.Backup(SAIDA / f"desfazer_relacao_djerj_{rodada}.sql", SAIDA / "relacao_djerj_backup.csv",
                             rodada, "SIMULACAO" if args.simulacao else "REAL")}
    if robo.GERAR_DESFAZER:
        rod["bk"].arquivo_sql.write_text("-- Desfaz o gravar_relacao_djerj.py (TJRJ): por crédito, apaga os vínculos e a "
                                         "tentativa criados e devolve a fila, o metadata, as filas mensais e a capa "
                                         "antiga.\n", encoding="utf-8")
    total, arquivo = Counter(), SAIDA / f"gravar_relacao_djerj_{rodada}.csv"
    for i in range(0, len(itens), LOTE):
        anexar_csv(arquivo, COLUNAS, gravar_lote(args, itens[i:i + LOTE], rod, total))
        if (i // LOTE) % 10 == 0 or i + LOTE >= len(itens):
            log.info(f"[{min(i + LOTE, len(itens))}/{len(itens)}] gravados "
                     f"{sum(n for k, n in total.items() if k[2] == 'GRAVADO')}, pulados "
                     f"{sum(n for k, n in total.items() if k[2].startswith('PULADO'))}, erros "
                     f"{sum(n for k, n in total.items() if k[2] == 'ERRO')}")
    log.info("resultado por ação/grupo:")
    for (a, g, r), n in sorted(total.items()):
        log.info(f"  {a}{'/' + g if g else ''} -> {r}: {n}")
    medida_depois = medir("depois" + (" (simulação: igual ao antes)" if args.simulacao else ""))
    log.info(f"ganho: {medida_depois[0] - medida_antes[0]} créditos com CPF/CNPJ "
             f"({medida_depois[1] - medida_antes[1]} não pagos) | CSV: {arquivo} | revisão: {len(revisao)} linhas"
             + (f" | desfazer: {rod['bk'].arquivo_sql}" if robo.GERAR_DESFAZER else ""))


if __name__ == "__main__":
    main()
