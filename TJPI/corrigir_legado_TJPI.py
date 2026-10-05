"""
corrigir_legado_TJPI.py - liga o credor dos precatórios do TJPI que estão sem CREDOR, usando o que o RPA antigo (modo
credor, com A3) já trouxe da capa do PJe 2º grau e gravou com o papel errado. Não consulta nenhuma fonte.

O que aconteceu: o RPA leu o polo ativo do PJe como um texto só e gravou o requerente em precatorios.partes_processuais
e precatorios.advogados como papel ADVOGADO, com o nome emendado aos advogados ("FULANA (REQUERENTE) ADV1 (ADVOGADO)
ADV2") e o CPF certo do requerente. O espelho do legado levou isso para o schema creditos: a pessoa do CPF ficou com o
nome emendado, a parte do processo e o vínculo do crédito ficaram como ADVOGADO, e em parte dos casos a OAB do advogado
grudou na pessoa do requerente. Conferido em 05/10/2026: o CPF bate com o PDPJ em 13 de 13 precatórios.

Por crédito ativo do TJPI sem CREDOR (e com capa antiga):
- requerentes = linhas da capa antiga com "(REQUERENTE)" no nome e CPF/CNPJ válido; nome = o texto antes da marca;
- papel de cada um: pessoa física -> CREDOR; FIDC/fundo de investimento -> CESSIONARIO; sociedade de advogados ->
  continua ADVOGADO (honorários, regra do TJRR/TJRJ); ente público -> não liga; outra empresa -> CREDOR se não houver
  pessoa física, senão CESSIONARIO;
- creditos.pessoa: nome emendado vira o nome limpo; OAB que é do advogado (a capa antiga mostra o advogado com outro
  CPF) volta para a pessoa do advogado;
- creditos.processo_parte (processo do próprio precatório): a parte do requerente passa ao papel certo, com origem
  CREDITOS (o espelho do legado só mexe em linha LEGADO), para o recálculo de credores chegar ao mesmo resultado;
- creditos.credito_credor: sai o vínculo ADVOGADO do requerente, entra o vínculo certo por registrar_credor (FONTE,
  documento confirmado);
- capa antiga: a linha do requerente em partes_processuais ganha papel e nome certos; a linha dele em advogados é
  apagada (sociedade de advogados: só o nome);
- fila: fila_credor_finalizar com o status abaixo e as filas mensais do legado (2026_08 em diante).

Documento que não é do requerente não liga nada (o RPA às vezes colou o CPF do advogado na linha): o mesmo documento
para requerentes diferentes, documento de um advogado do processo com outro nome, ou CPF de pessoa do banco com outro
nome. O requerente fica só com o nome.

Status: 1 credor e nenhum cessionário -> SUCESSO_PROCESSO_CREDITO (advogado em causa própria também); 1 credor só com
nome -> SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF'; 2+ credores, cessão (fundo/empresa) ou só cessionário ->
SUCESSO_ANALISAR; só sociedade de advogados -> SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO'; só ente
público -> FALHA REQTE_ORGAO_PUBLICO.

Tudo o que muda vai para TJPI/saida/desfazer_legado_<rodada>.sql (o SQL que volta ao estado de antes, em qualquer
ordem) e TJPI/saida/corrigir_legado_backup.csv; o resultado de cada crédito vai para
TJPI/saida/corrigir_legado_<rodada>.csv.

Uso:
    python TJPI/corrigir_legado_TJPI.py                      # simulação: faz tudo e dá ROLLBACK
    python TJPI/corrigir_legado_TJPI.py --limite 50          # simulação só dos 50 primeiros
    python TJPI/corrigir_legado_TJPI.py --credito 405530 406320
    python TJPI/corrigir_legado_TJPI.py --aplicar            # grava (COMMIT a cada --lote créditos)
"""
import argparse
import json
import os
import re
import socket
import sys
import unicodedata
from collections import Counter
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

AQUI = Path(__file__).resolve().parent
RAIZ = AQUI.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from utils import banco  # noqa: E402
from utils.arquivos import anexar_csv  # noqa: E402
from utils.banco import como_dicts  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, so_digitos  # noqa: E402

SAIDA = AQUI / "saida"
TRIBUNAL_TJPI = 118
WORKER = f"{socket.gethostname()}:TJPI:corrigir_legado:{os.getpid()}"
FILA_ANTIGA_DESDE = "processos_unificados_2026_08"
LOTE = 200
PAPEL = {"CREDOR": 1, "ADVOGADO": 2, "CESSIONARIO": 5, "PUBLICO": 9}
PAPEL_LEGADO = {"CREDOR": "REQUERENTE", "CESSIONARIO": "CESSIONARIO", "PUBLICO": "OUTRO", "ADVOGADO": "ADVOGADO"}
MARCA = re.compile(r"\(REQUERENTE\)")
NOME_EMENDADO = re.compile(r"\((REQUERENTE|ADVOGADO|REQUERIDO)\)")
RE_FUNDO = re.compile(r"DIREITOS CREDITORIOS|\bFIDC\b|FUNDO DE INVESTIMENTO")
RE_PUBLICO = re.compile(r"^(ESTADO D[OEA]S? |MUNICIPIO D|CAMARA MUNICIPAL|UNIAO FEDERAL|FAZENDA (PUBLICA|NACIONAL|ESTADUAL)"
                        r"|INSTITUTO NACIONAL DO SEGURO|PROCURADORIA|DEFENSORIA PUBLICA|TRIBUNAL D|ASSEMBLEIA LEGISLATIVA"
                        r"|SECRETARIA D|PREFEITURA|FUNDO (MUNICIPAL|ESTADUAL|NACIONAL|DE (SAUDE|PREVIDENCIA)))")
RE_SOCIEDADE_ADV = re.compile(r"\bADVOGADOS\b|\bADVOCACIA\b|SOCIEDADE INDIVIDUAL DE ADVOCACIA")
COLS_CC = ["id", "credito_id", "pessoa_id", "papel_id", "lista_item_id", "processo_id", "tentativa_id", "valor_credito",
           "percentual", "documento_confirmado", "created_at", "updated_at", "origem"]
COLS_PP = ["id", "processo_id", "pessoa_id", "polo", "papel_id", "papel_bruto", "representa_parte_id", "coletado_em",
           "nome", "origem"]
COLUNAS = ["processado_em", "modo", "credito_id", "precatorio", "status", "motivo", "credores", "credores_sem_cpf",
           "cessionarios",
           "honorarios", "publicos", "oab_devolvidas", "nomes_corrigidos", "legado_partes", "legado_advogados_apagados",
           "vinculos_antes", "vinculos_depois"]
COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]

log = None


def normal(t):
    """Maiúsculas, sem acento e sem pontuação, com espaços simples."""
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", t)).strip()


def nome_limpo(nome):
    """'FULANA (REQUERENTE) ADV1 (ADVOGADO) ADV2' -> 'FULANA'."""
    return re.sub(r"\s+", " ", MARCA.split(nome or "")[0]).strip().upper()


def chave_nome(nome):
    """Nome comparável: sem acento, sem 'ESPÓLIO DE' e sem o 'REGISTRADO(A) CIVILMENTE COMO ...'."""
    n = re.sub(r"^ESPOLIO (DE )?", "", normal(nome_limpo(nome)))
    return re.split(r"\bREGISTRAD[OA]\b", n)[0].strip()


def mesma_pessoa(a, b):
    """Mesmo nome (igual, ou SequenceMatcher >= 0.9, ou um começa com o outro)."""
    a, b = chave_nome(a), chave_nome(b)
    if not a or not b:
        return False
    return a == b or a.startswith(b + " ") or b.startswith(a + " ") or SequenceMatcher(None, a, b).ratio() >= 0.9


def papel_do_requerente(nome, documento, tem_pessoa_fisica):
    """CREDOR, CESSIONARIO, ADVOGADO (sociedade de advogados) ou PUBLICO."""
    if len(documento) == 11:
        return "CREDOR"
    n = normal(nome)
    if RE_FUNDO.search(n):
        return "CESSIONARIO"
    if RE_PUBLICO.search(n):
        return "PUBLICO"
    if RE_SOCIEDADE_ADV.search(n):
        return "ADVOGADO"
    return "CESSIONARIO" if tem_pessoa_fisica else "CREDOR"


class Backup:
    """Guarda o 'antes' de cada mudança e escreve o SQL que desfaz. Só a 1ª mudança de cada linha entra (e linha
    criada aqui só é apagada), então o SQL pode rodar em qualquer ordem. Três níveis: o crédito em gravação (p_*),
    o lote aberto (l_*) e o que já teve COMMIT."""

    def __init__(self, arquivo_sql, arquivo_csv, rodada, modo):
        self.arquivo_sql, self.arquivo_csv, self.rodada, self.modo = arquivo_sql, arquivo_csv, rodada, modo
        self.tocados, self.inseridos = set(), set()
        self.descartar_lote()

    def descartar(self):
        self.p_tocados, self.p_inseridos, self.p_sql, self.p_csv = set(), set(), [], []

    def descartar_lote(self):
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
        """Guarda o DELETE da linha criada aqui."""
        self.p_inseridos.add((tabela, id_))
        self.p_sql.append(cur.mogrify(f"DELETE FROM {tabela} WHERE id = %s;", [id_]).decode())
        self.p_csv.append(("INSERT", tabela, {"id": id_}, {}))

    def delete(self, cur, tabela, linha, identidade=False):
        """Guarda o INSERT que recria a linha apagada (identidade: coluna id GENERATED ALWAYS)."""
        if self._criada_aqui(tabela, linha["id"]):
            return
        colunas = list(linha)
        sobrepor = " OVERRIDING SYSTEM VALUE" if identidade else ""
        self.p_sql.append(cur.mogrify(f"INSERT INTO {tabela} ({', '.join(colunas)}){sobrepor} VALUES "
                                      f"({', '.join(['%s'] * len(colunas))}) ON CONFLICT DO NOTHING;",
                                      list(linha.values())).decode())
        self.p_csv.append(("DELETE", tabela, {"id": linha["id"]}, linha))

    def fechar_credito(self, credito_id):
        self.l_tocados |= self.p_tocados
        self.l_inseridos |= self.p_inseridos
        if self.p_sql:
            self.l_creditos.append((credito_id, self.p_sql, self.p_csv))
        self.descartar()

    def confirmar_lote(self, manter):
        """Depois do COMMIT: escreve o SQL e o CSV de cada crédito do lote. manter=False (simulação) não escreve."""
        if manter:
            for credito_id, sql, linhas in self.l_creditos:
                novo = not self.arquivo_sql.exists()
                with open(self.arquivo_sql, "a", encoding="utf-8") as f:
                    if novo:
                        f.write("-- Desfaz as mudanças do corrigir_legado_TJPI.py (pode rodar em qualquer ordem).\n")
                    f.write(f"-- crédito {credito_id}\nBEGIN;\n" + "\n".join(reversed(sql)) + "\nCOMMIT;\n")
                anexar_csv(self.arquivo_csv, COLUNAS_BACKUP,
                           [{"rodada": self.rodada, "modo": self.modo, "credito_id": credito_id, "op": op,
                             "tabela": t, "chave": json.dumps(ch, default=str),
                             "antes": json.dumps(a, default=str, ensure_ascii=False)} for op, t, ch, a in linhas])
            self.tocados |= self.l_tocados
            self.inseridos |= self.l_inseridos
        self.descartar_lote()


def foto(cur, tabela, colunas, onde, params):
    cur.execute(f"SELECT {', '.join(colunas)} FROM {tabela} WHERE {onde} ORDER BY id", params)
    return {linha["id"]: linha for linha in como_dicts(cur)}


def guardar_diferenca(cur, bk, tabela, antes, depois):
    """Backup das linhas de uma tabela de identidade comparando a foto de antes com a de depois."""
    for id_ in depois.keys() - antes.keys():
        bk.insert(cur, tabela, id_)
    for id_ in antes.keys() - depois.keys():
        bk.delete(cur, tabela, antes[id_], identidade=True)
    for id_ in antes.keys() & depois.keys():
        mudou = {c: antes[id_][c] for c in antes[id_] if c != "id" and antes[id_][c] != depois[id_][c]}
        if mudou:
            bk.update(cur, tabela, {"id": id_}, mudou)


SQL_ALVO = """
SELECT c.id AS credito_id, c.numero_exibicao AS precatorio, c.numero_norm,
       COALESCE(c.processo_id, (SELECT pr.id FROM creditos.processo pr WHERE pr.numero_cnj = c.numero_norm)) AS processo_id
  FROM creditos.credito c
 WHERE c.tribunal_id = %(trib)s AND c.saiu_da_lista_em IS NULL
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor x WHERE x.credito_id = c.id AND x.papel_id = 1)
   AND (%(ids)s::bigint[] IS NULL OR c.id = ANY(%(ids)s::bigint[]))
 ORDER BY c.id
"""


def requerentes_do_legado(cur, lead):
    """(capas antigas, linhas de partes com a marca, linhas de advogados com a marca, docs dos advogados do processo)."""
    cur.execute("""SELECT id FROM precatorios.processos_precatorios WHERE numero_cnj IN (%s, %s)""",
                (lead["precatorio"], lead["numero_norm"]))
    capas = [i for (i,) in cur.fetchall()]
    if not capas:
        return [], [], [], {}
    cur.execute("""SELECT id, processo_id, polo, nome, cpf_cnpj, papel, origem, data_raspagem, papel_bruto, polo_bruto,
                          status
                     FROM precatorios.partes_processuais WHERE processo_id = ANY(%s) ORDER BY id FOR UPDATE""", (capas,))
    partes = como_dicts(cur)
    cur.execute("""SELECT * FROM precatorios.advogados WHERE processo_id = ANY(%s) ORDER BY id FOR UPDATE""", (capas,))
    advogados = como_dicts(cur)
    docs_adv = {so_digitos(p["cpf_cnpj"]): p["nome"] for p in partes
                if re.match(r"represent", p["papel"] or "", re.I) and not MARCA.search(p["nome"] or "")}
    return (capas, [p for p in partes if MARCA.search(p["nome"] or "")],
            [a for a in advogados if MARCA.search(a["nome"] or "")], docs_adv)


def devolver_oab(cur, bk, pessoa_id, documento, cont):
    """OAB presa na pessoa do requerente volta para o advogado que a capa antiga mostra com outro CPF."""
    cur.execute("SELECT uf, numero FROM creditos.pessoa_oab WHERE pessoa_id = %s FOR UPDATE", (pessoa_id,))
    for uf, numero in cur.fetchall():
        cur.execute("""SELECT DISTINCT pe.id
                         FROM precatorios.advogados a
                         JOIN creditos.pessoa pe ON pe.documento = regexp_replace(a.cpf_cnpj, '\\D', '', 'g')
                        WHERE upper(btrim(a.oab_uf)) = %s AND regexp_replace(a.oab_numero, '\\D', '', 'g') = %s
                          AND a.nome !~ '\\(' AND length(regexp_replace(a.cpf_cnpj, '\\D', '', 'g')) = 11
                          AND regexp_replace(a.cpf_cnpj, '\\D', '', 'g') <> %s""", (uf, numero, documento))
        donos = [i for (i,) in cur.fetchall()]
        if len(donos) != 1:
            cont["oab_sem_dono"] += 1
            continue
        bk.update(cur, "creditos.pessoa_oab", {"uf": uf, "numero": numero}, {"pessoa_id": pessoa_id})
        cur.execute("UPDATE creditos.pessoa_oab SET pessoa_id = %s WHERE uf = %s AND numero = %s",
                    (donos[0], uf, numero))
        cont["oab_devolvidas"] += 1


def corrigir_parte(cur, lead, pessoa_id, papel, nome, cont):
    """A parte do requerente no processo do precatório passa ao papel certo (origem CREDITOS)."""
    if not lead["processo_id"]:
        return
    novo = PAPEL[papel]
    bruto = "ATIVO / " + PAPEL_LEGADO[papel]
    cur.execute("""SELECT id, papel_id FROM creditos.processo_parte
                    WHERE processo_id = %s AND pessoa_id = %s AND polo = 'ATIVO' AND papel_id IN (2, %s)
                    FOR UPDATE""", (lead["processo_id"], pessoa_id, novo))
    linhas = cur.fetchall()
    certa = [i for i, p in linhas if p == novo]
    erradas = [i for i, p in linhas if p == 2 and novo != 2]
    if papel == "ADVOGADO":                     # sociedade de advogados: continua ADVOGADO, só o nome fica limpo
        cur.execute("UPDATE creditos.processo_parte SET nome = %s WHERE id = ANY(%s) AND nome IS DISTINCT FROM %s",
                    (nome, [i for i, _ in linhas], nome))
        return
    if certa:
        cur.execute("UPDATE creditos.processo_parte SET nome = %s, origem = 'CREDITOS' WHERE id = ANY(%s)",
                    (nome, certa))
        if erradas:
            cur.execute("DELETE FROM creditos.processo_parte WHERE id = ANY(%s)", (erradas,))
    elif erradas:
        cur.execute("""UPDATE creditos.processo_parte
                          SET papel_id = %s, papel_bruto = %s, nome = %s, origem = 'CREDITOS', coletado_em = now()
                        WHERE id = %s""", (novo, bruto, nome, erradas[0]))
        if erradas[1:]:
            cur.execute("DELETE FROM creditos.processo_parte WHERE id = ANY(%s)", (erradas[1:],))
    elif papel != "PUBLICO":
        cur.execute("""INSERT INTO creditos.processo_parte (processo_id, pessoa_id, nome, polo, papel_id, papel_bruto,
                                                            origem)
                       VALUES (%s, %s, %s, 'ATIVO', %s, %s, 'CREDITOS')""",
                    (lead["processo_id"], pessoa_id, nome, novo, bruto))
    cont["partes_corrigidas"] += 1


def status_do_credito(papeis, docs_adv, sem_cpf):
    """(status, motivo) pelos papéis dos requerentes; sem_cpf: requerentes cujo documento do legado não é deles."""
    def lista(p):
        return [f"{n} ({d})" for d, (n, papel) in papeis.items() if papel == p]
    credores, cess, honor, pub = lista("CREDOR"), lista("CESSIONARIO"), lista("ADVOGADO"), lista("PUBLICO")
    credores += [f"{n} (sem CPF: {porque})" for n, porque in sem_cpf]
    base = "CORRECAO_LEGADO_TJPI: requerente lido como advogado pelo RPA; "
    if len(credores) == 1 and not cess:
        if sem_cpf:
            return ("SUCESSO_INCOMPLETO", f"SUCESSO_SEM_CPF: credor {sem_cpf[0][0]}; {sem_cpf[0][1]} "
                                          f"(CORRECAO_LEGADO_TJPI)")
        doc = next(d for d, (_, p) in papeis.items() if p == "CREDOR")
        proprio = " (advoga em causa própria)" if doc in docs_adv else ""
        return "SUCESSO_PROCESSO_CREDITO", base + f"credor {credores[0]}{proprio}"
    if not credores:
        if honor:
            return ("SUCESSO_INCOMPLETO", f"SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO beneficiario={honor[0]} doc=sim "
                                          f"(CORRECAO_LEGADO_TJPI)")
        if cess:
            return "SUCESSO_ANALISAR", base + f"SO_CESSIONARIO: {'; '.join(cess)}"
        return "FALHA", f"REQTE_ORGAO_PUBLICO: {'; '.join(pub)} (CORRECAO_LEGADO_TJPI)"
    if cess:
        return "SUCESSO_ANALISAR", base + f"CESSAO: credor(es) {'; '.join(credores)}; cessionario(s) {'; '.join(cess)}"
    return "SUCESSO_ANALISAR", base + f"VARIOS_REQUERENTES: {len(credores)}: {'; '.join(credores)}"


def atualizar_filas_mensais(cur, filas, lead, status, motivo, status_legado, bk, cont):
    """Status e motivo nas linhas do precatório nas filas mensais (pula o que o RPA está processando)."""
    for fila in filas:
        cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead,
                               ultima_atualizacao
                          FROM {fila}
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = 'TJPI'
                           FOR UPDATE""", (lead["numero_norm"][:20].rjust(20, "0"),))
        for linha in como_dicts(cur):
            if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
                cont["fila_pulada"] += 1
                continue
            chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                     "tribunal_origem": "TJPI"}
            bk.update(cur, fila, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                             "ultima_atualizacao")})
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJPI'
                               AND deleted = false""",
                        (status_legado[status], motivo[:500], linha["id_processo"], linha["numero_precatorio"]))
            cont["fila_mensal"] += 1


def corrigir(cur, lead, filas, status_legado, bk):
    """Corrige um crédito. Devolve a linha do CSV de resultado (status None = nada a fazer)."""
    cont = Counter()
    linha = {"credito_id": lead["credito_id"], "precatorio": lead["precatorio"]}
    cur.execute("SELECT * FROM creditos.coleta_credor WHERE credito_id = %s FOR UPDATE", (lead["credito_id"],))
    fila = como_dicts(cur)
    if not fila:
        return {**linha, "status": None, "motivo": "FORA_DA_FILA"}
    if fila[0]["lease_worker"]:
        return {**linha, "status": None, "motivo": f"RESERVADO_POR {fila[0]['lease_worker']}"}
    capas, partes, advogados, docs_adv = requerentes_do_legado(cur, lead)
    if not capas:
        return {**linha, "status": None, "motivo": "SEM_CAPA_LEGADA"}
    nomes_do_doc = {}
    for p in partes:
        doc = so_digitos(p["cpf_cnpj"])
        if documento_valido(doc):
            nomes_do_doc.setdefault(doc, [])
            if not any(mesma_pessoa(nome_limpo(p["nome"]), n) for n in nomes_do_doc[doc]):
                nomes_do_doc[doc].append(nome_limpo(p["nome"]))
        else:
            cont["doc_invalido"] += 1
    if not nomes_do_doc:
        return {**linha, "status": None, "motivo": "SEM_REQUERENTE_COM_DOCUMENTO"}
    # o RPA às vezes colou o CPF do advogado na linha do requerente: documento que é de outra pessoa não liga
    sem_cpf = [(n, f"o mesmo documento {doc} aparece para {len(ns)} requerentes")
               for doc, ns in nomes_do_doc.items() if len(ns) > 1 for n in ns]
    requerentes = {doc: ns[0] for doc, ns in nomes_do_doc.items() if len(ns) == 1}
    for doc, nome in list(requerentes.items()):
        if doc in docs_adv and not mesma_pessoa(nome, docs_adv[doc]):
            sem_cpf.append((nome, f"o documento {doc} do legado é do advogado {docs_adv[doc]}"))
        else:
            cur.execute("SELECT nome FROM creditos.pessoa WHERE documento = %s", (doc,))
            achada = cur.fetchone()
            if not achada or mesma_pessoa(nome, achada[0]) or len(doc) == 14:
                continue
            sem_cpf.append((nome, f"o documento {doc} do legado é de {achada[0][:80]} no banco"))
        del requerentes[doc]
        cont["documento_de_outra_pessoa"] += 1
    if not requerentes:
        status, motivo = status_do_credito({}, docs_adv, sem_cpf)
        return finalizar(cur, lead, fila[0], status, motivo, {}, sem_cpf, filas, status_legado, bk, cont, linha, {}, {})
    tem_pf = any(len(d) == 11 for d in requerentes)
    papeis = {d: (n, papel_do_requerente(n, d, tem_pf)) for d, n in requerentes.items()}

    cc_antes = foto(cur, "creditos.credito_credor", COLS_CC, "credito_id = %s", (lead["credito_id"],))
    pp_antes = foto(cur, "creditos.processo_parte", COLS_PP, "processo_id = %s", (lead["processo_id"],))
    for doc, (nome, papel) in papeis.items():
        cur.execute("SELECT id, nome FROM creditos.pessoa WHERE documento = %s FOR UPDATE", (doc,))
        achada = cur.fetchone()
        if achada and NOME_EMENDADO.search(achada[1]):
            bk.update(cur, "creditos.pessoa", {"id": achada[0]}, {"nome": achada[1]})
            cur.execute("UPDATE creditos.pessoa SET nome = %s WHERE id = %s", (nome, achada[0]))
            cont["nomes_corrigidos"] += 1
        if papel in ("CREDOR", "CESSIONARIO"):
            cur.execute("SELECT creditos.registrar_credor(%s, %s, %s, %s, NULL, NULL, NULL, %s, %s)",
                        (lead["credito_id"], papel, nome, doc, lead["processo_id"], "CORRECAO_LEGADO_TJPI"))
            cur.execute("SELECT id FROM creditos.pessoa WHERE documento = %s", (doc,))
            achada = (cur.fetchone()[0], nome)
        if not achada:
            continue
        pessoa_id = achada[0]
        if papel != "ADVOGADO":
            cur.execute("DELETE FROM creditos.credito_credor WHERE credito_id = %s AND pessoa_id = %s AND papel_id = 2",
                        (lead["credito_id"], pessoa_id))
            devolver_oab(cur, bk, pessoa_id, doc, cont)
        corrigir_parte(cur, lead, pessoa_id, papel, nome, cont)
    cc_depois = foto(cur, "creditos.credito_credor", COLS_CC, "credito_id = %s", (lead["credito_id"],))
    pp_depois = foto(cur, "creditos.processo_parte", COLS_PP, "processo_id = %s", (lead["processo_id"],))
    guardar_diferenca(cur, bk, "creditos.credito_credor", cc_antes, cc_depois)
    guardar_diferenca(cur, bk, "creditos.processo_parte", pp_antes, pp_depois)

    # capa antiga: o requerente ganha papel e nome certos; a linha dele entre os advogados sai
    for p in partes:
        doc = so_digitos(p["cpf_cnpj"])
        if doc not in papeis:
            continue
        nome, papel = papeis[doc]
        bk.update(cur, "precatorios.partes_processuais", {"id": p["id"]},
                  {"nome": p["nome"], "papel": p["papel"], "papel_bruto": p["papel_bruto"]})
        cur.execute("UPDATE precatorios.partes_processuais SET nome = %s, papel = %s, papel_bruto = %s WHERE id = %s",
                    (nome, PAPEL_LEGADO[papel], PAPEL_LEGADO[papel], p["id"]))
        cont["legado_partes"] += 1
    for a in advogados:
        doc = so_digitos(a["cpf_cnpj"])
        if doc not in papeis:
            continue
        nome, papel = papeis[doc]
        if papel == "ADVOGADO":
            bk.update(cur, "precatorios.advogados", {"id": a["id"]}, {"nome": a["nome"]})
            cur.execute("UPDATE precatorios.advogados SET nome = %s WHERE id = %s", (nome, a["id"]))
        else:
            bk.delete(cur, "precatorios.advogados", a)
            cur.execute("DELETE FROM precatorios.advogados WHERE id = %s", (a["id"],))
            cont["legado_advogados_apagados"] += 1

    status, motivo = status_do_credito(papeis, docs_adv, sem_cpf)
    return finalizar(cur, lead, fila[0], status, motivo, papeis, sem_cpf, filas, status_legado, bk, cont, linha,
                     cc_antes, cc_depois)


def finalizar(cur, lead, fila, status, motivo, papeis, sem_cpf, filas, status_legado, bk, cont, linha, cc_antes,
              cc_depois):
    """Fila (fila_credor_finalizar + filas mensais) e a linha do CSV de resultado."""
    antes = {k: fila[k] for k in ("status_id", "motivo_id", "motivo_detalhe", "sucesso_em", "ultima_tentativa_id",
                                  "tentativas")}
    bk.update(cur, "creditos.coleta_credor", {"credito_id": lead["credito_id"]}, antes)
    detalhe = {"fonte": "precatorios.partes_processuais (RPA credor, PJe 2g com A3), leitura corrigida",
               "script": "TJPI/corrigir_legado_TJPI.py",
               "requerentes": [{"nome": n, "documento": d, "papel": p} for d, (n, p) in papeis.items()]
               + [{"nome": n, "documento": None, "papel": "CREDOR", "obs": porque} for n, porque in sem_cpf]}
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => %s,
                                                         p_motivo => %s, p_via => 'PRECATORIO', p_sistema => 'PJE',
                                                         p_processo_cnj => %s, p_detalhe => %s::jsonb, p_host => %s)""",
                (lead["credito_id"], WORKER, status, motivo, lead["numero_norm"], json.dumps(detalhe, ensure_ascii=False),
                 socket.gethostname()))
    bk.p_sql.append(f"DELETE FROM creditos.coleta_credor_tentativa WHERE id = {int(cur.fetchone()[0])};")
    atualizar_filas_mensais(cur, filas, lead, status, motivo, status_legado, bk, cont)

    def nomes(p):
        return " | ".join(f"{n} ({d})" for d, (n, x) in papeis.items() if x == p)
    return {**linha, "status": status, "motivo": motivo, "credores": nomes("CREDOR"),
            "credores_sem_cpf": " | ".join(f"{n} ({porque})" for n, porque in sem_cpf),
            "cessionarios": nomes("CESSIONARIO"), "honorarios": nomes("ADVOGADO"), "publicos": nomes("PUBLICO"),
            "oab_devolvidas": cont["oab_devolvidas"], "nomes_corrigidos": cont["nomes_corrigidos"],
            "legado_partes": cont["legado_partes"], "legado_advogados_apagados": cont["legado_advogados_apagados"],
            "vinculos_antes": len(cc_antes), "vinculos_depois": len(cc_depois), "_cont": cont}


def main():
    global log
    ap = argparse.ArgumentParser(description="Liga o credor do TJPI a partir da capa antiga lida errado pelo RPA.")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, simulação (ROLLBACK)")
    ap.add_argument("--limite", type=int, help="só os N primeiros créditos")
    ap.add_argument("--credito", type=int, nargs="+", help="só estes créditos")
    ap.add_argument("--lote", type=int, default=LOTE, help=f"créditos por transação (padrão {LOTE})")
    args = ap.parse_args()
    log = configurar_log(__file__, SAIDA / "logs")
    rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
    modo = "CORRECAO" if args.aplicar else "SIMULACAO"
    bk = Backup(SAIDA / f"desfazer_legado_{rodada}.sql", SAIDA / "corrigir_legado_backup.csv", rodada, modo)
    csv_saida = SAIDA / f"corrigir_legado_{rodada}{'' if args.aplicar else '_simulacao'}.csv"

    con = banco.conectar("corrigir_legado_TJPI", escrita=True, worker=WORKER)
    total, por_status, cont_geral, erros = 0, Counter(), Counter(), 0
    try:
        with con.cursor() as cur:
            cur.execute(SQL_ALVO, {"trib": TRIBUNAL_TJPI, "ids": args.credito})
            alvos = como_dicts(cur)[: args.limite] if args.limite else como_dicts(cur)
            filas, sem_permissao = banco.filas_antigas(cur, FILA_ANTIGA_DESDE)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
            con.commit()
            log.info(f"{modo}: {len(alvos)} crédito(s) do TJPI sem CREDOR; filas mensais {filas} "
                     f"(sem permissão: {sem_permissao})")
            resultados = []
            for i, lead in enumerate(alvos, 1):
                cur.execute("SAVEPOINT credito")
                try:
                    r = corrigir(cur, lead, filas, status_legado, bk)
                    cur.execute("RELEASE SAVEPOINT credito")
                    bk.fechar_credito(lead["credito_id"])
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT credito")
                    bk.descartar()
                    erros += 1
                    log.error(f"{lead['credito_id']} {lead['precatorio']}: {type(e).__name__}: {e}")
                    r = {"credito_id": lead["credito_id"], "precatorio": lead["precatorio"], "status": "ERRO",
                         "motivo": f"{type(e).__name__}: {e}"[:500]}
                cont_geral.update(r.pop("_cont", Counter()))
                por_status[r["status"] or f"PULADO {r['motivo'].split(' ')[0]}"] += 1
                total += 1
                resultados.append({"processado_em": datetime.now().isoformat(timespec="seconds"), "modo": modo, **r})
                if i % args.lote == 0 or i == len(alvos):
                    if args.aplicar:
                        con.commit()
                        bk.confirmar_lote(manter=True)
                    else:
                        con.rollback()
                        bk.confirmar_lote(manter=False)
                    anexar_csv(csv_saida, COLUNAS, resultados)
                    resultados = []
                    log.info(f"{i}/{len(alvos)} | {dict(por_status)} | erros {erros}")
        log.info(f"FIM {modo}: {total} crédito(s) | {dict(por_status)} | {dict(cont_geral)} | resultado {csv_saida}")
        if args.aplicar:
            log.info(f"SQL de desfazer: {bk.arquivo_sql}")
        else:
            log.info("Simulação: nada foi gravado (ROLLBACK). Para gravar: --aplicar")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
