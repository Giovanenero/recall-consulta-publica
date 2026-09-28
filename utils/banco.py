"""
banco.py - Postgres dos robôs: conexão (credenciais no .env da raiz) e consultas que mais de um robô usa.

.env: PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD.
"""
import os
import re
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

RE_FILA_ANTIGA = re.compile(r"listas_primarias\.processos_unificados_\d{4}_\d{2}")


def conectar(aplicacao, escrita=False, worker=""):
    """Leitura: sessão readonly em autocommit (qualquer escrita dá erro no Postgres e nada fica preso enquanto o robô
    raspa). Escrita: transação manual (quem chama faz COMMIT ou ROLLBACK). O application_name aparece no
    pg_stat_activity como '<aplicacao> (leitura|escrita) <worker>' (o Postgres corta em 63 caracteres)."""
    modo = "escrita" if escrita else "leitura"
    con = psycopg2.connect(
        host=os.environ["PG_HOST"], port=os.environ["PG_PORT"], dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"], connect_timeout=15,
        application_name=f"{aplicacao} ({modo}) {worker}".strip())
    if escrita:
        con.autocommit = False
    else:
        con.set_session(readonly=True, autocommit=True)
    return con


def como_dicts(cur):
    """Linhas do último SELECT como lista de dicts (coluna -> valor)."""
    colunas = [d[0] for d in cur.description]
    return [dict(zip(colunas, linha)) for linha in cur.fetchall()]


def partes_do_banco(cur, numero_cnj):
    """Partes e advogados (com documento e OAB) que o schema creditos tem para o processo."""
    cur.execute("""SELECT COALESCE(pa.nome_chave, creditos.chave_texto(pe.nome)) AS chave,
                          COALESCE(pa.nome, pe.nome) AS nome, pa.polo, pp.codigo AS papel, pa.papel_bruto,
                          pe.documento::text AS documento, o.uf::text AS oab_uf, o.numero AS oab_numero
                     FROM creditos.processo pr
                     JOIN creditos.processo_parte pa ON pa.processo_id = pr.id
                     JOIN creditos.papel_parte pp    ON pp.id = pa.papel_id
                     LEFT JOIN creditos.pessoa pe    ON pe.id = pa.pessoa_id
                     LEFT JOIN creditos.pessoa_oab o ON o.pessoa_id = pa.pessoa_id
                    WHERE pr.numero_cnj = creditos.cnj_normalizar(%s)""", (numero_cnj,))
    return como_dicts(cur)


def chave_texto_lote(cur, nomes):
    """creditos.chave_texto de cada nome, na mesma ordem, numa consulta só."""
    cur.execute("SELECT creditos.chave_texto(x) FROM unnest(%s::text[]) WITH ORDINALITY t(x, i) ORDER BY i",
                (list(nomes),))
    return [c for (c,) in cur.fetchall()]


def filas_antigas(cur, desde):
    """Filas mensais do robô de credor legado (listas_primarias.processos_unificados_AAAA_MM, de `desde` em diante):
    (com permissão de UPDATE, sem permissão)."""
    cur.execute("""SELECT 'listas_primarias.' || tablename,
                          has_table_privilege(current_user, 'listas_primarias.' || tablename, 'UPDATE')
                     FROM pg_tables
                    WHERE schemaname = 'listas_primarias' AND tablename >= %s
                      AND tablename ~ '^processos_unificados_[0-9]{4}_[0-9]{2}$'
                    ORDER BY tablename""", (desde,))
    # o nome da fila entra no SQL por f-string: só passa o formato exato
    filas = [(tabela, pode) for tabela, pode in cur.fetchall() if RE_FILA_ANTIGA.fullmatch(tabela)]
    return [t for t, pode in filas if pode], [t for t, pode in filas if not pode]


def id_do_software(cur, codigo, nome, descricao, raspa_credor, criar=True):
    """Id do software em creditos.software; com criar, cadastra na 1ª vez (menor id livre entre 4 e 99).
    raspa_credor=True faz o registrar_credito pôr o crédito na fila (coleta_credor) deste software."""
    cur.execute("SELECT id FROM creditos.software WHERE codigo = %s", (codigo,))
    linha = cur.fetchone()
    if linha or not criar:
        return linha[0] if linha else None
    cur.execute("SELECT min(g) FROM generate_series(4, 99) g WHERE g NOT IN (SELECT id FROM creditos.software)")
    novo_id = cur.fetchone()[0]
    cur.execute("""INSERT INTO creditos.software (id, codigo, geracao, nome, raspa_credor, descricao)
                   VALUES (%s, %s, 'v2', %s, %s, %s)
                   ON CONFLICT (codigo) DO NOTHING RETURNING id""", (novo_id, codigo, nome, raspa_credor, descricao))
    linha = cur.fetchone()
    return linha[0] if linha else id_do_software(cur, codigo, nome, descricao, raspa_credor, criar=False)
