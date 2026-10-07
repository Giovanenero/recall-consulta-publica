"""
sincronizar_filas_mensais.py - leva para as filas mensais do RPA antigo (listas_primarias.processos_unificados_AAAA_MM)
o resultado que o fetch_TJRR.py já gravou no banco novo, nas linhas do precatório que ficaram com o status vazio.

O que aconteceu: nas rodadas de 02 e 05/10/2026 (RECALL-0144) o robô só atualizou a fila 2026_08; a 2026_09 ficou com
o status vazio em 1.339 créditos já finalizados (auditoria de 07/10). Com o status vazio, o RPA antigo (token A3) pode
pegar o precatório de novo por essa fila. As rodadas a partir de 07/10 já atualizam as duas filas.

Para cada crédito do software CONSULTA_PUBLICA_TJRR finalizado (grupo SUCESSO ou FALHA), em cada fila mensal com
permissão de UPDATE, só nas linhas com status_coleta_lead vazio:
- status_coleta_lead = o código legado do status do banco novo (creditos.status_coleta.codigo_legado);
- motivo_coleta_lead = o motivo da fila (coleta_credor.motivo_detalhe);
- numero_originario = o originário que o robô escolheu (metadata.motor.originario), sem apagar o que já havia;
- ultima_atualizacao = agora.
Linha com status preenchido (pelo robô, pelo RPA antigo ou por outro script) não é tocada. Cada mudança vai para
TJRR/saida/desfazer_sincronizar_filas_<data>.sql (o UPDATE que devolve a linha ao que era).

Uso:
    python TJRR/sincronizar_filas_mensais.py              # só mostra o que faria (ROLLBACK)
    python TJRR/sincronizar_filas_mensais.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
"""
import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI) not in sys.path:
    sys.path.insert(0, str(AQUI))

import fetch_TJRR as F  # noqa: E402
from utils.banco import como_dicts  # noqa: E402

SQL_ALVOS = """
WITH f AS MATERIALIZED (
    SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead, numero_originario,
           ultima_atualizacao, lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') AS n20
      FROM {fila}
     WHERE tribunal_origem = 'TJRR' AND deleted = false AND numero_precatorio IS NOT NULL
       AND status_coleta_lead IS NULL),
r AS MATERIALIZED (
    SELECT cc.credito_id, lpad(left(c.numero_norm, 20), 20, '0') AS n20, st.codigo AS status, cc.motivo_detalhe,
           fo.metadata->'motor'->>'originario' AS originario
      FROM creditos.coleta_credor cc
      JOIN creditos.status_coleta st ON st.id = cc.status_id
      JOIN creditos.software s       ON s.id = cc.software_id AND s.codigo = %(software)s
      JOIN creditos.credito c        ON c.id = cc.credito_id
      LEFT JOIN creditos.credito_fonte fo ON fo.credito_id = cc.credito_id AND fo.software_id = cc.software_id
     WHERE cc.tribunal_id = %(tribunal)s AND (st.grupo = 'SUCESSO' OR st.codigo = 'FALHA'))
SELECT r.credito_id, r.status, r.motivo_detalhe, r.originario, f.id_processo, f.numero_precatorio,
       f.status_coleta_lead, f.motivo_coleta_lead, f.numero_originario, f.ultima_atualizacao
  FROM r JOIN f ON f.n20 = r.n20
 ORDER BY r.credito_id
"""


def main():
    ap = argparse.ArgumentParser(description="Preenche nas filas mensais o status que o robô do TJRR já gravou.")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, só mostra (ROLLBACK)")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_desfazer = F.SAIDA / f"desfazer_sincronizar_filas_{rodada}.sql"
    bk = F.Backup(sql_desfazer, F.SAIDA / "fetch_legado_backup.csv", rodada,
                  "SINCRONIZAR" if args.aplicar else "SINCRONIZAR_TESTE")
    con = F.conectar(escrita=True)
    try:
        with con.cursor() as cur:
            filas = F.filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
            cont, por_credito = Counter(), defaultdict(int)
            for fila in filas:
                cur.execute(SQL_ALVOS.format(fila=fila), {"software": F.SOFTWARE, "tribunal": F.TRIBUNAL_TJRR})
                linhas = como_dicts(cur)
                for ln in linhas:
                    status = status_legado[ln["status"]]
                    chave = {"id_processo": ln["id_processo"], "numero_precatorio": ln["numero_precatorio"],
                             "tribunal_origem": "TJRR"}
                    bk.update(cur, fila, chave, {k: ln[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                                   "numero_originario", "ultima_atualizacao")})
                    cur.execute(f"""UPDATE {fila}
                                       SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                           numero_originario = COALESCE(%s::text[], numero_originario),
                                           ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                                     WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = 'TJRR'
                                       AND deleted = false AND status_coleta_lead IS NULL""",
                                (status, (ln["motivo_detalhe"] or "")[:500],
                                 [ln["originario"]] if ln["originario"] else None,
                                 ln["id_processo"], ln["numero_precatorio"]))
                    if cur.rowcount:
                        cont[(fila.split(".")[-1], status)] += 1
                        por_credito[ln["credito_id"]] += 1
                    bk.fechar_credito(ln["credito_id"])
            for (fila, status), n in sorted(cont.items()):
                print(f"  {fila}: {n:5d} linha(s) -> {status}")
            print(f"{sum(cont.values())} linha(s) preenchida(s) em {len(por_credito)} crédito(s); filas: {', '.join(filas)}")
        if not args.aplicar:
            con.rollback()
            bk.descartar_lote()
            print("Nada foi gravado (rode com --aplicar).")
            return
        con.commit()
        bk.confirmar_lote(manter=True)
        print(f"COMMIT. SQL de desfazer: {sql_desfazer}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
