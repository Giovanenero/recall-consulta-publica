"""
reabrir_sem_credor.py - devolve à fila os créditos do TJRR que o fetch_TJRR.py deixou em SUCESSO_ANALISAR sem credor
nem candidato para revisar (motivos CREDOR_NAO_IDENTIFICADO e FORA_DO_MURAKI).

Decisão do usuário (05/10/2026): esses leads não ficam em SUCESSO_ANALISAR (o fila_credor_repescar só reabre FALHA, então
eles nunca voltariam). O robô agora os adia por 60 dias e, na 3ª vez sem credor, grava FALHA SEM_CREDOR. Este script
faz o mesmo com os que já tinham sido gravados antes da mudança: contam como a 1ª vez.

Para cada crédito do software CONSULTA_PUBLICA_TJRR em SUCESSO_ANALISAR com esses motivos e SEM credor ligado:
- status PENDENTE;
- disponivel_em = data em que foi finalizado + 60 dias (o robô só pega depois disso);
- motivo da fila com o contador ' [sem_credor=1]' (o robô lê e soma).
Crédito com credor ligado (CONTATOS/LEGADO) fica como está. Cada UPDATE vai para
TJRR/saida/desfazer_reabrir_sem_credor_<data>.sql (o UPDATE que devolve a linha ao que era).

Uso:
    python TJRR/reabrir_sem_credor.py              # só mostra o que faria
    python TJRR/reabrir_sem_credor.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
"""
import argparse
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from utils.banco import como_dicts, conectar  # noqa: E402

SOFTWARE = "CONSULTA_PUBLICA_TJRR"
TRIBUNAL_TJRR = 123
INTERVALO = "60 days"                  # o mesmo ADIAMENTO_SEM_CREDOR do fetch_TJRR.py
MOTIVOS = ("CREDOR_NAO_IDENTIFICADO", "FORA_DO_MURAKI")

SQL_ALVO = """
SELECT cc.credito_id, cc.status_id, cc.disponivel_em, cc.motivo_detalhe, cc.sucesso_em, cc.updated_at
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id
  JOIN creditos.status_coleta st ON st.id = cc.status_id
 WHERE cc.tribunal_id = %s AND s.codigo = %s AND st.codigo = 'SUCESSO_ANALISAR'
   AND split_part(cc.motivo_detalhe, ':', 1) = ANY(%s)
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id = 1)
 ORDER BY cc.credito_id
   FOR UPDATE OF cc
"""


def main():
    ap = argparse.ArgumentParser(description="Devolve à fila os SUCESSO_ANALISAR sem credor do TJRR (1ª vez).")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, só mostra")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    con = conectar("reabrir_sem_credor_TJRR", escrita=True)
    cur = con.cursor()
    try:
        cur.execute(SQL_ALVO, (TRIBUNAL_TJRR, SOFTWARE, list(MOTIVOS)))
        alvos = como_dicts(cur)
        cur.execute("SELECT id FROM creditos.status_coleta WHERE codigo = 'PENDENTE'")
        pendente = cur.fetchone()[0]
        cont = Counter(a["motivo_detalhe"].split(":", 1)[0] for a in alvos)
        print(f"{len(alvos)} crédito(s) a devolver à fila: {dict(cont)}")
        desfazer = []
        for a in alvos:
            novo_motivo = (a["motivo_detalhe"] or "")[:1900] + " [sem_credor=1]"
            cur.execute("""UPDATE creditos.coleta_credor
                               SET status_id = %s, disponivel_em = COALESCE(%s, now()) + %s::interval,
                                   motivo_detalhe = %s, updated_at = now()
                             WHERE credito_id = %s RETURNING disponivel_em""",
                        (pendente, a["sucesso_em"] or a["updated_at"], INTERVALO, novo_motivo, a["credito_id"]))
            volta = cur.fetchone()[0]
            desfazer.append(cur.mogrify("UPDATE creditos.coleta_credor SET status_id = %s, disponivel_em = %s, "
                                        "motivo_detalhe = %s, updated_at = now() WHERE credito_id = %s;",
                                        (a["status_id"], a["disponivel_em"], a["motivo_detalhe"],
                                         a["credito_id"])).decode())
            if len(desfazer) <= 5:
                print(f"  {a['credito_id']}: volta em {volta:%d/%m/%Y} | {novo_motivo[:110]}")
        if not args.aplicar:
            con.rollback()
            print("nada gravado (rode com --aplicar).")
            return
        arquivo = AQUI / "saida" / f"desfazer_reabrir_sem_credor_{datetime.now():%Y%m%d_%H%M%S}.sql"
        arquivo.parent.mkdir(exist_ok=True)
        arquivo.write_text("-- Desfaz o reabrir_sem_credor.py (TJRR): volta cada crédito ao status de antes.\nBEGIN;\n"
                           + "\n".join(desfazer) + "\nCOMMIT;\n", encoding="utf-8")
        con.commit()
        print(f"{len(alvos)} crédito(s) devolvido(s) à fila (COMMIT). Desfazer: {arquivo}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
