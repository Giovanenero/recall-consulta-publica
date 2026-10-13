"""
reabrir_nao_encontrado.py - devolve à fila os créditos do TJMA que o fetch_TJMA.py gravou como FALHA
PROCESSO_NAO_ENCONTRADO na primeira tentativa, para o robô buscar de novo.

Desde 09/10/2026 o robô repete a busca uma vez antes de aceitar "não encontrado" e, se continuar vazio, devolve o
crédito para a fila em 7 dias; só na 3ª vez grava FALHA (contador '[nao_encontrado=N]' no motivo da fila). Os FALHA
gravados antes disso nunca voltariam sozinhos: o robô só pega PENDENTE. Este script faz o empurrão inicial.

Para cada crédito do software CONSULTA_PUBLICA_TJMA em FALHA com motivo PROCESSO_NAO_ENCONTRADO e SEM credor ligado
(ainda na lista e sem lease):
- status PENDENTE e disponivel_em = agora (o robô pega na próxima rodada);
- o motivo da fila ganha o prefixo 'REABERTO_NAO_ENCONTRADO: ' (o robô troca pelo resultado novo).
Cada UPDATE vai para TJMA/saida/desfazer_reabrir_nao_encontrado_<data>.sql (o UPDATE que devolve a linha ao que era).

Uso:
    python TJMA/reabrir_nao_encontrado.py              # só mostra o que faria
    python TJMA/reabrir_nao_encontrado.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from dotenv import load_dotenv  # noqa: E402

from utils.banco import como_dicts, conectar  # noqa: E402

load_dotenv(AQUI.parent / ".env")

SOFTWARE = "CONSULTA_PUBLICA_TJMA"
TRIBUNAL_TJMA = 110
PREFIXO = "REABERTO_NAO_ENCONTRADO: "
MOTIVO_LIKE = "PROCESSO\\_NAO\\_ENCONTRADO%"
DESCRICAO = "FALHA PROCESSO_NAO_ENCONTRADO"
NOME = "reabrir_nao_encontrado"

SQL_ALVO = """
SELECT cc.credito_id, cc.status_id, cc.disponivel_em, cc.motivo_detalhe
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id
  JOIN creditos.status_coleta st ON st.id = cc.status_id
  JOIN creditos.credito c        ON c.id = cc.credito_id
 WHERE cc.tribunal_id = %s AND s.codigo = %s AND st.codigo = 'FALHA' AND c.saiu_da_lista_em IS NULL
   AND cc.motivo_detalhe LIKE %s
   AND cc.lease_worker IS NULL
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id = 1)
 ORDER BY cc.credito_id
   FOR UPDATE OF cc
"""


def reabrir(prefixo=PREFIXO, motivo_like=MOTIVO_LIKE, descricao=DESCRICAO, nome=NOME):
    ap = argparse.ArgumentParser(description=f"Devolve à fila os {descricao} do TJMA.")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, só mostra")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    con = conectar(f"{nome}_TJMA", escrita=True)
    cur = con.cursor()
    try:
        cur.execute(SQL_ALVO, (TRIBUNAL_TJMA, SOFTWARE, motivo_like))
        alvos = como_dicts(cur)
        cur.execute("SELECT id FROM creditos.status_coleta WHERE codigo = 'PENDENTE'")
        pendente = cur.fetchone()[0]
        print(f"{len(alvos)} crédito(s) {descricao} sem credor a devolver à fila")
        desfazer = []
        for a in alvos:
            cur.execute("""UPDATE creditos.coleta_credor
                               SET status_id = %s, disponivel_em = now(), motivo_detalhe = %s, updated_at = now()
                             WHERE credito_id = %s""",
                        (pendente, (prefixo + (a["motivo_detalhe"] or ""))[:2000], a["credito_id"]))
            desfazer.append(cur.mogrify("UPDATE creditos.coleta_credor SET status_id = %s, disponivel_em = %s, "
                                        "motivo_detalhe = %s, updated_at = now() WHERE credito_id = %s;",
                                        (a["status_id"], a["disponivel_em"], a["motivo_detalhe"],
                                         a["credito_id"])).decode())
            if len(desfazer) <= 5:
                print(f"  {a['credito_id']}: {(a['motivo_detalhe'] or '')[:110]}")
        if not args.aplicar:
            con.rollback()
            print("nada gravado (rode com --aplicar).")
            return
        arquivo = AQUI / "saida" / f"desfazer_{nome}_{datetime.now():%Y%m%d_%H%M%S}.sql"
        arquivo.parent.mkdir(exist_ok=True)
        arquivo.write_text(f"-- Desfaz o {nome}.py (TJMA): volta cada crédito ao status de antes.\n"
                           "BEGIN;\n" + "\n".join(desfazer) + "\nCOMMIT;\n", encoding="utf-8")
        con.commit()
        print(f"{len(alvos)} crédito(s) devolvido(s) à fila (COMMIT). Desfazer: {arquivo}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    reabrir()
