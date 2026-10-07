"""
reabrir_sem_beneficiario.py - devolve à fila os créditos do TJMA que o fetch_TJMA.py gravou como FALHA SEM_BENEFICIARIO,
para o robô decidir de novo com a regra de honorários do próprio advogado.

Achado de 07/10/2026: em ~27% desses leads o DJEN publica o credor em iniciais ('CREDOR: R. A. T.') e as iniciais são do
advogado que assina pelo requerente ('R. A. T. - PE19443'): o credor do precatório é o próprio advogado. O robô
descartava os advogados ao procurar o credor e gravava FALHA. Agora (decisão do usuário, 07/10/2026) ele liga o advogado
como CREDOR, com o CPF que o banco tem pela OAB, e também como ADVOGADO: SUCESSO_PROCESSO_CREDITO 'CREDOR_ADVOGADO ...
regra=HONORARIOS_ADVOGADO'; sem CPF no banco, só ADVOGADO e SUCESSO_INCOMPLETO. Os outros SEM_BENEFICIARIO voltam a dar
o mesmo resultado.

Para cada crédito do software CONSULTA_PUBLICA_TJMA em FALHA com motivo SEM_BENEFICIARIO e SEM credor ligado:
- status PENDENTE e disponivel_em = agora (o robô pega na próxima rodada);
- o motivo da fila ganha o prefixo 'REABERTO_HONORARIOS: ' (o robô troca pelo resultado novo).
Cada UPDATE vai para TJMA/saida/desfazer_reabrir_sem_beneficiario_<data>.sql (o UPDATE que devolve a linha ao que era).

--honorarios-sem-cpf: em vez dos SEM_BENEFICIARIO, devolve os SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF:
regra=HONORARIOS_ADVOGADO' (o advogado é o credor, mas a OAB não tinha CPF no banco). O robô agora busca o CPF dele no
PJe 1º grau (teste de 07/10/2026: 276 de 281). Prefixo 'REABERTO_CPF_ADVOGADO: '.

Uso:
    python TJMA/reabrir_sem_beneficiario.py              # só mostra o que faria
    python TJMA/reabrir_sem_beneficiario.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
    python TJMA/reabrir_sem_beneficiario.py --honorarios-sem-cpf --aplicar
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from utils.banco import como_dicts, conectar  # noqa: E402

SOFTWARE = "CONSULTA_PUBLICA_TJMA"
TRIBUNAL_TJMA = 110
# alvo: (status, início do motivo, prefixo que o motivo ganha, nome no arquivo de desfazer)
ALVOS = {"sem_beneficiario": ("FALHA", "SEM_BENEFICIARIO", "REABERTO_HONORARIOS: ", "sem_beneficiario"),
         "honorarios_sem_cpf": ("SUCESSO_INCOMPLETO", "SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO",
                                "REABERTO_CPF_ADVOGADO: ", "honorarios_sem_cpf")}

SQL_ALVO = """
SELECT cc.credito_id, cc.status_id, cc.disponivel_em, cc.motivo_detalhe
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id
  JOIN creditos.status_coleta st ON st.id = cc.status_id
  JOIN creditos.credito c        ON c.id = cc.credito_id
 WHERE cc.tribunal_id = %s AND s.codigo = %s AND st.codigo = %s AND c.saiu_da_lista_em IS NULL
   AND cc.motivo_detalhe LIKE %s
   AND NOT EXISTS (SELECT 1 FROM creditos.credito_credor k WHERE k.credito_id = cc.credito_id AND k.papel_id = 1)
 ORDER BY cc.credito_id
   FOR UPDATE OF cc
"""


def main():
    ap = argparse.ArgumentParser(description="Devolve à fila os FALHA SEM_BENEFICIARIO do TJMA (regra de honorários).")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, só mostra")
    ap.add_argument("--honorarios-sem-cpf", action="store_true",
                    help="devolve os SUCESSO_INCOMPLETO de honorários sem CPF (o robô busca o CPF no PJe)")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    status, motivo, PREFIXO, nome_arq = ALVOS["honorarios_sem_cpf" if args.honorarios_sem_cpf else "sem_beneficiario"]
    con = conectar("reabrir_sem_beneficiario_TJMA", escrita=True)
    cur = con.cursor()
    try:
        cur.execute(SQL_ALVO, (TRIBUNAL_TJMA, SOFTWARE, status, motivo.replace("%", r"\%") + "%"))
        alvos = como_dicts(cur)
        cur.execute("SELECT id FROM creditos.status_coleta WHERE codigo = 'PENDENTE'")
        pendente = cur.fetchone()[0]
        print(f"{len(alvos)} crédito(s) {status} {motivo} a devolver à fila")
        desfazer = []
        for a in alvos:
            cur.execute("""UPDATE creditos.coleta_credor
                               SET status_id = %s, disponivel_em = now(), motivo_detalhe = %s, updated_at = now()
                             WHERE credito_id = %s""",
                        (pendente, (PREFIXO + (a["motivo_detalhe"] or ""))[:2000], a["credito_id"]))
            desfazer.append(cur.mogrify("UPDATE creditos.coleta_credor SET status_id = %s, disponivel_em = %s, "
                                        "motivo_detalhe = %s, updated_at = now() WHERE credito_id = %s;",
                                        (a["status_id"], a["disponivel_em"], a["motivo_detalhe"],
                                         a["credito_id"])).decode())
            if len(desfazer) <= 3:
                print(f"  {a['credito_id']}: {(a['motivo_detalhe'] or '')[:110]}")
        if not args.aplicar:
            con.rollback()
            print("nada gravado (rode com --aplicar).")
            return
        arquivo = AQUI / "saida" / f"desfazer_reabrir_{nome_arq}_{datetime.now():%Y%m%d_%H%M%S}.sql"
        arquivo.parent.mkdir(exist_ok=True)
        arquivo.write_text("-- Desfaz o reabrir_sem_beneficiario.py (TJMA): volta cada crédito ao status de antes.\n"
                           "BEGIN;\n" + "\n".join(desfazer) + "\nCOMMIT;\n", encoding="utf-8")
        con.commit()
        print(f"{len(alvos)} crédito(s) devolvido(s) à fila (COMMIT). Desfazer: {arquivo}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
