"""
reabrir_data_migracao.py - devolve à fila os precatórios físicos do TJDFT que o fetch_TJDF.py gravou como
SUCESSO_PROCESSO_CREDITO 'CREDOR_SEM_ORIGINARIO', para o robô procurar o originário de novo.

Achado de 08/10/2026: processo físico que migrou para o PJe tem como dataDistribuicao a data da migração
(0010789-11.2008.8.07.0001 -> 2024-05-14) e o robô descartava o originário certo por "distribuído depois do
precatório" (8 de 40 numa amostra). O robô agora compara pelo ano do número CNJ quando ele é anterior ao ano da
dataDistribuicao (data_do_processo no fetch_TJDF.py).

Para cada crédito do software CONSULTA_PUBLICA_TJDFT com numero_norm começando com '00' (precatório físico), em
SUCESSO_PROCESSO_CREDITO com motivo CREDOR_SEM_ORIGINARIO:
- status PENDENTE e disponivel_em = agora (ou a data de --disponivel-em);
- o motivo da fila ganha o prefixo 'REABERTO_DATA_MIGRACAO: ' (o robô troca pelo resultado novo).
Nada mais muda: o credor com CPF (origem FONTE) continua ligado. Ao reprocessar, o robô só acrescenta (o
registrar_credor não duplica, o recálculo das partes não apaga vínculo FONTE e o corrigir_credor só tira a mesma
pessoa com OUTRO documento); sem originário, grava de novo CREDOR_SEM_ORIGINARIO com o mesmo credor.

Robô antigo (sem a correção) rodando em outra máquina pega esses créditos e repete o defeito: use --disponivel-em
para segurá-los até a cópia nova chegar lá e depois --liberar.

Grava em lotes de LOTE créditos (um COMMIT por lote). --com-desfazer escreve
TJDF/saida/desfazer_reabrir_data_migracao_<data>.sql (o UPDATE que devolve cada linha ao que era, só se ela ainda
estiver PENDENTE com o prefixo).

Uso:
    python TJDF/reabrir_data_migracao.py --simulacao                  # faz e desfaz (ROLLBACK), mostra quantos
    python TJDF/reabrir_data_migracao.py --com-desfazer               # grava (disponível já)
    python TJDF/reabrir_data_migracao.py --com-desfazer --disponivel-em 2026-10-15
    python TJDF/reabrir_data_migracao.py --liberar                    # os reabertos seguros ficam disponíveis já
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from utils.banco import como_dicts, conectar  # noqa: E402

SOFTWARE = "CONSULTA_PUBLICA_TJDFT"
TRIBUNAL_TJDFT = 107
LOTE = 500                              # créditos por COMMIT
PREFIXO = "REABERTO_DATA_MIGRACAO: "

SQL_ALVO = """
SELECT cc.credito_id, cc.status_id, cc.disponivel_em, cc.motivo_detalhe
  FROM creditos.coleta_credor cc
  JOIN creditos.credito c        ON c.id = cc.credito_id
  JOIN creditos.software s       ON s.id = cc.software_id
  JOIN creditos.status_coleta st ON st.id = cc.status_id
 WHERE cc.tribunal_id = %s AND s.codigo = %s AND st.codigo = 'SUCESSO_PROCESSO_CREDITO'
   AND c.numero_norm LIKE '00%%'
   AND split_part(cc.motivo_detalhe, ':', 1) = 'CREDOR_SEM_ORIGINARIO'
 ORDER BY cc.credito_id
"""


def liberar(con, simulacao):
    """Os reabertos que ainda esperam (PENDENTE com o prefixo e disponivel_em no futuro) ficam disponíveis já."""
    cur = con.cursor()
    cur.execute("""UPDATE creditos.coleta_credor cc SET disponivel_em = now(), updated_at = now()
                     FROM creditos.software s
                    WHERE s.id = cc.software_id AND s.codigo = %s AND cc.tribunal_id = %s AND cc.status_id = 1
                      AND cc.motivo_detalhe LIKE %s AND cc.disponivel_em > now()""",
                (SOFTWARE, TRIBUNAL_TJDFT, PREFIXO + "%"))
    n = cur.rowcount
    (con.rollback if simulacao else con.commit)()
    print(f"{n} crédito(s) liberado(s) ({'ROLLBACK, simulação' if simulacao else 'COMMIT'}).")


def main():
    ap = argparse.ArgumentParser(description="Devolve à fila os físicos CREDOR_SEM_ORIGINARIO do TJDFT.")
    ap.add_argument("--simulacao", action="store_true", help="faz e desfaz (ROLLBACK)")
    ap.add_argument("--com-desfazer", action="store_true", help="escreve o SQL de desfazer em TJDF/saida")
    ap.add_argument("--disponivel-em", default=None, help="AAAA-MM-DD: o robô só pega a partir dessa data")
    ap.add_argument("--liberar", action="store_true", help="só deixa disponíveis já os reabertos que esperam")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    con = conectar("reabrir_data_migracao_TJDF", escrita=True)
    try:
        if args.liberar:
            return liberar(con, args.simulacao)
        cur = con.cursor()
        cur.execute("SELECT id FROM creditos.status_coleta WHERE codigo = 'PENDENTE'")
        pendente = cur.fetchone()[0]
        cur.execute(SQL_ALVO, (TRIBUNAL_TJDFT, SOFTWARE))
        alvos = como_dicts(cur)
        con.commit()
        print(f"{len(alvos)} crédito(s) físico(s) CREDOR_SEM_ORIGINARIO a devolver à fila")
        desfazer, n, arquivo = [], 0, None
        if args.com_desfazer and not args.simulacao:
            arquivo = AQUI / "saida" / f"desfazer_reabrir_data_migracao_{datetime.now():%Y%m%d_%H%M%S}.sql"
            arquivo.parent.mkdir(exist_ok=True)
            arquivo.write_text("-- Desfaz o reabrir_data_migracao.py (TJDFT): volta ao status de antes cada crédito "
                               "que ainda está PENDENTE com o prefixo (rode numa transação: psql -1 -f).\n",
                               encoding="utf-8")
        for i in range(0, len(alvos), LOTE):
            for a in alvos[i:i + LOTE]:
                novo = (PREFIXO + (a["motivo_detalhe"] or ""))[:2000]
                # SKIP LOCKED e o status de novo no WHERE: se o robô mexeu na linha depois da leitura, fica como está
                cur.execute("""UPDATE creditos.coleta_credor
                                  SET status_id = %s, disponivel_em = COALESCE(%s::timestamptz, now()),
                                      motivo_detalhe = %s, updated_at = now()
                                WHERE credito_id = %s AND status_id = %s AND motivo_detalhe = %s
                                  AND credito_id IN (SELECT credito_id FROM creditos.coleta_credor
                                                      WHERE credito_id = %s FOR UPDATE SKIP LOCKED)""",
                            (pendente, args.disponivel_em, novo, a["credito_id"], a["status_id"],
                             a["motivo_detalhe"], a["credito_id"]))
                if not cur.rowcount:
                    continue
                n += 1
                desfazer.append(cur.mogrify(
                    "UPDATE creditos.coleta_credor SET status_id = %s, disponivel_em = %s, motivo_detalhe = %s, "
                    "updated_at = now() WHERE credito_id = %s AND status_id = %s AND motivo_detalhe = %s;",
                    (a["status_id"], a["disponivel_em"], a["motivo_detalhe"], a["credito_id"], pendente,
                     novo)).decode())
            if args.simulacao:
                con.rollback()
            else:
                if arquivo and desfazer:       # o desfazer do lote vai para o arquivo antes do COMMIT
                    with open(arquivo, "a", encoding="utf-8") as f:
                        f.write("\n".join(desfazer) + "\n")
                con.commit()
            desfazer.clear()
            print(f"  lote {i // LOTE + 1}: {n} reaberto(s) até aqui")
        if args.simulacao:
            print(f"{n} crédito(s) seriam reabertos (ROLLBACK, simulação).")
            return
        if arquivo:
            print(f"Desfazer: {arquivo}")
        print(f"{n} crédito(s) devolvido(s) à fila (COMMIT).")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
