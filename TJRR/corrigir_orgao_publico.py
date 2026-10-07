"""
corrigir_orgao_publico.py - volta para FALHA (REQTE_ORGAO_PUBLICO) os créditos do TJRR que o fetch_TJRR.py gravou com
um ente público como credor (fundos estaduais: FREBOM, fundo da Polícia Militar).

O que aconteceu: na 1ª rodada real (02/10/2026) o RE_ORGAO_PUBLICO não reconhecia 'Fundo de Reequipamento do Corpo de
Bombeiros...' e 'Fundo de Reaparelhamento ... da Polícia Militar...', e 6 créditos ficaram com o fundo como CREDOR. O
robô foi corrigido; este script arruma o que já foi gravado, como os outros robôs fazem com credor público.
Em 07/10/2026 o filtro passou a reconhecer também a ADVOCACIA GERAL DA UNIAO (crédito 1231459 tinha a AGU como
CREDOR: o nome começa com ADVOCACIA e passava como sociedade de advogados). O script:
- apaga os vínculos de CREDOR do crédito cuja pessoa é ente público (o fundo);
- desliga o originário (credito_originario) e tira o precatório de originarios.processos_originarios
  .precatorio_relacionado, senão a sincronização do legado religa o originário;
- finaliza na fila como FALHA 'REQTE_ORGAO_PUBLICO: ...' (fila_credor_finalizar: nova tentativa), atualiza as filas
  mensais do legado e o resultado no metadata do crédito.
Os advogados ligados e a capa do originário (partes reais do processo) ficam. Tudo o que muda vai para
TJRR/saida/desfazer_orgao_publico_<data>.sql (o SQL que volta ao estado de antes).

Uso:
    python TJRR/corrigir_orgao_publico.py              # só mostra o que faria (ROLLBACK)
    python TJRR/corrigir_orgao_publico.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
"""
import argparse
import json
import socket
import sys
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI) not in sys.path:
    sys.path.insert(0, str(AQUI))

import fetch_TJRR as F  # noqa: E402
from utils.banco import como_dicts  # noqa: E402


def creditos_com_orgao_publico(cur):
    """{credito_id: [vínculos de CREDOR com ente público]} nos créditos do robô."""
    cur.execute("""SELECT x.*, p.nome AS _nome
                     FROM creditos.credito_credor x
                     JOIN creditos.pessoa p ON p.id = x.pessoa_id
                     JOIN creditos.coleta_credor k ON k.credito_id = x.credito_id
                     JOIN creditos.software s ON s.id = k.software_id
                    WHERE s.codigo = %s AND k.tribunal_id = %s AND x.papel_id = 1
                    ORDER BY x.credito_id, x.id FOR UPDATE OF x""", (F.SOFTWARE, F.TRIBUNAL_TJRR))
    achados = {}
    for linha in como_dicts(cur):
        nome = linha.pop("_nome")
        if F.RE_ORGAO_PUBLICO.search(F.chave_nome(nome)):
            achados.setdefault(linha["credito_id"], []).append((nome, linha))
    return achados


def corrigir(cur, cid, vinculos, filas, status_legado, bk):
    nome = vinculos[0][0]
    for _, linha in vinculos:
        bk.delete(cur, "creditos.credito_credor", linha)
        cur.execute("DELETE FROM creditos.credito_credor WHERE id = %s", (linha["id"],))
    cur.execute("""SELECT co.credito_id, co.processo_id, co.origem, co.created_at
                     FROM creditos.credito_originario co WHERE co.credito_id = %s FOR UPDATE""", (cid,))
    origs = como_dicts(cur)
    for o in origs:
        bk.p_sql.append(cur.mogrify("INSERT INTO creditos.credito_originario (credito_id, processo_id, origem, "
                                    "created_at) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                                    (o["credito_id"], o["processo_id"], o["origem"], o["created_at"])).decode())
    cur.execute("DELETE FROM creditos.credito_originario WHERE credito_id = %s", (cid,))
    cur.execute("SELECT numero_exibicao, numero_norm FROM creditos.credito WHERE id = %s", (cid,))
    precatorio, numero_norm = cur.fetchone()
    cur.execute("""SELECT id, precatorio_relacionado FROM originarios.processos_originarios
                    WHERE precatorio_relacionado = %s::text[] FOR UPDATE""", ([precatorio],))
    capas = cur.fetchall()
    for pid, rel in capas:
        bk.update(cur, "originarios.processos_originarios", {"id": pid}, {"precatorio_relacionado": rel})
        cur.execute("UPDATE originarios.processos_originarios SET precatorio_relacionado = NULL WHERE id = %s", (pid,))
    motivo = (f"REQTE_ORGAO_PUBLICO: {nome} (fundo público; corrigido por corrigir_orgao_publico.py em "
              f"{datetime.now():%d/%m/%Y})")
    cur.execute("SELECT status_id, motivo_id, motivo_detalhe, sucesso_em, ultima_tentativa_id, tentativas "
                "FROM creditos.coleta_credor WHERE credito_id = %s", (cid,))
    antes = dict(zip(("status_id", "motivo_id", "motivo_detalhe", "sucesso_em", "ultima_tentativa_id", "tentativas"),
                     cur.fetchone()))
    bk.update(cur, "creditos.coleta_credor", {"credito_id": cid}, antes)
    cur.execute("""SELECT creditos.fila_credor_finalizar(p_credito_id => %s, p_worker => %s, p_status => 'FALHA',
                                                         p_motivo => %s, p_sistema => 'PROJUDI', p_host => %s)""",
                (cid, F.WORKER, motivo, socket.gethostname()))
    tentativa = cur.fetchone()[0]
    bk.p_sql.append(f"DELETE FROM creditos.coleta_credor_tentativa WHERE id = {int(tentativa)};")
    lead = {"numero_norm": numero_norm}
    fila = F.atualizar_filas_mensais(cur, filas, lead, {"status": "FALHA", "motivo": motivo, "originario": None},
                                     status_legado, bk)
    cur.execute("""SELECT f.credito_id, f.software_id, f.metadata FROM creditos.credito_fonte f
                     JOIN creditos.software s ON s.id = f.software_id
                    WHERE f.credito_id = %s AND s.codigo = %s FOR UPDATE OF f""", (cid, F.SOFTWARE))
    meta = cur.fetchone()
    if meta:
        bk.update(cur, "creditos.credito_fonte", {"credito_id": cid, "software_id": meta[1]},
                  {"metadata": json.dumps(meta[2], ensure_ascii=False)})
        novo = dict(meta[2] or {})
        novo.setdefault("motor", {})
        novo["correcao"] = {"em": datetime.now().isoformat(timespec="seconds"), "de": novo["motor"].get("resultado"),
                            "para": "FALHA", "motivo": motivo}
        novo["motor"]["resultado"], novo["motor"]["motivo"] = "FALHA", motivo
        cur.execute("UPDATE creditos.credito_fonte SET metadata = %s::jsonb WHERE credito_id = %s AND software_id = %s",
                    (json.dumps(novo, ensure_ascii=False, default=str), cid, meta[1]))
    print(f"crédito {cid} {precatorio}: {len(vinculos)} vínculo(s) de CREDOR ({nome[:60]}), "
          f"{len(origs)} originário(s) desligado(s), {len(capas)} capa(s) antiga(s), "
          f"{fila.get('fila_mensal', 0)} linha(s) de fila mensal -> FALHA REQTE_ORGAO_PUBLICO")


def main():
    ap = argparse.ArgumentParser(description="Volta para FALHA os créditos do TJRR gravados com ente público credor.")
    ap.add_argument("--aplicar", action="store_true", help="grava (COMMIT); sem isso, só mostra (ROLLBACK)")
    args = ap.parse_args()
    F.GERAR_DESFAZER = True                 # correção manual: o desfazer é sempre escrito (o robô não escreve por padrão)
    rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql = F.SAIDA / f"desfazer_orgao_publico_{rodada}.sql"
    bk = F.Backup(sql, F.SAIDA / "fetch_legado_backup.csv", rodada, "CORRECAO" if args.aplicar else "CORRECAO_TESTE")
    con = F.conectar(escrita=True)
    try:
        with con.cursor() as cur:
            filas = F.filas_antigas(cur)
            cur.execute("SELECT codigo, codigo_legado FROM creditos.status_coleta")
            status_legado = {codigo: legado or codigo for codigo, legado in cur.fetchall()}
            achados = creditos_com_orgao_publico(cur)
            for cid, vinculos in achados.items():
                corrigir(cur, cid, vinculos, filas, status_legado, bk)
                bk.fechar_credito(cid)
        print(f"\n{len(achados)} crédito(s).")
        if not args.aplicar:
            con.rollback()
            bk.descartar_lote()
            print("Nada foi gravado (rode com --aplicar).")
            return
        con.commit()
        bk.confirmar_lote(manter=True)
        print(f"COMMIT. SQL de desfazer: {sql}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
