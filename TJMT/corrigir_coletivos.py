"""
corrigir_coletivos.py - desfaz os credores ligados por engano pelo recálculo do banco em ações coletivas (TJMT).

O que aconteceu: quando o fetch_TJMT.py ligava um crédito a um originário que já tinha no banco vários autores (ação
coletiva vinda de raspagens antigas) e era o único crédito ligado a ele, o registrar_capa chamava
recalcular_credores_do_processo, que liga TODOS os autores do processo como credores desse crédito (origem CREDITOS).
O robô foi corrigido (outros_credores_no_banco: nesse caso liga só o credor e não liga o originário). Este script
arruma o que já foi gravado, do mesmo jeito:
- apaga, nos créditos do robô (software CONSULTA_PUBLICA_TJMT), os vínculos de CREDOR com origem CREDITOS que não são
  o credor escolhido pelo robô (nome no motivo, 'credor=...');
- desliga o originário desses créditos (credito_originario), para um recálculo futuro não recriar os vínculos;
- tira o precatório de originarios.processos_originarios.precatorio_relacionado (capa antiga), senão a sincronização
  do legado religa o originário (visto em 02/10/2026).
Cada linha apagada ou alterada vai para TJMT/saida/desfazer_coletivos_<data>.sql (o SQL que a recria).

Uso:
    python TJMT/corrigir_coletivos.py                       # só mostra o que faria
    python TJMT/corrigir_coletivos.py --aplicar             # apaga (COMMIT) e grava o SQL de desfazer
    python TJMT/corrigir_coletivos.py --creditos 1,2 --aplicar   # também desliga o originário destes créditos
"""
import argparse
import json
import re
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from utils.banco import conectar, como_dicts  # noqa: E402
from utils.texto import formatar_cnj  # noqa: E402

SOFTWARE = "CONSULTA_PUBLICA_TJMT"
DESDE = "2026-10-01 17:00"            # início das gravações reais do robô


def chave(nome):
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", (nome or "").upper())).strip()


def mesma_pessoa(a, b):
    a, b = chave(a), chave(b)
    return a == b or SequenceMatcher(None, a, b).ratio() >= 0.9


def main():
    ap = argparse.ArgumentParser(description="Desfaz credores ligados por engano em ações coletivas (TJMT).")
    ap.add_argument("--aplicar", action="store_true", help="apaga de verdade (COMMIT); sem isso, só mostra")
    ap.add_argument("--creditos", default="", help="ids de crédito a desligar do originário mesmo sem vínculo errado")
    args = ap.parse_args()
    extras = [int(x) for x in re.findall(r"\d+", args.creditos)]
    con = conectar("corrigir_coletivos_TJMT", escrita=True)
    cur = con.cursor()
    try:
        cur.execute("""SELECT x.*, p.nome AS _nome, k.motivo_detalhe AS _motivo
                         FROM creditos.credito_credor x
                         JOIN creditos.pessoa p ON p.id = x.pessoa_id
                         JOIN creditos.coleta_credor k ON k.credito_id = x.credito_id
                         JOIN creditos.software s ON s.id = k.software_id
                        WHERE s.codigo = %s AND x.papel_id = 1 AND x.origem = 'CREDITOS' AND x.created_at >= %s
                        ORDER BY x.credito_id, x.id""", (SOFTWARE, DESDE))
        linhas = como_dicts(cur)
        apagar, creditos = [], {}
        for ln in linhas:
            nome, motivo = ln.pop("_nome"), ln.pop("_motivo") or ""
            m = re.search(r"credor=(.+?)(?: \(honor|$)", motivo)
            if m and mesma_pessoa(nome, m.group(1)):
                continue                                   # é o próprio credor do robô (veio pelo recálculo)
            apagar.append(ln)
            creditos.setdefault(ln["credito_id"], []).append(nome)
        for cid in extras:
            creditos.setdefault(cid, [])
        desfazer = []

        def guarda(tabela, linhas_antes):
            for linha in linhas_antes:
                cols = list(linha)
                vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in linha.values()]
                desfazer.append(cur.mogrify(f"INSERT INTO {tabela} ({', '.join(cols)}) VALUES "
                                            f"({', '.join(['%s'] * len(cols))}) ON CONFLICT DO NOTHING;", vals).decode())

        for cid, nomes in creditos.items():
            cur.execute("""SELECT co.*, pr.numero_cnj AS _cnj FROM creditos.credito_originario co
                             JOIN creditos.processo pr ON pr.id = co.processo_id WHERE co.credito_id = %s""", (cid,))
            origs = como_dicts(cur)
            cnjs = [formatar_cnj(o.pop("_cnj")) for o in origs]
            print(f"crédito {cid}: {len(nomes)} credor(es) ligado(s) por engano ({', '.join(n[:30] for n in nomes[:3])}"
                  f"{'...' if len(nomes) > 3 else ''}); originário a desligar: {', '.join(cnjs) or '-'}")
            guarda("creditos.credito_originario", origs)
            cur.execute("DELETE FROM creditos.credito_originario WHERE credito_id = %s", (cid,))
            # capa antiga: sem o precatório relacionado, a sincronização do legado não religa o originário
            cur.execute("""SELECT po.id, po.precatorio_relacionado FROM originarios.processos_originarios po
                             JOIN creditos.credito c ON c.id = %s
                            WHERE po.precatorio_relacionado = ARRAY[c.numero_exibicao]::text[] FOR UPDATE OF po""", (cid,))
            for pid, rel in cur.fetchall():
                desfazer.append(cur.mogrify("UPDATE originarios.processos_originarios SET precatorio_relacionado = %s "
                                            "WHERE id = %s;", (rel, pid)).decode())
                cur.execute("UPDATE originarios.processos_originarios SET precatorio_relacionado = NULL WHERE id = %s",
                            (pid,))
                print(f"   capa antiga {pid}: precatório relacionado {rel} retirado")
        guarda("creditos.credito_credor", apagar)
        if apagar:
            cur.execute("DELETE FROM creditos.credito_credor WHERE id = ANY(%s)", ([ln["id"] for ln in apagar],))
        print(f"\n{len(apagar)} vínculo(s) de credor em {len(creditos)} crédito(s).")
        if not args.aplicar:
            con.rollback()
            print("Nada foi gravado (rode com --aplicar).")
            return
        con.commit()
        if desfazer:
            arquivo = AQUI / "saida" / f"desfazer_coletivos_{datetime.now():%Y%m%d_%H%M%S}.sql"
            arquivo.write_text("-- Recria o que corrigir_coletivos.py apagou.\nBEGIN;\n" + "\n".join(desfazer) +
                               "\nCOMMIT;\n", encoding="utf-8")
            print(f"COMMIT. SQL de desfazer: {arquivo}")
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
