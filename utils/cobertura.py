"""
cobertura.py - Medição de cobertura dos robôs do modo 5 (pedida na "etapa comum" do artifact Ordem Cronológica dos TJs):
quanto dos precatórios que estão hoje na lista do tribunal já tem credor com CPF/CNPJ, originário e advogado com OAB,
no total, nos não pagos e por ano orçamentário. Só leitura.

Base: creditos.credito do tribunal com saiu_da_lista_em nulo (o TJDFT não tem lista_item: os leads vêm do legado).
- não pago: situação que não é TERMINAL (creditos.vw_credito_situacao); sem situação conta como não pago;
- credor com CPF/CNPJ: vínculo CREDOR com pessoa com documento;
- originário: linha em creditos.credito_originario (o confirmado);
- originário provável: sem originário e com metadata.originario_provavel (utils/originario_provavel.py);
- advogado com OAB: vínculo ADVOGADO com pessoa que tem OAB.

Uso:
    python utils/cobertura.py                      # os 8 tribunais do modo 5, resumo na tela + CSV por ano
    python utils/cobertura.py --tribunal TJMA TJBA
Nos robôs: logar_cobertura(log, "TJMA", 110) no fim de cada rodada (linha [cobertura] no log, ~2-5 s).
"""
import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.banco import RAIZ, conectar

TRIBUNAIS = {"TJAL": 102, "TJBA": 105, "TJDFT": 107, "TJMA": 110, "TJMT": 111, "TJPI": 118, "TJRJ": 119, "TJRR": 123}

SQL_COBERTURA = r"""
WITH c AS MATERIALIZED (
    SELECT c.id, c.ano_orcamentario AS ano, coalesce(v.classe, '') <> 'TERMINAL' AS nao_pago
      FROM creditos.credito c
      LEFT JOIN creditos.vw_credito_situacao v ON v.credito_id = c.id
     WHERE c.tribunal_id = %(tribunal)s AND c.saiu_da_lista_em IS NULL),
k AS MATERIALIZED (
    SELECT DISTINCT cc.credito_id FROM creditos.credito_credor cc JOIN c ON c.id = cc.credito_id
      JOIN creditos.papel_parte pp ON pp.id = cc.papel_id AND pp.codigo = 'CREDOR'
      JOIN creditos.pessoa p ON p.id = cc.pessoa_id
     WHERE p.documento IS NOT NULL),
o AS MATERIALIZED (
    SELECT DISTINCT co.credito_id FROM creditos.credito_originario co JOIN c ON c.id = co.credito_id),
p AS MATERIALIZED (
    SELECT DISTINCT f.credito_id FROM creditos.credito_fonte f JOIN c ON c.id = f.credito_id
     WHERE f.metadata ? 'originario_provavel'),
a AS MATERIALIZED (
    SELECT DISTINCT cc.credito_id FROM creditos.credito_credor cc JOIN c ON c.id = cc.credito_id
      JOIN creditos.papel_parte pp ON pp.id = cc.papel_id AND pp.codigo = 'ADVOGADO'
     WHERE EXISTS (SELECT 1 FROM creditos.pessoa_oab po WHERE po.pessoa_id = cc.pessoa_id))
SELECT c.ano, c.nao_pago,
       count(*) AS total,
       count(k.credito_id) AS credor_cpf,
       count(o.credito_id) AS originario,
       count(*) FILTER (WHERE k.credito_id IS NOT NULL AND o.credito_id IS NOT NULL) AS credor_e_originario,
       count(*) FILTER (WHERE o.credito_id IS NULL AND p.credito_id IS NOT NULL) AS so_provavel,
       count(a.credito_id) AS advogado_oab
  FROM c
  LEFT JOIN k ON k.credito_id = c.id
  LEFT JOIN o ON o.credito_id = c.id
  LEFT JOIN p ON p.credito_id = c.id
  LEFT JOIN a ON a.credito_id = c.id
 GROUP BY c.ano, c.nao_pago
"""

MEDIDAS = ("total", "credor_cpf", "originario", "credor_e_originario", "so_provavel", "advogado_oab")


def medir(cur, tribunal_id):
    """Linhas (ano, nao_pago, contagens...) do tribunal."""
    cur.execute(SQL_COBERTURA, {"tribunal": tribunal_id})
    colunas = [d[0] for d in cur.description]
    return [dict(zip(colunas, linha)) for linha in cur.fetchall()]


def somar(linhas, filtro=lambda x: True):
    soma = dict.fromkeys(MEDIDAS, 0)
    for x in linhas:
        if filtro(x):
            for m in MEDIDAS:
                soma[m] += x[m]
    return soma


def pct(parte, total):
    return f"{100 * parte / total:.1f}%" if total else "-"


def texto(s):
    n = s["total"]
    return (f"{n} | credor com CPF/CNPJ {pct(s['credor_cpf'], n)} | originário {pct(s['originario'], n)} "
            f"| os dois {pct(s['credor_e_originario'], n)} | só provável {pct(s['so_provavel'], n)} "
            f"| advogado com OAB {pct(s['advogado_oab'], n)}")


def logar_cobertura(log, sigla, tribunal_id):
    """Loga a cobertura do tribunal no fim da rodada (total e não pagos). Falhar aqui não atrapalha o encerramento."""
    try:
        con = conectar(f"cobertura_{sigla}")
        try:
            with con.cursor() as cur:
                linhas = medir(cur, tribunal_id)
        finally:
            con.close()
    except Exception as e:
        log.warning(f"cobertura do {sigla} não medida ({e.__class__.__name__}: {(str(e).splitlines() or [''])[0][:120]})")
        return
    log.info(f"[cobertura] {sigla} na lista: {texto(somar(linhas))}")
    log.info(f"[cobertura] {sigla} não pagos: {texto(somar(linhas, lambda x: x['nao_pago']))}")


def main():
    ap = argparse.ArgumentParser(description="Cobertura dos robôs do modo 5 (só leitura).")
    ap.add_argument("--tribunal", nargs="+", choices=sorted(TRIBUNAIS), default=list(TRIBUNAIS))
    ap.add_argument("--csv", default=str(RAIZ / "saida" / f"cobertura_{datetime.now():%Y%m%d_%H%M}.csv"))
    args = ap.parse_args()

    con = conectar("cobertura")
    saida = []
    try:
        with con.cursor() as cur:
            for sigla in args.tribunal:
                linhas = medir(cur, TRIBUNAIS[sigla])
                print(f"\n{sigla}")
                print(f"  na lista:  {texto(somar(linhas))}")
                print(f"  não pagos: {texto(somar(linhas, lambda x: x['nao_pago']))}")
                for ano in sorted({x["ano"] for x in linhas}, key=lambda a: (a is None, a)):
                    for grupo, filtro in (("todos", lambda x: True), ("nao_pagos", lambda x: x["nao_pago"])):
                        s = somar(linhas, lambda x, ano=ano, filtro=filtro: x["ano"] == ano and filtro(x))
                        if s["total"]:
                            saida.append({"tribunal": sigla, "ano_orcamentario": ano if ano is not None else "",
                                          "grupo": grupo, **s,
                                          **{f"pct_{m}": pct(s[m], s["total"]) for m in MEDIDAS[1:]}})
    finally:
        con.close()

    arq = Path(args.csv)
    arq.parent.mkdir(parents=True, exist_ok=True)
    with arq.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(saida[0]), delimiter=";")
        w.writeheader()
        w.writerows(saida)
    print(f"\npor ano orçamentário: {arq}")


if __name__ == "__main__":
    main()
