#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTE (só leitura): consultar o PRÓPRIO PRECATÓRIO no PJe público do TJBA.

POR QUE
O fetch_TJBA usa o PJe só para confirmar um candidato a originário, sempre
pesquisando um processo de 1º grau. Mas o precatório TAMBÉM é um processo
(…8.05.0000) e tem a sua própria ficha. Se essa ficha trouxer as partes, temos o
credor e o CPF sem precisar achar o originário — foi exatamente o que destravou
o TJSP (a Consulta de Requisitórios saiu de 49% para 97% ao consultar o
precatório em vez do processo de origem).

A tela de busca ainda tem um campo "Processo referência", que existe para ligar
precatório e originário. Este teste olha os dois caminhos.

Não grava nada.

    python TJBA/teste_precatorio_no_pje.py [quantos]
"""
import json
import pathlib
import re
import sys
import time

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import Pje, PjeInstavel, formatar_cnj, so_digitos  # noqa: E402
from utils.banco import conectar  # noqa: E402

RE_CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.\d{2}\.\d{4}")


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    con = conectar("teste_prec_pje")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    # precatorios do grupo que o robo NAO resolveu
    cur.execute("""
        SELECT cr.numero_exibicao
          FROM creditos.coleta_credor cc
          JOIN creditos.credito cr ON cr.id = cc.credito_id
         WHERE cc.tribunal_id = 105 AND cc.software_id = 6 AND cc.status_id = 30
           AND cc.motivo_detalhe LIKE '%%nenhum candidato%%'
         ORDER BY random() LIMIT %s""", (quantos,))
    precs = [r[0] for r in cur.fetchall()]
    con.close()

    print(f"consultando {len(precs)} PRECATÓRIOS direto no PJe\n", flush=True)
    conta = {}
    resultados = []
    pje = Pje()
    pje.abrir()
    t0 = time.time()
    try:
        for i, prec in enumerate(precs, 1):
            d = so_digitos(prec)
            try:
                r = pje.consultar(d)
            except PjeInstavel as e:
                r = {"resultado": "INSTAVEL", "erro": str(e)[:60]}
            except Exception as e:
                r = {"resultado": f"ERRO_{type(e).__name__}"}

            partes = r.get("partes") or []
            capa = r.get("capa") or {}
            ativos = [p for p in partes
                      if p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"]
            com_doc = [p for p in ativos if p.get("documento")]
            # algum outro CNJ na ficha (candidato a processo de referencia)?
            outros = {n for n in RE_CNJ.findall(json.dumps(r, ensure_ascii=False))
                      if so_digitos(n) != d}

            if r.get("resultado") in ("INSTAVEL",) or str(r.get("resultado", "")).startswith("ERRO"):
                veredito = r["resultado"]
            elif r.get("resultado") == "NAO_ENCONTRADO":
                veredito = "NAO_ENCONTRADO"
            elif com_doc:
                veredito = "CREDOR_COM_CPF"
            elif ativos:
                veredito = "CREDOR_SEM_CPF"
            else:
                veredito = "SEM_PARTES"
            conta[veredito] = conta.get(veredito, 0) + 1
            resultados.append({"prec": prec, "veredito": veredito,
                               "credor": ativos[0]["nome"] if ativos else "",
                               "documento": com_doc[0]["documento"] if com_doc else "",
                               "classe": capa.get("classe_judicial", "")[:40],
                               "outros_cnj": sorted(outros)[:3]})
            print(f"[{i}/{len(precs)}] {formatar_cnj(d)} {veredito:<16} "
                  f"{(ativos[0]['nome'][:28] if ativos else ''):<28} "
                  f"{com_doc[0]['documento'] if com_doc else ''}"
                  f"{'  ref:' + sorted(outros)[0] if outros else ''}", flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "teste_precatorio_no_pje.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<18} {v}")
    validos = sum(v for k, v in conta.items() if not k.startswith(("INSTAVEL", "ERRO")))
    bons = conta.get("CREDOR_COM_CPF", 0)
    if validos:
        print(f"\n   respondidos: {validos}")
        print(f"   COM CREDOR E CPF: {bons}/{validos} = {100*bons/validos:.0f}%")


if __name__ == "__main__":
    main()
