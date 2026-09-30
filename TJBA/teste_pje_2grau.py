#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTE (só leitura): o PRECATÓRIO no PJe de 2º GRAU do TJBA.

O QUE MUDA
O fetch_TJBA consulta `consultapublicapje.tjba.jus.br`, que é o **1º grau**.
O precatório é processo de 2º grau (…8.05.0000) e por isso não está lá — medido
em 30/09/2026: 12 de 12 precatórios deram NAO_ENCONTRADO naquele endereço.

O 2º grau fica em `pje2g.tjba.jus.br/pje/ConsultaPublica/listView.seam`, com a
MESMA tela e o MESMO captcha Tencent. Então dá para reaproveitar a classe `Pje`
inteira trocando só o endereço.

O QUE ESTE TESTE QUER SABER
Se a ficha do precatório traz:
  1. as PARTES (credor com CPF) — aí o originário vira dispensável, que foi o
     que aconteceu no TJSP (49% -> 97% ao consultar o precatório em vez do
     processo de origem);
  2. o PROCESSO DE REFERÊNCIA — o originário de graça.

Não grava nada.

    python TJBA/teste_pje_2grau.py [quantos]
"""
import json
import pathlib
import re
import sys
import time

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA import fetch_TJBA as F  # noqa: E402
from utils.banco import conectar  # noqa: E402

# Redireciona a classe Pje para o 2º grau. BASE e URL são lidos a cada chamada,
# então trocar os atributos do módulo basta — nenhuma cópia de código.
F.BASE = "https://pje2g.tjba.jus.br"
F.URL = F.BASE + "/pje/ConsultaPublica/listView.seam"

RE_CNJ = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.\d{2}\.\d{4}")


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    con = conectar("teste_2grau")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    cur.execute("""
        SELECT cr.numero_exibicao
          FROM creditos.coleta_credor cc
          JOIN creditos.credito cr ON cr.id = cc.credito_id
         WHERE cc.tribunal_id = 105 AND cc.software_id = 6 AND cc.status_id = 30
           AND cc.motivo_detalhe LIKE '%%nenhum candidato%%'
           AND cr.numero_exibicao LIKE '8%%'
         ORDER BY random() LIMIT %s""", (quantos,))
    precs = [r[0] for r in cur.fetchall()]
    con.close()

    print(f"{F.URL}\nconsultando {len(precs)} precatórios\n", flush=True)
    conta, resultados = {}, []
    pje = F.Pje()
    pje.abrir()
    t0 = time.time()
    try:
        for i, prec in enumerate(precs, 1):
            d = F.so_digitos(prec)
            try:
                r = pje.consultar(d)
            except F.PjeInstavel:
                r = {"resultado": "INSTAVEL"}
            except Exception as e:
                r = {"resultado": f"ERRO_{type(e).__name__}"}

            partes = r.get("partes") or []
            ativos = [p for p in partes if p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"]
            com_doc = [p for p in ativos if p.get("documento")]
            refs = {n for n in RE_CNJ.findall(json.dumps(r, ensure_ascii=False))
                    if F.so_digitos(n) != d and F.so_digitos(n)[16:20] != "0000"}

            res = r.get("resultado")
            if res in ("INSTAVEL",) or str(res).startswith("ERRO"):
                v = str(res)
            elif res == "NAO_ENCONTRADO":
                v = "NAO_ENCONTRADO"
            elif com_doc:
                v = "CREDOR_COM_CPF"
            elif ativos:
                v = "CREDOR_SEM_CPF"
            else:
                v = "SEM_PARTES"
            conta[v] = conta.get(v, 0) + 1
            resultados.append({"prec": prec, "veredito": v,
                               "credor": ativos[0]["nome"] if ativos else "",
                               "documento": com_doc[0]["documento"] if com_doc else "",
                               "referencia": sorted(refs)[:2]})
            print(f"[{i}/{len(precs)}] {F.formatar_cnj(d)} {v:<16} "
                  f"{(ativos[0]['nome'][:26] if ativos else ''):<26} "
                  f"{com_doc[0]['documento'] if com_doc else '':<15}"
                  f"{('ref ' + sorted(refs)[0]) if refs else ''}", flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "teste_pje_2grau.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<18} {v}")
    validos = sum(v for k, v in conta.items() if not str(k).startswith(("INSTAVEL", "ERRO")))
    if validos:
        cpf = conta.get("CREDOR_COM_CPF", 0)
        ref = sum(1 for r in resultados if r["referencia"])
        print(f"\n   respondidos       : {validos}")
        print(f"   CREDOR COM CPF    : {cpf}/{validos} = {100*cpf/validos:.0f}%")
        print(f"   com referência    : {ref}/{validos} = {100*ref/validos:.0f}%")


if __name__ == "__main__":
    main()
