#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDAÇÃO (só leitura): os "acertos" da busca por nome são o originário CERTO?

O PROBLEMA QUE ESTE SCRIPT RESOLVE
O `medir_busca_por_nome.py` conta acerto por critério ESTRUTURAL: 1º grau, o
credor no polo ativo, ente público no passivo, não mais novo que o precatório.
Isso casa com QUALQUER ação daquela pessoa contra a Fazenda — e uma pessoa pode
ter várias. Só uma gerou aquele precatório. Ou seja, aquela taxa mede "achei um
processo plausível", não "achei o originário".

Aqui a pergunta é outra e tem gabarito: rodando a mesma busca nos créditos que
JÁ TÊM originário conhecido no banco, ela devolve o MESMO processo?

    IGUAL            o 1º escolhido é o originário conhecido -> acerto de verdade
    ESTAVA_NA_LISTA  o certo apareceu, mas não foi o escolhido -> falta desempate
    DIFERENTE        escolheu outro e o certo nem apareceu -> falso positivo
    NAO_ACHOU        a busca não trouxe candidato

DIFERENTE é o número que importa: é a taxa de falso positivo.

Não grava nada.

    python TJBA/validar_busca_por_nome.py [quantos]
"""
import json
import pathlib
import sys
import time

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import Pje, PjeInstavel, chave_nome, so_digitos  # noqa: E402
from TJBA.medir_busca_por_nome import abrir_detalhe, buscar, e_empresa, e_ente  # noqa: E402
from utils.banco import conectar  # noqa: E402

MAX_DETALHES = 6


def escolher(pje, prec20, nome, candidatos):
    """Devolve (escolhido, todos_os_candidatos) pelo MESMO critério da medição."""
    alvo = chave_nome(nome)
    for link, numero in candidatos[:MAX_DETALHES]:
        partes, _capa = abrir_detalhe(pje, link)
        credor = next((p for p in partes
                       if p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"
                       and chave_nome(p.get("nome", "")) == alvo), None)
        ente = any(p.get("polo") == "PASSIVO" and e_ente(p.get("nome", "")) for p in partes)
        if credor and ente:
            return numero
    return None


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    con = conectar("validar")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    # gabarito: creditos do TJBA que JA tem originario ligado, com o nome na lista
    cur.execute("""
        SELECT cr.numero_exibicao, p.numero_cnj, l2.requerentes
          FROM creditos.credito cr
          JOIN creditos.credito_originario co ON co.credito_id = cr.id
          JOIN creditos.processo p            ON p.id = co.processo_id
          JOIN listas_primarias.processos_unificados_2026_09 l2
            ON regexp_replace(l2.numero_precatorio,'[^0-9]','','g') = cr.numero_norm
         WHERE cr.tribunal_id = 105 AND l2.requerentes IS NOT NULL
         ORDER BY random() LIMIT %s""", (quantos * 3,))
    casos = []
    for prec, orig, req in cur.fetchall():
        nomes = req if isinstance(req, list) else []
        if nomes and not e_ente(nomes[0]) and not e_empresa(nomes[0]):
            casos.append({"prec": prec, "gabarito": so_digitos(orig), "nome": nomes[0]})
        if len(casos) >= quantos:
            break
    con.close()

    print(f"validando {len(casos)} créditos QUE JÁ TÊM originário conhecido\n", flush=True)
    conta, resultados, pje = {}, [], Pje()
    pje.abrir()
    t0 = time.time()
    try:
        for i, c in enumerate(casos, 1):
            prec20 = so_digitos(c["prec"])
            try:
                achados = buscar(pje, c["nome"])
                cands = [(l, n) for l, n in achados
                         if (d := so_digitos(n)) and d != prec20 and d[13:16] == "805"
                         and d[16:20] != "0000" and int(d[9:13]) <= int(prec20[9:13])]
                numeros = {so_digitos(n) for _l, n in cands}
                escolhido = escolher(pje, prec20, c["nome"], cands) if cands else None
                esc20 = so_digitos(escolhido) if escolhido else None
                if esc20 and esc20 == c["gabarito"]:
                    v = "IGUAL"
                elif c["gabarito"] in numeros:
                    v = "ESTAVA_NA_LISTA"
                elif esc20:
                    v = "DIFERENTE"
                else:
                    v = "NAO_ACHOU"
            except PjeInstavel:
                v, esc20, numeros = "INSTAVEL", None, set()
            except Exception as e:
                v, esc20, numeros = f"ERRO_{type(e).__name__}", None, set()
            conta[v] = conta.get(v, 0) + 1
            resultados.append({"prec": c["prec"], "nome": c["nome"], "veredito": v,
                               "gabarito": c["gabarito"], "escolhido": esc20,
                               "candidatos": len(numeros)})
            print(f"[{i}/{len(casos)}] {c['nome'][:28]:<28} {v:<16} "
                  f"gab={c['gabarito'][-12:]} esc={(esc20 or '-')[-12:]}", flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "validacao_busca_por_nome.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<18} {v}")
    resp = [r for r in resultados if not str(r["veredito"]).startswith(("INSTAVEL", "ERRO"))]
    if resp:
        ig = sum(1 for r in resp if r["veredito"] == "IGUAL")
        li = sum(1 for r in resp if r["veredito"] == "ESTAVA_NA_LISTA")
        di = sum(1 for r in resp if r["veredito"] == "DIFERENTE")
        n = len(resp)
        print(f"\n   respondidos          : {n}")
        print(f"   acertou de primeira  : {ig}/{n} = {100*ig/n:.0f}%")
        print(f"   o certo estava junto : {li}/{n} = {100*li/n:.0f}%  (falta desempate)")
        print(f"   FALSO POSITIVO       : {di}/{n} = {100*di/n:.0f}%  <- o que importa")


if __name__ == "__main__":
    main()
