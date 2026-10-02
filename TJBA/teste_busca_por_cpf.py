#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTE (só leitura): buscar o originário pelo CPF do credor, no PJe público.

POR QUE
A tela de pesquisa do PJe tem campo "CPF CNPJ" (`fPP:dpDec:documentoParte`), e
o robô não usa — nem ele, nem a nossa rota por nome. Para os ~6.586 créditos do
TJBA onde o documento JÁ está no banco, essa busca é EXATA:

  - some o homônimo, que é o risco de fundo da busca por nome;
  - some o caso de empresa, onde o nome traz dezenas de processos sem critério
    (CLARO S.A. devolveu 30, ARCELORMITTAL idem);
  - e devolve só os processos daquela pessoa, não de quem se chama como ela.

A pergunta deste teste é: ela traz os mesmos processos que a busca por nome, ou
menos? Se trouxer o originário certo com menos ruído, vira o primeiro caminho
para quem tem CPF, e a busca por nome fica como alternativa.

GABARITO: roda em créditos que JÁ TÊM originário conhecido, para comparar o que
ela devolve com a resposta certa.

Não grava nada.

    python TJBA/teste_busca_por_cpf.py [quantos]
"""
import json
import pathlib
import re
import sys
import time

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import (  # noqa: E402
    BOTAO_PESQUISAR, RE_NADA, URL, Pje, PjeInstavel, so_digitos,
)
from TJBA.medir_busca_por_nome import buscar as buscar_por_nome  # noqa: E402
from utils.banco import conectar  # noqa: E402

CAMPO_CPF = "[id='fPP:dpDec:documentoParte']"
RE_DETALHE = re.compile(
    r"/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+")
RE_NUM = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.\d{2}\.\d{4}")


def buscar_por_cpf(pje, documento):
    """Processos da pessoa, pesquisando pelo DOCUMENTO."""
    pg = pje.pagina
    pje.imagens.clear()
    pje.conferir_no_ar(pg.goto(URL, wait_until="domcontentloaded", timeout=60000), pg.content())
    campo = pg.locator(CAMPO_CPF)
    campo.wait_for(timeout=30000)
    campo.click()
    campo.fill("")
    campo.press_sequentially(documento, delay=30)
    pg.locator(BOTAO_PESQUISAR).click()
    achou = pje.esperar(pg, lambda h: RE_DETALHE.search(h) or ("NADA" if RE_NADA.search(h) else None))
    if achou == "NADA":
        return []
    html = pg.content()
    saida, vistos = [], set()
    for bloco in re.findall(r"<tr.*?</tr>", html, re.S):
        link = RE_DETALHE.search(bloco)
        num = RE_NUM.search(re.sub(r"<[^>]+>", " ", bloco))
        if link and num and num.group(0) not in vistos:
            vistos.add(num.group(0))
            saida.append((link.group(0), num.group(0)))
    return saida


def util(achados, prec20):
    """Candidatos plausíveis: 1º grau, não o próprio precatório, não mais novo."""
    out = set()
    for _l, n in achados:
        d = so_digitos(n)
        if (d != prec20 and d[13:16] == "805" and d[16:20] != "0000"
                and int(d[9:13]) <= int(prec20[9:13])):
            out.add(d)
    return out


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    con = conectar("teste_cpf")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    # creditos do TJBA com originario conhecido E com CPF do credor ja no banco
    cur.execute("""
        SELECT DISTINCT ON (cr.numero_norm)
               cr.numero_norm, p.numero_cnj, pe.documento, pe.nome
          FROM creditos.credito cr
          JOIN creditos.credito_originario co ON co.credito_id = cr.id
          JOIN creditos.processo p            ON p.id = co.processo_id
          JOIN creditos.processo_parte pp     ON pp.processo_id = p.id AND pp.papel_id = 1
          JOIN creditos.pessoa pe             ON pe.id = pp.pessoa_id
         WHERE cr.tribunal_id = 105
           AND pe.documento IS NOT NULL AND length(pe.documento) = 11
         ORDER BY cr.numero_norm, random() LIMIT %s""", (quantos,))
    casos = [{"prec20": so_digitos(a)[:20].rjust(20, "0"), "gabarito": so_digitos(b),
              "doc": c, "nome": d} for a, b, c, d in cur.fetchall()]
    con.close()

    print(f"{len(casos)} casos — comparando busca por CPF x busca por NOME\n", flush=True)
    conta, resultados = {}, []
    pje = Pje()
    pje.abrir()
    t0 = time.time()
    try:
        for i, c in enumerate(casos, 1):
            try:
                por_cpf = util(buscar_por_cpf(pje, c["doc"]), c["prec20"])
            except PjeInstavel:
                por_cpf = None
            except Exception:
                por_cpf = None
            try:
                por_nome = util(buscar_por_nome(pje, c["nome"]), c["prec20"]) if c["nome"] else set()
            except Exception:
                por_nome = set()

            if por_cpf is None:
                v = "INSTAVEL"
            elif c["gabarito"] in por_cpf:
                v = "CPF_ACHOU"
            elif not por_cpf:
                v = "CPF_VAZIO"
            else:
                v = "CPF_ERROU_ALVO"
            conta[v] = conta.get(v, 0) + 1
            resultados.append({"prec": c["prec20"], "veredito": v, "doc": c["doc"][:3] + "***",
                               "n_cpf": len(por_cpf or []), "n_nome": len(por_nome),
                               "nome_achou": c["gabarito"] in por_nome})
            print(f"[{i}/{len(casos)}] {str(c['nome'])[:24]:<24} {v:<15} "
                  f"cpf={len(por_cpf or [])!s:>3} nome={len(por_nome):>3} "
                  f"nome_achou={'sim' if c['gabarito'] in por_nome else 'nao'}", flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "teste_busca_por_cpf.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<18} {v}")
    resp = [r for r in resultados if r["veredito"] != "INSTAVEL"]
    if resp:
        n = len(resp)
        cpf_ok = sum(1 for r in resp if r["veredito"] == "CPF_ACHOU")
        nome_ok = sum(1 for r in resp if r["nome_achou"])
        med_cpf = sum(r["n_cpf"] for r in resp) / n
        med_nome = sum(r["n_nome"] for r in resp) / n
        print(f"\n   respondidos             : {n}")
        print(f"   CPF trouxe o certo      : {cpf_ok}/{n} = {100*cpf_ok/n:.0f}%")
        print(f"   NOME trouxe o certo     : {nome_ok}/{n} = {100*nome_ok/n:.0f}%")
        print(f"   candidatos por busca    : CPF {med_cpf:.1f}  x  NOME {med_nome:.1f}")
        print("   (menos candidatos com o mesmo acerto = menos ruído para desempatar)")


if __name__ == "__main__":
    main()
