#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTE (só leitura): QUAIS informações a busca por nome do PJe/TJBA devolve.

Mostra as colunas da tabela de resultados e o que há no detalhe de um processo
(partes, polos e documento), para decidir se a rota serve. Não grava nada.

    python TJBA/teste_campos_por_nome.py "NOME DA PESSOA"
"""
import pathlib
import re
import sys
from urllib.parse import urljoin

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import (  # noqa: E402
    BASE, BOTAO_PESQUISAR, RE_NADA, URL, Pje, partes_da_pagina, campos_da_capa,
)

CAMPO_NOME = "[id='fPP:dnp:nomeParte']"
RE_DETALHE = re.compile(
    r"/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+")


def limpar(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).replace("\xa0", " ").strip()


def main():
    nome = sys.argv[1] if len(sys.argv) > 1 else "ANGELA MARIA DA SILVA E SILVA"
    pje = Pje()
    pje.abrir()
    try:
        pg = pje.pagina
        pje.imagens.clear()
        pje.conferir_no_ar(pg.goto(URL, wait_until="domcontentloaded", timeout=60000), pg.content())
        campo = pg.locator(CAMPO_NOME)
        campo.wait_for(timeout=30000)
        campo.click(); campo.fill(""); campo.press_sequentially(nome, delay=30)
        pg.locator(BOTAO_PESQUISAR).click()
        achou = pje.esperar(pg, lambda h: RE_DETALHE.search(h) or ("NADA" if RE_NADA.search(h) else None))
        if achou == "NADA":
            print("não achou nada"); return
        html = pg.content()

        print("=== COLUNAS DA TABELA DE RESULTADOS ===")
        cab = re.search(r"<thead.*?</thead>", html, re.S)
        if cab:
            for th in re.findall(r"<th.*?</th>", cab.group(0), re.S):
                t = limpar(th)
                if t:
                    print("   -", t[:60])
        print("\n=== PRIMEIRAS LINHAS ===")
        corpo = re.search(r"<tbody.*?</tbody>", html, re.S)
        if corpo:
            for tr in re.findall(r"<tr.*?</tr>", corpo.group(0), re.S)[:4]:
                celulas = [limpar(td) for td in re.findall(r"<td.*?</td>", tr, re.S)]
                celulas = [c for c in celulas if c]
                if celulas:
                    print("   |", " | ".join(c[:34] for c in celulas))

        link = RE_DETALHE.search(html)
        det = pg.context.new_page()
        try:
            pje.conferir_no_ar(det.goto(urljoin(BASE, link.group(0)), wait_until="domcontentloaded",
                                        timeout=60000), det.content())
            pje.esperar(det, lambda h: "processoPartesPoloAtivo" in h or "Polo ativo" in h)
            dhtml = det.content()
            print("\n=== CAPA DO PROCESSO ===")
            for k, v in (campos_da_capa(dhtml) or {}).items():
                print(f"   {k}: {str(v)[:60]}")
            print("\n=== PARTES ===")
            for p in (partes_da_pagina(dhtml) or [])[:8]:
                print("   ", p)
            print("\n=== documento no detalhe? ===")
            for pad, rot in [(r"\d{3}\.\d{3}\.\d{3}-\d{2}", "CPF"),
                             (r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", "CNPJ"),
                             (r"\d{3}\.\*{3}\.\*{3}-\d{2}|\*{3}\.\d{3}\.\d{3}-\*{2}", "CPF mascarado")]:
                a = re.findall(pad, limpar(dhtml))
                print(f"   {rot:<14} {len(a)}  {a[:2]}")
        finally:
            det.close()
    finally:
        pje.fechar()


if __name__ == "__main__":
    main()
