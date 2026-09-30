#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTE (só leitura): o PJe público do TJBA acha o originário pesquisando pelo
NOME do credor?

POR QUE ESTE TESTE EXISTE
O fetch_TJBA acha o originário pelo DJEN e só usa o PJe para CONFIRMAR um
candidato — pesquisando por NÚMERO de processo. Sobram 4.380 créditos com
"nenhum candidato no DJEN nem pista": o nome do credor está no banco (medido:
40 de 40 da amostra têm `requerentes` preenchido), mas o DJEN não devolve
processo que passe nos filtros, provavelmente porque o originário é antigo e a
cobertura do DJEN começa em meados de 2025.

A tela de pesquisa do PJe tem campo "Nome da Parte" e campo "CPF CNPJ", que o
robô não usa. Se a busca por nome devolver os processos da pessoa, existe uma
rota que não depende de publicação recente.

Este script NÃO grava nada e NÃO altera o robô: só pergunta e mostra o que veio.

    python TJBA/teste_busca_por_nome.py
"""
import json
import pathlib
import re
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import (  # noqa: E402
    BASE, BOTAO_PESQUISAR, RE_NADA, URL, Pje, formatar_cnj, so_digitos,
)

CAMPO_NOME = "[id='fPP:dnp:nomeParte']"
# Linhas da tabela de resultados: cada uma tem o link do detalhe e o número.
RE_LINHA = re.compile(
    r"(/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+)"
    r".{0,600}?(\d{7}-\d{2}\.\d{4}\.8\.\d{2}\.\d{4})", re.S)


def buscar_por_nome(pje, nome):
    """Pesquisa pelo nome da parte e devolve os processos listados."""
    pg = pje.pagina
    pje.imagens.clear()
    pje.conferir_no_ar(pg.goto(URL, wait_until="domcontentloaded", timeout=60000), pg.content())
    campo = pg.locator(CAMPO_NOME)
    campo.wait_for(timeout=30000)
    campo.click()
    campo.fill("")
    campo.press_sequentially(nome, delay=30)
    pg.locator(BOTAO_PESQUISAR).click()
    achou = pje.esperar(pg, lambda h: RE_LINHA.search(h) or ("NADA" if RE_NADA.search(h) else None))
    if achou == "NADA":
        return []
    html = pg.content()
    vistos, saida = set(), []
    for _link, numero in RE_LINHA.findall(html):
        if numero not in vistos:
            vistos.add(numero)
            saida.append(numero)
    return saida


def parece_originario(numero, prec20):
    """1º grau (foro != 0000), do mesmo tribunal e não mais novo que o precatório."""
    d = so_digitos(numero)
    return (len(d) == 20 and d != prec20 and d[13:16] == "805"
            and d[16:20] != "0000" and int(d[9:13]) <= int(prec20[9:13]))


def main():
    """Pega a amostra do próprio banco: sem arquivo intermediário, o script roda
    em qualquer máquina com o .env configurado."""
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    from utils.banco import conectar
    con = conectar("teste_nome")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    cur.execute("""
        SELECT cr.numero_exibicao, l2.requerentes
          FROM creditos.coleta_credor cc
          JOIN creditos.credito cr ON cr.id = cc.credito_id
          JOIN listas_primarias.processos_unificados_2026_09 l2
            ON regexp_replace(l2.numero_precatorio,'[^0-9]','','g') = cr.numero_norm
         WHERE cc.tribunal_id = 105 AND cc.software_id = 6 AND cc.status_id = 1
           AND l2.requerentes IS NOT NULL
         ORDER BY random() LIMIT %s""", (quantos,))
    casos = [{"prec": prec, "nome": req[0]}
             for prec, req in cur.fetchall() if isinstance(req, list) and req]
    con.close()

    print(f"{len(casos)} casos para testar\n")
    pje = Pje()
    pje.abrir()
    try:
        for c in casos:
            prec20 = so_digitos(c["prec"])
            try:
                achados = buscar_por_nome(pje, c["nome"])
            except Exception as e:
                print(f"{c['nome'][:34]:<34} ERRO {type(e).__name__}: {str(e)[:60]}")
                continue
            candidatos = [n for n in achados if parece_originario(n, prec20)]
            print(f"{c['nome'][:34]:<34} {len(achados):>3} processo(s) | "
                  f"{len(candidatos)} candidato(s) a originário")
            for n in candidatos[:4]:
                print(f"      {n}")
    finally:
        pje.fechar()


if __name__ == "__main__":
    main()
