#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEDIÇÃO (só leitura): taxa de acerto da busca por NOME no PJe público do TJBA,
sobre os créditos que o robô encerrou como "nenhum candidato no DJEN nem pista".

Um caso conta como ACERTO quando, entre os processos que a busca pelo nome
devolve, existe um que:
  - é de 1º grau do TJBA (foro != 0000) e não é o próprio precatório;
  - não é mais novo que o precatório;
  - tem o CREDOR no polo ATIVO (e não como advogado);
  - tem um ente público no polo PASSIVO;
  - e traz o CPF/CNPJ do credor.

Ou seja: mede o que interessa, não "achou algum processo".

O PJe do TJBA anda instável (pesquisa que não volta em 90 s). Falha assim NÃO é
"não encontrado": conta em `instavel` e fica de fora do denominador — misturar as
duas coisas é o erro que faz uma rota boa parecer ruim.

    python TJBA/medir_busca_por_nome.py [quantos]

Não grava nada no banco. Saída em TJBA/saida/medicao_busca_por_nome.json.
"""
import json
import pathlib
import re
import sys
import time
from urllib.parse import urljoin

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import (  # noqa: E402
    BASE, BOTAO_PESQUISAR, RE_NADA, URL, Pje, PjeInstavel, campos_da_capa,
    chave_nome, normal, partes_da_pagina, so_digitos,
)
from utils.banco import conectar  # noqa: E402

CAMPO_NOME = "[id='fPP:dnp:nomeParte']"
RE_DETALHE = re.compile(
    r"/pje/ConsultaPublica/DetalheProcessoConsultaPublica/listView\.seam\?ca=[^'\"\s<>]+")
RE_NUM = re.compile(r"\d{7}-\d{2}\.\d{4}\.8\.\d{2}\.\d{4}")
ENTE = ("ESTADO", "MUNICIPIO", "SECRETARIA", "FAZENDA", "INSTITUTO", "AUTARQUIA",
        "PREFEITURA", "PROCURADORIA", "FUNDACAO", "DEPARTAMENTO", "UNIAO")
MAX_DETALHES = 6          # abrir detalhe custa caro; os mais promissores bastam


def e_ente(nome):
    return any(t in normal(nome).upper() for t in ENTE)


def linhas_do_resultado(html):
    """[(link, numero)] da tabela de resultados, sem repetir."""
    saida, vistos = [], set()
    for bloco in re.findall(r"<tr.*?</tr>", html, re.S):
        link = RE_DETALHE.search(bloco)
        num = RE_NUM.search(re.sub(r"<[^>]+>", " ", bloco))
        if link and num and num.group(0) not in vistos:
            vistos.add(num.group(0))
            saida.append((link.group(0), num.group(0)))
    return saida


def buscar(pje, nome):
    pg = pje.pagina
    pje.imagens.clear()
    pje.conferir_no_ar(pg.goto(URL, wait_until="domcontentloaded", timeout=60000), pg.content())
    campo = pg.locator(CAMPO_NOME)
    campo.wait_for(timeout=30000)
    campo.click(); campo.fill(""); campo.press_sequentially(nome, delay=30)
    pg.locator(BOTAO_PESQUISAR).click()
    achou = pje.esperar(pg, lambda h: RE_DETALHE.search(h) or ("NADA" if RE_NADA.search(h) else None))
    return [] if achou == "NADA" else linhas_do_resultado(pg.content())


def abrir_detalhe(pje, link):
    det = pje.pagina.context.new_page()
    try:
        pje.conferir_no_ar(det.goto(urljoin(BASE, link), wait_until="domcontentloaded",
                                    timeout=60000), det.content())
        pje.esperar(det, lambda h: "processoPartesPoloAtivo" in h or "Polo ativo" in h)
        html = det.content()
        return partes_da_pagina(html) or [], campos_da_capa(html) or {}
    finally:
        det.close()


def avaliar(pje, caso):
    """Devolve o veredito de um caso: ACERTO / SEM_CANDIDATO / NAO_CONFIRMADO."""
    prec20, nome = so_digitos(caso["prec"]), caso["nome"]
    achados = buscar(pje, nome)
    candidatos = [(l, n) for l, n in achados
                  if (d := so_digitos(n)) and d != prec20 and d[13:16] == "805"
                  and d[16:20] != "0000" and int(d[9:13]) <= int(prec20[9:13])]
    if not candidatos:
        return {"veredito": "SEM_CANDIDATO", "achados": len(achados), "candidatos": 0}

    alvo = chave_nome(nome)
    for link, numero in candidatos[:MAX_DETALHES]:
        partes, capa = abrir_detalhe(pje, link)
        credor = next((p for p in partes
                       if p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"
                       and chave_nome(p.get("nome", "")) == alvo), None)
        tem_ente = any(p.get("polo") == "PASSIVO" and e_ente(p.get("nome", "")) for p in partes)
        if credor and tem_ente:
            return {"veredito": "ACERTO", "achados": len(achados),
                    "candidatos": len(candidatos), "originario": numero,
                    "documento": credor.get("documento") or "",
                    "classe": capa.get("classe_judicial", "")[:48]}
    return {"veredito": "NAO_CONFIRMADO", "achados": len(achados), "candidatos": len(candidatos)}


GRUPOS = {
    # O RESÍDUO: o robô já tentou e desistiu. Medir uma rota nova aqui e comparar
    # com a taxa geral do robô é comparar populações diferentes — foi o erro que
    # fez a busca por nome parecer 15%.
    "residuo": "cc.status_id = 30 AND cc.motivo_detalhe LIKE '%%nenhum candidato%%'",
    # A FILA DE VERDADE: nunca tentados. É aqui que a taxa da rota se compara
    # com a do robô, porque é a mesma população.
    "pendente": "cc.status_id = 1",
}


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    grupo = sys.argv[2] if len(sys.argv) > 2 else "residuo"
    if grupo not in GRUPOS:
        sys.exit(f"grupo deve ser um de {list(GRUPOS)}")
    con = conectar("medicao_nome")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    print(f"grupo: {grupo}")
    cur.execute(f"""
        SELECT cr.numero_exibicao, l2.requerentes
          FROM creditos.coleta_credor cc
          JOIN creditos.credito cr ON cr.id = cc.credito_id
          JOIN listas_primarias.processos_unificados_2026_09 l2
            ON regexp_replace(l2.numero_precatorio,'[^0-9]','','g') = cr.numero_norm
         WHERE cc.tribunal_id = 105 AND cc.software_id = 6
           AND {GRUPOS[grupo]}
           AND l2.requerentes IS NOT NULL
         ORDER BY random() LIMIT %s""", (quantos * 2,))
    casos = []
    for prec, req in cur.fetchall():
        nomes = req if isinstance(req, list) else []
        if nomes and not e_ente(nomes[0]):          # empresa traz processo demais: fora da medição
            casos.append({"prec": prec, "nome": nomes[0]})
        if len(casos) >= quantos:
            break
    con.close()

    print(f"medindo {len(casos)} casos (pessoas físicas do grupo 'nenhum candidato')\n", flush=True)
    resultados, pje = [], Pje()
    pje.abrir()
    t0 = time.time()
    try:
        for i, caso in enumerate(casos, 1):
            try:
                r = avaliar(pje, caso)
            except PjeInstavel as e:
                r = {"veredito": "INSTAVEL", "erro": str(e)[:70]}
            except Exception as e:
                r = {"veredito": "ERRO", "erro": f"{type(e).__name__}: {str(e)[:60]}"}
            r.update(prec=caso["prec"], nome=caso["nome"])
            resultados.append(r)
            print(f"[{i}/{len(casos)}] {caso['nome'][:30]:<30} {r['veredito']:<15} "
                  f"{r.get('originario','')} {r.get('documento','')}", flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "medicao_busca_por_nome.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    conta = {}
    for r in resultados:
        conta[r["veredito"]] = conta.get(r["veredito"], 0) + 1
    validos = len(resultados) - conta.get("INSTAVEL", 0) - conta.get("ERRO", 0)
    acertos = conta.get("ACERTO", 0)
    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<16} {v}")
    print(f"\n   respondidos: {validos}  (instável/erro fica fora do denominador)")
    if validos:
        print(f"   TAXA DE ACERTO: {acertos}/{validos} = {100*acertos/validos:.0f}%")
    com_cpf = sum(1 for r in resultados if r.get("documento"))
    print(f"   com CPF/CNPJ do credor: {com_cpf}")


if __name__ == "__main__":
    main()
