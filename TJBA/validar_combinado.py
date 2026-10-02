#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDAÇÃO (só leitura) da rota COMBINADA: DJEN + busca por nome no PJe, com o
desempate por evidência forte.

DE ONDE VEIO ESTA IDEIA
Medido em 30/09/2026, contra créditos com originário JÁ CONHECIDO no banco:

    só busca por nome no PJe
        acertou de primeira   32%
        o certo estava junto  57%   <- achou, mas escolheu o candidato errado
        falso positivo         4%

Ou seja: a busca por nome ENCONTRA o originário certo em 89% das vezes; o que
falha é ESCOLHER. O critério estrutural (1º grau, credor no polo ativo, ente no
passivo) casa com qualquer ação daquela pessoa contra a Fazenda, e uma pessoa
costuma ter várias.

O robô já resolve isso para os candidatos do DJEN, com o que ele chama de
evidência forte: o VALOR do precatório citado na publicação do candidato, ou o
NÚMERO do precatório citado no texto. Esta rota junta as duas pontas:

    candidatos = DJEN (o que o robô já faz)  +  busca por nome no PJe (novo)
    escolha    = evidência forte primeiro; estrutural só como último recurso

A expectativa é subir o acerto sem aumentar o falso positivo — e, de quebra,
alcançar originário antigo, que é onde o DJEN não chega (cobertura começa em
meados de 2025).

VEREDITOS
    IGUAL_FORTE        escolheu o certo, com evidência forte
    IGUAL_FRACO        escolheu o certo, só pelo estrutural
    ESTAVA_NA_LISTA    o certo apareceu, escolheu outro
    DIFERENTE          escolheu outro e o certo nem apareceu  <- falso positivo
    NAO_ACHOU          nenhum candidato

Não grava nada.

    python TJBA/validar_combinado.py [quantos]
"""
import json
import pathlib
import re
import sys
import time

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from TJBA.fetch_TJBA import (  # noqa: E402
    CNJ, Djen, Pje, PjeInstavel, candidatos_djen, chave_nome, chaves_ente,
    formatos_valor, nomes_para_buscar, oabs_do_precatorio, saida_djen, so_digitos,
)
from TJBA.medir_busca_por_nome import abrir_detalhe, buscar, e_empresa, e_ente  # noqa: E402
from utils.banco import conectar  # noqa: E402

MAX_DETALHES = 8          # candidatos do PJe que valem abrir


def forte(djen, cnj20, prec20, valores):
    """Evidência forte: a publicação do candidato cita o VALOR do precatório ou o
    PRÓPRIO número do precatório. É o mesmo teste que o robô usa no DJEN."""
    try:
        itens = djen.por_numero(cnj20)
    except Exception:
        return False
    texto = " ".join(x["texto"] for x in itens)
    citados = {so_digitos(x) for x in CNJ.findall(texto)}
    return prec20 in citados or any(v in texto for v in valores)


def confirma_estrutura(pje, link, alvo):
    """Credor no polo ATIVO (não como advogado) e ente público no PASSIVO."""
    partes, _ = abrir_detalhe(pje, link)
    credor = next((p for p in partes
                   if p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"
                   and chave_nome(p.get("nome", "")) == alvo), None)
    ente = any(p.get("polo") == "PASSIVO" and e_ente(p.get("nome", "")) for p in partes)
    return bool(credor and ente)


def confirma_por_numero(pje, cnj20, alvo):
    """Mesma checagem estrutural, mas pesquisando o processo PELO NÚMERO.

    POR QUE EXISTE: o candidato vindo do DJEN não tem link do PJe — só o número.
    Na primeira versão eu só abria detalhe de quem TINHA link, então o desempate
    estrutural nunca rodava para os candidatos do DJEN, que são a maioria. O
    resultado saiu com uma perna amarrada (quase tudo com escolhido = '-')."""
    r = pje.consultar(cnj20)
    partes = r.get("partes") or []
    credor = any(p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"
                 and chave_nome(p.get("nome", "")) in alvo for p in partes)
    ente = any(p.get("polo") == "PASSIVO" and e_ente(p.get("nome", "")) for p in partes)
    return bool(credor and ente)


def detalhe_por_numero(pje, cnj20):
    """(partes, capa) pesquisando o processo PELO NÚMERO.

    POR QUE EXISTE: candidato vindo do DJEN não tem link do PJe, só o número. Na
    primeira versão eu só abria detalhe de quem TINHA link, então a checagem
    estrutural nunca rodava para os candidatos do DJEN — que são a maioria."""
    r = pje.consultar(cnj20)
    return (r.get("partes") or []), (r.get("capa") or {})


def anterior_ao_precatorio(data_autuacao, prec20):
    """A ação é anterior ao precatório? Precatório nasce de sentença já
    transitada, então candidato autuado DEPOIS está descartado.

    Compara pelo ano, que é o que o número CNJ garante — data de autuação vem
    como texto do PJe e nem sempre está lá."""
    if not data_autuacao:
        return False
    ano = re.search(r"(19|20)\d{2}", str(data_autuacao))
    return bool(ano) and int(ano.group(0)) <= int(prec20[9:13])


def oabs_do_candidato(djen, cnj20):
    try:
        return {a["oab"] + a["uf"] for x in djen.por_numero(cnj20) for a in x["advogados"]}
    except Exception:
        return set()


def escolher(pje, djen, prec20, nomes, valores, chaves):
    """Devolve (escolhido, evidencia, todos_os_candidatos).

    `evidencia` diz QUÃO confiável foi a escolha, e é isso que decide se o
    resultado pode ser gravado ou só sugerido:
        "forte"  valor do precatório ou o próprio número citados na publicação
        "oab"    advogado em comum com o precatório
        "unico"  um único candidato passou na checagem estrutural
        None     não escolheu
    """
    alvo = {chave_nome(n) for n in nomes}
    candidatos, links = [], {}

    # 1) o que o robô já faz: candidatos do DJEN pelo nome
    try:
        do_djen, _ = candidatos_djen(djen, {"precatorio20": prec20, "valores": valores},
                                     nomes, chaves)
        candidatos += [n for n, _info in do_djen]
    except Exception:
        pass

    # 2) o que é novo: a busca por nome no PJe, que alcança processo antigo
    for nome in nomes:
        try:
            for link, numero in buscar(pje, nome):
                d = so_digitos(numero)
                if (d != prec20 and d[13:16] == "805" and d[16:20] != "0000"
                        and int(d[9:13]) <= int(prec20[9:13])):
                    links.setdefault(d, link)
                    if d not in candidatos:
                        candidatos.append(d)
        except PjeInstavel:
            raise
        except Exception:
            continue

    if not candidatos:
        return None, None, []

    # ORDENAR ANTES DE CORTAR.
    #
    # Só os MAX_DETALHES primeiros são examinados, e a ordem em que eles chegam
    # é arbitrária (vem do DJEN e da busca, sem critério). Medido em 01/10/2026:
    # os casos que ficaram sem decisão eram justamente os de muitos candidatos —
    # mediana 30 nos "não achou", 15 dos 43 "estava na lista" tinham mais de 8.
    # Ou seja, o certo costuma estar na posição 9 ou além, e eu nunca olhava.
    #
    # Ordenar por OAB em comum primeiro sai de graça: o DJEN já devolve os
    # advogados, e a OAB foi o sinal presente em TODAS as decisões corretas.
    oabs_prec = oabs_do_precatorio(djen, prec20)
    if len(candidatos) > MAX_DETALHES and oabs_prec:
        com_oab = {d for d in candidatos if oabs_do_candidato(djen, d) & oabs_prec}
        candidatos.sort(key=lambda d: (d not in com_oab, -int(d[9:13])))

    # PONTUAÇÃO COM CORROBORAÇÃO
    #
    # Nenhum sinal sozinho prova, e é por isso que não decidimos por um só:
    #   - o NOME não basta: existe homônimo, e a mesma pessoa costuma ter várias
    #     ações contra a Fazenda;
    #   - a OAB não basta: o mesmo advogado pode ter entrado com DUAS ações
    #     diferentes para o MESMO cliente;
    #   - o estrutural não basta: casa com qualquer ação dela contra ente público.
    #
    # Só o valor/número citados na publicação prova sozinho. O resto precisa se
    # somar. E a decisão exige VANTAGEM: o primeiro colocado tem de estar à
    # frente do segundo, senão é empate e a gente não escolhe.
    PESO = {"forte": 4, "oab": 2, "estrutura": 1, "data": 1}
    MINIMO = 3          # ou a prova forte, ou dois indícios independentes

    placar = {}
    for d in candidatos[:MAX_DETALHES]:
        sinais = []
        if forte(djen, d, prec20, valores):
            sinais.append("forte")
        if oabs_prec and (oabs_do_candidato(djen, d) & oabs_prec):
            sinais.append("oab")
        try:
            partes, capa = ((abrir_detalhe(pje, links[d]) if d in links
                             else detalhe_por_numero(pje, d)))
        except PjeInstavel:
            raise
        except Exception:
            partes, capa = [], {}
        credor = any(p.get("polo") == "ATIVO" and p.get("papel") != "ADVOGADO"
                     and chave_nome(p.get("nome", "")) in alvo for p in partes)
        ente = any(p.get("polo") == "PASSIVO" and e_ente(p.get("nome", "")) for p in partes)
        if credor and ente:
            sinais.append("estrutura")
        # A ação tem de ser ANTERIOR ao precatório: precatório nasce de sentença
        # já transitada. Candidato autuado depois está descartado de saída.
        if anterior_ao_precatorio(capa.get("data_autuacao"), prec20):
            sinais.append("data")
        placar[d] = (sum(PESO[s] for s in sinais), sinais)

    if not placar:
        return None, None, candidatos
    ordem = sorted(placar.items(), key=lambda kv: -kv[1][0])
    (melhor, (pts, sinais)) = ordem[0]
    segundo = ordem[1][1][0] if len(ordem) > 1 else 0

    # CANDIDATO ÚNICO exige menos. Com um só na mesa não há com quem confundir:
    # ou é ele, ou não é nenhum — o risco de ESCOLHER ERRADO não existe, que é o
    # risco que o mínimo de 3 pontos protege. Medido: 7 dos 43 casos sem decisão
    # tinham candidato único e foram recusados por somar 2 pontos.
    minimo = 2 if len(candidatos) == 1 else MINIMO

    if pts < minimo or pts == segundo:          # fraco demais, ou empatado
        return None, None, candidatos
    return melhor, "+".join(sinais), candidatos


def main():
    quantos = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    con = conectar("validar_combinado")
    cur = con.cursor()
    cur.execute("SET statement_timeout='300s'")
    cur.execute("""
        SELECT cr.numero_norm, p.numero_cnj, l2.requerentes, l2.entidade_devedora,
               l2.valor_lista
          FROM creditos.credito cr
          JOIN creditos.credito_originario co ON co.credito_id = cr.id
          JOIN creditos.processo p            ON p.id = co.processo_id
          JOIN listas_primarias.processos_unificados_2026_09 l2
            ON regexp_replace(l2.numero_precatorio,'[^0-9]','','g') = cr.numero_norm
         WHERE cr.tribunal_id = 105 AND l2.requerentes IS NOT NULL
         ORDER BY random() LIMIT %s""", (quantos * 3,))
    casos = []
    for prec, orig, req, ente, valor in cur.fetchall():
        nomes = req if isinstance(req, list) else []
        if nomes and not e_ente(nomes[0]) and not e_empresa(nomes[0]):
            # entidade_devedora e valor_lista são ARRAY na lista antiga (como
            # requerentes e numero_originario). chaves_ente() espera texto.
            ente_txt = " - ".join(ente) if isinstance(ente, list) else (ente or "")
            valor_1 = valor[0] if isinstance(valor, list) and valor else valor
            casos.append({"prec20": so_digitos(prec)[:20].rjust(20, "0"),
                          "gabarito": so_digitos(orig), "nomes": nomes_para_buscar(nomes[0]),
                          "ente": ente_txt, "valor": valor_1})
        if len(casos) >= quantos:
            break
    con.close()

    print(f"validando {len(casos)} créditos com originário conhecido (rota combinada)\n", flush=True)
    djen = Djen(saida_djen(1))
    pje = Pje()
    pje.abrir()
    conta, resultados = {}, []
    t0 = time.time()
    try:
        for i, c in enumerate(casos, 1):
            valores = set()
            if c["valor"]:
                try:
                    valores = formatos_valor(float(str(c["valor"]).replace(",", ".")))
                except Exception:
                    valores = set()
            chaves = chaves_ente(c["ente"])
            try:
                esc, ev, cands = escolher(pje, djen, c["prec20"], c["nomes"], valores, chaves)
                if esc and esc == c["gabarito"]:
                    v = "IGUAL"
                elif c["gabarito"] in cands:
                    v = "ESTAVA_NA_LISTA"
                elif esc:
                    v = "DIFERENTE"
                else:
                    v = "NAO_ACHOU"
            except PjeInstavel:
                v, esc, ev, cands = "INSTAVEL", None, None, []
            except Exception as e:
                v, esc, ev, cands = f"ERRO_{type(e).__name__}", None, None, []
            conta[v] = conta.get(v, 0) + 1
            resultados.append({"prec": c["prec20"], "veredito": v, "gabarito": c["gabarito"],
                               "escolhido": esc, "evidencia": ev, "candidatos": len(cands),
                               "nome": c["nomes"][0]})
            print(f"[{i}/{len(casos)}] {c['nomes'][0][:26]:<26} {v:<16} "
                  f"gab={c['gabarito'][-12:]} esc={(esc or '-')[-12:]} cands={len(cands)} {ev or ''}",
                  flush=True)
    finally:
        pje.fechar()

    saida = RAIZ / "TJBA" / "saida"
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "validacao_combinada.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")

    # CSV para CONFERÊNCIA NA MÃO. O resultado não pode depender de acreditar no
    # script: aqui vai, caso a caso, o que ele escolheu, qual era a resposta já
    # conhecida no banco e com que evidência decidiu — com o número formatado,
    # pronto para colar na consulta pública e conferir.
    from TJBA.fetch_TJBA import formatar_cnj
    from utils.arquivos import gravar_csv
    linhas = []
    for r in resultados:
        bate = ("SIM" if r["veredito"] == "IGUAL"
                else "NAO - ERROU" if r["veredito"] == "DIFERENTE" else "")
        linhas.append({
            "precatorio": formatar_cnj(r["prec"]),
            "credor": r.get("nome", ""),
            "originario_conhecido": formatar_cnj(r["gabarito"]) if r.get("gabarito") else "",
            "originario_escolhido": formatar_cnj(r["escolhido"]) if r.get("escolhido") else "",
            "bateu": bate,
            "veredito": r["veredito"],
            "evidencia": r.get("evidencia") or "",
            "candidatos": r["candidatos"],
        })
    gravar_csv(saida / "conferencia_manual.csv", linhas)
    print(f"conferência manual: {saida / 'conferencia_manual.csv'}")

    print(f"\n=== RESULTADO ({time.time()-t0:.0f}s) ===")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"   {k:<18} {v}")
    resp = [r for r in resultados if not str(r["veredito"]).startswith(("INSTAVEL", "ERRO"))]
    if resp:
        n = len(resp)
        f_ = sum(1 for r in resp if r["veredito"] == "IGUAL")
        w = 0
        li = sum(1 for r in resp if r["veredito"] == "ESTAVA_NA_LISTA")
        di = sum(1 for r in resp if r["veredito"] == "DIFERENTE")
        print(f"\n   respondidos            : {n}")
        print(f"   ACERTO                 : {f_}/{n} = {100*f_/n:.0f}%")
        from collections import Counter
        comb = Counter(r["evidencia"] for r in resp if r["veredito"] == "IGUAL")
        for k, qt in comb.most_common():
            print(f"      por {k:<24} {qt}")
        print(f"   o certo estava junto   : {li}/{n} = {100*li/n:.0f}%")
        print(f"   FALSO POSITIVO         : {di}/{n} = {100*di/n:.0f}%")


if __name__ == "__main__":
    main()
