"""
testar_cpf_advogado_pje.py - TESTE (só leitura): CPF do advogado que é o próprio credor do precatório, quando o banco
não tem, pelo PJe 1º grau do TJMA.

Alvo: créditos do fetch_TJMA.py em SUCESSO_INCOMPLETO 'SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO beneficiario=<nome>
oab=<UF><número>' (o advogado é o credor, mas o banco não tem o CPF dele pela OAB).

Para cada um:
1. confere de novo o banco (pessoa_oab -> pessoa.documento);
2. DJEN pela OAB no TJMA (últimos 3 anos, 100 publicações): os processos de 1º grau em que ele atua;
3. abre até --processos deles no PJe 1º grau (consulta pública) e lê, na 1ª página das partes, a linha
   'NOME - OAB MA8135-A - CPF: 627.790.013-72 (ADVOGADO)'. Aceita a OAB com sufixo ('-A', suplementar), que a leitura
   de partes do robô hoje não pega.
O CPF vale se a OAB (UF e número) bate; sem OAB na linha, se o nome é o mesmo.

Não grava nada no banco. Resultado em TJMA/saida/teste_cpf_advogado_pje_<data>.csv.

Uso:
    python TJMA/testar_cpf_advogado_pje.py                 # todos os alvos
    python TJMA/testar_cpf_advogado_pje.py --limite 40     # amostra
    python TJMA/testar_cpf_advogado_pje.py --continuar     # retoma o último CSV (refaz os que deram ERRO)
"""
import argparse
import csv
import re
import sys
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

AQUI = Path(__file__).resolve().parent
for p in (AQUI, AQUI.parent):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import fetch_TJMA as F  # noqa: E402

RE_MOTIVO = re.compile(r"beneficiario=(?P<nome>.+?) oab=(?P<uf>[A-Z]{2})(?P<num>\d+)")
RE_ADVOGADO = re.compile(
    r"([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ][A-Za-zÁÂÃÀÉÊÍÓÔÕÚÜÇáâãàéêíóôõúüç'\.\s]{3,90}?)"
    r"\s*-\s*OAB\s*([A-Z]{2})\s*0*(\d+)(?:\s*-?\s*[A-Z])?"
    r"\s*-\s*CPF:\s*([\d\.\-]{11,14})\s*\(ADVOGADO\)")
JANELA_DJEN = 3 * 365

SQL_ALVOS = """
SELECT cc.credito_id, c.numero_exibicao, cc.motivo_detalhe
  FROM creditos.coleta_credor cc
  JOIN creditos.software s       ON s.id = cc.software_id
  JOIN creditos.status_coleta st ON st.id = cc.status_id
  JOIN creditos.credito c        ON c.id = cc.credito_id
 WHERE cc.tribunal_id = 110 AND s.codigo = 'CONSULTA_PUBLICA_TJMA' AND st.codigo = 'SUCESSO_INCOMPLETO'
   AND cc.motivo_detalhe LIKE 'SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO%%'
 ORDER BY cc.credito_id
"""


def cpf_no_banco(cur, uf, num):
    cur.execute("""SELECT DISTINCT p.documento::text FROM creditos.pessoa_oab o JOIN creditos.pessoa p ON p.id = o.pessoa_id
                    WHERE o.uf::text = %s AND o.numero::text = %s AND p.documento IS NOT NULL""", (uf, num))
    docs = {F.so_digitos(d) for (d,) in cur.fetchall()}
    return next(iter(docs)) if len(docs) == 1 else ""


def advogados_da_pagina(html):
    """[(nome, uf, número, cpf)] das linhas de advogado da página de detalhe do PJe."""
    texto = " ".join(re.sub(r"<[^>]+>", " ", F.sem_script(html)).split())
    return [(" ".join(m[1].split()), m[2], m[3], F.so_digitos(m[4])) for m in RE_ADVOGADO.finditer(texto)]


def cpf_no_pje(djen, pje, nome, uf, num, max_processos):
    """(cpf, processo, processos abertos, publicações) pelo DJEN (OAB) -> PJe 1º grau."""
    hoje = date.today()
    pubs = djen.buscar(False, True, numeroOab=num, ufOab=uf, siglaTribunal="TJMA", pagina=1, itensPorPagina=100,
                       dataDisponibilizacaoInicio=(hoje - timedelta(days=JANELA_DJEN)).isoformat(),
                       dataDisponibilizacaoFim=hoje.isoformat())
    numeros = list(dict.fromkeys(i["numero"] for i in pubs if F.cnj_1g_tjma(i["numero"])))
    abertos = 0
    for n in numeros[:max_processos]:
        linha = pje.por_numero(n)
        if not linha:
            continue
        abertos += 1
        html = pje._http("GET", urljoin(F.BASE, linha["link"]), headers={"Referer": F.URL}).text
        time.sleep(F.PAUSA_PJE)
        for a_nome, a_uf, a_num, cpf in advogados_da_pagina(html):
            if not F.documento_valido(cpf):
                continue
            if (a_uf == uf and a_num == num.lstrip("0")) or F.mesma_pessoa(a_nome, nome):
                return cpf, F.formatar_cnj(n), abertos, len(pubs)
    return "", "", abertos, len(pubs)


def main():
    ap = argparse.ArgumentParser(description="Teste: CPF do advogado-credor do TJMA pelo PJe (só leitura).")
    ap.add_argument("--limite", type=int, default=None, help="só os N primeiros alvos")
    ap.add_argument("--processos", type=int, default=3, help="processos abertos no PJe por advogado (padrão 3)")
    ap.add_argument("--continuar", action="store_true",
                    help="retoma o CSV mais recente: pula os créditos já feitos e refaz os que deram ERRO")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    con = F.conectar()
    cur = con.cursor()
    cur.execute(SQL_ALVOS)
    alvos = cur.fetchall()[:args.limite or None]
    F.SAIDA.mkdir(exist_ok=True)
    feitos, anteriores = set(), sorted(F.SAIDA.glob("teste_cpf_advogado_pje_*.csv"))
    if args.continuar and anteriores:
        arquivo = anteriores[-1]
        linhas = list(csv.DictReader(open(arquivo, encoding="utf-8-sig"), delimiter=";"))
        feitos = {int(x["credito_id"]) for x in linhas if not x["resultado"].startswith("ERRO")}
        with open(arquivo, "w", newline="", encoding="utf-8-sig") as f:     # tira os ERRO, que serão refeitos
            w = csv.DictWriter(f, fieldnames=list(linhas[0].keys()) if linhas else [], delimiter=";")
            w.writeheader()
            w.writerows(x for x in linhas if not x["resultado"].startswith("ERRO"))
    else:
        arquivo = F.SAIDA / f"teste_cpf_advogado_pje_{datetime.now():%Y%m%d_%H%M%S}.csv"
    alvos = [a for a in alvos if a[0] not in feitos]
    print(f"{len(alvos)} crédito(s) com o advogado-credor sem CPF no banco"
          + (f" (retomando {arquivo.name}: {len(feitos)} já feitos)" if feitos else ""))
    parar = threading.Event()
    djen, pje = F.Djen(parar, False), F.Pje(parar)
    cont, cache = Counter(), {}
    novo = not feitos
    with open(arquivo, "w" if novo else "a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        if novo:
            w.writerow(["credito_id", "precatorio", "advogado", "oab", "resultado", "cpf", "processo_pje",
                        "processos_abertos", "publicacoes_djen", "segundos"])
        for i, (cid, prec, motivo) in enumerate(alvos, 1):
            t0 = time.time()
            m = RE_MOTIVO.search(motivo or "")
            if not m:
                cont["motivo sem OAB"] += 1
                continue
            nome, uf, num = m["nome"], m["uf"], m["num"]
            cpf, proc, abertos, pubs = cpf_no_banco(cur, uf, num), "", 0, 0
            if cpf:
                resultado = "BANCO"
            else:
                try:
                    if (uf, num) not in cache:
                        cache[(uf, num)] = cpf_no_pje(djen, pje, nome, uf, num, args.processos)
                    cpf, proc, abertos, pubs = cache[(uf, num)]
                    resultado = "PJE" if cpf else ("SEM_PUBLICACAO_DJEN" if not pubs else "NAO_ACHOU")
                except F.ErroTecnico as e:
                    resultado = f"ERRO: {e}"[:80]
            cont[resultado.split(":")[0]] += 1
            w.writerow([cid, prec, nome, f"{uf}{num}", resultado, cpf, proc, abertos, pubs, round(time.time() - t0)])
            f.flush()
            print(f"[{i}/{len(alvos)}] {cid} {prec} {nome} {uf}{num} -> {resultado} "
                  f"{(cpf[:3] + '…') if cpf else ''} {proc} ({time.time() - t0:.0f}s)", flush=True)
    con.close()
    print(f"== resumo: {dict(cont)} | CSV: {arquivo}")


if __name__ == "__main__":
    main()
