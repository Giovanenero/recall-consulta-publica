"""
relacao_djerj.py - a "relação de precatórios" que o TJRJ publicou no DJERJ (achado de 08/10/2026).

Em julho de cada ano o TJRJ publicou a lista dos precatórios apresentados no ano anterior (ordem cronológica do
orçamento seguinte). Cada linha traz:

    <ordem 9 dígitos> <precatório> <beneficiário> (<CPF/CNPJ completo>) <data de protocolo>
    <ADVOGADO> (<UF><OAB>)
    <originário CNJ> ( <número antigo> )
    <assunto - classe>
    <cartório>

Edições com CPF/CNPJ (as de antes de 2019 trazem só "NOME; ADVOGADO OAB/RJ-"; as de 2024 em diante, só o número):

    16/07/2019 Caderno II (Judicial)  apresentados 07/2018 a 06/2019 (a lista sai 4 vezes iguais na edição)
    14/07/2020 Caderno II (Judicial)  apresentados 07/2019 a 06/2020
    14/07/2021 Caderno I (Administrativo)  07/2020 a 06/2021
    28/04/2022 Caderno I  07/2021 a 04/2022
    19/05/2023 Caderno I  05/2022 a 04/2023

Busca pública, sem login, sem captcha e sem os PROXY_0x: GET consultadje/pdf.aspx (dá o guid da página) e
GET Handlers/DownloadPdf.ashx (o PDF de uma página), no mínimo RITMO s entre duas chamadas.

Como acha as páginas: a partir das páginas-semente de cada edição (da busca por número de precatório), anda para trás
e para frente página a página até FIM_SEM_LINHA páginas seguidas sem linha da relação (ou até a edição acabar). Para
a frente também para quando FIM_REPETIDA páginas seguidas só repetem precatórios já lidos na edição (a lista de 2019
sai 4 vezes) e, então, ainda confere amostras adiante (SONDAS) para não perder um trecho novo.

Cache em disco (TJRJ/saida/relacao_djerj/<AAAA-MM-DD>_<caderno>/<página>.txt, o texto da página): rodar de novo
retoma de onde parou e não baixa de novo. Só o texto fica no cache (o PDF de uma página volta a ser baixado se
for preciso conferir).

O leitor junta as linhas de cada registro (inclusive as que continuam na página seguinte) e aceita as quebras de
linha da coluna do beneficiário: nome em duas linhas, data entre o nome e o documento, documento na linha de baixo.

Saída: TJRJ/saida/relacao_djerj.csv (precatório, nome, documento, OAB, advogado, originário, edição e página; ';' e
UTF-8 com BOM). Com --medir, conta a qualidade: % com documento de dígito válido e % cujo precatório existe no banco.

    python TJRJ/relacao_djerj.py                    # baixa (retoma) e gera o CSV
    python TJRJ/relacao_djerj.py --so-ler --medir   # só relê o cache, gera o CSV e mede
    python TJRJ/relacao_djerj.py --edicoes 2021-07-14
"""
import argparse
import html as H
import logging
import re
import sys
import time
import unicodedata
from collections import Counter
from io import BytesIO
from pathlib import Path

import pdfplumber
import requests

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))

from utils.arquivos import gravar_csv  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import documento_valido, formatar_cnj, so_digitos  # noqa: E402

log = logging.getLogger("relacao_djerj")

SAIDA = AQUI / "saida"
CACHE = SAIDA / "relacao_djerj"
ARQ_CSV = SAIDA / "relacao_djerj.csv"
DJERJ = "https://www3.tjrj.jus.br/consultadje/"
HANDLERS = "https://www3.tjrj.jus.br/CONSULTADJE/Handlers/"
COD_CADERNO = {"I": "A", "II": "S", "III": "C", "IV": "I", "V": "E"}
RITMO = 2.5                             # s mínimos entre duas chamadas ao DJERJ
TENTATIVAS = 5
FIM_SEM_LINHA = 3                       # páginas seguidas sem linha da relação: fim do trecho
FIM_REPETIDA = 5                        # páginas seguidas que só repetem precatórios da edição: fim (lista repetida)
SONDAS = (100, 200, 300, 400)           # páginas adiante conferidas depois do fim por repetição
SEM_PAGINA = "__SEM_PAGINA__"           # marca no cache: a página não existe na edição

# (data, caderno, páginas-semente): sementes da busca Result.aspx?tipoPesq=PROC por precatórios da janela
EDICOES = [
    ("14/07/2021", "I", [41, 139, 207, 247, 429]),
    ("28/04/2022", "I", [125, 401, 500]),
    ("19/05/2023", "I", [12, 38, 215, 359]),
    ("14/07/2020", "II", [230, 250, 570]),
    ("16/07/2019", "II", [338]),
]

RE_INICIO = re.compile(r"^(20\d{7})\s+(\d{4}\.\d{5}-\d)\b\s*(.*)$")
RE_CABECALHO = re.compile(r"^(?:Ano \d+ – nº|Caderno [IV]+ –|\d{1,4}$|Natureza\b|Ordem Precat|Protocolo\s*$|"
                          r"Protocolo Protocolo\s*$)")
RE_DATA = re.compile(r"(?<![\d/])(\d{2}/\d{2}/\d{4})(?![\d/])")
RE_DOC = re.compile(r"\(\s*(\d[\d.\-/ ]{8,24}\d)\s*\)")
RE_OAB = re.compile(r"\(\s*([A-Z]{2})\s*(\d{2,7}[A-Z]?)\s*\)")
RE_CNJ = re.compile(r"(\d{7})\s?-\s?(\d{2})\s?[.\-]\s?(\d{4})\s?\.\s?(\d)\s?\.\s?(\d{2})\s?\.\s?(\d{4})")
RE_ANTIGO = re.compile(r"^\s*\(\s*([^()]{5,40}?)\s*\)")

COLUNAS = ["precatorio", "ordem", "nome", "documento", "tipo_documento", "documento_valido", "oab", "advogado",
           "originario", "originario_antigo", "protocolo", "edicao", "caderno", "pagina", "nome_com_oab", "aviso",
           "texto"]


def sem_acento(t):
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode()


# =============================================================================== download (com cache)


class Djerj:
    def __init__(self):
        self.s = requests.Session()
        self.s.trust_env = False                    # sem proxy do ambiente (os PROXY_0x não entram aqui)
        self.s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        self.ultima, self.chamadas, self.baixadas = 0.0, 0, 0

    def get(self, url, **kw):
        for t in range(TENTATIVAS):
            espera = self.ultima + RITMO - time.time()
            if espera > 0:
                time.sleep(espera)
            self.ultima = time.time()
            self.chamadas += 1
            try:
                r = self.s.get(url, timeout=90, **kw)
            except requests.RequestException as e:
                log.warning(f"erro de rede ({e.__class__.__name__}); tentativa {t + 1}")
                time.sleep(20 * (t + 1))
                continue
            if r.status_code == 200:
                return r
            log.warning(f"HTTP {r.status_code} em {url[:80]}; tentativa {t + 1}")
            time.sleep(60 if r.status_code in (403, 429, 502, 503, 504) else 10)
        raise RuntimeError("DJERJ não respondeu")

    def pagina(self, data, caderno, pag):
        """Linhas do texto da página (cache em disco), ou None se a página não existe na edição."""
        d, m, a = data.split("/")
        arq = CACHE / f"{a}-{m}-{d}_{caderno}" / f"{pag}.txt"
        if arq.exists():
            t = arq.read_text(encoding="utf-8")
            return None if t == SEM_PAGINA else t.split("\n")
        r = self.get(DJERJ + "pdf.aspx", params={"dtPub": data, "caderno": COD_CADERNO[caderno], "pagina": pag})
        m_ = re.search(r"DownloadPdf\.ashx\?guid=[^\"'&]+\.pdf&amp;dtPub=[^\"'&]+&amp;caderno=\w+&amp;pagina=\d+", r.text)
        texto = SEM_PAGINA
        if m_:
            pdf = self.get(HANDLERS + H.unescape(m_.group(0))).content
            if pdf[:4] == b"%PDF":
                with pdfplumber.open(BytesIO(pdf)) as p:
                    texto = "\n".join((pg.extract_text() or "") for pg in p.pages)
            else:
                raise RuntimeError(f"página {pag} de {data} não veio em PDF")
        arq.parent.mkdir(parents=True, exist_ok=True)
        arq.write_text(texto, encoding="utf-8")
        self.baixadas += 1
        return None if texto == SEM_PAGINA else texto.split("\n")


def precatorios_da_pagina(linhas):
    return [m.group(2) for m in (RE_INICIO.match(x.strip()) for x in linhas or []) if m]


def baixar_edicao(dj, data, caderno, sementes):
    """Anda a partir das sementes até o fim de cada trecho da relação. Devolve as páginas que têm linhas."""
    paginas, vistos, feitas = {}, set(), set()

    def ler(p):
        if p < 1:
            return None
        linhas = dj.pagina(data, caderno, p)
        feitas.add(p)
        if linhas is None:
            return None
        precs = precatorios_da_pagina(linhas)
        if precs:
            paginas[p] = precs
        return precs

    def andar(inicio, passo):
        p, sem, rep = inicio, 0, 0
        while True:
            p += passo
            if p < 1 or p in feitas:                    # chegou num trecho que outra semente já leu
                break
            precs = ler(p)
            if precs is None:
                break                                   # a edição acabou
            if not precs:
                sem += 1
                if sem >= FIM_SEM_LINHA:
                    break
                continue
            sem = 0
            novos = [x for x in precs if x not in vistos]
            vistos.update(precs)
            rep = rep + 1 if (passo > 0 and not novos) else 0
            if rep >= FIM_REPETIDA:
                return p
            if len(feitas) % 25 == 0:
                log.info(f"{data} cad. {caderno}: página {p}, {len(paginas)} páginas com linhas, "
                         f"{len(vistos)} precatórios, {dj.chamadas} chamadas ({dj.baixadas} páginas baixadas)")
        return None

    fila = list(sementes)
    while fila:
        s = fila.pop(0)
        if s in feitas:
            continue
        precs = ler(s)
        if not precs or not [x for x in precs if x not in vistos]:
            if precs:
                vistos.update(precs)
            continue
        vistos.update(precs)
        andar(s, -1)
        fim = andar(s, +1)
        if fim:                                         # lista repetida: confere amostras adiante
            fila += [fim + k for k in SONDAS]
    log.info(f"{data} cad. {caderno}: {len(paginas)} páginas com linhas ({min(paginas, default=0)}-"
             f"{max(paginas, default=0)}), {len(vistos)} precatórios distintos")
    return sorted(paginas)


# =============================================================================== leitura


def ler_registro(ordem, prec, texto):
    """Um registro (as linhas já juntas) -> dicionário das colunas."""
    r = {"precatorio": prec, "ordem": ordem, "texto": texto[:400]}
    avisos = []
    mc = RE_CNJ.search(texto)
    seg = texto[:mc.start()] if mc else texto
    md = RE_DATA.search(seg)
    mdoc = next((m for m in RE_DOC.finditer(seg) if len(so_digitos(m.group(1))) in (11, 14)), None)
    if not mdoc:
        mdoc = next((m for m in RE_DOC.finditer(seg) if 9 <= len(so_digitos(m.group(1))) <= 15), None)
    if mdoc and md:
        if md.start() < mdoc.start():                   # 'Nome parte 1 <data> Nome parte 2 (doc)'
            nome = seg[:md.start()] + " " + seg[md.end():mdoc.start()]
            resto = seg[mdoc.end():]
        else:
            nome = seg[:mdoc.start()]
            resto = seg[mdoc.end():md.start()] + " " + seg[md.end():]
    elif md:
        nome, resto = seg[:md.start()], seg[md.end():]
        avisos.append("sem_documento")
    elif mdoc:
        nome, resto = seg[:mdoc.start()], seg[mdoc.end():]
        avisos.append("sem_data")
    else:
        nome, resto = seg, ""
        avisos.append("sem_documento")
    # 'NOME (doc) E OUTROS': precatório de vários beneficiários; a linha dá o primeiro
    if re.match(r"\s*E\s+OUTR[OA]S?\b", resto, re.I) or re.search(r"\s+E\s+OUTR[OA]S?\s*$", nome, re.I):
        resto = re.sub(r"^\s*E\s+OUTR[OA]S?\b", "", resto, flags=re.I)
        nome = re.sub(r"\s+E\s+OUTR[OA]S?\s*$", "", nome, flags=re.I)
        avisos.append("e_outros")
    nome_oab = RE_OAB.search(nome)
    nome = re.sub(r"\s+", " ", RE_OAB.sub(" ", nome)).strip(" -;,")
    doc = so_digitos(mdoc.group(1)) if mdoc else ""
    oabs = list(RE_OAB.finditer(resto))
    if oabs:
        adv = re.sub(r"\s+", " ", resto[:oabs[0].start()]).strip(" -;,")
        oab = ";".join(f"{m.group(1)}{m.group(2)}" for m in oabs)
    else:
        adv, oab = re.sub(r"\s+", " ", resto).strip(" -;,"), ""
        avisos.append("sem_oab")
    if mc:
        cnj = "".join(mc.groups())
        ma = RE_ANTIGO.match(texto[mc.end():])
        r["originario"], r["originario_antigo"] = formatar_cnj(cnj), (ma.group(1).strip() if ma else "")
    else:
        r["originario"], r["originario_antigo"] = "", ""
        avisos.append("sem_originario")
    if not nome:
        avisos.append("sem_nome")
    r.update(nome=nome, documento=doc, tipo_documento={11: "CPF", 14: "CNPJ"}.get(len(doc), "OUTRO" if doc else ""),
             documento_valido=documento_valido(doc), oab=oab, advogado=adv, protocolo=md.group(1) if md else "",
             nome_com_oab=nome_oab.group(1) + nome_oab.group(2) if nome_oab else "", aviso=",".join(avisos))
    return r


def ler_edicao(data, caderno):
    """Lê todas as páginas do cache da edição, em ordem, como um fluxo de linhas (o registro que continua na
    página seguinte é juntado). Devolve os registros."""
    d, m, a = data.split("/")
    pasta = CACHE / f"{a}-{m}-{d}_{caderno}"
    if not pasta.exists():
        return []
    paginas = sorted(int(p.stem) for p in pasta.glob("*.txt"))
    registros, atual, ant = [], None, None
    for p in paginas:
        t = (pasta / f"{p}.txt").read_text(encoding="utf-8")
        if t == SEM_PAGINA:
            continue
        if ant is not None and p != ant + 1 and atual:      # buraco no cache: não junta através dele
            registros.append(atual)
            atual = None
        ant = p
        for linha in t.split("\n"):
            linha = linha.strip()
            if not linha or RE_CABECALHO.match(linha):
                continue
            mi = RE_INICIO.match(linha)
            if mi:
                if atual:
                    registros.append(atual)
                atual = {"ordem": mi.group(1), "prec": mi.group(2), "linhas": [mi.group(3)], "pagina": p}
            elif atual:
                if len(atual["linhas"]) < 12:                # registro tem até ~6 linhas; depois disso é outra coisa
                    atual["linhas"].append(linha)
    if atual:
        registros.append(atual)
    out = []
    for x in registros:
        r = ler_registro(x["ordem"], x["prec"], " ".join(x["linhas"]))
        r.update(edicao=f"{a}-{m}-{d}", caderno=caderno, pagina=x["pagina"])
        out.append(r)
    return out


def montar_csv(edicoes):
    linhas = []
    for data, caderno, _ in edicoes:
        regs = ler_edicao(data, caderno)
        # a mesma linha repetida na edição (2019 sai 4 vezes) entra uma vez: fica a 1ª página
        vistos, unicos = set(), []
        for r in regs:
            k = (r["precatorio"], r["documento"], r["originario"])
            if k not in vistos:
                vistos.add(k)
                unicos.append(r)
        log.info(f"{data} cad. {caderno}: {len(regs)} linhas lidas, {len(unicos)} sem repetição")
        linhas += unicos
    gravar_csv(ARQ_CSV, linhas, COLUNAS)
    log.info(f"CSV: {ARQ_CSV} ({len(linhas)} linhas)")
    return linhas


def medir(linhas):
    """Qualidade: documento de dígito válido, avisos do leitor e precatório no banco (tribunal 119)."""
    n = len(linhas) or 1
    valido = sum(1 for r in linhas if r["documento_valido"])
    log.info(f"linhas {len(linhas)}; documento válido {valido} ({100 * valido / n:.1f}%); "
             f"tipo {dict(Counter(r['tipo_documento'] or '-' for r in linhas))}")
    log.info(f"avisos: {dict(Counter(a for r in linhas for a in (r['aviso'].split(',') if r['aviso'] else [])))}; "
             f"com originário {sum(1 for r in linhas if r['originario'])}; com OAB {sum(1 for r in linhas if r['oab'])}")
    por_ed = Counter(r["edicao"] for r in linhas)
    log.info(f"por edição: {dict(sorted(por_ed.items()))}")
    precs = sorted({r["precatorio"] for r in linhas})
    log.info(f"precatórios distintos: {len(precs)}")
    from utils import banco
    con = banco.conectar("relacao_djerj")
    with con.cursor() as cur:
        cur.execute("""SELECT numero_exibicao FROM creditos.credito
                        WHERE tribunal_id = 119 AND numero_exibicao = ANY(%s)""", (precs,))
        no_banco = {x[0] for x in cur.fetchall()}
    con.close()
    log.info(f"precatório existe no banco: {len(no_banco)} de {len(precs)} ({100 * len(no_banco) / max(len(precs), 1):.1f}%)")
    for ed in sorted(por_ed):
        ps = {r["precatorio"] for r in linhas if r["edicao"] == ed}
        log.info(f"  {ed}: {len(ps & no_banco)}/{len(ps)} no banco")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--so-ler", action="store_true", help="não baixa: só relê o cache e gera o CSV")
    p.add_argument("--medir", action="store_true", help="mede a qualidade (lê o banco)")
    p.add_argument("--edicoes", default="", help="AAAA-MM-DD separados por vírgula (padrão: todas)")
    args = p.parse_args()
    configurar_log(__file__, SAIDA / "logs")
    edicoes = EDICOES
    if args.edicoes:
        quer = set(args.edicoes.split(","))
        edicoes = [e for e in EDICOES if "-".join(reversed(e[0].split("/"))) in quer]
    if not args.so_ler:
        dj = Djerj()
        for data, caderno, sementes in edicoes:
            baixar_edicao(dj, data, caderno, sementes)
        log.info(f"download: {dj.chamadas} chamadas, {dj.baixadas} páginas baixadas")
    linhas = montar_csv(edicoes)
    if args.medir:
        medir(linhas)


if __name__ == "__main__":
    main()
