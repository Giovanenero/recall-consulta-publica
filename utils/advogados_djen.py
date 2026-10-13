"""
advogados_djen.py - advogados do lado do credor nas publicações do DJEN de um precatório e o vínculo ADVOGADO deles
no crédito (registrar_credor). Usado pelos robôs que leem o DJEN do precatório (TJBA, TJMA, TJRR).

O DJEN lista em `destinatarioadvogados` todo advogado intimado na publicação: o do credor, mas também o do ente
(município com advogado contratado: 'Advogado do(a) REQUERIDO: ...') e o de terceiros (cessionário, fundo). O lado de
cada um sai de duas pistas, nesta ordem:
1. o rótulo do texto antes da 1ª menção da OAB (ou do nome): 'Advogados do(a) REQUERENTE: FULANO - MA14694-A',
   'CREDOR: ... ADVOGADOS: ...', 'Advogado do(a) REQUERIDO: ...', 'PROCURADOR: ...';
2. sem rótulo (precatório sigiloso do TJBA: 'Classe: PRECATÓRIO FULANO (OAB:BA16020-A)'), o destinatário com as
   iniciais dele: o DJEN do TJBA põe o advogado entre os destinatários, no polo A (credor) ou T (terceiro);
3. sem as duas, numa publicação sem rótulo nenhum, a publicação em que os destinatários são só o credor (polo A) e órgãos públicos (polo P), sem
   terceiro: o advogado intimado ali é o do credor (SO_CREDOR; amostra de 08/10/2026: nenhum caiu no polo passivo
   da capa do originário).
Com fundo/cessionário no polo A dos destinatários, as pistas 2 e 3 não valem naquela publicação (o advogado pode ser o
do cessionário). O mesmo advogado (nome) com outra OAB do outro lado também fica de fora.
Fica só o advogado que aparece do lado do credor e nunca do outro lado (cessionário, terceiro, ente). Sem pista
nenhuma, ou com nome de órgão no lugar do nome ('(SUB)', 'SUBSTITUTO', 'POLO PASSIVO', procuradoria), fica de fora.
TJRR: guarda_da_lista() deixa só o advogado cujo nome está na lista do SGP (o SGP dá os advogados do credor).

OAB: o número que o DJEN dá (`numero_oab`, só dígitos; o texto escreve 'MA14694-A', com a letra do tipo de inscrição)
e a UF de `uf_oab`. O banco guarda só os dígitos (creditos.oab_numero_normalizar).

Gravação (ligar_advogados): pela OAB, a pessoa que o banco já tem (pessoa_oab); sem ela, o registrar_credor cria a
pessoa só com a OAB. Não liga (e conta) quando a OAB do banco é de outro nome, quando o crédito já tem esse advogado e
quando já tem um ADVOGADO com o mesmo nome (outra pessoa: CPF de outra fonte, OAB de outra UF). O vínculo vai direto
no crédito (credito_credor, origem FONTE, sem processo): não passa pelo registrar_capa nem pelo recálculo dos credores
do processo, então não mexe em outro crédito.
"""
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher

SEMELHANCA_MESMA_PESSOA = 0.9          # grafia próxima (SOUSA x SOUZA) conta como o mesmo nome
JANELA_ROTULO = 400                    # caracteres antes da menção do advogado em que o rótulo do lado vale

# rótulo do lado da parte no cabeçalho da publicação ('REQUERENTE:', 'Credor(a):', 'CREDOR(A)/REQUERENTE:',
# 'Advogado do(a) REQUERIDO:', 'PROCURADOR DO ESTADO:'), já sem acento e em maiúsculas
_LADOS = [
    ("ATIVO", r"REQUERENTES?|CREDOR(?:A|ES|AS)?|EXEQUENTES?|AUTOR(?:A|ES|AS)?|IMPETRANTES?|BENEFICIARI[OA]S?|"
              r"HERDEIR[OA]S?|INVENTARIANTE|SUCESSOR(?:A|ES|AS)?|ESPOLIO|CEDENTES?"),
    ("PASSIVO", r"REQUERID[OA]S?|DEVEDOR(?:A|ES|AS)?|ENTE DEVEDOR|EXECUTAD[OA]S?|REUS?|IMPETRAD[OA]S?|"
                r"PROCURADOR(?:A|ES|AS)?(?: GERAL)?(?: D[OA] (?:ESTADO|MUNICIPIO))?"),
    ("TERCEIRO", r"TERCEIR[OA]S?(?: INTERESSAD[OA]S?)?|INTERESSAD[OA]S?|CESSIONARI[OA]S?"),
]
RE_ROTULO = re.compile(r"(?<![A-Z])(?:" + "|".join(f"(?P<{lado}>{rx})" for lado, rx in _LADOS) + r")"
                       r"(?:\s*\((?:A|O|AS|OS|ES|S)\))*\s*(?::|/)")
POLO_DESTINATARIO = {"A": "ATIVO", "P": "PASSIVO", "T": "TERCEIRO"}
RE_FUNDO = re.compile(r"FUNDO|\bFIDC\b|CREDITORIO|INVESTIMENTO|SECURITIZ|CESSION")
# destinatário do polo P que é órgão público (a regra SO_CREDOR só vale com esses no polo passivo)
RE_ORGAO = re.compile(r"^(?:ESTADO D|MUNICIPIO|PREFEITURA|UNIAO\b|DISTRITO FEDERAL|INSTITUTO|SUPERINTENDENCIA|"
                      r"CADASTRO NACIONAL|PROCURADORIA|SECRETARIA|FAZENDA|FUNDACAO|DEPARTAMENTO|AGENCIA|AUTARQUIA|"
                      r"UNIVERSIDADE|CAMARA MUNICIPAL|TRIBUNAL|DETRAN|INSS\b)")
# 'advogado' que é o próprio órgão ou um cargo (o TJRR traz 'Z SANDRO (SUB)', 'POLO PASSIVO EXECUÇÕES'); sem acento
RE_NOME_DE_ORGAO = re.compile(r"\(SUB\)|\bSUBSTITUT[OA]\b|\bSUB\b|POLO (?:PASSIVO|ATIVO)|PROCURADORI|PROCURADOR GERAL|"
                              r"DEFENSORIA|ADVOCACIA GERAL|^ESTADO D|^MUNICIPIO|FAZENDA|PREFEITURA|\bEXECUC")


def sem_acento(t):
    """Maiúsculas e sem acento, com a pontuação preservada."""
    return unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().upper()


def chave(nome):
    """Nome comparável: sem acento e pontuação e sem o 'REGISTRADO(A) CIVILMENTE COMO ...'."""
    n = re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", sem_acento(nome))).strip()
    return re.split(r"\bREGISTRAD[OA]\b", n)[0].strip()


def nome_limpo(nome):
    """Nome do advogado como vai para o banco: sem o 'registrado(a) civilmente como ...' e com espaços simples."""
    return " ".join(re.split(r"\s+REGISTRAD[OA]\(?A?\)?\s+CIVILMENTE", nome or "", flags=re.I)[0].split())


def eh_iniciais(t):
    """'M. L. C. B.' -> True."""
    tokens = re.findall(r"[^\s.]+", t or "")
    return "." in (t or "") and len(tokens) >= 2 and all(len(x) == 1 for x in tokens)


def iniciais(t):
    """Primeira letra de cada palavra: 'MARCOS LUIZ CARMELO BARROSO' e 'M. L. C. B.' -> 'MLCB'."""
    return "".join(x[0] for x in re.findall(r"[^\s.]+", sem_acento(t)))


def mesma_pessoa(a, b):
    """Mesmo nome, ou grafia próxima o bastante."""
    a, b = chave(a), chave(b)
    return bool(a) and (a == b or SequenceMatcher(None, a, b).ratio() >= SEMELHANCA_MESMA_PESSOA)


def mesmo_advogado(a, b):
    """Mesmo nome com folga para sobrenome a mais ou a menos ('ANA F. MOREIRA' x 'ANA F. MOREIRA LIMA'): grafia
    próxima, ou um nome contido no outro com o mesmo primeiro nome (como no TJMT)."""
    ka, kb = chave(a).split(), chave(b).split()
    if not ka or not kb:
        return False
    curto, longo = sorted((ka, kb), key=len)
    return mesma_pessoa(a, b) or (ka[0] == kb[0] and len(curto) >= 2 and set(curto) <= set(longo))


def oab_numero(numero):
    """Só os dígitos, sem zeros à esquerda (como creditos.oab_numero_normalizar)."""
    d = re.sub(r"\D", "", str(numero or "")).lstrip("0")
    return d if 0 < len(d) <= 10 else ""


def _mencao(texto, adv):
    """Posição da 1ª menção do advogado no texto (sem acento): a OAB ('MA14694-A', 'OAB:BA16020-A',
    'OAB/MA nº. 6469', '6469/MA') ou, sem ela, o nome inteiro. None se não aparece."""
    uf, num = adv["uf"], adv["oab"]
    achados = [m.start() for m in (
        re.search(rf"(?<![A-Z]){uf}\s*[/:\-]?\s*(?:NO?\s*\.?\s*)?0*{num}(?!\d)", texto),
        re.search(rf"(?<![\d.]){num[:-3] + '.?' + num[-3:] if len(num) > 3 else num}\s*[/\-]\s*{uf}(?![A-Z])", texto))
        if m]
    if not achados:
        tokens = chave(adv["nome"]).split()
        if len(tokens) >= 2:
            m = re.search(r"(?<![A-Z])" + r"\W+".join(map(re.escape, tokens)) + r"(?![A-Z])", texto)
            if m:
                achados.append(m.start())
    return min(achados) if achados else None


def _lado_no_texto(texto, pos):
    """Lado do último rótulo antes da menção (até JANELA_ROTULO caracteres antes), ou None."""
    ultimo = None
    for m in RE_ROTULO.finditer(texto, max(0, pos - JANELA_ROTULO), pos):
        ultimo = m
    return ultimo.lastgroup if ultimo else None


def _cessao_no_polo_a(partes):
    """Fundo/cessionário entre os destinatários do polo A: o polo A deixa de ser só o credor original."""
    return any(polo == "A" and RE_FUNDO.search(chave(nome)) for polo, nome in partes)


def _lado_nos_destinatarios(partes, adv):
    """Polos dos destinatários que são o advogado (pelo nome inteiro ou pelas iniciais). Com fundo/cessionário no
    polo A, o polo A não conta (o advogado pode ser o do cessionário)."""
    k, ini = chave(adv["nome"]), iniciais(nome_limpo(adv["nome"]))
    polos = {POLO_DESTINATARIO.get(polo) for polo, nome in partes
             if (iniciais(nome) == ini if eh_iniciais(nome) else chave(nome) == k)} - {None}
    return polos - {"ATIVO"} if _cessao_no_polo_a(partes) else polos


def _so_credor(partes):
    """A publicação foi só para o credor e o ente: polo A presente (sem fundo/cessionário), nenhum terceiro e só órgão
    público no polo P."""
    return any(polo == "A" for polo, _ in partes) and not any(polo == "T" for polo, _ in partes) and \
        not _cessao_no_polo_a(partes) and all(RE_ORGAO.search(chave(nome)) for polo, nome in partes if polo == "P")


def advogados_do_credor(itens):
    """(aceitos, descartados) das publicações do DJEN do precatório (formato enxuto dos robôs: 'partes' [[polo, nome]],
    'advogados' [{nome, oab, uf}], 'texto'). aceitos: [{nome, oab_uf, oab_numero, pista}] do lado do credor (uma vez
    por OAB); descartados: [{nome, oab_uf, oab_numero, motivo}] (lado do ente ou de terceiro, sem lado, OAB inválida)."""
    por_oab, lados, pistas = {}, {}, {}
    for it in itens:
        texto = sem_acento(it.get("texto") or "")
        for a in it.get("advogados") or []:
            uf, num = (a.get("uf") or "").upper().strip(), oab_numero(a.get("oab"))
            k = (uf, num)
            por_oab.setdefault(k, {"nome": nome_limpo(a.get("nome")), "oab_uf": uf, "oab_numero": num})
            lados.setdefault(k, Counter())
            if not (num and re.fullmatch(r"[A-Z]{2}", uf)):
                continue
            adv = {"nome": a.get("nome") or "", "uf": uf, "oab": num}
            pos = _mencao(texto, adv) if texto else None
            lado = _lado_no_texto(texto, pos) if pos is not None else None
            if lado:
                lados[k][lado] += 1
                pistas.setdefault(k, set()).add("ROTULO")
                continue
            polos = _lado_nos_destinatarios(it.get("partes") or [], adv)
            for lado in polos:
                lados[k][lado] += 1
                pistas.setdefault(k, set()).add("DESTINATARIO")
            # só em publicação sem rótulo nenhum (sigilosa): com rótulo, o advogado fora dele não é do credor por isso
            if not polos and not RE_ROTULO.search(texto) and _so_credor(it.get("partes") or []):
                lados[k]["ATIVO"] += 1
                pistas.setdefault(k, set()).add("SO_CREDOR")
    aceitos, descartados = [], []
    for k, a in por_oab.items():
        c = lados[k]
        if not (a["oab_numero"] and re.fullmatch(r"[A-Z]{2}", a["oab_uf"]) and a["nome"]):
            descartados.append({**a, "motivo": "OAB_INVALIDA"})
        elif RE_NOME_DE_ORGAO.search(sem_acento(a["nome"])):
            descartados.append({**a, "motivo": "NOME_DE_ORGAO"})
        elif c.get("PASSIVO") or c.get("TERCEIRO"):
            lado = "PASSIVO" if c.get("PASSIVO") else "TERCEIRO"
            descartados.append({**a, "motivo": f"LADO_{lado}" + ("_E_ATIVO" if c.get("ATIVO") else "")})
        elif c.get("ATIVO"):
            aceitos.append({**a, "pista": "+".join(sorted(pistas[k]))})
        else:
            descartados.append({**a, "motivo": "SEM_LADO"})
    # o mesmo advogado com outra OAB (outra UF) do lado do ente ou de terceiro: fica de fora também
    outro_lado = [d["nome"] for d in descartados if d["motivo"].startswith(("LADO_PASSIVO", "LADO_TERCEIRO"))]
    for a in [a for a in aceitos if any(mesmo_advogado(a["nome"], n) for n in outro_lado)]:
        aceitos.remove(a)
        descartados.append({**{k: v for k, v in a.items() if k != "pista"}, "motivo": "MESMO_NOME_DO_OUTRO_LADO"})
    return aceitos, descartados


def guarda_da_lista(aceitos, descartados, nomes_lista, promover_sem_lado=True):
    """Guarda do TJRR (o SGP dá os nomes dos advogados do credor, sem OAB): com a lista, fica só o advogado cujo nome
    bate com um dela (mesmo_advogado). O SEM_LADO que bate com a lista também entra (no TJRR a publicação do precatório
    é sigilosa e quase nunca tem rótulo; a lista do SGP já diz o lado), menos com promover_sem_lado=False (lista que não
    é a do crédito, como a dos advogados conhecidos do SGP). Sem lista, (aceitos, descartados) como vieram.
    O nome de órgão e o lado do ente/terceiro continuam de fora."""
    nomes = [n for n in nomes_lista or [] if n and n.strip()]
    if not nomes:
        return aceitos, descartados
    bate = lambda a: any(mesmo_advogado(a["nome"], n) for n in nomes)  # noqa: E731
    novos_aceitos, novos_fora = [], []
    for a in aceitos:
        if bate(a):
            novos_aceitos.append(a)
        else:
            novos_fora.append({**{k: v for k, v in a.items() if k != "pista"}, "motivo": "FORA_DA_LISTA"})
    for d in descartados:
        if promover_sem_lado and d["motivo"] == "SEM_LADO" and bate(d):
            novos_aceitos.append({**{k: v for k, v in d.items() if k != "motivo"}, "pista": "LISTA"})
        else:
            novos_fora.append(d)
    return novos_aceitos, novos_fora


def fmt_advogados(advogados):
    """[{nome, oab_uf, oab_numero}] -> ['FULANO (MA14694)'] para o metadata e o CSV."""
    return [f"{a['nome']} ({a['oab_uf']}{a['oab_numero']})" + (f" {a['motivo']}" if a.get("motivo") else "")
            for a in advogados]


def ligar_advogados(cur, credito_id, advogados, fonte, desfazer=None):
    """Liga cada advogado ao crédito como ADVOGADO pela OAB (registrar_credor, sem documento). Devolve um Counter
    (ligado, ja_ligado, mesmo_nome_ja_ligado, oab_de_outro_nome, pessoa_nova). desfazer (lista): recebe
    ('vinculo', SQL) e, se a pessoa nasceu aqui, ('pessoa', SQL) que a apaga quando ninguém mais a usa (rodar os de
    pessoa depois de todos os de vínculo)."""
    cont = Counter()
    if not advogados:
        return cont
    cur.execute("""SELECT x.pessoa_id, p.nome FROM creditos.credito_credor x JOIN creditos.pessoa p ON p.id = x.pessoa_id
                    WHERE x.credito_id = %s AND x.papel_id = 2""", (credito_id,))
    ligados = dict(cur.fetchall())
    for a in advogados:
        cur.execute("""SELECT p.id, p.nome FROM creditos.pessoa_oab o JOIN creditos.pessoa p ON p.id = o.pessoa_id
                        WHERE o.uf = %s::char(2) AND o.numero = creditos.oab_numero_normalizar(%s)""",
                    (a["oab_uf"], a["oab_numero"]))
        linha = cur.fetchone()
        if linha and linha[1] != "(SEM NOME)" and not mesmo_advogado(linha[1], a["nome"]):
            cont["oab_de_outro_nome"] += 1            # a OAB do banco é de outra pessoa (ou grafia muito diferente)
            continue
        if linha and linha[0] in ligados:
            cont["ja_ligado"] += 1
            continue
        if any(mesmo_advogado(nome, a["nome"]) for nome in ligados.values()):
            cont["mesmo_nome_ja_ligado"] += 1          # o advogado já está no crédito por outra pessoa
            continue
        cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'ADVOGADO', p_nome => %s,
                                                        p_oab_uf => %s, p_oab_numero => %s, p_fonte => %s)""",
                    (credito_id, a["nome"], a["oab_uf"], a["oab_numero"], fonte))
        vinculo = cur.fetchone()[0]
        cur.execute("SELECT pessoa_id FROM creditos.credito_credor WHERE id = %s", (vinculo,))
        pid = cur.fetchone()[0]
        ligados[pid] = a["nome"]
        cont["ligado"] += 1
        if desfazer is not None:
            desfazer.append(("vinculo", cur.mogrify("DELETE FROM creditos.credito_credor WHERE id = %s;",
                                                    (vinculo,)).decode()))
        if not linha:
            cont["pessoa_nova"] += 1
            if desfazer is not None:                    # só some se ninguém mais usar a pessoa
                livre = ("NOT EXISTS (SELECT 1 FROM creditos.credito_credor WHERE pessoa_id = %(p)s) "
                         "AND NOT EXISTS (SELECT 1 FROM creditos.processo_parte WHERE pessoa_id = %(p)s)")
                desfazer.append(("pessoa", cur.mogrify(
                    f"DELETE FROM creditos.pessoa_oab WHERE pessoa_id = %(p)s AND {livre};", {"p": pid}).decode()))
                desfazer.append(("pessoa", cur.mogrify(
                    f"DELETE FROM creditos.pessoa WHERE id = %(p)s AND documento IS NULL "
                    f"AND NOT EXISTS (SELECT 1 FROM creditos.pessoa_oab WHERE pessoa_id = %(p)s) AND {livre};",
                    {"p": pid}).decode()))
    return cont
