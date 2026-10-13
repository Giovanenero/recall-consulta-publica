"""
originario_provavel.py - "originário provável" com nível de confiança, como o artifact "Ordem Cronológica dos TJs" pede
(saída da etapa comum: originario_provavel + confianca + candidatos[]). Usado pelos robôs (TJMA, TJBA, TJMT, TJDFT,
TJRR, TJPI) quando o originário NÃO foi confirmado, e pelos scripts que recalculam os créditos já processados.

Decisão do usuário (08/10/2026): o provável fica só no metadata do robô (credito_fonte.metadata.originario_provavel).
Não entra em creditos.credito_originario (lá ele ficaria igual ao confirmado para a RPA de íntegra e as campanhas)
e não muda o status do crédito. O originário confirmado continua sendo ligado como antes, pelas regras de cada robô.

Cada robô traduz o que sabe de cada candidato para os SINAIS abaixo (mesmo vocabulário em todos os tribunais) e chama
avaliar(). A confiança:
- alta:  o melhor candidato tem o credor identificado (CPF ou nome inteiro) e o ente no polo passivo, mais um sinal do
         vínculo com ESTE precatório (cita o número, valor ou expedição perto da data; advogado em comum só vale
         junto com o CPF do credor), e ganha do segundo por MARGEM_ALTA pontos ou mais (ou é o único);
- media: o melhor tem o credor identificado e o ente e ganha do segundo; candidato único só é "media" com o CPF do
         credor ou um sinal do vínculo (TJPI: 0 de 6 sem eles);
- baixa: o resto que passou no mínimo (credor só pelas iniciais, empate, ente não conferido).
Validação de 08/10/2026 (originários confirmados com a confirmação escondida, casos com 2+ candidatos confirmados):
alta 97-100% de acerto; media ~91% no TJMA e ~75% no TJBA (testada uma margem fora de ano/classe: subia para ~99%,
mas a "media" quase sumia dos casos reais — 968 -> 15 no TJMA); baixa ~50-65% (quase um sorteio entre empatados).
Mínimo para ser candidato: algum sinal do credor (CPF, nome ou iniciais). Candidato com ANO_POSTERIOR (cumprimento
mais novo que o precatório) não entra: o artifact diz que não pode.
"""
from datetime import datetime

REGRA_VERSAO = "2026-10-08.2"           # .2: advogado sozinho não dá "alta"; único sem CPF nem vínculo é "baixa"
MAX_CANDIDATOS = 5                     # candidatos guardados no metadata (os de maior pontuação)
MARGEM_ALTA = 2                        # pontos à frente do segundo para a confiança alta

# sinal -> pontos. Os do credor contam só o maior (CPF > NOME > INICIAIS).
PONTOS = {
    "CITA_PRECATORIO": 5,              # publicação/documento do candidato cita o número do precatório
    "VALOR": 4,                        # valor do precatório (requisitado/bruto) aparece no candidato
    "CPF_CREDOR": 3,                   # credor no polo ativo com o mesmo CPF/CNPJ
    "NOME_CREDOR": 2,                  # credor no polo ativo com o mesmo nome inteiro
    "INICIAIS_CREDOR": 1,              # credor no polo ativo só pelas iniciais
    "ENTE_PASSIVO": 2,                 # ente devedor do precatório no polo passivo
    "ADVOGADO_EM_COMUM": 2,            # advogado (OAB) do precatório também no candidato
    "EXPEDICAO_PERTO": 2,              # expedição de precatório/RPV ("expeça-se", requisitório) perto da data
    "CLASSE_CUMPRIMENTO": 1,           # cumprimento de sentença / execução contra a Fazenda
    "ANO_ANTERIOR": 1,                 # ano do CNJ (ou distribuição) anterior ao do precatório
    "VARA_FAZENDA": 1,                 # vara/juizado da Fazenda Pública compatível com o ente
}
SINAIS_CREDOR = ("CPF_CREDOR", "NOME_CREDOR", "INICIAIS_CREDOR")
SINAIS_VINCULO = ("CITA_PRECATORIO", "VALOR", "ADVOGADO_EM_COMUM", "EXPEDICAO_PERTO")
EXCLUI = ("ANO_POSTERIOR",)


def pontuar(sinais):
    """Pontos de um candidato: soma dos sinais, contando só o melhor sinal do credor."""
    sinais = set(sinais)
    credor = max((PONTOS[s] for s in SINAIS_CREDOR if s in sinais), default=0)
    return credor + sum(PONTOS.get(s, 0) for s in sinais if s not in SINAIS_CREDOR)


def avaliar(candidatos, fonte="robo"):
    """candidatos: [{"numero_cnj": "...", "sinais": [...], **extras}] -> dict para metadata.originario_provavel,
    ou None quando nenhum candidato passa no mínimo. Os extras de cada candidato (documento, classe...) vão juntos."""
    validos = []
    for c in candidatos or []:
        sinais = sorted(set(c.get("sinais") or []))
        if not c.get("numero_cnj") or any(s in EXCLUI for s in sinais):
            continue
        if not any(s in SINAIS_CREDOR for s in sinais):
            continue
        validos.append({**c, "sinais": sinais, "pontos": pontuar(sinais)})
    if not validos:
        return None
    validos.sort(key=lambda c: (-c["pontos"], c["numero_cnj"]))
    melhor = validos[0]
    margem = melhor["pontos"] - validos[1]["pontos"] if len(validos) > 1 else None
    sinais = set(melhor["sinais"])
    identificado = bool(sinais & {"CPF_CREDOR", "NOME_CREDOR"}) and "ENTE_PASSIVO" in sinais
    # advogado em comum sozinho só prova o vínculo quando o CPF do credor bate (TJPI 2/3, TJMT 54% sem o credor)
    vinculo = bool(sinais & set(SINAIS_VINCULO) - {"ADVOGADO_EM_COMUM"}) or (
        "ADVOGADO_EM_COMUM" in sinais and "CPF_CREDOR" in sinais)
    if identificado and vinculo and (margem is None or margem >= MARGEM_ALTA):
        confianca = "alta"
    elif identificado and (margem or 0) > 0:
        confianca = "media"
    elif identificado and margem is None and (vinculo or "CPF_CREDOR" in sinais):
        confianca = "media"            # candidato único: sem CPF nem vínculo fica "baixa" (TJPI: 0 de 6)
    else:
        confianca = "baixa"
    return {
        "numero_cnj": melhor["numero_cnj"],
        "confianca": confianca,
        "pontos": melhor["pontos"],
        "margem": margem,
        "sinais": melhor["sinais"],
        "empate": margem == 0,
        "qtd_candidatos": len(validos),
        "candidatos": validos[:MAX_CANDIDATOS],
        "regra_versao": REGRA_VERSAO,
        "fonte": fonte,
        "calculado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# Grava só a chave originario_provavel, numa operação atômica: não sobrescreve o resto do metadata, que outros
# scripts podem estar mudando ao mesmo tempo (ex.: o robô em motor.djen).
SQL_GRAVAR = """UPDATE creditos.credito_fonte f
                   SET metadata = coalesce(f.metadata, '{}'::jsonb) || jsonb_build_object('originario_provavel', %s::jsonb)
                 WHERE f.credito_id = %s AND f.software_id = %s"""
SQL_APAGAR = """UPDATE creditos.credito_fonte f SET metadata = f.metadata - 'originario_provavel'
                 WHERE f.credito_id = %s AND f.software_id = %s AND f.metadata ? 'originario_provavel'"""
