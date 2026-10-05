"""
carregar_TJDF.py - põe os precatórios do TJDFT no schema creditos, na fila do fetch_TJDF.py.

O TJDFT não tem lista no schema creditos (a lista do SAPRE tem captcha e nunca passou pelo coletor da ordem
cronológica): os 37 mil precatórios estão só na tabela antiga listas_primarias.processos_unificados (tribunal_origem
'TJDFT', coletados em 04/2026, status 'BLOQUEIO TOTAL'). Decisão do usuário (05/10/2026): este script cadastra cada um
com creditos.registrar_credito, que cria o crédito, o credito_fonte (com a lista no metadata) e a linha em
coleta_credor do software CONSULTA_PUBLICA_TJDFT (raspa_credor=True: a fila já nasce do robô).

Entra: número de 20 dígitos com justiça/tribunal 8.07. Fica de fora (CSV): número de outro tribunal (TRF1, TJMG...) e
fora do padrão. Número repetido na tabela vira um crédito só (os id_processo ficam no metadata).

O registrar_credito é idempotente (ON CONFLICT): rodar de novo não duplica, só atualiza o metadata da lista.

Saídas em TJDF/saida: carga_TJDF_<rodada>.csv (crédito criado/atualizado por precatório) e carga_TJDF_fora_<rodada>.csv.

Uso:
    python TJDF/carregar_TJDF.py --simulacao             # faz tudo e desfaz (ROLLBACK a cada lote)
    python TJDF/carregar_TJDF.py --simulacao --limite 200
    python TJDF/carregar_TJDF.py                         # grava (COMMIT a cada lote)
"""
import argparse
import json
import logging
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils import banco  # noqa: E402
from utils.arquivos import gravar_csv  # noqa: E402
from utils.banco import como_dicts  # noqa: E402
from utils.log import configurar_log  # noqa: E402
from utils.texto import formatar_cnj, so_digitos  # noqa: E402

SAIDA = AQUI / "saida"
log = logging.getLogger("carregar_TJDF")

SOFTWARE = "CONSULTA_PUBLICA_TJDFT"
TRIBUNAL_TJDFT = 107
LOTE = 500

SQL_LEGADO = """
SELECT id_processo, numero_precatorio, entidade_devedora, natureza, ano_orcamentario, ordem_cronologica, prioridade,
       data_apresentacao, tipo_regime, esfera, ultima_atualizacao
  FROM listas_primarias.processos_unificados
 WHERE tribunal_origem = 'TJDFT' AND deleted IS NOT TRUE
 ORDER BY ordem_cronologica NULLS LAST, id_processo
"""


def id_do_software(cur, criar=True):
    """Id do software do robô (o mesmo do fetch_TJDF.py); cria na 1ª vez."""
    return banco.id_do_software(cur, SOFTWARE, "Consulta pública do TJDFT",
                                "Credor do TJDFT: DJe do TJDFT + DJEN + PJe consulta pública (TJDF/fetch_TJDF.py)",
                                raspa_credor=True, criar=criar)


def ler_legado(cur):
    """{numero20: [linhas]} dos precatórios do TJDFT e as linhas que ficam de fora (com o motivo)."""
    cur.execute(SQL_LEGADO)
    por_numero, fora = defaultdict(list), []
    for linha in como_dicts(cur):
        d = so_digitos(linha["numero_precatorio"])
        if len(d) != 20:
            fora.append({**linha, "motivo": "NUMERO_FORA_DO_PADRAO"})
        elif d[13:16] != "807":
            fora.append({**linha, "motivo": f"OUTRO_TRIBUNAL_{d[13]}.{d[14:16]}"})
        else:
            por_numero[d].append(linha)
    return por_numero, fora


def metadata_da_lista(linhas):
    """O que a lista do TJDFT diz do precatório (o fetch_TJDF.py lê daqui: não há lista_item)."""
    p = linhas[0]
    entes = list(dict.fromkeys(e.strip() for x in linhas for e in (x["entidade_devedora"] or []) if e and e.strip()))
    return {"lista": {"fonte": "listas_primarias.processos_unificados", "id_processo": [x["id_processo"] for x in linhas],
                      "ordem": p["ordem_cronologica"], "prioridade": p["prioridade"],
                      "apresentacao": (p["data_apresentacao"] or "")[:10] or None, "natureza": p["natureza"],
                      "ano_orcamentario": p["ano_orcamentario"], "entes": entes, "regime": p["tipo_regime"],
                      "esfera": p["esfera"], "coletado_em": str(p["ultima_atualizacao"] or "")[:19]}}


def registrar(cur, numero, linhas):
    """registrar_credito do precatório; devolve o id do crédito."""
    meta = metadata_da_lista(linhas)
    ente = (meta["lista"]["entes"] or [None])[0]
    ano = linhas[0]["ano_orcamentario"]
    cur.execute("""SELECT creditos.registrar_credito(p_tipo_credito => 'PRECATORIO', p_tribunal => 'TJDFT',
                                                     p_numero => %s, p_origem => 'LISTA_CRONOLOGICA',
                                                     p_ente_devedor => %s, p_ano_orcamentario => %s::smallint,
                                                     p_software => %s, p_metadata => %s::jsonb)""",
                (formatar_cnj(numero), ente, ano, SOFTWARE, json.dumps(meta, ensure_ascii=False, default=str)))
    return cur.fetchone()[0]


def contar(cur):
    cur.execute("""SELECT (SELECT count(*) FROM creditos.credito WHERE tribunal_id = %s),
                          (SELECT count(*) FROM creditos.coleta_credor WHERE tribunal_id = %s)""",
                (TRIBUNAL_TJDFT, TRIBUNAL_TJDFT))
    return cur.fetchone()


def main():
    ap = argparse.ArgumentParser(description="Cadastra os precatórios do TJDFT (tabela antiga) no schema creditos.")
    ap.add_argument("--simulacao", action="store_true", help="faz tudo e desfaz (ROLLBACK a cada lote)")
    ap.add_argument("--limite", type=int, default=None, help="só os N primeiros precatórios (na ordem cronológica)")
    args = ap.parse_args()
    configurar_log(__file__, SAIDA / "logs")
    rodada = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo = "_simulacao" if args.simulacao else ""
    inicio = time.time()

    con = banco.conectar("carregar_TJDF", escrita=True)
    try:
        with con.cursor() as cur:
            por_numero, fora = ler_legado(cur)
            antes = contar(cur)
        con.rollback()
        numeros = list(por_numero)[:args.limite] if args.limite else list(por_numero)
        repetidos = sum(1 for n in numeros if len(por_numero[n]) > 1)
        log.info(f"{'SIMULACAO' if args.simulacao else 'REAL'} | {len(numeros)} precatórios do TJDFT a cadastrar "
                 f"({repetidos} repetidos na tabela antiga) | {len(fora)} de fora | antes: {antes[0]} créditos, "
                 f"{antes[1]} na fila")
        saida, cont = [], Counter()
        for i in range(0, len(numeros), LOTE):
            lote = numeros[i:i + LOTE]
            with con.cursor() as cur:
                id_do_software(cur, criar=True)
                cur.execute("SELECT max(id) FROM creditos.credito")
                ultimo = cur.fetchone()[0] or 0
                for n in lote:
                    cid = registrar(cur, n, por_numero[n])
                    novo = cid > ultimo
                    cont["criados" if novo else "ja_existiam"] += 1
                    saida.append({"rodada": rodada, "precatorio": formatar_cnj(n), "credito_id": cid,
                                  "novo": "sim" if novo else "nao",
                                  "id_processo": ",".join(str(x["id_processo"]) for x in por_numero[n])})
                depois = contar(cur)
            if args.simulacao:
                con.rollback()
            else:
                con.commit()
            log.info(f"lote {i // LOTE + 1}: {i + len(lote)}/{len(numeros)} | {dict(cont)} | créditos 107 na "
                     f"transação: {depois[0]}, fila: {depois[1]} | {'ROLLBACK' if args.simulacao else 'COMMIT'}")
        with con.cursor() as cur:
            final = contar(cur)
        con.rollback()
    finally:
        con.close()
    gravar_csv(SAIDA / f"carga_TJDF_{rodada}{sufixo}.csv", saida)
    gravar_csv(SAIDA / f"carga_TJDF_fora_{rodada}{sufixo}.csv",
               [{k: (",".join(v) if isinstance(v, list) else v) for k, v in x.items()} for x in fora])
    log.info(f"fim em {time.time() - inicio:.0f} s | {dict(cont)} | banco agora: {final[0]} créditos do TJDFT, "
             f"{final[1]} na fila" + (" (simulação: nada ficou gravado)" if args.simulacao else ""))
    if args.simulacao and final != antes:
        log.error(f"a simulação deixou o banco diferente: antes {antes}, depois {final}")


if __name__ == "__main__":
    main()
