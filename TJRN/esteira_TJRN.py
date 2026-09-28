"""
esteira_TJRN.py - esteira do TJRN: leads sem credor -> consulta pública do PJe 1º grau -> CSV.

1. Pega do banco (sessão somente leitura) os próximos --lote leads vivos do TJRN sem credor (CREDOR/CESSIONARIO em
   creditos.credito_credor) cujo originário está no PJe 1º grau.
2. Consulta cada originário com o fetch_TJRN (Chrome + Buster). Um originário é consultado uma vez por execução,
   mesmo que vários leads apontem para ele.
3. Acrescenta uma linha por lead no CSV e pega os próximos --lote, até acabar, bater em --limite ou o site bloquear.

Nada é gravado no banco (ainda). O CSV é o único registro e também o controle: ao iniciar, a esteira pula os leads
cuja última linha em algum CSV de leads tem status "ok" ou "sem resultado"; os que deram erro ou bloqueio voltam numa
próxima execução. Credenciais do banco no .env da raiz (PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD).

Vários workers (todos pelo IP da máquina): --workers N sobe os workers 1..N neste terminal; --worker i --total N roda
só o worker i (um por terminal). Cada worker fica com os originários cujo hash cai no resto i-1 da divisão por N (os
leads de um mesmo originário ficam no mesmo worker e nunca em dois) e tem perfil do Chrome, CSV, log e trava próprios:
o worker 1 usa os de sempre (TJRN/.chrome-profile-pje-tjrn, saida/leads_TJRN.csv), o worker i usa
.chrome-profile-pje-tjrn-<i> e saida/leads_TJRN_w<i>.csv. Perfil novo não tem o Buster: na 1ª vez o worker abre a
página dele na Chrome Web Store e espera a instalação (um clique em "Usar no Chrome").

Uso:
    python TJRN/esteira_TJRN.py                       # lotes de 10 até acabar
    python TJRN/esteira_TJRN.py --lote 10 --limite 20
    python TJRN/esteira_TJRN.py --workers 3           # 3 workers neste terminal (Ctrl+C para todos)
    python TJRN/esteira_TJRN.py --worker 2 --total 3  # só o worker 2 de 3, em outro terminal

Log: terminal e TJRN/saida/logs/esteira_TJRN[_w<N>]_AAAAMMDD.log, no formato padrão (utils/log.py).
"""
import argparse
import csv
import logging
import random
import sys
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

import psycopg2.extras
from playwright.sync_api import Error as PlaywrightError

import fetch_TJRN  # também põe utils/ no path
from fetch_TJRN import (COLUNAS_CSV, PAUSA_ENTRE, REABERTURAS, SAIDA, Bloqueado, abrir_chrome, consultar,
                        garantir_buster, linha_csv)
from utils import workers
from utils.arquivos import anexar_csv
from utils.banco import conectar
from utils.log import configurar_log

# =============================================================================== configuração

log = logging.getLogger("esteira_TJRN")

PERFIL_BASE = fetch_TJRN.PERFIL          # perfil do worker 1 (CHROME_PERFIL no ambiente troca a pasta)
CSV_BASE = SAIDA / "leads_TJRN.csv"      # CSV do worker 1; o worker i usa leads_TJRN_w<i>.csv
CSV_LEADS = CSV_BASE                     # o deste processo (definir_worker troca)
COLUNAS_LEAD = ("credito_id", "precatorio", "ente", "originario", "leads_vivos_no_originario")
# "processo" do fetch_TJRN é o próprio originário: sai para não repetir a coluna.
COLUNAS = COLUNAS_LEAD + tuple(c for c in COLUNAS_CSV if c != "processo")
FEITO = ("ok", "sem resultado")  # status que não voltam; erro e bloqueio voltam numa próxima execução
PARADA = "interrompida (Ctrl+C) depois do lead em andamento, que foi registrado"
PAUSA_ENTRE_WORKERS = 15         # s entre subir um worker e o próximo (--workers): Akamai e reCAPTCHA em escada

# Leads vivos do TJRN (tribunal 120) sem credor, com originário no PJe 1º grau (J=8, TR=20, origem 5xxx/6xxx).
# Um originário por lead (o primeiro, se houver mais) e, dele, só a fatia deste worker: resto %(indice)s da divisão do
# hash do originário por %(total)s (com um worker só, tudo). O count dos leads vivos fica fora, só para o lote.
SQL_PROXIMOS = r"""
select x.credito_id, x.precatorio, x.ente, creditos.cnj_formatar(x.originario20) as originario,
       (select count(*) from creditos.credito_originario o2
          join creditos.credito c2 on c2.id = o2.credito_id
         where o2.processo_id = x.processo_id and c2.saiu_da_lista_em is null) as leads_vivos_no_originario
  from (select distinct on (c.id)
               c.id               as credito_id,
               c.numero_exibicao  as precatorio,
               e.nome             as ente,
               pr.id              as processo_id,
               pr.numero_cnj      as originario20
          from creditos.credito c
          join creditos.credito_originario co on co.credito_id = c.id
          join creditos.processo pr           on pr.id = co.processo_id
          left join creditos.ente_alias a     on a.id = c.ente_alias_id
          left join creditos.ente e           on e.id = a.ente_id
         where c.tribunal_id = 120
           and c.saiu_da_lista_em is null
           and pr.numero_cnj ~ '^\d{13}820[56]\d{3}$'
           and not exists (select 1 from creditos.credito_credor k
                            where k.credito_id = c.id and k.papel_id in (1, 5))  -- CREDOR, CESSIONARIO
           and c.id <> all(%(pular)s::bigint[])
         order by c.id, pr.numero_cnj) x
 where mod(hashtext(x.originario20)::bigint + 2147483648, %(total)s) = %(indice)s
 order by x.credito_id
 limit %(lote)s
"""

# =============================================================================== worker


def definir_worker(n):
    """Liga este processo ao worker n: perfil do Chrome (o fetch_TJRN abre o PERFIL dele), CSV e nome no log (com
    vários workers no terminal, cada linha diz de qual é)."""
    global CSV_LEADS, log
    fetch_TJRN.PERFIL = PERFIL_BASE if n == 1 else PERFIL_BASE.with_name(f"{PERFIL_BASE.name}-{n}")
    CSV_LEADS = CSV_BASE if n == 1 else SAIDA / f"leads_TJRN_w{n}.csv"
    log = logging.getLogger(f"esteira_TJRN_w{n}")
    fetch_TJRN.log = logging.getLogger(f"fetch_TJRN_w{n}")


def sufixo_worker(n):
    """Sufixo do log do worker: o 1 mantém o nome de sempre, os outros ganham _w<N>."""
    return "" if n == 1 else f"_w{n}"

# =============================================================================== banco (só leitura) e CSV


def proximos_leads(pular: set[int], lote: int, indice: int = 0, total: int = 1) -> list[dict]:
    """Próximos leads da fatia deste worker. Uma conexão por lote: a esteira roda por horas e uma conexão ociosa cai."""
    with (closing(conectar("esteira_TJRN")) as con,
          con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur):
        cur.execute(SQL_PROXIMOS, {"pular": sorted(pular), "lote": lote, "indice": indice, "total": total})
        return [dict(r) for r in cur.fetchall()]


def arquivos_de_leads() -> list[Path]:
    """Os CSVs de leads de todos os workers (o do worker 1 e os leads_TJRN_w<N>.csv)."""
    return [CSV_BASE, *sorted(SAIDA.glob("leads_TJRN_w*.csv"))]


def ja_feitos() -> set[int]:
    """Leads cuja última linha em algum CSV de leads tem status que não volta (FEITO). Lê os CSVs de todos os
    workers: com outro número de workers, um lead pode ter ido para outro worker numa execução anterior."""
    feitos = set()
    for arquivo in arquivos_de_leads():
        if not arquivo.exists():
            continue
        ultimo = {}
        with arquivo.open(encoding="utf-8-sig", newline="") as f:
            for linha in csv.DictReader(f, delimiter=";"):
                ultimo[int(linha["credito_id"])] = linha["status"]
        feitos |= {credito_id for credito_id, status in ultimo.items() if status in FEITO}
    return feitos


def registrar(linha: dict) -> None:
    """Acrescenta uma linha ao CSV (padrão do projeto); com o arquivo aberto no Excel, espera ele ser fechado."""
    avisou = False
    while True:
        try:
            anexar_csv(CSV_LEADS, COLUNAS, [linha])
            return
        except PermissionError:
            if not avisou:
                log.warning(f"{CSV_LEADS.name} está aberto em outro programa (Excel?); feche para a esteira continuar.")
                avisou = True
            time.sleep(10)

# =============================================================================== execução


def ler_argumentos():
    """--lote, --limite (por worker), --workers N (N workers neste terminal) ou --worker i --total N (só o i)."""
    parser = argparse.ArgumentParser(
        description="Esteira do TJRN: leads sem credor -> consulta pública -> leads_TJRN.csv.")
    parser.add_argument("--lote", type=int, default=10, help="leads por lote (padrão: 10)")
    parser.add_argument("--limite", type=int, help="total de leads nesta execução, em cada worker (padrão: até acabar)")
    quantos = parser.add_mutually_exclusive_group()
    quantos.add_argument("--workers", type=int, help="sobe os workers 1..N neste terminal, todos pelo IP da máquina")
    quantos.add_argument("--worker", type=int, default=1, help="roda só o worker i (padrão: 1); use com --total")
    parser.add_argument("--total", type=int, default=1, help="quantos workers dividem os leads (com --worker)")
    args = parser.parse_args()
    if args.workers is not None and args.workers < 1:
        parser.error("--workers precisa ser 1 ou mais")
    if args.workers is None and not 1 <= args.worker <= args.total:
        parser.error(f"--worker {args.worker} precisa estar entre 1 e --total ({args.total}): "
                     f"ex.: --worker 2 --total 3")
    return args


def main() -> None:
    """--workers N: supervisiona os N workers. Senão roda o worker com a trava dele pega (solta ao sair)."""
    args = ler_argumentos()
    if args.workers:
        supervisionar(args)
        return
    with workers.travar_worker(SAIDA, args.worker):   # saida/worker_<n>.lock: um processo por worker
        try:
            rodar(args)
        except Exception:
            log.exception("worker parou por erro inesperado")      # no log do worker, não só no terminal
            raise


def supervisionar(args) -> None:
    """--workers N: sobe os workers 1..N (--worker i --total N) como processos filhos neste terminal, um a cada
    PAUSA_ENTRE_WORKERS s, e espera todos acabarem. O Ctrl+C chega a todos: cada um fecha o Chrome e sai; o lead em
    andamento não entra no CSV e volta na próxima execução."""
    sup = configurar_log("esteira_TJRN_workers", SAIDA / "logs")
    if args.workers > 1:
        sup.warning(f"{args.workers} workers pelo mesmo IP: o Akamai e o reCAPTCHA podem bloquear mais cedo "
                    "(acompanhe 'bloqueio' no log)")
    extras = ["--lote", str(args.lote)] + (["--limite", str(args.limite)] if args.limite else [])
    workers.supervisionar(sup, args.workers,
                          lambda n: [sys.executable, str(Path(__file__).resolve()), "--worker", str(n),
                                     "--total", str(args.workers), *extras],
                          PAUSA_ENTRE_WORKERS, ao_forcar="o lead em andamento volta na próxima execução")


def consultar_originario(pagina, originario):
    """Consulta um originário no PJe. Devolve (status, resultado, tele, parar, chrome_fechou): parar é o motivo para a
    esteira parar (bloqueio) ou None; chrome_fechou diz se o erro foi o Chrome fechando no meio da consulta."""
    tele = {"hora": datetime.now().isoformat(timespec="seconds")}
    inicio, parar, chrome_fechou = time.time(), None, False
    try:
        resultado = consultar(pagina, originario, tele)
        status = "ok" if resultado else "sem resultado"
    except Bloqueado as exc:
        resultado, status, parar = None, f"bloqueio: {exc}", f"bloqueio no originário {originario}"
    except (PlaywrightError, TimeoutError) as exc:
        chrome_fechou = pagina.is_closed()
        resultado, status = None, f"erro: {str(exc).strip().splitlines()[0]}"
    tele["duracao_s"] = round(time.time() - inicio)
    credor = " | ".join(p["nome"] for p in resultado["polo_ativo"]) if resultado else ""
    nivel = logging.INFO if resultado or status == "sem resultado" else logging.WARNING
    log.log(nivel, f"{originario}: {status} em {tele['duracao_s']}s{' -- ' + credor if credor else ''}")
    return status, resultado, tele, parar, chrome_fechou


def rodar(args) -> None:
    """Lotes de leads do banco (a fatia deste worker) -> consulta de cada originário -> uma linha por lead no CSV.
    Ctrl+C é um pedido de parada (conferido entre um lead e outro): o lead em andamento termina e entra no CSV."""
    definir_worker(args.worker)
    configurar_log(f"esteira_TJRN{sufixo_worker(args.worker)}", SAIDA / "logs")
    fetch_TJRN.PERFIL.mkdir(parents=True, exist_ok=True)
    SAIDA.mkdir(parents=True, exist_ok=True)
    pular = ja_feitos()
    indice = args.worker - 1
    if args.total > 1:
        log.info(f"worker {args.worker} de {args.total}: perfil {fetch_TJRN.PERFIL.name}, CSV {CSV_LEADS.name}")
    log.info(f"{len(pular)} leads já estão nos CSVs de leads e serão pulados.")
    log.info("Não minimize a janela do Chrome: o Akamai barra janela minimizada.")

    consultas = {}  # originário -> (status, resultado, tele) desta execução
    lote, n_lote, registrados, reaberturas = [], 0, 0, 0
    fim = parar = None
    buster_ok = False
    parada = workers.ParadaSuave(log, ao_forcar=lambda: workers.matar_chrome_do_perfil(fetch_TJRN.PERFIL))
    with parada:
        while not (fim or parar):
            with abrir_chrome() as contexto:
                pagina = contexto.pages[0] if contexto.pages else contexto.new_page()
                if not buster_ok and not (buster_ok := garantir_buster(contexto, pagina, lambda: bool(parada))):
                    parar = PARADA if parada else f"o Buster não foi instalado no perfil {fetch_TJRN.PERFIL.name}"
                    break
                while not (fim or parar):
                    if parada:
                        parar = PARADA
                        break
                    if not lote:
                        quantos = args.lote if args.limite is None else min(args.lote, args.limite - registrados)
                        if quantos <= 0:
                            fim = f"limite de {args.limite} leads"
                            break
                        lote = proximos_leads(pular, quantos, indice, args.total)
                        if not lote:
                            fim = "não há mais leads sem credor para consultar"
                            break
                        n_lote += 1
                        log.info(f"=== Lote {n_lote}: {len(lote)} leads")

                    lead = lote[0]
                    originario = lead["originario"]
                    if originario in consultas:
                        status, resultado, tele = consultas[originario]
                        log.info(f"lead {lead['credito_id']}: mesmo originário {originario}, consulta reaproveitada")
                    else:
                        if consultas:
                            time.sleep(random.uniform(*PAUSA_ENTRE))
                        log.info(f"lead {lead['credito_id']} ({lead['precatorio']}) -> originário {originario}")
                        status, resultado, tele, parar, chrome_fechou = consultar_originario(pagina, originario)
                        if chrome_fechou:                               # o Chrome fechou no meio da consulta
                            if reaberturas < REABERTURAS:
                                reaberturas += 1
                                log.warning(f"o Chrome fechou; reabrindo ({reaberturas}/{REABERTURAS}) "
                                            "e repetindo o originário")
                                break                                   # sai do with, reabre e tenta o mesmo lead
                            parar = f"o Chrome fechou mais de {REABERTURAS} vezes"
                        consultas[originario] = (status, resultado, tele)

                    linha = linha_csv(originario, status, resultado, tele)
                    del linha["processo"]
                    linha.update({k: lead[k] for k in COLUNAS_LEAD})
                    registrar(linha)
                    pular.add(lead["credito_id"])  # erro não volta nesta execução, só numa próxima
                    registrados += 1
                    lote.pop(0)

    if fim:
        nivel, motivo = logging.INFO, f"Fim: {fim}"
    elif parar == PARADA:
        nivel, motivo = logging.WARNING, f"Esteira {PARADA}"
    else:
        nivel, motivo = logging.ERROR, f"Esteira interrompida: {parar}"
    log.log(nivel, f"{motivo}. {registrados} leads registrados nesta execução em {CSV_LEADS}")


if __name__ == "__main__":
    main()
