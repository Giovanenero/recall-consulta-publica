"""
log.py - log padrão dos robôs.

Uma linha por evento, igual no terminal e no arquivo:
    2026-09-28 09:45:12 | INFO    | fetch_TJAL | 9679 leads do TJAL na lista (lidos em 1.2s)

O arquivo fica em <pasta_logs>/<script>_AAAAMMDD.log (um por dia, acrescentando). Cada módulo pega o seu logger
com logging.getLogger(<nome do arquivo sem .py>); configurar_log() liga os dois destinos no logger raiz, então as
mensagens de um módulo importado (ex.: fetch_TJRN dentro da esteira_TJRN) saem no mesmo arquivo, com o nome dele.
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
FORMATO_DATA = "%Y-%m-%d %H:%M:%S"


def configurar_log(script, pasta_logs):
    """Liga o log no terminal (UTF-8) e em pasta_logs/<script>_AAAAMMDD.log. Devolve o logger do script."""
    nome = Path(script).stem
    sys.stdout.reconfigure(encoding="utf-8")
    Path(pasta_logs).mkdir(parents=True, exist_ok=True)
    formato = logging.Formatter(FORMATO, FORMATO_DATA)
    terminal = logging.StreamHandler(sys.stdout)
    arquivo = logging.FileHandler(Path(pasta_logs) / f"{nome}_{datetime.now():%Y%m%d}.log", encoding="utf-8")
    raiz = logging.getLogger()
    raiz.handlers.clear()
    for destino in (terminal, arquivo):
        destino.setFormatter(formato)
        raiz.addHandler(destino)
    raiz.setLevel(logging.INFO)
    return logging.getLogger(nome)
