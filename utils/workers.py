"""
workers.py - vários workers de um robô na mesma máquina: trava por worker e supervisor (--workers N num terminal só).

Cada worker é um processo com um número (1, 2, ...); o robô decide o que é de cada um (perfil do Chrome, arquivos,
fatia dos leads). O supervisor só sobe os processos em escada, espera todos e cuida do Ctrl+C.
"""
import logging
import msvcrt
import os
import re
import signal
import subprocess
import time
from pathlib import Path


class ParadaSuave:
    """Troca o Ctrl+C por um pedido de parada: o robô confere `if parada:` entre um item e outro, termina o que está
    fazendo e sai limpo. Serve para robôs com o Playwright síncrono, que não aguenta um KeyboardInterrupt no meio de
    uma chamada (o contexto.close() fica esperando para sempre). O 2º Ctrl+C sai na hora: chama ao_forcar() (ex.:
    fechar o Chrome) e encerra o processo."""

    def __init__(self, log, ao_forcar=None):
        self.log, self.ao_forcar, self.pedida = log, ao_forcar, False

    def __enter__(self):
        self._anterior = signal.signal(signal.SIGINT, self._ctrl_c)
        return self

    def __exit__(self, *_):
        signal.signal(signal.SIGINT, self._anterior)

    def __bool__(self):
        return self.pedida

    def _ctrl_c(self, *_):
        if not self.pedida:
            self.pedida = True
            self.log.warning("Ctrl+C: termina o que está fazendo e sai (Ctrl+C de novo sai na hora)")
            return
        self.log.error("Ctrl+C de novo: saindo na hora")
        if self.ao_forcar:
            self.ao_forcar()
        os._exit(1)


def matar_chrome_do_perfil(perfil):
    """Mata o Chrome aberto com este perfil (--user-data-dir exato: o perfil do worker 1 é prefixo do dos outros).
    Serve para o worker que caiu sem fechar o navegador e para a saída à força. Devolve quantos processos matou."""
    alvo = re.escape(f"--user-data-dir={perfil}") + r'(\s|"|$)'
    comando = ("Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
               f"Where-Object {{ $_.CommandLine -match '{alvo}' }} | ForEach-Object {{ $_.ProcessId }}")
    r = subprocess.run(["powershell", "-NoProfile", "-Command", comando], capture_output=True, text=True, timeout=60)
    pids = [p for p in r.stdout.split() if p.isdigit()]
    for pid in pids:
        subprocess.run(["taskkill", "/F", "/T", "/PID", pid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return len(pids)


def travar_worker(pasta, n):
    """Trava exclusiva em <pasta>/worker_<n>.lock enquanto o processo roda: um 2º processo com o mesmo número usaria o
    mesmo perfil do Chrome e os mesmos arquivos, então para aqui. Devolve o arquivo aberto (a trava dura enquanto ele
    estiver aberto, e o Windows a solta sozinho se o processo morrer)."""
    Path(pasta).mkdir(parents=True, exist_ok=True)
    trava = open(Path(pasta) / f"worker_{n}.lock", "a+")  # noqa: SIM115 - fica aberto de propósito: é a trava
    trava.seek(0)
    try:
        msvcrt.locking(trava.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        trava.close()
        raise SystemExit(f"o worker {n} já está rodando nesta máquina: use outro número.") from None
    return trava


def supervisionar(sup, total, comando, pausa, ao_forcar=""):
    """Sobe os workers 1..total como processos filhos deste terminal (comando(n) = linha de comando do worker n), um a
    cada `pausa` s, e espera todos acabarem, registrando cada um em `sup` (logger). O Ctrl+C do terminal chega a
    todos (mesmo console): cada worker encerra do seu jeito. Um 2º Ctrl+C mata os que ainda não saíram (`ao_forcar`
    explica o que acontece com o trabalho deles). Devolve {n: código de saída}."""
    workers, avisados = {}, set()
    try:
        for n in range(1, total + 1):
            if n > 1:
                time.sleep(pausa)
            workers[n] = subprocess.Popen(comando(n))
            sup.info(f"worker {n} de {total} subiu (pid {workers[n].pid})")
        esperar_workers(sup, workers, avisados)
    except KeyboardInterrupt:
        sup.warning("Ctrl+C: cada worker encerra o que está fazendo e sai (Ctrl+C de novo mata os que faltarem)")
        try:
            esperar_workers(sup, workers, avisados)
        except KeyboardInterrupt:
            signal.signal(signal.SIGINT, signal.SIG_IGN)   # daqui em diante mais Ctrl+C não interrompe a limpeza
            for n, processo in workers.items():
                if processo.poll() is None:
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(processo.pid)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    sup.error(f"worker {n} encerrado à força{': ' + ao_forcar if ao_forcar else ''}")
    codigos = {n: p.poll() for n, p in workers.items()}
    sup.info("workers encerrados: " + ", ".join(f"{n}={c}" for n, c in codigos.items())
             + " (0 = saiu normal; outro código = caiu, veja o log dele)")
    return codigos


def esperar_workers(sup, workers, avisados):
    """Espera todos os workers saírem, registrando uma vez cada um que termina (avisados guarda os já registrados).
    Espera com timeout: no Windows, a espera sem timeout não atende o Ctrl+C."""
    while True:
        for n, processo in workers.items():
            if processo.poll() is not None and n not in avisados:
                avisados.add(n)
                nivel = logging.INFO if processo.returncode == 0 else logging.WARNING
                sup.log(nivel, f"worker {n} terminou (código {processo.returncode})")
        if len(avisados) == len(workers):
            return
        time.sleep(1)
