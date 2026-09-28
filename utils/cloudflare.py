import os
import shutil
import socket
import subprocess
import time
import urllib.request
import winreg
from pathlib import Path
from urllib.parse import urlsplit

from patchright.sync_api import sync_playwright
from scrapling.parser import Selector

URL = "https://eproc-consulta.jfrj.jus.br/eproc/externo_controlador.php?acao=processo_consulta_publica"
PERFIL = Path("./.chrome-profile-eproc").resolve()

# Sinal real de validação do Turnstile do eproc (window.bolCloudflareSucess,
# hdnInfraCaptcha ou o input hidden cf-turnstile-response) -- confirmado no
# TRF2/autenticacao.py. Detectar por texto/sumiço do modal dá falso negativo.
JS_TURNSTILE_OK = (
    "window.bolCloudflareSucess === true"
    " || document.getElementById('hdnInfraCaptcha')?.value === '1'"
    " || !!document.querySelector(\"[name='cf-turnstile-response']\")?.value"
)

HOST = 'http://127.0.0.1'
TIMEOUT_VALIDAR = 60


def localizar_chrome() -> str:
    """Tenta encontrar o chrome.exe """
    chave = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe"
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(hive, chave) as k:
                caminho = winreg.QueryValueEx(k, None)[0]
                if caminho and Path(caminho).is_file():
                    return caminho
        except FileNotFoundError:
            pass

    candidatos = [
        Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe",
    ]
    for c in candidatos:
        if c.is_file():
            return str(c)

    encontrado = shutil.which("chrome")
    if encontrado:
        return encontrado

    raise FileNotFoundError("Chrome não encontrado nesta máquina. Instale o Google Chrome.")


def porta_livre() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def limpar_locks_perfil(perfil: Path) -> None:
    """Remove locks deixados por um Chrome anterior que não fechou direito
    -- sem isso, o Chrome recusa abrir de novo nesse mesmo user-data-dir."""
    for nome in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        alvo = perfil / nome
        try:
            alvo.unlink(missing_ok=True)
        except OSError:
            pass


def esperar_cdp(porta: int, timeout: float = 20.0) -> None:
    fim = time.time() + timeout
    ultimo_erro: Exception | None = None
    while time.time() < fim:
        try:
            with urllib.request.urlopen(f"{HOST}:{porta}/json/version", timeout=1):
                return
        except Exception as exc:
            ultimo_erro = exc
            time.sleep(0.3)
    raise TimeoutError(f"Chrome não respondeu no CDP em {timeout}s: {ultimo_erro}")


def achar_pagina(browser, url_alvo: str):
    dominio = urlsplit(url_alvo).netloc
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if dominio in pg.url:
                return pg
    return browser.contexts[0].pages[0]


def matar_arvore(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/F", "/T", "/PID", str(pid)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> None:
    chrome_exe = localizar_chrome()
    porta = porta_livre()
    PERFIL.mkdir(parents=True, exist_ok=True)
    limpar_locks_perfil(PERFIL)

    processo = subprocess.Popen([
        chrome_exe,
        f"--remote-debugging-port={porta}",
        f"--user-data-dir={PERFIL}",
        "--no-first-run",
        "--no-default-browser-check",
        URL,
    ])

    try:
        esperar_cdp(porta)

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(f"{HOST}:{porta}")
            pagina_pw = achar_pagina(browser, URL)  # a aba que já abriu com a URL -- sem goto()

            print("Aguardando o Turnstile validar (não mexa na janela nem minimize)...")
            for _ in range(TIMEOUT_VALIDAR):
                if pagina_pw.evaluate(JS_TURNSTILE_OK):
                    print("Turnstile validado!")
                    break
                time.sleep(1)
            else:
                print(f"Turnstile não validou em {TIMEOUT_VALIDAR}s.")

            page = Selector(content=pagina_pw.content(), url=URL)



            time.sleep(40)
    finally:
        matar_arvore(processo.pid)

    print("Navegador fechado.")


if __name__ == "__main__":
    main()
