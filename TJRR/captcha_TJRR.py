"""
captcha_TJRR.py - captcha Tencent (slider) do TJRR resolvido sozinho, no Chrome instalado controlado por CDP.

Murakî (SGP, precatorios.tjrr.jus.br/rest): a consulta "Tenho precatório a receber?" exige o header X-Captcha-Token.
O token sai de POST /captcha/verificar {ticket, randstr} depois do captcha (appId 189999713) e vale 30 min; com ele,
GET /precatorios/muraki/{processo} devolve o CPF/CNPJ do beneficiário e o valor, e ?documento=<CPF> confere um candidato.

Projudi (consulta pública, consultaprojudi.tjrr.jus.br): a página /captcha mostra a caixa "Eu sou humano" (appId
189992716, TJNCaptcha-global.js); o clique abre o slider embutido na própria página (sem iframe). Resolvido, o app
manda 'randstr:ticket' no header Authorization de cada chamada ao /consilium-api (busca por número, parte, CPF/CNPJ,
advogado e OAB; detalhe com polos; movimentações e arquivos). O robô lê esse header da 1ª chamada do app.

Resolução do slider (a mesma do PJe do TJBA): baixa o fundo e a peça (respostas 'getcapbysig'), acha o buraco com
OpenCV (bordas Canny + matchTemplate) e arrasta o slider com aceleração e tremor. Os dois layouts do Tencent têm a
mesma geometria; muda só onde ficam os elementos (iframe drag_ele no Murakî, DOM da página no Projudi).

Uso:  python TJRR/captcha_TJRR.py [muraki|projudi]      (abre o Chrome, resolve e mostra o token)
"""
import os
import queue
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import winreg
from concurrent.futures import Future, TimeoutError as FuturoTimeout
from pathlib import Path

import cv2                                                   # pip install opencv-python-headless
import numpy as np

try:
    from patchright.sync_api import Error as ErroNavegador, sync_playwright
    NO_MUNDO_DA_PAGINA = {"isolated_context": False}    # o patchright roda o evaluate num mundo isolado por padrão
except ImportError:
    from playwright.sync_api import Error as ErroNavegador, sync_playwright
    NO_MUNDO_DA_PAGINA = {}                            # o playwright já roda no mundo da página (e não tem a opção)

AQUI = Path(__file__).resolve().parent
if str(AQUI.parent) not in sys.path:
    sys.path.insert(0, str(AQUI.parent))            # utils/ fica na raiz do projeto

from utils import workers  # noqa: E402

# =============================================================================== configuração

HOST = "http://127.0.0.1"
PERFIL_MURAKI = AQUI / ".chrome-profile-muraki-tjrr"
PERFIL_PROJUDI = AQUI / ".chrome-profile-projudi-tjrr"
URL_MURAKI = "https://muraki.tjrr.jus.br/muraki/tem-precatorio"
URL_PROJUDI = "https://consultaprojudi.tjrr.jus.br/captcha"
API_SGP = "https://precatorios.tjrr.jus.br/rest"
API_PROJUDI = "https://consultaprojudi.tjrr.jus.br/consilium-api"
APP_MURAKI = "189999713"
SCRIPT_TENCENT = "https://turing.captcha.qcloud.com/TCaptcha-global.js"
ESPERA_CAPTCHA = 90                    # s até o token sair
ESPERA_CAIXA = 60                      # s até a caixa "Eu sou humano" do Projudi aparecer (já levou 25 s)
TENTATIVAS_CAPTCHA = 4
FOLGA_TOKEN = 5 * 60                   # s antes de expirar em que o token já é renovado
# O Authorization do Projudi cai ("Captcha expirado", HTTP 401) com ~8 min ou ~50 chamadas, o que vier primeiro
# (medido em 02/10/2026): renova antes, por tempo e por uso.
VALIDADE_PROJUDI = 7 * 60 + FOLGA_TOKEN   # s; token() renova quando faltam FOLGA_TOKEN, ou seja, com 7 min de uso
USOS_PROJUDI = 45                      # chamadas por token

# Murakî (iframe drag_ele_global.html): #slideBg = fundo com o buraco (img_index=1);
# a peça é o .tc-fg-item quadrado, recorte do sprite img_index=0; o slider é .tc-slider-normal.
JS_CAPTCHA = r"""() => {
  const r = e => e.getBoundingClientRect(), url = s => (s.match(/url\("?(.*?)"?\)/) || [])[1];
  const bg = document.getElementById('slideBg');
  const peca = [...document.querySelectorAll('.tc-fg-item:not(.tc-slider-normal)')]
                 .find(e => r(e).width > 0 && Math.abs(r(e).width - r(e).height) < 2);
  const slider = document.querySelector('.tc-fg-item.tc-slider-normal');
  if (!bg || !peca || !slider) return null;
  const cp = getComputedStyle(peca);
  return {bg_url: url(getComputedStyle(bg).backgroundImage), bg_x: r(bg).x, bg_y: r(bg).y, bg_w: r(bg).width,
          sp_url: url(cp.backgroundImage), sp_w: parseFloat(cp.backgroundSize),
          pos: cp.backgroundPosition.split(' ').map(parseFloat),
          peca_x: r(peca).x, peca_y: r(peca).y, peca_w: r(peca).width, slider_w: r(slider).width};
}"""
JS_IFRAME_VISIVEL = ("() => { const f = document.getElementById('tcaptcha_iframe_dy'); "
                     "return !!f && f.getBoundingClientRect().y >= 0 }")
SLIDER_IFRAME = ".tc-fg-item.tc-slider-normal"
RECARREGAR_IFRAME = "() => document.getElementById('reload')?.click()"
# Projudi (popup #tCaptchaDyContent no DOM da página): fundo .tencent-captcha-dy__verify-bg-img, peça
# .tencent-captcha-dy__fg-item (recorte do sprite), slider .tencent-captcha-dy__slider-block.
JS_CAPTCHA_DY = r"""() => {
  const r = e => e.getBoundingClientRect(), url = s => (s.match(/url\("?(.*?)"?\)/) || [])[1];
  const box = document.getElementById('tCaptchaDyContent');
  if (!box || r(box).width === 0 || r(box).y < 0 || getComputedStyle(box).visibility === 'hidden') return null;
  const bg = box.querySelector('.tencent-captcha-dy__verify-bg-img');
  const peca = [...box.querySelectorAll('.tencent-captcha-dy__fg-item')]
                 .find(e => r(e).width > 0 && Math.abs(r(e).width - r(e).height) < 2);
  const slider = box.querySelector('.tencent-captcha-dy__slider-block');
  if (!bg || !peca || !slider) return null;
  const cp = getComputedStyle(peca);
  return {bg_url: url(getComputedStyle(bg).backgroundImage), bg_x: r(bg).x, bg_y: r(bg).y, bg_w: r(bg).width,
          sp_url: url(cp.backgroundImage), sp_w: parseFloat(cp.backgroundSize),
          pos: cp.backgroundPosition.split(' ').map(parseFloat),
          peca_x: r(peca).x, peca_y: r(peca).y, peca_w: r(peca).width, slider_w: r(slider).width};
}"""
SLIDER_DY = "#tCaptchaDyContent .tencent-captcha-dy__slider-block"
RECARREGAR_DY = ("() => document.querySelector('#tCaptchaDyContent [class*=\"refresh\"], "
                 "#tCaptchaDyContent [class*=\"reload\"]')?.click()")
CAIXA_PROJUDI = "#tencent-captcha-dy__robot_checkBox_id"
# O botão do Murakî só habilita com o formulário preenchido: o captcha é aberto pelo mesmo caminho que a página usa
# (TencentCaptcha + POST /captcha/verificar) e o token vai para o localStorage, como a página guarda.
JS_ABRIR_MURAKI = """async ([app, script, api]) => {
  localStorage.removeItem('muraki_captcha_token'); localStorage.removeItem('muraki_captcha_exp');
  if (!window.TencentCaptcha) await new Promise((ok, erro) => { const s = document.createElement('script');
    s.src = script; s.onload = ok; s.onerror = erro; document.head.appendChild(s); });
  new window.TencentCaptcha(app, async r => {
    if (r.ret !== 0 || !r.ticket) return;
    const resp = await fetch(api + '/captcha/verificar', {method: 'POST', headers: {'Content-Type': 'application/json'},
                             body: JSON.stringify({ticket: r.ticket, randstr: r.randstr})});
    const j = await resp.json();
    if (j.token) { localStorage.setItem('muraki_captcha_token', j.token);
                   localStorage.setItem('muraki_captcha_exp', String(j.expiraEm)); }
  }).show();
}"""
JS_TOKEN_MURAKI = ("() => [localStorage.getItem('muraki_captcha_token'), "
                   "Number(localStorage.getItem('muraki_captcha_exp') || 0)]")


class ErroCaptcha(Exception):
    """O captcha não saiu (imagens estranhas, tentativas esgotadas ou o Chrome caiu): falha passageira."""

# =============================================================================== Chrome por CDP


def localizar_chrome():
    """Caminho do chrome.exe instalado (registro do Windows, pastas padrão ou PATH)."""
    chave = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe"
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(hive, chave) as k:
                caminho = winreg.QueryValueEx(k, None)[0]
                if caminho and Path(caminho).is_file():
                    return caminho
        except FileNotFoundError:
            pass
    for c in (Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
              Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
              Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe"):
        if c.is_file():
            return str(c)
    if shutil.which("chrome"):
        return shutil.which("chrome")
    raise FileNotFoundError("Chrome não encontrado nesta máquina.")


def porta_livre():
    """Uma porta TCP livre em 127.0.0.1 para a depuração remota do Chrome."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def esperar_cdp(porta, timeout=20.0):
    """Espera o Chrome abrir a porta de depuração (CDP); TimeoutError se não abrir."""
    fim = time.time() + timeout
    while time.time() < fim:
        try:
            with urllib.request.urlopen(f"{HOST}:{porta}/json/version", timeout=1):
                return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"Chrome não respondeu no CDP em {timeout}s")

# =============================================================================== slider Tencent


def captcha_aberto(pagina, imagens, dy=False):
    """(frame, geometria) do captcha visível e com as duas imagens já baixadas; senão (None, None).
    dy: o popup embutido na página (Projudi); senão o iframe drag_ele (Murakî)."""
    try:
        if dy:
            frame = pagina.main_frame
            g = pagina.evaluate(JS_CAPTCHA_DY)
        else:
            frame = next((f for f in pagina.frames if "drag_ele" in f.url), None)
            g = frame and pagina.evaluate(JS_IFRAME_VISIVEL) and frame.evaluate(JS_CAPTCHA)
    except Exception:                                   # frame recarregando
        return None, None
    if g and g["bg_w"] > 0 and g["bg_url"] in imagens and g["sp_url"] in imagens:
        return frame, g
    return None, None


def achar_buraco(fundo, sprite, g):
    """x (px do frame) do buraco: casa as bordas do contorno da peça com as bordas do fundo, na altura da peça."""
    k_bg = fundo.shape[1] / g["bg_w"]
    k_sp = sprite.shape[1] / g["sp_w"]
    x0, y0, w = round(-g["pos"][0] * k_sp), round(-g["pos"][1] * k_sp), round(g["peca_w"] * k_sp)
    alfa = cv2.resize(sprite[y0:y0 + w, x0:x0 + w, 3], None, fx=k_bg / k_sp, fy=k_bg / k_sp)
    topo = max(round((g["peca_y"] - g["bg_y"]) * k_bg) - 6, 0)
    faixa = cv2.cvtColor(fundo[topo: topo + alfa.shape[0] + 12], cv2.COLOR_BGR2GRAY)
    res = cv2.matchTemplate(cv2.Canny(cv2.GaussianBlur(faixa, (3, 3), 0), 50, 150), cv2.Canny(alfa, 100, 200),
                            cv2.TM_CCOEFF_NORMED)
    return g["bg_x"] + cv2.minMaxLoc(res)[3][0] / k_bg


def arrastar_slider(pagina, frame, g, alvo_x, seletor=SLIDER_IFRAME):
    """Arrasta com aceleração, tremor e leve passada do ponto (a peça anda 1:1 com o slider)."""
    caixa = frame.locator(seletor).bounding_box()
    dist = (alvo_x - g["peca_x"]) * caixa["width"] / g["slider_w"]
    x = caixa["x"] + caixa["width"] / 2 + random.uniform(-5, 5)
    y = caixa["y"] + caixa["height"] / 2 + random.uniform(-4, 4)
    pagina.mouse.move(x - random.uniform(20, 40), y + random.uniform(5, 15))
    pagina.mouse.move(x, y, steps=5)
    time.sleep(random.uniform(0.15, 0.35))
    pagina.mouse.down()
    time.sleep(random.uniform(0.08, 0.2))
    passos, extra = random.randint(28, 40), random.uniform(2, 5)
    for d in [(dist + extra) * (1 - (1 - i / passos) ** 3) for i in range(1, passos + 1)] + [dist + extra / 2, dist]:
        pagina.mouse.move(x + d, y + random.uniform(-1.5, 1.5))
        time.sleep(random.uniform(0.012, 0.03))
    time.sleep(random.uniform(0.2, 0.4))
    pagina.mouse.up()


def resolver_slider(pagina, imagens, pronto, espera=ESPERA_CAPTCHA, dy=False):
    """Resolve o slider que abrir até `pronto()` devolver algo verdadeiro (e devolve esse valor). Imagem nova a cada
    erro; sem imagem nova em 6 s, clica em recarregar. ErroCaptcha se não sair em `espera` s ou nas tentativas."""
    seletor, recarregar = (SLIDER_DY, RECARREGAR_DY) if dy else (SLIDER_IFRAME, RECARREGAR_IFRAME)
    fim = time.time() + espera
    tentadas, ultima = set(), 0.0
    while time.time() < fim:
        achou = pronto()
        if achou:
            return achou
        frame, g = captcha_aberto(pagina, imagens, dy)
        if g and g["bg_url"] not in tentadas:
            if len(tentadas) >= TENTATIVAS_CAPTCHA:
                raise ErroCaptcha(f"CAPTCHA: não resolvido em {TENTATIVAS_CAPTCHA} tentativas")
            tentadas.add(g["bg_url"])
            time.sleep(random.uniform(0.8, 1.5))        # a imagem acabou de aparecer
            try:
                fundo = cv2.imdecode(np.frombuffer(imagens[g["bg_url"]].body(), np.uint8), cv2.IMREAD_COLOR)
                sprite = cv2.imdecode(np.frombuffer(imagens[g["sp_url"]].body(), np.uint8), cv2.IMREAD_UNCHANGED)
                arrastar_slider(pagina, frame, g, achar_buraco(fundo, sprite, g), seletor)
            except ErroNavegador:
                raise
            except Exception:                           # imagem estranha: conta como tentativa
                pass
            ultima = time.time()
        elif g and time.time() - ultima > 6:
            frame.evaluate(recarregar)                  # errou e não trocou a imagem
            ultima = time.time()
        time.sleep(1)
    raise ErroCaptcha(f"CAPTCHA: o token não saiu em {espera} s")


def clicar_humano(pagina, seletor):
    """Clique de pessoa: chega perto, ajusta e clica num ponto ao acaso dentro do elemento."""
    caixa = pagina.locator(seletor).bounding_box()
    x = caixa["x"] + caixa["width"] * random.uniform(0.3, 0.7)
    y = caixa["y"] + caixa["height"] * random.uniform(0.3, 0.7)
    pagina.mouse.move(x - random.uniform(30, 80), y + random.uniform(-20, 20))
    pagina.mouse.move(x, y, steps=random.randint(6, 12))
    time.sleep(random.uniform(0.1, 0.3))
    pagina.mouse.click(x, y)

# =============================================================================== Chrome de captcha

_LOCAL = threading.local()


def playwright_da_thread():
    """O Playwright síncrono desta thread: só pode haver um por thread, então o Murakî e o Projudi o dividem."""
    if not getattr(_LOCAL, "pw", None):
        _LOCAL.pw = sync_playwright().start()
    return _LOCAL.pw


def parar_playwright():
    """Desliga o Playwright desta thread (depois de fechar os Chromes)."""
    pw = getattr(_LOCAL, "pw", None)
    _LOCAL.pw = None
    if pw:
        try:
            pw.stop()
        except Exception:
            pass


class _ChromeCaptcha:
    """Chrome instalado, com perfil próprio, aberto só para tirar o token do captcha (as consultas vão por requests).
    `token()` devolve o token válido e renova sozinho perto de expirar; `descartar()` quando o servidor recusar."""
    URL = ""
    NOME = ""

    def __init__(self, perfil):
        self.perfil = perfil
        self.proc = self.pw = self.pagina = self.navegador = None
        self.imagens = {}                               # url -> resposta das imagens do captcha
        self._token, self._expira = None, 0.0           # expira em s (epoch)

    def abrir(self):
        """Abre o Chrome no perfil próprio, conecta por CDP e passa a guardar as imagens do captcha.
        A janela não é desacelerada quando fica atrás de outras (senão o captcha atrasa)."""
        self.perfil.mkdir(parents=True, exist_ok=True)
        workers.matar_chrome_do_perfil(self.perfil)     # sobra de uma execução que caiu
        for nome in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            try:
                (self.perfil / nome).unlink(missing_ok=True)
            except OSError:
                pass
        porta = porta_livre()
        self.proc = subprocess.Popen([localizar_chrome(), f"--remote-debugging-port={porta}",
                                      f"--user-data-dir={self.perfil}", "--no-first-run",
                                      "--no-default-browser-check", "--disable-backgrounding-occluded-windows",
                                      "--disable-renderer-backgrounding", "--disable-background-timer-throttling",
                                      "about:blank"])
        esperar_cdp(porta)
        self.navegador = playwright_da_thread().chromium.connect_over_cdp(f"{HOST}:{porta}")
        self.pagina = [pg for ctx in self.navegador.contexts for pg in ctx.pages][0]
        self.pagina.context.on("response",
                               lambda r: self.imagens.__setitem__(r.url, r) if "getcapbysig" in r.url else None)

    def fechar(self):
        """Fecha a conexão CDP e mata a árvore de processos do Chrome."""
        try:
            if getattr(self, "navegador", None):
                self.navegador.close()              # só a conexão CDP; o Playwright da thread continua
        except Exception:
            pass
        if self.proc:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(self.proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.proc = self.pw = self.pagina = self.navegador = None

    def _resolver(self):
        """(token, expira em s epoch) de um captcha novo, com a página já em self.URL."""
        raise NotImplementedError

    def renovar(self):
        """Resolve um captcha novo e guarda o token. ErroCaptcha se não sair; o Chrome é reaberto se caiu."""
        for vez in (1, 2):
            try:
                if not self.pagina or self.pagina.is_closed():
                    self.fechar()
                    self.abrir()
                self.imagens.clear()
                self.pagina.goto(self.URL, wait_until="domcontentloaded", timeout=60000)
                self._token, self._expira = self._resolver()
                return self._token
            except ErroNavegador:
                self.fechar()
                if vez == 2:
                    raise ErroCaptcha(f"CAPTCHA: o Chrome do {self.NOME} caiu duas vezes")
        raise ErroCaptcha("CAPTCHA: não renovado")

    def token(self):
        """Token válido (renova antes de expirar)."""
        if not self._token or time.time() > self._expira - FOLGA_TOKEN:
            self.renovar()
        return self._token

    def descartar(self):
        """O servidor recusou o token: o próximo token() resolve outro captcha."""
        self._token, self._expira = None, 0.0


class Muraki(_ChromeCaptcha):
    """Token do Murakî (header X-Captcha-Token, 30 min). Recusa = HTTP 428."""
    URL = URL_MURAKI
    NOME = "Murakî"

    def __init__(self, perfil=PERFIL_MURAKI):
        super().__init__(perfil)

    def _resolver(self):
        self.pagina.evaluate(JS_ABRIR_MURAKI, [APP_MURAKI, SCRIPT_TENCENT, API_SGP], **NO_MUNDO_DA_PAGINA)
        token, expira = resolver_slider(self.pagina, self.imagens,
                                        lambda: (lambda t: t if t[0] else None)(self.pagina.evaluate(JS_TOKEN_MURAKI)))
        return token, expira / 1000


class Projudi(_ChromeCaptcha):
    """Authorization do Projudi ('randstr:ticket'), lido da 1ª chamada do app ao /consilium-api depois do captcha.
    A caixa "Eu sou humano" demora a aparecer (até ~25 s); às vezes o Tencent aprova sem slider."""
    URL = URL_PROJUDI
    NOME = "Projudi"

    def __init__(self, perfil=PERFIL_PROJUDI):
        super().__init__(perfil)
        self._vistos = []                               # Authorization das chamadas do app
        self._usos = 0

    def token(self):
        """Token para UMA chamada ao /consilium-api: conta o uso e renova com USOS_PROJUDI chamadas ou 7 min."""
        if self._usos >= USOS_PROJUDI:
            self.descartar()
        tk = super().token()
        self._usos += 1
        return tk

    def descartar(self):
        super().descartar()
        self._usos = 0

    def abrir(self):
        super().abrir()
        self.pagina.context.on("request", lambda r: self._vistos.append(r.headers.get("authorization"))
                               if API_PROJUDI in r.url and r.headers.get("authorization") else None)

    def _resolver(self):
        self._vistos.clear()
        pronto = lambda: self._vistos[-1] if self._vistos else None     # noqa: E731
        fim = time.time() + ESPERA_CAIXA
        while time.time() < fim and not pronto():
            caixa = self.pagina.locator(CAIXA_PROJUDI)
            if caixa.count() and caixa.first.is_visible():
                time.sleep(random.uniform(0.5, 1.2))
                clicar_humano(self.pagina, CAIXA_PROJUDI)
                break
            time.sleep(1)
        else:
            if not pronto():
                raise ErroCaptcha(f"CAPTCHA: a caixa do Projudi não apareceu em {ESPERA_CAIXA} s")
        return resolver_slider(self.pagina, self.imagens, pronto, dy=True), time.time() + VALIDADE_PROJUDI


# =============================================================================== serviço para várias threads


class ServicoCaptcha:
    """Tokens do Murakî e do Projudi para os workers (threads) de um robô. O Playwright só funciona na thread que o
    criou, então os dois Chromes vivem numa thread própria; quem precisa de token novo pede a ela e espera.
    token(nome) devolve o token válido e conta um uso; descartar(nome, token) quando o servidor recusou."""
    LIMITES = {"muraki": None, "projudi": USOS_PROJUDI}
    ESPERA = 4 * 60                                     # s que um worker espera por um token novo

    def __init__(self, parar):
        self.parar = parar
        self.fila = queue.Queue()
        self.trava = threading.Lock()
        self.renovando = {n: threading.Lock() for n in self.LIMITES}
        self.estado = {n: {"token": None, "expira": 0.0, "usos": 0} for n in self.LIMITES}
        self.renovacoes = {n: 0 for n in self.LIMITES}
        self.thread = threading.Thread(target=self._rodar, name="captcha", daemon=True)
        self.thread.start()

    def _rodar(self):
        chromes = {"muraki": Muraki(), "projudi": Projudi()}
        try:
            while True:
                pedido = self.fila.get()
                if pedido is None:
                    return
                nome, futuro = pedido
                try:
                    c = chromes[nome]
                    token = c.renovar()
                    futuro.set_result((token, c._expira))
                except BaseException as e:                  # noqa: BLE001 (repassa ao worker que pediu)
                    futuro.set_exception(e)
        finally:
            for c in chromes.values():
                c.fechar()
            parar_playwright()

    def _valido(self, nome):
        e, limite = self.estado[nome], self.LIMITES[nome]
        return e["token"] and time.time() < e["expira"] - FOLGA_TOKEN and (limite is None or e["usos"] < limite)

    def token(self, nome):
        """Token válido de 'muraki' ou 'projudi' (conta 1 uso). ErroCaptcha se não sair."""
        with self.trava:
            if self._valido(nome):
                self.estado[nome]["usos"] += 1
                return self.estado[nome]["token"]
        with self.renovando[nome]:                       # um worker renova; os outros esperam e usam o novo
            with self.trava:
                if self._valido(nome):
                    self.estado[nome]["usos"] += 1
                    return self.estado[nome]["token"]
            if self.parar.is_set():
                raise ErroCaptcha("INTERROMPIDO")
            futuro = Future()
            self.fila.put((nome, futuro))
            try:
                token, expira = futuro.result(timeout=self.ESPERA)
            except FuturoTimeout:
                raise ErroCaptcha(f"CAPTCHA: o token do {nome} não saiu em {self.ESPERA} s") from None
            with self.trava:
                self.estado[nome] = {"token": token, "expira": expira, "usos": 1}
                self.renovacoes[nome] += 1
            return token

    def descartar(self, nome, token):
        """O servidor recusou este token (expirou antes da conta): o próximo token() resolve outro captcha."""
        with self.trava:
            if self.estado[nome]["token"] == token:
                self.estado[nome] = {"token": None, "expira": 0.0, "usos": 0}

    def fechar(self):
        """Fecha os Chromes (na thread deles) e espera a thread terminar."""
        self.fila.put(None)
        self.thread.join(timeout=60)


if __name__ == "__main__":
    c = Projudi() if (sys.argv[1:] or ["muraki"])[0] == "projudi" else Muraki()
    try:
        t0 = time.time()
        tk = c.token()
        print(f"{c.NOME}: token em {time.time() - t0:.1f}s, renova em {(c._expira - time.time()) / 60:.0f} min: "
              f"{tk[:24]}…")
    finally:
        c.fechar()
        parar_playwright()
