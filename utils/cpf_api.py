"""
cpf_api.py - CPF pelo NOME na API interna de CPFs (a mesma do modo 5 do RPA_SISTEMAS), com a regra de aceite ESTRITA.

.env: CPF_API_URL (ex.: https://recall-0127-1.tail796cdb.ts.net) e CPF_API_TOKEN (Bearer; rotaciona). O token vai só
no header: nunca em URL, log ou exceção.

Endpoint: GET /v1/cpfs?nome=<nome exato>&limit=200[&cursor=<proximo_cursor>] (documentação da API base_cpfs repassada
pelo usuário em 06/10/2026):
  - limite por token: 60 req/min em token bucket (rajada de 60, depois 1/s), até 200 linhas por página (o valor que
    valeu vem em `limite_aplicado`). Dividido com quem mais usar o mesmo token: não paralelizar;
  - paginação: repetir EXATAMENTE a mesma consulta com &cursor=<proximo_cursor>; proximo_cursor null = acabou;
  - 429 sem Retry-After: esperar >= 1 s, nunca repetir em loop (cada recusa vai para a auditoria);
  - 400 também gasta cota: 'consulta_ampla_demais' (o nome casa resultados demais), 'consulta_demorada' (passou de
    15 s), 'cursor_invalido'; 401 token inválido/expirado/revogado; 403 'sem_escopo'.

Regra de aceite (decisão do usuário de 06/10/2026), diferente do resolver() do RPA, que aceita um candidato único sem
conferir o nome:
  - só conta candidato com o nome IDÊNTICO ao procurado depois de `normalizar`;
  - aceita só com UM CPF assim no país inteiro, com todas as páginas lidas (sobrou página: PAGINACAO_INCOMPLETA);
  - com a data de nascimento da fonte, ela também tem de bater com a da API (NOME_E_NASCIMENTO);
  - com o CPF tarjado da fonte ('***.340.134-**', '044.***.***-**'), só contam os candidatos que batem com TODOS os
    dígitos visíveis; aí os homônimos deixam de importar: aceita o único que bate (NOME_E_MASCARA). Máscara que mostra
    os 3 primeiros dígitos vai pelo /v1/busca (nome + começo do CPF);
  - (versão .2, decisão do usuário de 06/10/2026) o CPF aceito tem de ser da região fiscal do tribunal (9º dígito,
    REGIAO_FISCAL); senão REGIAO_DIVERGE. A região não desempata homônimos: só recusa.
Erro da API (rede, timeout, 5xx, 429 persistente, 4xx) é exceção (ErroApi / ErroToken), nunca "não achou".

Uso avulso (sondagem; não mostra CPF inteiro):
    python -m utils.cpf_api --nome "MARIA DA SILVA" [--nascimento 01/02/1950] [--forma]
"""
import argparse
import json
import os
import random
import re
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from utils.texto import documento_valido  # noqa: E402

TIMEOUT = 30                    # a API desiste sozinha em 15 s (400 consulta_demorada): esperar a resposta dela
INTERVALO = 1.05                # s entre chamadas no processo (60/min do token, com folga)
LIMITE_PAGINA = 200
MAX_PAGINAS = 30                # mais que isso: PAGINACAO_INCOMPLETA
TENTATIVAS = 4                  # rede/timeout/5xx
ESPERAS_429 = (2, 4, 8)         # sem Retry-After: espera crescente, depois desiste
MAX_CANDIDATOS_CACHE = 5
REGRA_VERSAO = "2026-10-06.2"   # muda quando a regra muda: a medição vale só para a mesma versão
                                # .2: máscara do CPF (dígitos visíveis) e região fiscal do tribunal

ACEITAS = ("NOME_UNICO_NO_PAIS", "NOME_E_NASCIMENTO", "NOME_E_MASCARA")
REJEITADAS = ("SEM_CANDIDATO", "HOMONIMOS", "NASCIMENTO_DIVERGE", "NASCIMENTO_AUSENTE_NA_API", "PAGINACAO_INCOMPLETA",
              "CPF_INVALIDO", "NOME_INVALIDO", "CONSULTA_AMPLA_DEMAIS", "CONSULTA_DEMORADA", "REGIAO_DIVERGE",
              "MASCARA_INVALIDA")
# Região fiscal = 9º dígito do CPF (onde ele foi emitido). Decisão do usuário (06/10/2026): só aceita o CPF emitido na
# região do tribunal. Medido: corta os CPFs errados de 1,4% para 0,8% e perde ~12% da cobertura.
REGIAO_FISCAL = {"DF": "1", "GO": "1", "MS": "1", "MT": "1", "TO": "1", "AC": "2", "AM": "2", "AP": "2", "PA": "2",
                 "RO": "2", "RR": "2", "CE": "3", "MA": "3", "PI": "3", "AL": "4", "PB": "4", "PE": "4", "RN": "4",
                 "BA": "5", "SE": "5", "MG": "6", "ES": "7", "RJ": "7", "SP": "8", "PR": "9", "SC": "9", "RS": "0"}
# 400 que são RESPOSTA sobre o nome (não falha da API): viram rejeição; só a 1ª é guardada no cache (a demorada pode
# passar numa hora de menos carga)
RECUSAS = {"consulta_ampla_demais": "CONSULTA_AMPLA_DEMAIS", "consulta_demorada": "CONSULTA_DEMORADA"}
RECUSAS_NO_CACHE = ("CONSULTA_AMPLA_DEMAIS",)


class ErroApi(Exception):
    """A API não respondeu de forma utilizável (rede, timeout, 5xx, 429 persistente, 4xx, JSON inválido)."""


class ErroToken(ErroApi):
    """Sem credenciais, token inválido/expirado/revogado (401) ou sem escopo (403)."""


class ConsultaRecusada(Exception):
    """400 consulta_ampla_demais / consulta_demorada: a API respondeu sobre o nome; `regra` é a rejeição."""

    def __init__(self, regra):
        super().__init__(regra)
        self.regra = regra


def codigo_do_erro(corpo):
    """Código do erro no corpo de uma resposta 4xx ('consulta_ampla_demais', 'sem_escopo'...), ou ''."""
    try:
        dados = json.loads(corpo)
    except ValueError:
        dados = None
    if isinstance(dados, dict):
        for chave in ("erro", "error", "codigo", "code", "detail", "detalhe", "mensagem", "message"):
            valor = dados.get(chave)
            if isinstance(valor, dict):
                valor = valor.get("codigo") or valor.get("code") or valor.get("erro")
            if isinstance(valor, str) and valor.strip():
                return valor.strip()
    m = re.search(r"[a-z]+(?:_[a-z]+)+", corpo or "")
    return m.group(0) if m else ""


def normalizar(nome):
    """Nome comparável: sem acento, maiúsculas, fora de A-Z vira espaço, espaços simples."""
    t = unicodedata.normalize("NFKD", nome or "").encode("ascii", "ignore").decode().upper()
    return " ".join(re.sub(r"[^A-Z]", " ", t).split())


def data(valor):
    """date de 'dd/mm/aaaa', 'aaaa-mm-dd' (com ou sem hora) ou date; None se não der."""
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto = str(valor or "").strip()
    for formato, tamanho in (("%d/%m/%Y", 10), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(texto[:tamanho], formato).date()
        except ValueError:
            pass
    return None


def mascarar(cpf):
    """'12345678901' -> '***.456.789-**' (para log/CSV)."""
    d = re.sub(r"\D", "", cpf or "")
    return f"***.{d[3:6]}.{d[6:9]}-**" if len(d) == 11 else ""


def mascara_normal(mascara):
    """CPF tarjado da fonte -> 11 posições com '*' onde não aparece ('***.340.134-**' -> '***340134**'); None se não
    for uma máscara de CPF com pelo menos 3 dígitos visíveis."""
    m = re.sub(r"[.\-/\s]", "", str(mascara or "")).upper().replace("X", "*")
    if len(m) != 11 or not re.fullmatch(r"[0-9*]{11}", m) or "*" not in m or sum(c.isdigit() for c in m) < 3:
        return None
    return m


def casa_mascara(cpf, mascara):
    """O CPF tem os mesmos dígitos que a máscara mostra, posição a posição?"""
    return len(cpf) == 11 and all(m == "*" or m == c for c, m in zip(cpf, mascara))


def regioes_da_uf(*ufs):
    """Dígitos de região fiscal aceitos para as UFs (o 9º dígito do CPF)."""
    return {REGIAO_FISCAL[u] for u in ufs if u in REGIAO_FISCAL}


@dataclass(frozen=True)
class Resultado:
    regra: str
    cpf: str | None = None
    n_candidatos: int = 0          # CPFs distintos com o nome idêntico
    n_brutos: int = 0              # linhas devolvidas pela API (todas as páginas lidas)
    paginas: int = 0
    nascimento_api: date | None = None
    do_cache: bool = False
    candidatos: tuple = ()         # CPFs dos idênticos (até MAX_CANDIDATOS_CACHE), para a medição

    @property
    def aceito(self):
        return self.regra in ACEITAS


def decidir(registro, nascimento=None, regioes=None):
    """Aplica a regra de aceite a um registro de consulta (o que a API devolveu para um nome; com máscara, só os
    candidatos que batem com ela). Função pura. `regioes`: dígitos de região fiscal aceitos (None = qualquer)."""
    r = _decidir(registro, nascimento)
    if r.aceito and registro.get("mascara"):
        r = Resultado(**{**r.__dict__, "regra": "NOME_E_MASCARA"})
    if r.aceito and regioes and r.cpf[8] not in regioes:
        return Resultado(**{**r.__dict__, "regra": "REGIAO_DIVERGE", "cpf": None})
    return r


def _decidir(registro, nascimento=None):
    base = {"n_brutos": registro["n_brutos"], "paginas": registro["paginas"],
            "candidatos": tuple(c["cpf"] for c in registro["candidatos"])}
    unicos = {c["cpf"]: c for c in registro["candidatos"]}
    if len(unicos) >= 2:
        return Resultado("HOMONIMOS", n_candidatos=len(unicos), **base)
    if registro.get("recusada"):
        return Resultado(registro["recusada"], n_candidatos=len(unicos), **base)
    if not registro["completo"]:
        return Resultado("PAGINACAO_INCOMPLETA", n_candidatos=len(unicos), **base)
    if not unicos:
        return Resultado("SEM_CANDIDATO", **base)
    cand = next(iter(unicos.values()))
    if not documento_valido(cand["cpf"]) or len(cand["cpf"]) != 11:
        return Resultado("CPF_INVALIDO", n_candidatos=1, **base)
    nasc_api = data(cand.get("nasc"))
    nasc_fonte = data(nascimento)
    if nasc_fonte:
        if not nasc_api:
            return Resultado("NASCIMENTO_AUSENTE_NA_API", n_candidatos=1, **base)
        if nasc_api != nasc_fonte:
            return Resultado("NASCIMENTO_DIVERGE", n_candidatos=1, nascimento_api=nasc_api, **base)
        return Resultado("NOME_E_NASCIMENTO", cand["cpf"], 1, nascimento_api=nasc_api, **base)
    return Resultado("NOME_UNICO_NO_PAIS", cand["cpf"], 1, nascimento_api=nasc_api, **base)


class Limitador:
    """Intervalo mínimo entre chamadas, valendo para todas as threads do processo."""

    def __init__(self, intervalo=INTERVALO):
        self.intervalo, self._ultima, self._trava = intervalo, 0.0, threading.Lock()

    def esperar(self):
        with self._trava:
            espera = self.intervalo - (time.monotonic() - self._ultima)
            if espera > 0:
                time.sleep(espera)
            self._ultima = time.monotonic()


class Cache:
    """Resposta decisiva de cada nome (JSONL, uma linha por consulta; a última vale), para nome repetido custar uma
    consulta e a rodada poder ser retomada. Tem CPF: fica em saida/ (fora do git). Erro nunca entra."""

    def __init__(self, arquivo, dias=30):
        self.arquivo, self.dias, self._trava, self.dados = Path(arquivo), dias, threading.Lock(), {}
        if self.arquivo.exists():
            limite = datetime.now() - timedelta(days=dias)
            with open(self.arquivo, encoding="utf-8") as f:
                for linha in f:
                    try:
                        r = json.loads(linha)
                        if datetime.fromisoformat(r["em"]) >= limite:
                            self.dados[r["nome"]] = r
                    except (ValueError, KeyError, TypeError):
                        continue

    def ler(self, chave):
        return self.dados.get(chave)

    def guardar(self, registro):
        with self._trava:
            self.dados[registro["nome"]] = registro
            self.arquivo.parent.mkdir(parents=True, exist_ok=True)
            with open(self.arquivo, "a", encoding="utf-8") as f:
                f.write(json.dumps(registro, ensure_ascii=False) + "\n")


class Cliente:
    """Cliente da API. `chamadas` conta as requisições feitas (inclusive as repetidas)."""

    def __init__(self, cache=None, intervalo=INTERVALO):
        self.cache, self.limitador, self.chamadas = cache, Limitador(intervalo), 0
        self._carregar_env(override=False)

    def _carregar_env(self, override):
        load_dotenv(RAIZ / ".env", override=override)
        self.url = (os.getenv("CPF_API_URL") or "").strip().rstrip("/")
        self.token = (os.getenv("CPF_API_TOKEN") or "").strip()
        if not self.url or not self.token:
            raise ErroToken("CPF_API_URL e CPF_API_TOKEN precisam estar no .env da raiz")

    def _get(self, caminho, params):
        """JSON da resposta. 401/403 recarrega o .env uma vez (token rotacionado) antes de desistir."""
        recarregou, n_429, n_rede = False, 0, 0
        while True:
            self.limitador.esperar()
            self.chamadas += 1
            req = urllib.request.Request(f"{self.url}{caminho}?{urllib.parse.urlencode(params)}",
                                         headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                    corpo = r.read().decode("utf-8")
                try:
                    return json.loads(corpo)
                except ValueError:
                    raise ErroApi("resposta não é JSON") from None
            except urllib.error.HTTPError as e:
                try:
                    codigo = codigo_do_erro(e.read().decode("utf-8", "replace"))
                except Exception:
                    codigo = ""
                if e.code == 400:
                    if codigo in RECUSAS:
                        raise ConsultaRecusada(RECUSAS[codigo]) from None
                    raise ErroApi(f"HTTP 400 {codigo or 'requisição recusada'}") from None
                if e.code in (401, 403):
                    if not recarregou:
                        recarregou = True
                        self._carregar_env(override=True)
                        continue
                    motivo = ("token inválido, expirado ou revogado" if e.code == 401
                              else f"token sem acesso a {caminho} ({codigo or 'sem_escopo'})")
                    raise ErroToken(f"HTTP {e.code}: {motivo}; falar com o responsável pelo token") from None
                if e.code == 429:
                    if n_429 < len(ESPERAS_429):
                        time.sleep(ESPERAS_429[n_429] + random.random())
                        n_429 += 1
                        continue
                    raise ErroApi("HTTP 429 persistente (limite do token)") from None
                if e.code >= 500 and n_rede < TENTATIVAS - 1:
                    n_rede += 1
                    time.sleep(2 * n_rede)
                    continue
                raise ErroApi(f"HTTP {e.code}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                if n_rede < TENTATIVAS - 1:
                    n_rede += 1
                    time.sleep(2 * n_rede)
                    continue
                raise ErroApi(f"rede: {type(e).__name__}") from None

    @staticmethod
    def linhas(dados):
        """Os dicts com CPF de qualquer envelope ({'resultados': [...]}, {'cpfs': [...]}, lista solta...)."""
        achados = []

        def anda(x):
            if isinstance(x, dict):
                if "cpf" in x or "CPF" in x:
                    achados.append(x)
                    return
                for v in x.values():
                    anda(v)
            elif isinstance(x, list):
                for v in x:
                    anda(v)
        anda(dados)
        return [{"cpf": re.sub(r"\D", "", str(x.get("cpf") or x.get("CPF") or "")),
                 "nome": x.get("nome") or x.get("NOME") or "",
                 "nasc": x.get("nasc") or x.get("data_nascimento") or x.get("nascimento") or x.get("NASC")}
                for x in achados]

    def consultar(self, nome, mascara=None):
        """Registro da consulta de um nome: só os candidatos com o nome idêntico (e, com máscara, que batem com ela;
        até MAX_CANDIDATOS_CACHE), quantas linhas e páginas vieram e se a resposta é decisiva (lida até o fim, ou já
        com 2 candidatos). Máscara com os 3 primeiros dígitos visíveis vai pelo /v1/busca (nome + começo do CPF), que
        devolve bem menos linhas; senão, pelo /v1/cpfs (nome exato), filtrando pela máscara aqui."""
        chave = normalizar(nome)
        prefixo = re.match(r"\d+", mascara or "")
        if mascara and prefixo and len(prefixo.group(0)) >= 3:
            caminho = "/v1/busca"     # cada termo do nome com 2 letras ou mais (senão 400, que também gasta cota)
            params = {"nome": " ".join(p for p in chave.split() if len(p) >= 2), "cpf": prefixo.group(0),
                      "limit": LIMITE_PAGINA}
        else:
            caminho, params = "/v1/cpfs", {"nome": chave, "limit": LIMITE_PAGINA}
        identicos, n_brutos, paginas = {}, 0, 0
        completo = parou_cedo = False
        recusada = None
        while paginas < MAX_PAGINAS:
            try:
                dados = self._get(caminho, params)
            except ConsultaRecusada as e:
                recusada = e.regra
                break
            paginas += 1
            for linha in self.linhas(dados):
                n_brutos += 1
                if (len(linha["cpf"]) == 11 and normalizar(linha["nome"]) == chave
                        and (not mascara or casa_mascara(linha["cpf"], mascara))):
                    identicos.setdefault(linha["cpf"], {"cpf": linha["cpf"], "nasc": linha["nasc"]})
            if len(identicos) >= 2:
                parou_cedo = True
                break
            cursor = (dados.get("proximo_cursor") or dados.get("next_cursor")) if isinstance(dados, dict) else None
            if not cursor:
                completo = True
                break
            params = {**params, "cursor": cursor}
        return {"nome": f"{chave}|{mascara}" if mascara else chave, "mascara": mascara,
                "em": datetime.now().isoformat(timespec="seconds"), "completo": completo, "parou_cedo": parou_cedo,
                "recusada": recusada, "n_brutos": n_brutos, "paginas": paginas,
                "candidatos": list(identicos.values())[:MAX_CANDIDATOS_CACHE], "regra_versao": REGRA_VERSAO}

    def cpf_unico(self, nome, nascimento=None, renovar=False, so_cache=False, mascara=None, regioes=None):
        """Resultado da regra para o nome: com a máscara do CPF (se a fonte deu uma), o nascimento da fonte (se houver) e
        as regiões fiscais aceitas (`regioes_da_uf`). Usa o cache (com máscara, a chave é nome|máscara); so_cache=True
        não chama a API (fora do cache volta None)."""
        chave = normalizar(nome)
        if len(chave.split()) < 2:
            return Resultado("NOME_INVALIDO")
        if mascara is not None and str(mascara).strip():
            mascara = mascara_normal(mascara)
            if mascara is None:
                return Resultado("MASCARA_INVALIDA")
        else:
            mascara = None
        chave_cache = f"{chave}|{mascara}" if mascara else chave
        registro = None if renovar or not self.cache else self.cache.ler(chave_cache)
        if registro is not None:
            r = decidir(registro, nascimento, regioes)
            return Resultado(**{**r.__dict__, "do_cache": True})
        if so_cache:
            return None
        registro = self.consultar(chave, mascara)
        if self.cache and (registro["completo"] or registro["parou_cedo"] or registro["recusada"] in RECUSAS_NO_CACHE):
            self.cache.guardar(registro)
        return decidir(registro, nascimento, regioes)


def forma(valor):
    """Estrutura de uma resposta sem os dados (para a sondagem: chaves e tipos)."""
    if isinstance(valor, dict):
        return {k: forma(v) for k, v in list(valor.items())[:20]}
    if isinstance(valor, list):
        return [forma(valor[0]), f"... {len(valor)} itens"] if valor else []
    if isinstance(valor, str):
        return "str(digitos)" if re.fullmatch(r"[\d.\-/ ]+", valor) else "str"
    return type(valor).__name__


def main():
    ap = argparse.ArgumentParser(description="Sondagem da API de CPF (não mostra CPF inteiro).")
    ap.add_argument("--nome", required=True)
    ap.add_argument("--nascimento")
    ap.add_argument("--mascara", help="CPF tarjado da fonte, ex.: ***.340.134-**")
    ap.add_argument("--uf", help="UF do tribunal (região fiscal aceita), ex.: RJ")
    ap.add_argument("--forma", action="store_true", help="mostra a estrutura da 1ª página (chaves e tipos, sem dados)")
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    try:
        sondar(a)
    except ErroApi as e:
        raise SystemExit(f"{type(e).__name__}: {e}") from None


def sondar(a):
    cli = Cliente()
    if a.forma:
        dados = cli._get("/v1/cpfs", {"nome": normalizar(a.nome), "limit": LIMITE_PAGINA})
        print(json.dumps(forma(dados), ensure_ascii=False, indent=2))
        linhas = cli.linhas(dados)
        print(f"linhas com CPF: {len(linhas)}; formatos de nasc: "
              f"{sorted({re.sub(r'[0-9]', '9', str(x['nasc'])) for x in linhas})[:5]}")
    r = cli.cpf_unico(a.nome, a.nascimento, mascara=a.mascara, regioes=regioes_da_uf(a.uf) if a.uf else None)
    print(f"regra={r.regra} aceito={r.aceito} candidatos={r.n_candidatos} linhas={r.n_brutos} paginas={r.paginas} "
          f"cpf={mascarar(r.cpf)} chamadas={cli.chamadas}")


if __name__ == "__main__":
    main()
