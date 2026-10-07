"""
cpf_robo.py - a API de CPF dentro do robô do tribunal (decisão do usuário, 07/10/2026): quando o robô acha o credor na
fonte pública mas sem o CPF, ele mesmo consulta a API pelo nome e, se a regra aceita, já grava o credor com o CPF, no
mesmo lote e com as mesmas marcas do CPF_API/completar_cpf.py (que continua para o estoque antigo):

- status SUCESSO_API_TERCEIRO, motivo 'CPF_API: <regra> cnj=<originário>; <motivo do robô>';
- credito_credor do CREDOR com p_fonte CPF_API_NOME e tentativa_id = a tentativa do fila_credor_finalizar, cujo
  detalhe.software é CPF_API_NOME e detalhe.robo é o software do robô (a auditoria do README do CPF_API acha os dois);
- metadata.cpf_api {regra, aceito, regra_versao, ...} no credito_fonte do robô: também é a marca de "já tentado nesta
  versão da regra" (o completar_cpf.py não consulta de novo). metadata.credor.cpf_encontrado continua false: o CPF não
  veio da fonte, e a medição do completar_cpf.py não o usa como gabarito.

A regra, o gate e o cache são os do completar_cpf.py: nome idêntico e um só CPF no país, CPF da região fiscal do
tribunal, nascimento e máscara da fonte quando houver; só consulta tribunal liberado no CPF_API/liberacao.json; o
cache é o mesmo arquivo (CPF_API/saida/cache_cpf_api.jsonl). Erro da API nunca vira resultado: o crédito fica
SUCESSO_SEM_CPF, sem a marca, e o completar_cpf.py tenta depois. Token recusado desliga a API até o fim da rodada.

O limite da API (60/min por token) é dividido com quem mais usa o mesmo token: o Limitador do cliente vale para todas
as threads do robô, mas não para outros processos.
"""
import json
import logging
import threading
from datetime import datetime

from utils.cpf_api import REGRA_VERSAO, Cache, Cliente, ErroApi, ErroToken, mascarar, regioes_da_uf
from CPF_API.completar_cpf import (ARQ_LIBERACAO, MARCA, RE_PAPEL_PULO, SAIDA as SAIDA_CPF_API, STATUS_NOVO, conflito,
                                   liberado, limpar_nome, normalizar, pulo_do_nome)

log = logging.getLogger("cpf_robo")

ERROS_PARA_DESLIGAR = 10               # erros seguidos da API que a desligam até o fim da rodada


class CpfNoRobo:
    """Consulta da API de CPF para um tribunal, compartilhada pelas threads do robô. `ativo` diz se ela está ligada
    (gate liberado, token presente e sem erro de token)."""

    def __init__(self, sigla, uf, ligar=True):
        self.sigla, self.regioes = sigla, regioes_da_uf(uf)
        self.trava, self.erros_seguidos, self.chamadas_api = threading.Lock(), 0, 0
        self.ativo, self.motivo = False, ""
        if not ligar:
            self.motivo = "desligada (--sem-cpf-api)"
            return
        try:
            lib = json.loads(ARQ_LIBERACAO.read_text(encoding="utf-8")).get(sigla)
        except (OSError, ValueError):
            lib = None
        if not liberado(sigla, lib):
            self.motivo = f"{sigla} não liberado no {ARQ_LIBERACAO.name} (regra {REGRA_VERSAO})"
            return
        try:
            self.cliente = Cliente(cache=Cache(SAIDA_CPF_API / "cache_cpf_api.jsonl"))
        except ErroToken as e:
            self.motivo = f"sem token: {e}"
            return
        self.ativo = True
        self.motivo = (f"ligada: {sigla} liberado (cobertura {lib.get('cobertura')}, erro {lib.get('taxa_erro')}), "
                       f"região fiscal {''.join(sorted(self.regioes))}")

    def consultar(self, nome, nascimento=None, mascara=None, papel=None):
        """{aceito, regra, cpf, n_candidatos, nascimento_api, nome_consultado, do_cache} para o nome, ou None quando a API
        não foi consultada (desligada ou erro: o crédito fica para o completar_cpf.py). Nome barrado pelos filtros
        volta com aceito False e a regra do pulo (também o papel da fonte que não é a própria pessoa: espólio,
        inventariante, representante...)."""
        if not self.ativo or not nome:
            return None
        papel_n = normalizar(papel or "")
        pulo = ("ESPOLIO" if "ESPOLIO" in papel_n else "PAPEL_REPRESENTANTE") if RE_PAPEL_PULO.search(papel_n) else             pulo_do_nome(nome)
        nome_api = normalizar(limpar_nome(nome))
        if pulo:
            return {"aceito": False, "regra": pulo, "cpf": None, "n_candidatos": None, "nascimento_api": None,
                    "nome_consultado": nome_api, "do_cache": False}
        try:
            r = self.cliente.cpf_unico(nome_api, nascimento or None, mascara=mascara or None, regioes=self.regioes)
        except ErroToken as e:
            with self.trava:
                if self.ativo:
                    self.ativo, self.motivo = False, f"token recusado: {e}"
                    log.error(f"API de CPF desligada até o fim da rodada: {e}")
            return None
        except ErroApi as e:
            with self.trava:
                self.erros_seguidos += 1
                if self.erros_seguidos >= ERROS_PARA_DESLIGAR and self.ativo:
                    self.ativo, self.motivo = False, f"{ERROS_PARA_DESLIGAR} erros seguidos ({e})"
                    log.error(f"API de CPF desligada até o fim da rodada: {self.motivo}")
            log.warning(f"API de CPF falhou para um nome ({e}): fica para o completar_cpf.py")
            return None
        with self.trava:
            self.erros_seguidos = 0
            self.chamadas_api = getattr(self.cliente, "chamadas", 0)
        return {"aceito": r.aceito, "regra": r.regra, "cpf": r.cpf if r.aceito else None,
                "n_candidatos": r.n_candidatos, "nascimento_api": r.nascimento_api, "nome_consultado": nome_api,
                "do_cache": r.do_cache}


def texto_csv(res):
    """A coluna cpf_api do CSV do robô: a regra da API, o CPF mascarado se ligou, ou por que não ligou."""
    if not res:
        return ""
    if res.get("ligado"):
        return f"{res['regra']} ligado {mascarar(res['cpf'])}"
    return res["regra"] + (f" não ligado: {res['nao_ligado']}" if res.get("nao_ligado") else "")


def metadata_cpf_api(res, nascimento=False, mascara=False):
    """A chave metadata.cpf_api do credito_fonte do robô (sem o CPF), no formato do completar_cpf.py."""
    if not res:
        return None
    return {"regra": res.get("nao_ligado") or res["regra"], "regra_api": res["regra"],
            "aceito": bool(res.get("ligado", res["aceito"])),
            "n_candidatos": res["n_candidatos"], "nascimento_conferido": bool(nascimento),
            "mascara_conferida": bool(mascara), "regra_versao": REGRA_VERSAO,
            "consultado_em": datetime.now().isoformat("T", "seconds"), "nome_consultado": res["nome_consultado"],
            "via": "robo"}


def ligar_credor(cur, credito_id, nome, res, processo_id):
    """Liga o CREDOR com o CPF que a API aceitou (quem chama cuida do SAVEPOINT). Devolve o id do vínculo, ou None
    quando não liga: o CPF já é de outra pessoa no banco (CONFLITO_*), é inválido, ou o crédito já tem CREDOR. Nesses
    casos res['nao_ligado'] diz o porquê e res['ligado'] fica False (res['regra'] continua a da API)."""
    res["ligado"] = False
    cur.execute("SELECT 1 FROM creditos.credito_credor WHERE credito_id = %s AND papel_id = 1 LIMIT 1", (credito_id,))
    if cur.fetchone():
        res["nao_ligado"] = "JA_TEM_CREDOR"
        return None
    motivo = conflito(cur, res["cpf"], nome, res["nascimento_api"])
    if motivo:
        res["nao_ligado"] = motivo
        return None
    cur.execute("SELECT creditos.documento_de_parte(%s)", (res["cpf"],))
    documento = cur.fetchone()[0]
    if not documento:
        res["nao_ligado"] = "CPF_INVALIDO"
        return None
    cur.execute("""SELECT creditos.registrar_credor(p_credito_id => %s, p_papel => 'CREDOR', p_nome => %s,
                                                    p_documento => %s, p_processo_id => %s, p_fonte => %s)""",
                (credito_id, limpar_nome(nome), documento, processo_id, MARCA))
    res["ligado"] = True
    return cur.fetchone()[0]


def motivo_e_detalhe(res, originario_fmt, motivo_robo, detalhe_robo, software_robo):
    """(status, motivo, detalhe) do fila_credor_finalizar quando o CPF veio da API."""
    motivo = f"CPF_API: {res['regra']} cnj={originario_fmt or '-'}; {motivo_robo}"[:2000]
    detalhe = {**detalhe_robo, "software": MARCA, "robo": software_robo, "regra_cpf_api": res["regra"],
               "regra_versao": REGRA_VERSAO, "n_candidatos": res["n_candidatos"]}
    return STATUS_NOVO, motivo, detalhe


def marcar_vinculo(cur, vinculo, tentativa):
    """credito_credor.tentativa_id = a tentativa do fila_credor_finalizar: a marca de que o CPF veio da API."""
    cur.execute("UPDATE creditos.credito_credor SET tentativa_id = %s WHERE id = %s", (tentativa, vinculo))
