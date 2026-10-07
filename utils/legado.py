"""
legado.py - filas do RPA antigo (listas_primarias.processos_unificados_AAAA_MM): reserva do lead e status final.

Reserva (usada pelos robôs TJAL, TJBA, TJMA, TJMT e TJRJ): enquanto o robô processa um lead, as linhas do precatório
nas filas mensais ficam em CREDOR_EM_ANDAMENTO com o motivo 'CONSULTA_PUBLICA_RESERVA: <worker>'. O modo credor do
RPA (token A3) pela fila legada só pega linha com status vazio, então não disputa o lead; a fila `creditos` já fica
reservada pela reserva do robô em creditos.coleta_credor. A marca é transitória e não entra no desfazer: a gravação
final do robô troca pelo status do resultado, e soltar_filas_mensais / limpar_reservas_orfas voltam a linha a vazio.

Também: Backup e status final para scripts que não são robô (cópia parametrizada do Backup e do
atualizar_filas_mensais do TJRJ/fetch_TJRJ.py e do atualizar_legado do TJDF/fetch_TJDF.py).
"""
import json

from utils.arquivos import anexar_csv
from utils.banco import como_dicts

COLUNAS_BACKUP = ["rodada", "modo", "credito_id", "op", "tabela", "chave", "antes"]
TABELA_ANTIGA = "listas_primarias.processos_unificados"
EM_ANDAMENTO = "CREDOR_EM_ANDAMENTO"
MARCA_RESERVA = "CONSULTA_PUBLICA_RESERVA"

# linhas do precatório numa fila mensal (o índice pu_AAAA_MM_numprec_lpad_idx acha; a 2ª condição confere o número)
SQL_LINHAS_DO_PRECATORIO = """SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead
                                FROM {fila}
                               WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                                 AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = %s
                                 AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                              creditos.so_digitos(numero_precatorio)) = %s
                                 FOR UPDATE"""


def marca_reserva(worker):
    """Motivo gravado na linha reservada: diz quem está com ela."""
    return f"{MARCA_RESERVA}: {worker}"[:500]


def numero_do_credito(cur, credito_id):
    """creditos.credito.numero_norm do crédito (a chave das linhas dele nas filas mensais), ou None."""
    cur.execute("SELECT numero_norm FROM creditos.credito WHERE id = %s", (credito_id,))
    linha = cur.fetchone()
    return linha[0] if linha else None


def reserva_de_robo(linha):
    """A linha da fila mensal está reservada por um robô de consulta pública (e não pelo RPA)?"""
    return (linha.get("status_coleta_lead") == EM_ANDAMENTO
            and (linha.get("motivo_coleta_lead") or "").startswith(MARCA_RESERVA))


def reservar_filas_mensais(cur, filas, numero_norm, tribunal_origem, worker):
    """Reserva o precatório nas filas mensais, na transação de quem chama (a mesma da reserva em coleta_credor).
    False = o RPA está processando uma das linhas (CREDOR_EM_ANDAMENTO sem a marca de robô): nada muda e quem chama
    desfaz a reserva do crédito. True = as linhas vazias ficaram em CREDOR_EM_ANDAMENTO com a marca deste worker
    (linha com resultado fica como está: o RPA não a pega)."""
    linhas = []
    for fila in filas:
        cur.execute(SQL_LINHAS_DO_PRECATORIO.format(fila=fila),
                    (numero_norm[:20].rjust(20, "0"), tribunal_origem, numero_norm))
        linhas += [(fila, linha) for linha in como_dicts(cur)]
    if any(linha["status_coleta_lead"] == EM_ANDAMENTO and not reserva_de_robo(linha) for _, linha in linhas):
        return False
    for fila, linha in linhas:
        if linha["status_coleta_lead"] is None:
            cur.execute(f"""UPDATE {fila}
                               SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                                   ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                             WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = %s""",
                        (EM_ANDAMENTO, marca_reserva(worker), linha["id_processo"], linha["numero_precatorio"],
                         tribunal_origem))
    return True


def soltar_filas_mensais(cur, filas, numero_norm, tribunal_origem, worker):
    """Volta a vazio as linhas do precatório reservadas por este worker (crédito devolvido sem resultado)."""
    n = 0
    for fila in filas:
        cur.execute(f"""UPDATE {fila}
                           SET status_coleta_lead = NULL, motivo_coleta_lead = NULL
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = %s
                           AND status_coleta_lead = %s AND motivo_coleta_lead = %s""",
                    (numero_norm[:20].rjust(20, "0"), tribunal_origem, EM_ANDAMENTO, marca_reserva(worker)))
        n += cur.rowcount
    return n


def limpar_reservas_orfas(cur, filas, tribunal_origem):
    """Solta as reservas de robô cujo worker não tem mais nenhum crédito reservado em coleta_credor (robô que caiu, ou
    crédito que terminou sem passar pela gravação das filas). Devolve quantas linhas voltaram a vazio."""
    n = 0
    for fila in filas:
        cur.execute(f"""UPDATE {fila} q
                           SET status_coleta_lead = NULL, motivo_coleta_lead = NULL
                         WHERE q.tribunal_origem = %s AND q.status_coleta_lead = %s
                           AND q.motivo_coleta_lead LIKE %s
                           AND NOT EXISTS (SELECT 1 FROM creditos.coleta_credor cc
                                            WHERE cc.status_id = 2
                                              AND cc.lease_worker = substr(q.motivo_coleta_lead, %s))""",
                    (tribunal_origem, EM_ANDAMENTO, MARCA_RESERVA + ": %", len(MARCA_RESERVA) + 3))
        n += cur.rowcount
    return n


class Backup:
    """Guarda o 'antes' de cada mudança e escreve o SQL que desfaz. Só a 1ª mudança de cada linha entra (e linha
    criada aqui só é apagada). Três níveis: o crédito em gravação (p_*), o lote aberto (l_*) e o que já teve COMMIT.
    O SQL de cada crédito roda na ordem inversa da gravação, num BEGIN...COMMIT próprio."""

    gravar_arquivos = True              # False: só controla as linhas tocadas, sem escrever o desfazer

    def __init__(self, arquivo_sql, arquivo_csv, rodada, modo, script):
        self.arquivo_sql, self.arquivo_csv, self.rodada, self.modo, self.script = (arquivo_sql, arquivo_csv, rodada,
                                                                                   modo, script)
        self.tocados, self.inseridos = set(), set()
        self.descartar_lote()

    def descartar(self):
        """Esquece o crédito em gravação (ROLLBACK TO SAVEPOINT)."""
        self.p_tocados, self.p_inseridos, self.p_sql, self.p_csv = set(), set(), [], []

    def descartar_lote(self):
        """Esquece o lote inteiro (ROLLBACK)."""
        self.l_tocados, self.l_inseridos, self.l_creditos = set(), set(), []
        self.descartar()

    def _ja_tocada(self, chave):
        return chave in self.tocados | self.l_tocados | self.p_tocados

    def _criada_aqui(self, tabela, id_):
        return (tabela, id_) in self.inseridos | self.l_inseridos | self.p_inseridos

    def update(self, cur, tabela, chave, antes):
        """Guarda o UPDATE que devolve a linha ao 'antes'."""
        k = (tabela, tuple(sorted(chave.items())))
        if not antes or self._ja_tocada(k) or self._criada_aqui(tabela, chave.get("id")):
            return
        self.p_tocados.add(k)
        onde = " AND ".join(f"{c} = %s" for c in chave)
        self.p_sql.append(cur.mogrify(f"UPDATE {tabela} SET {', '.join(f'{c} = %s' for c in antes)} WHERE {onde};",
                                      [*antes.values(), *chave.values()]).decode())
        self.p_csv.append(("UPDATE", tabela, chave, antes))

    def insert(self, cur, tabela, id_):
        """Guarda o DELETE da linha criada aqui."""
        self.p_inseridos.add((tabela, id_))
        self.p_sql.append(cur.mogrify(f"DELETE FROM {tabela} WHERE id = %s;", [id_]).decode())
        self.p_csv.append(("INSERT", tabela, {"id": id_}, {}))

    def sql(self, cur, tabela, chave, texto, params):
        """Guarda um SQL de desfazer sob medida (ex.: apagar a pessoa só se nada mais aponta para ela)."""
        self.p_sql.append(cur.mogrify(texto, params).decode())
        self.p_csv.append(("SQL", tabela, chave, {}))

    def fechar_credito(self, credito_id):
        """O crédito gravou sem erro (RELEASE SAVEPOINT): o que ele guardou passa para o lote."""
        self.l_tocados |= self.p_tocados
        self.l_inseridos |= self.p_inseridos
        if self.p_sql:
            self.l_creditos.append((credito_id, self.p_sql, self.p_csv))
        self.descartar()

    def confirmar_lote(self, manter):
        """Depois do COMMIT: escreve o SQL e o CSV de cada crédito do lote. manter=False (simulação) não escreve
        nada e não lembra as linhas tocadas."""
        if manter:
            for credito_id, sql, linhas in (self.l_creditos if self.gravar_arquivos else []):
                novo = not self.arquivo_sql.exists()
                with open(self.arquivo_sql, "a", encoding="utf-8") as f:
                    if novo:
                        f.write(f"-- Desfaz as mudanças do {self.script} (cada crédito no seu BEGIN...COMMIT; "
                                f"os créditos podem rodar em qualquer ordem).\n")
                    f.write(f"-- crédito {credito_id}\nBEGIN;\n" + "\n".join(reversed(sql)) + "\nCOMMIT;\n")
                anexar_csv(self.arquivo_csv, COLUNAS_BACKUP,
                           [{"rodada": self.rodada, "modo": self.modo, "credito_id": credito_id, "op": op,
                             "tabela": t, "chave": json.dumps(ch, default=str),
                             "antes": json.dumps(a, default=str, ensure_ascii=False)}
                            for op, t, ch, a in linhas])
            self.tocados |= self.l_tocados
            self.inseridos |= self.l_inseridos
        self.descartar_lote()


def _gravar_linhas(cur, tabela, linhas, tribunal_origem, status, motivo, bk, so_de, cont):
    """UPDATE de status/motivo nas linhas lidas (já travadas), com backup. Pula CREDOR_EM_ANDAMENTO (o RPA está nela) e
    status fora de `so_de` (None = qualquer um), para não rebaixar o que outro robô já melhorou."""
    for linha in linhas:
        if linha["status_coleta_lead"] == "CREDOR_EM_ANDAMENTO":
            cont["legado_em_andamento"] += 1
            continue
        if so_de is not None and linha["status_coleta_lead"] not in so_de:
            cont["legado_mantido"] += 1
            continue
        chave = {"id_processo": linha["id_processo"], "numero_precatorio": linha["numero_precatorio"],
                 "tribunal_origem": tribunal_origem}
        bk.update(cur, tabela, chave, {k: linha[k] for k in ("status_coleta_lead", "motivo_coleta_lead",
                                                            "ultima_atualizacao")})
        cur.execute(f"""UPDATE {tabela}
                           SET status_coleta_lead = %s, motivo_coleta_lead = %s,
                               ultima_atualizacao = (now() AT TIME ZONE 'America/Sao_Paulo')
                         WHERE id_processo = %s AND numero_precatorio = %s AND tribunal_origem = %s""",
                    (status, motivo[:500], linha["id_processo"], linha["numero_precatorio"], tribunal_origem))
        cont["legado_atualizado"] += 1


def atualizar_filas_mensais(cur, filas, numero_norm, tribunal_origem, status, motivo, bk, cont, so_de=None):
    """Status e motivo nas linhas do precatório nas filas mensais (listas_primarias.processos_unificados_AAAA_MM)."""
    for fila in filas:
        cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead, ultima_atualizacao
                          FROM {fila}
                         WHERE lpad(regexp_replace(numero_precatorio, '[^0-9]', '', 'g'), 20, '0') = %s
                           AND deleted = false AND numero_precatorio IS NOT NULL AND tribunal_origem = %s
                           AND COALESCE(creditos.cnj_normalizar(numero_precatorio),
                                        creditos.so_digitos(numero_precatorio)) = %s
                           FOR UPDATE""", (numero_norm[:20].rjust(20, "0"), tribunal_origem, numero_norm))
        _gravar_linhas(cur, fila, como_dicts(cur), tribunal_origem, status, motivo, bk, so_de, cont)


def atualizar_tabela_antiga(cur, numero_norm, tribunal_origem, status, motivo, bk, cont, so_de=None):
    """Status e motivo nas linhas do precatório na tabela antiga processos_unificados (o TJDFT só está nela)."""
    cur.execute(f"""SELECT id_processo, numero_precatorio, status_coleta_lead, motivo_coleta_lead, ultima_atualizacao
                      FROM {TABELA_ANTIGA}
                     WHERE regexp_replace(numero_precatorio, '\\D', '', 'g') = %s
                       AND tribunal_origem = %s AND deleted IS NOT TRUE
                       FOR UPDATE""", (numero_norm, tribunal_origem))
    _gravar_linhas(cur, TABELA_ANTIGA, como_dicts(cur), tribunal_origem, status, motivo, bk, so_de, cont)
