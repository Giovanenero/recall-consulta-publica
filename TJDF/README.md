# TJDF — `carregar_TJDF.py` e `fetch_TJDF.py`

Robô de credor do TJDFT (tribunal 107 em `creditos.tribunal`) pelas **fontes públicas, sem login e sem o token de
advogado**. Ele toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`), que nunca rodou no TJDFT.

No TJDFT o número do crédito (`0722180-36.2019.8.07.0000`) é o processo do **próprio precatório** no PJe 2º grau
(COORPRE), que é **sigiloso**. A lista do SAPRE tem captcha e nunca passou pelo coletor da ordem cronológica: os 37 mil
precatórios estão só na tabela antiga `listas_primarias.processos_unificados` (coletados em 04/2026, `BLOQUEIO TOTAL`),
com número, ano, prioridade, ordem, devedor e data de apresentação. Sem valor, credor, advogado nem originário.

## Como rodar

```bash
python TJDF/carregar_TJDF.py --simulacao             # cadastra tudo e dá ROLLBACK (confere a contagem)
python TJDF/carregar_TJDF.py                         # cadastra os precatórios no schema creditos (uma vez)
python TJDF/fetch_TJDF.py --simulacao                # 20 créditos da ordem: faz tudo e dá ROLLBACK
python TJDF/fetch_TJDF.py --simulacao --creditos 1,2 # só esses créditos, tudo desfeito
python TJDF/fetch_TJDF.py --limite 50                # modo real: para depois de 50 créditos
python TJDF/fetch_TJDF.py                            # modo real: roda até acabar a carga (Ctrl+C para)
python TJDF/fetch_TJDF.py --faixa NOME               # só os apresentados até 2021 (rápido, ~17 créditos/min)
python TJDF/fetch_TJDF.py --faixa INICIAIS           # só os de 2022 em diante (lento, ~2 créditos/min)
python TJDF/fetch_TJDF.py --workers 6 --ritmo 4      # padrão: 6 threads, até 4 req/s ao PJe
```

- **`.env`:** `PG_*`. O lote de gravação (créditos por transação) é a constante `LOTE` no início do robô (20).
- **Reserva (modo 5 do RPA_SISTEMAS, 07/10/2026):** cada crédito pego fica reservado em `coleta_credor` (lease) e,
  como nos outros robôs do modo 5, nas filas mensais do RPA antigo (`utils/legado.py`). Hoje o TJDFT não tem linha
  nessas filas (o legado dele é a tabela antiga `processos_unificados`), então essa reserva só vale se ele entrar
  nelas. Reserva sem dono é solta na partida e a cada 30 min.
- **Arquivos de desfazer:** só com `--com-desfazer`; o padrão é não escrever (`--sem-desfazer`, que os ciclos do modo
  5 passam, continua aceito).
- **API de CPF no robô (08/10/2026, como no TJPI e no TJRJ):** credor só com nome (fora os honorários) -> o robô
  consulta a API pelo nome (`utils/cpf_robo.py`; regra, gate `CPF_API/liberacao.json` e cache do
  `CPF_API/completar_cpf.py`). Aceito: credor ligado com o CPF e status `SUCESSO_API_TERCEIRO`; senão fica
  `SUCESSO_SEM_CPF` com `metadata.cpf_api`. O candidato por iniciais (`CREDOR_SO_POR_INICIAIS`, SUCESSO_ANALISAR)
  não é consultado: o nome não foi confirmado. `--sem-cpf-api` desliga.
- **Conexão parada:** o Postgres derruba sessão parada há mais de 15 min; depois da espera pelos adiados o robô abre
  conexões novas, e a leitura do crédito tenta de novo uma vez com conexão nova.
- **Carga (`carregar_TJDF.py`):** decisão do usuário de 05/10/2026. Para cada precatório da tabela antiga (número de
  20 dígitos com `8.07`; os de outro tribunal ficam num CSV), chama `creditos.registrar_credito` com o software
  `CONSULTA_PUBLICA_TJDFT` (`raspa_credor`): nasce o crédito, o `credito_fonte` com a lista no metadata (`lista`:
  ordem, prioridade, apresentação, entes, `id_processo` da tabela antiga) e a linha em `coleta_credor` do robô.
  Idempotente; lotes de 500 com COMMIT.
- **Pré-carga do DJEN:** na 1ª rodada o robô baixa tudo o que a COORPRE publicou no DJEN desde 01/2023 (~1,2–1,9 mil
  publicações por mês, ~15 min) para `TJDF/saida/djen_coorpre/AAAA-MM.json`; depois só o mês corrente e o anterior.
- **Rodar até acabar a carga:** com a fila vazia, espera os créditos adiados por erro passageiro (30 min) e continua.
  Se uma rodada para (fonte fora, 8 falhas técnicas seguidas, banco caiu), começa outra depois de 5 min, até 300 vezes
  (~25 h de fonte fora antes de desistir).
- **Ritmo do PJe:** 429/502/503/504/403 e conexão derrubada freiam todos os workers (metade do ritmo e pausa de 60 s).
  O HTTP 500 não freia: é defeito do próprio processo no servidor (o `/dados` de alguns processos volta 500 sempre, em
  0,1 s; ex.: o originário 0707163-66.2020.8.07.0018). O robô tenta 3 vezes com 2 s de intervalo; no `/dados` segue
  sem ele (classe e assunto vêm da busca; sem vara, comarca e data), nos outros pedidos o crédito é adiado. Antes de
  07/10, cada 500 freava todos e derrubava o ritmo para 0,3 req/s, e os créditos das INICIAIS passavam dos 8 min.
- **Simulação:** não reserva nada na fila; faz as consultas e as gravações e dá ROLLBACK no fim de cada lote.
- **Ctrl+C:** os créditos em andamento voltam para a fila e o lote já processado é gravado.
- **Log:** terminal e `TJDF/saida/logs/fetch_TJDF_AAAAMMDD.log`.

## Fontes (anônimas, sem captcha)

- **DJe do TJDFT** (`pesquisadje.tjdft.jus.br/api/v1/buscador?query=&pagina=&dataInicio=&dataFim=`; datas
  obrigatórias, 10 por página, termos entre aspas com E; conteúdo judicial até ~2024):
  - publicações do precatório até ~2021: `N. <prec> - PRECATÓRIO - A: NOME. Adv(s).: DF8583 - ADVOGADO. R: DISTRITO
    FEDERAL` e a certidão `<prec> NOME (CPF: 416.346.911-72); ADVOGADO (CPF: ...)` com o **CPF completo**
    (`"<prec>" CPF` traz esse trecho na prévia). De 2022 em diante, só os advogados.
  - pelo nome: os processos do 1º grau em que a pessoa é autora (candidatos a originário);
  - pelo advogado (`"<advogado>" "FAZENDA"`): os processos dele contra a Fazenda, com o polo ativo por extenso.
- **DJEN** (`comunicaapi.pje.jus.br`, TJDFT só de 2023 em diante): publicações da COORPRE (`orgaoId` 42759 Gabinete e
  51634 Secretaria) com o polo ativo em **iniciais** ("J. A. D. O."), advogados com OAB e, no texto, às vezes o
  primeiro nome ("SABINA N. M.") ou o nome inteiro; por OAB (`numeroOab`, `ufOab`), os processos do advogado.
- **PJe consulta pública** (`pje-consultapublica-api.tjdft.jus.br/v1` e `pje2i-…`, API REST da tela Angular):
  `/processos?numeroProcesso=` abre **até processo arquivado** (as buscas por nome, CPF e advogado não mostram
  arquivados, e o originário fica "arquivado provisoriamente" enquanto o precatório espera); `/processos/{id}/dados`,
  `/poloAtivo` (CPF completo, advogados com OAB e CPF; página base 0, 10 por página), `/poloPassivo`, `/documentos` e
  `/documentos/{id}` (texto). O precatório nunca abre (sigiloso).
- **Descartadas:** SAPRE (captcha, só a ordem), DataJud (não tem os originários do TJDFT), DJEN pelo texto (o
  originário quase nunca cita o precatório no DJEN).

## Esteira

Faixas, na ordem da fila (dentro de cada uma: prioridade de campanha, superpreferência, ordem cronológica):
**NOME** (apresentado até 2021) e **INICIAIS** (de 2022 em diante).

1. **Precatório:** DJe pelo número → nomes, CPFs, advogados com OAB; DJEN → iniciais, advogados, pistas do texto
   (primeiro nome, nome inteiro, "autos de execução n. X"). Advogado (pelo nome ou pela OAB), sociedade de advogados
   e órgão público não contam como credor; o CPF de advogado vira o CPF dele.
2. **Rota NOME** (uma pessoa com nome): DJe pelo nome → processos com ela no polo ativo (fora recursos, precatório,
   execução fiscal) → PJe pelo número → credor no polo ativo (mesmo CPF; sem CPF, mesmo nome), devedor no polo
   passivo, distribuído antes do precatório. Evidência: um **documento do originário que cita o número do
   precatório** ("Precatório distribuído na COORPRE com o número ..."), procurado nas certidões/decisões mais perto da
   apresentação. Sem candidato pelo nome, tenta os processos dos advogados com o nome inteiro.
3. **Rota INICIAIS** (só iniciais): processos dos advogados do precatório no DJEN (por OAB) e no DJe, de 540 dias
   antes a 60 dias depois da apresentação, contra a Fazenda, cujo polo ativo bate com as iniciais (e o primeiro
   nome, se o texto deu) → PJe → polo ativo inteiro → documento citando o precatório. Iniciais publicadas com CPF
   ("M. G. B. D. S. (CPF: ...)"): o CPF no polo ativo também confirma.
4. Sem publicação no DJe nem no DJEN: o crédito volta para a fila em 15 dias (não é falha).
5. Publicações sem credor identificável (nem candidato para revisar): decisão do usuário de 05/10/2026, não fica em
   `SUCESSO_ANALISAR`. Volta para a fila em **60 dias** (a COORPRE segue publicando no DJEN e o nome pode aparecer);
   na **3ª vez** vira `FALHA SEM_CREDOR`. O contador vai no motivo da fila (`[sem_credor=N]`) e sobrevive a erro
   passageiro.

**Status:**

| Situação | Status |
|---|---|
| CPF + originário (documento citando o precatório ou único processo do credor contra o ente) | `SUCESSO_PROCESSO_ORIGINARIO` |
| CPF do DJe, sem originário | `SUCESSO_PROCESSO_CREDITO` (credor confirmado na certidão do precatório) |
| Credor com CPF, originário empatado | `SUCESSO_ANALISAR CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO` (credor ligado) |
| Só o nome | `SUCESSO_INCOMPLETO SUCESSO_SEM_CPF` |
| Só iniciais, pessoa sem documento confirmando | `SUCESSO_ANALISAR CREDOR_SO_POR_INICIAIS` (candidato só no metadata; decisão do usuário, como no TJMT) |
| Várias pessoas com as iniciais / no polo ativo do precatório | `SUCESSO_ANALISAR CANDIDATOS_POR_INICIAIS` / `VARIOS_CREDORES_NO_PRECATORIO` |
| Só um advogado pessoa física no polo ativo, com CPF (DJe/PJe, ou o único documento da OAB no banco) | `SUCESSO_PROCESSO_CREDITO CREDOR_ADVOGADO … regra=HONORARIOS_ADVOGADO` (CREDOR com o CPF e ADVOGADO com a OAB; decisão do usuário de 08/10/2026, a regra do TJMA) |
| Só sociedade de advogados no polo ativo, ou o advogado sem CPF | `SUCESSO_INCOMPLETO SUCESSO_SEM_CPF … regra=HONORARIOS_ADVOGADO` (só ADVOGADO) |
| Ninguém identificado (1ª e 2ª vez) | volta para a fila em 60 dias (`CREDOR_NAO_IDENTIFICADO … [sem_credor=N]`) |
| Ninguém identificado (3ª vez) | `FALHA SEM_CREDOR` |
| Órgão público; número de outro tribunal | `FALHA` |

## Originário provável (desde 08/10/2026)

Quando o originário **não** é confirmado (e o crédito não é FALHA), o robô grava
`credito_fonte.metadata.originario_provavel`: o candidato mais provável, a **confiança** (alta/media/baixa), os pontos,
a margem para o 2º e até 5 candidatos com os sinais (escala comum dos robôs, `utils/originario_provavel.py`).
**Decisão do usuário (08/10/2026): fica só no metadata**; não liga `credito_originario` e não muda o status. Com o
originário confirmado a chave não existe (e sai, se existia).

Entram os candidatos de `motor.candidatos` e, desde 08/10/2026, os de `motor.descartados`: processos com o credor no
polo ativo que a regra do confirmado tira, mas podem ser o originário — polo passivo vazio no PJe ("Não definida",
comum no Juizado da Fazenda) ou distribuído depois da apresentação. Processo contra outra parte que não o ente não
entra. Sinais (`sinais_do_candidato`):

| Sinal | No TJDFT |
|---|---|
| `CPF_CREDOR` | rota NOME com o CPF da certidão do DJe e a pessoa do polo com documento (o robô só a aceita com o mesmo CPF); rota INICIAIS com `CPF_DJE`; ou CPF de credor que o banco já tinha de outra origem |
| `NOME_CREDOR` / `INICIAIS_CREDOR` | rota NOME pelo nome / rota INICIAIS |
| `CITA_PRECATORIO` | documento do processo cita o número do precatório |
| `ENTE_PASSIVO` | devedor no polo passivo (falta no descartado com o polo passivo vazio) |
| `ADVOGADO_EM_COMUM` | OAB de advogado do precatório no polo ativo (guardado desde 08/10/2026; antes, inferido na rota INICIAIS) |
| `CLASSE_CUMPRIMENTO`, `VARA_FAZENDA` | cumprimento/execução; Vara/Juizado da Fazenda Pública ou execução no próprio TJDFT (2º grau, competência originária) |
| `ANO_ANTERIOR` / `ANO_POSTERIOR` | ano do CNJ antes do precatório (ou do mesmo ano e distribuído antes) / depois (sai) |

**Data da migração (corrigido em 08/10/2026):** processo físico migrado para o PJe traz como `dataDistribuicao` a
data da migração (ex.: 0010789-11.2008 → 2024-05-14), e o `antes_do_precatorio()` descartava o originário certo. Agora
ele compara pela `data_do_processo()`: com o ano do número CNJ menor que o ano da `dataDistribuicao`, vale 1º/jan do
ano do CNJ (a data gravada na capa continua a do PJe). No provável, o candidato que ainda cair como distribuído depois
leva `data_migracao: true` e conta `ANO_ANTERIOR`. Teste (simulação, 27 físicos `CREDOR_SEM_ORIGINARIO`): os 4
exemplos conhecidos e 2 de 20 sorteados passaram a `SUCESSO_PROCESSO_ORIGINARIO`; credor com CPF mantido em todos.

Os gravados antes: `TJDF/reabrir_data_migracao.py` (`--simulacao`, `--com-desfazer`, `--disponivel-em`, `--liberar`)
devolve para PENDENTE os físicos (`numero_norm` 00…) em `SUCESSO_PROCESSO_CREDITO CREDOR_SEM_ORIGINARIO`, com o
prefixo `REABERTO_DATA_MIGRACAO:` no motivo; o credor ligado fica (o robô só acrescenta o originário). Rodou em
08/10/2026 17:24: 3.581 reabertos, **segurados até 31/12/2026** (o robô antigo da RECALL-0162 repetiria o defeito);
com o código novo em todas as máquinas, `python TJDF/reabrir_data_migracao.py --liberar`. Desfazer:
`TJDF/saida/desfazer_reabrir_data_migracao_20261008_172402.sql`.

Créditos processados antes de 08/10/2026: acertados pelo `recalcular_originario_provavel.py` (script de uma vez só, já rodado e removido do repositório). O
`CREDOR_SEM_ORIGINARIO` antigo não tem candidato guardado (o robô não guardava os descartados): só o reprocessamento
pelo robô o completa.

## Honorários: o advogado é o credor (desde 08/10/2026)

Decisão do usuário (a regra do TJMA de 07/10): precatório só com um advogado pessoa física no polo ativo -> CREDOR
com o CPF (certidão do DJe/PJe; sem ele, o único documento da pessoa dona da OAB no banco; dígito conferido) e
ADVOGADO com a OAB, `SUCESSO_PROCESSO_CREDITO` "CREDOR_ADVOGADO: … regra=HONORARIOS_ADVOGADO". Sem CPF, ou sociedade
de advogados (CNPJ), fica como antes (só ADVOGADO, `SUCESSO_INCOMPLETO`).

Os gravados antes: `religar_honorarios.py` (script de uma vez só, já rodado e removido do repositório). Rodou em
08/10/2026 14:37: 1.042 religados; ficaram 215 sociedades, 169 sem CPF, 37 sem vínculo ADVOGADO com OAB do mesmo
nome e 5 com sociedade entre os nomes do DJe. Desfazer: `TJDF/saida/desfazer_religar_honorarios_20261008_143715.sql`.

## Gravação

Igual ao TJRR/TJMT: lote numa transação, um SAVEPOINT por crédito; `registrar_credor` do CREDOR com CPF (a fonte
corrige o CPF divergente do banco); ADVOGADO com OAB (e CPF quando o DJe ou o PJe dão) e a sociedade dos honorários
com o CNPJ; capa recortada do originário (`registrar_capa`: só o credor, o polo passivo como REU e os advogados do
credor que estão no precatório; ação coletiva sem advogados; originário que já tem outros credores no banco não é
ligado, só o credor); `creditos.processo.metadata.capa_pje` com `fonte: pje_consultapublica_tjdft`; metadata do crédito
(`registrar_credito`, mantendo a `lista`); capa antiga (`originarios.*`) e a linha do precatório na tabela antiga
`processos_unificados` (status, motivo, originário; decisão do usuário); `fila_credor_finalizar` com
`p_sistema => 'PJE'`. Software `CONSULTA_PUBLICA_TJDFT`, criado pela carga ou na 1ª rodada real.

## Saídas (`TJDF/saida/`, fora do git)

`carga_TJDF_*.csv`, `carga_TJDF_fora_*.csv`, `fetch_TJDF.csv` (1 linha por crédito), `fetch_credores_trocados.csv`
e `djen_coorpre/`; com `--com-desfazer`, também `fetch_legado_backup.csv`, `desfazer_legado_*.sql` e
`desfazer_fila_*.sql`; na simulação, os mesmos nomes com `_simulacao`.

## Banco (05/10/2026)

37.231 precatórios do TJDFT na tabela antiga (37.200 do TJDFT; 31 de outro tribunal ou fora do padrão), 0 no schema
`creditos`. Apresentação: 21.491 até 2021 (2020 sozinho: 10.428) e 15.709 de 2022 em diante. 28 com o originário
(usados como gabarito).
