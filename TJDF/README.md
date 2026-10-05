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

- **`.env`:** `PG_*` e `LOTE_GRAVACAO_TJDF` (créditos por transação, inteiro maior que zero).
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
| Só sociedade de advogados no polo ativo | `SUCESSO_INCOMPLETO SUCESSO_SEM_CPF … regra=HONORARIOS_ADVOGADO` |
| Ninguém identificado (1ª e 2ª vez) | volta para a fila em 60 dias (`CREDOR_NAO_IDENTIFICADO … [sem_credor=N]`) |
| Ninguém identificado (3ª vez) | `FALHA SEM_CREDOR` |
| Órgão público; número de outro tribunal | `FALHA` |

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

`carga_TJDF_*.csv`, `carga_TJDF_fora_*.csv`, `fetch_TJDF.csv` (1 linha por crédito), `fetch_credores_trocados.csv`,
`fetch_legado_backup.csv`, `desfazer_legado_*.sql`, `desfazer_fila_*.sql`, `djen_coorpre/`; na simulação, os mesmos
nomes com `_simulacao`.

## Banco (05/10/2026)

37.231 precatórios do TJDFT na tabela antiga (37.200 do TJDFT; 31 de outro tribunal ou fora do padrão), 0 no schema
`creditos`. Apresentação: 21.491 até 2021 (2020 sozinho: 10.428) e 15.709 de 2022 em diante. 28 com o originário
(usados como gabarito).
