# TJRR — `fetch_TJRR.py`

Robô de credor do TJRR (tribunal 123 em `creditos.tribunal`) pelas **fontes públicas, sem login e sem o token de
advogado**. Ele toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`) nos leads do TJRR. O RPA entra no Projudi
com SSO + 2FA e mesmo assim não abre o precatório: ele é **sigiloso** (988 `SEGREDO_DE_JUSTICA` e 552
`PROCESSO_NAO_ENCONTRADO` nas tentativas do RPA).

No TJRR o número do crédito (`0805606-90.2024.8.23.0010`) é o processo do **próprio precatório** no Projudi (Núcleo de
Precatórios), não o originário. A lista traz o nº do ofício (`Autos`, ex. `2025/900096`), o `ID SGP`, a vara de
origem, o ente e os valores; cada precatório aparece em duas linhas (credor e "Honorários Advocatícios"). O
originário não vem em lugar nenhum.

## Como rodar

```bash
python TJRR/fetch_TJRR.py --simulacao                         # 20 créditos da ordem: faz tudo e dá ROLLBACK
python TJRR/fetch_TJRR.py --simulacao --limite 100            # 100 créditos da ordem, tudo desfeito
python TJRR/fetch_TJRR.py --simulacao --creditos 1232084,1231538   # só esses créditos, tudo desfeito
python TJRR/fetch_TJRR.py --limite 50                         # modo real: para depois de 50 créditos
python TJRR/fetch_TJRR.py                                     # modo real: roda até acabar a carga (Ctrl+C para)
python TJRR/fetch_TJRR.py --workers 3 --ritmo 2               # padrão: 3 threads, até 2 req/s ao Projudi
python TJRR/captcha_TJRR.py muraki                            # só testa o captcha (muraki ou projudi)
```

- **`.env`:** `PG_*`. O lote de gravação (créditos por transação) é a constante `LOTE` no início do robô (20).
- **Reserva (modo 5 do RPA_SISTEMAS, 07/10/2026):** cada crédito pego fica reservado em `coleta_credor` (lease) e, na
  mesma transação, nas filas mensais do RPA antigo (`CREDOR_EM_ANDAMENTO` com o motivo
  `CONSULTA_PUBLICA_RESERVA: <worker>`, `utils/legado.py`): o RPA com token não pega o lead. Se o RPA já está com o
  precatório numa fila mensal, o robô não o pega. A gravação troca a marca pelo resultado; devolução e falha a soltam;
  reserva sem dono é solta na partida e a cada 30 min.
- **Arquivos de desfazer:** só com `--com-desfazer`; o padrão é não escrever (`--sem-desfazer`, que os ciclos do modo
  5 passam, continua aceito).
- **API de CPF no robô (08/10/2026, como no TJPI e no TJRJ):** credor só com nome (fora os honorários) -> o robô
  consulta a API pelo nome (`utils/cpf_robo.py`; regra, gate `CPF_API/liberacao.json` e cache do
  `CPF_API/completar_cpf.py`). Aceito: credor ligado com o CPF e status `SUCESSO_API_TERCEIRO`; senão fica
  `SUCESSO_SEM_CPF` com `metadata.cpf_api` (o completar não consulta de novo). `--sem-cpf-api` desliga.
- **Conexão parada:** o Postgres derruba sessão parada há mais de 15 min; depois da espera pelos adiados o robô abre
  conexões novas, e a leitura do crédito tenta de novo uma vez com conexão nova.
- **Chrome:** o robô abre o Chrome instalado (perfis `TJRR/.chrome-profile-muraki-tjrr` e
  `.chrome-profile-projudi-tjrr`, fora do git) só para resolver os captchas; as consultas vão por HTTP.
- **Rodar até acabar a carga:** com a fila vazia, espera os créditos adiados por erro passageiro (30 min) e continua.
  Se uma rodada para (fonte fora, captcha, 8 falhas técnicas seguidas, banco caiu), começa outra depois de 5 min,
  até 30 vezes.
- **Simulação:** não reserva nada na fila; faz as consultas e as gravações e dá ROLLBACK no fim de cada lote.
- **Ctrl+C:** os créditos em andamento voltam para a fila e o lote já processado é gravado.
- **Log:** terminal e `TJRR/saida/logs/fetch_TJRR_AAAAMMDD.log`.

## Captchas (`captcha_TJRR.py`)

Os dois são o slider Tencent ("Deslize para completar o quebra-cabeça"), o mesmo do PJe do TJBA. O módulo tem uma
cópia das funções do TJBA (Chrome por CDP, as imagens `getcapbysig`, o buraco achado com OpenCV, o arrasto com
aceleração e tremor), sem importar nada do TJBA.

| | Murakî (SGP) | Projudi (consulta pública) |
|---|---|---|
| Página | `muraki.tjrr.jus.br/muraki/tem-precatorio` | `consultaprojudi.tjrr.jus.br/captcha` |
| appId | 189999713 (`TCaptcha-global.js`) | 189992716 (`TJNCaptcha-global.js`) |
| Como abre | o robô chama `new TencentCaptcha(...)` na página (o botão só habilita com o formulário) | caixa "Eu sou humano" (demora até ~25 s); o slider abre na própria página, sem iframe |
| Token | `POST /rest/captcha/verificar` → `X-Captcha-Token`, **30 min** | `randstr:ticket` lido do `Authorization` da 1ª chamada do app ao `/consilium-api`, **~8 min ou ~50 chamadas** |
| Recusa | HTTP 428 | HTTP 401 "Captcha expirado" |

O Playwright síncrono só funciona na thread que o criou: os dois Chromes vivem numa thread própria
(`ServicoCaptcha`), que entrega o token aos workers e renova sozinha (Murakî 5 min antes de vencer; Projudi a cada
45 chamadas ou 7 min). Captcha resolvido na 1ª tentativa em 13–32 s nos testes.

## Fontes

- **SGP** (`precatorios.tjrr.jus.br/rest`, sem captcha):
  - `/precatorios/pagos?pageNumber=&size=500`: todos os pagos com o **nome do credor** (`credorPrincipal`). Carregada
    uma vez por rodada (~4 mil, ~2 min).
  - `/precatorios/{ID SGP}`: juízo de origem, advogados (só nomes), situação, valor atualizado.
- **Murakî** (mesma API, captcha): `GET /precatorios/muraki/{processo}` → CPF/CNPJ **completo** de UM beneficiário e
  o valor. **valor > 0 = o credor; valor = 0 = o beneficiário dos honorários** (advogado ou sociedade).
  `?documento=<CPF>` confirma um candidato (200 com valor > 0) ou não (404). Pagos, cancelados e indeferidos: 404.
- **Projudi** (`consultaprojudi.tjrr.jus.br/consilium-api`, captcha): `/processos?cpfCnpj=|nomeParte=|nomeAdvogado=|
  oabNumero=&oabUF=` (advogado e sociedade sozinhos: "excedeu o limite"; sem filtro de data), `/processos/{cnj}`
  (polos, advogados com OAB, vara, classe, as 20 movimentações mais novas) e `/processos/{cnj}/movimentacoes`
  (10 por página). Não mostra CPF. O processo do precatório dá 404 (sigiloso).
- **DJEN:** nome do credor na "Lista de distribuição" do precatório (o TJRR só publica no DJEN de 2025 em diante) e o
  texto do originário (valor, nº do precatório) para desempatar.
  - Desde 09/10/2026 o DJEN do precatório é lido **em todo lead** (1 chamada, ≥ 1,2 s, saída direta, sem os
    PROXY_0x), também em `FORA_DO_MURAKI` e `CREDOR_SEM_NOME`. Antes era lido só com valor 0 no Murakî, e o robô
    ligava como ADVOGADO todo advogado de RR intimado, inclusive os procuradores do Estado ("Z SANDRO (SUB) …",
    "JONES POLO PASSIVO EXECUÇÕES", "GABRIEL SUBSTITUTO", Paulo Estevão Sales Cruz, Luciana Briglia).
  - **Advogados:** só os do credor (`utils/advogados_djen.py`: lado do credor, sem nome de órgão) **e** com o nome
    na lista de advogados do SGP (`guarda_da_lista`, regra `mesmo_advogado`; o SEM_LADO que bate com o SGP entra,
    porque a publicação do precatório é sigilosa e quase nunca tem rótulo). Sem a lista do SGP (precatório de 2026
    ainda sem ID SGP), o módulo sozinho aceitaria o procurador pela pista SO_CREDOR (a "Lista de distribuição" só tem
    o credor e o Estado): então fica só o advogado de credor conhecido, com o nome na lista do SGP de ≥ 2 créditos do
    TJRR (`MIN_CREDITOS_ADV_CONHECIDO`; procurador quase nunca está, advogado particular está em dezenas). Vão para o crédito com `ligar_advogados` (pela OAB); `metadata.motor.djen` guarda `advogados`,
    `advogados_fora` e a marca `advogados_completado` (via ROBO).
  - **Credor só pelo nome:** sem credor ainda (`FORA_DO_MURAKI`, ou CPF do Murakî sem nome = `CREDOR_SEM_NOME`), o
    nome da "Lista de distribuição" entra quando é um só (pessoa comum), `fonte_nome = DJEN_LISTA`. Só o nome: o CPF
    continua vindo do Murakî, da API de CPF do robô, ou fica no metadata.
  - Os já processados foram acertados pelo `utils/completar_advogados_djen.py` e pelo
    `limpar_advogados_procuradores.py` (09 e 13/10/2026; scripts de uma vez só, removidos do repositório). Desde 13/10 o advogado que vem do
    originário no Projudi passa pelo mesmo filtro de órgão (`RE_NOME_DE_ORGAO` e o "Z " do começo).

## Esteira

Faixas, na ordem da fila (dentro de cada uma: prioridade, ordem cronológica da lista, maior valor):

1. **NAO_PAGO** (Requisitado, Autuado) — Murakî:
   - valor > 0 → **CPF do credor**;
   - valor = 0 → o beneficiário dos honorários vira ADVOGADO e o credor sai de um candidato **confirmado no
     Murakî** (CPF do CONTATOS/LEGADO do crédito; ou nome do DJEN → pessoas do banco com esse nome), ou fica só
     com o nome do DJEN; sem nada → `SUCESSO_ANALISAR CREDOR_NAO_IDENTIFICADO`.
   - Com o CPF, **Projudi pela busca do CPF**: o nome do credor (o que o banco já liga ao CPF e aparece num polo;
     senão o nome comum a todos os processos achados; senão o único autor; senão o nome do DJEN) e o originário.
2. **HONORARIOS** (a lista só tem a linha "Honorários Advocatícios") — decisão do usuário de 02/10/2026, igual ao
   TJRJ: o advogado/sociedade fica **só como ADVOGADO** (com o documento do Murakî), sem CREDOR;
   `SUCESSO_INCOMPLETO` "SUCESSO_SEM_CPF … regra=HONORARIOS_ADVOGADO".
   - **Advogado pessoa física que é o próprio credor** (decisão do usuário de 08/10/2026, a regra do TJMA): CREDOR
     com o CPF do Murakî e ADVOGADO, `SUCESSO_PROCESSO_CREDITO` "CREDOR_ADVOGADO: … regra=HONORARIOS_ADVOGADO". Só
     quando o CPF é dele: Murakî com **valor > 0** (valor 0 é a linha de honorários dentro do precatório de um
     cliente: o credor é outro) e a pessoa do banco com esse CPF **tem OAB** e o nome de um advogado do precatório no
     SGP. Na base de 08/10/2026 nenhum caso passou: nessa faixa o Murakî dá, com valor > 0, o CPF do **cliente** (47
     créditos, 34 CPFs diferentes para o mesmo advogado), e com o CPF do advogado o valor é 0 (10 créditos). O robô
     antigo criou pessoas com o nome do advogado e o CPF do cliente (sem OAB). O `religar_honorarios.py`
     (script de uma vez só, já rodado e removido do repositório) aplicou a mesma conferência aos já gravados: em 08/10/2026,
     0 de 282 (124 sociedades, 100 sem CPF, 43 CPF sem OAB no banco, 10 valor 0, 5 já com credor).
3. **PAGO** — nome do SGP; Projudi pelo nome (ou pelo CPF do CONTATOS com o mesmo nome: se o Projudi mostra essa
   pessoa nos processos do CPF, o CPF vale como confirmado). CPF achado só por nome no banco é **candidato** (vai
   para o metadata, nunca é ligado; decisão do usuário).
4. **OUTRA_SITUACAO** (Suspenso, Aguardando Baixa, Pago Parcialmente, sem situação; o Murakî não mostra, ~50) —
   só o credor do banco (CONTATOS/LEGADO) confirmado pelo Projudi na busca pelo CPF; senão
   `SUCESSO_ANALISAR FORA_DO_MURAKI`. Ficam depois dos pagos (têm ordem cronológica antiga e subiriam para o topo).
5. **CANCELADO/INDEFERIDO** — FALHA definitiva (decisão do usuário).

**Originário:** processo do Projudi com o credor no polo ativo, o ente no passivo, classe que gera precatório
(cumprimento, execução, juizado; fora execução fiscal, recursos etc.) e distribuído **antes** da apresentação do
precatório. Evidências (força): nº do precatório no DJEN do processo (4), valor no DJEN (3), movimentação de
precatório entre 15 dias depois e 180 dias antes da apresentação (2), vara = vara de origem da lista/SGP (1). Vale o
mais forte com força ≥ 2 ou o único candidato. Empate: procura a expedição nas movimentações antigas dos candidatos
(busca binária pelas páginas até a data da apresentação; o detalhe só traz as 20 mais novas) e depois o DJEN.

**Originário provável (08/10/2026):** quando não confirma (empate ou sem evidência suficiente), o robô grava
`metadata.originario_provavel` com a escala comum de `utils/originario_provavel.py`: o mais provável, a confiança
(`alta`/`media`/`baixa`), os pontos, a margem para o segundo e até 5 candidatos com os sinais. Decisão do usuário: fica
**só no metadata**; não entra em `creditos.credito_originario` e não muda o status. Tradução para os sinais
(`sinais_do_candidato`): nº do precatório no DJEN → `CITA_PRECATORIO`; valor → `VALOR`; expedição perto →
`EXPEDICAO_PERTO`; vara de origem → `VARA_FAZENDA`; processos da busca pelo CPF confirmado → `CPF_CREDOR` (senão
`NOME_CREDOR`); ente da lista conferido no passivo → `ENTE_PASSIVO`; advogado do credor no processo que também está
no precatório (nome no SGP ou OAB no DJEN; não entra na força) → `ADVOGADO_EM_COMUM`; cumprimento/execução →
`CLASSE_CUMPRIMENTO`; CNJ de ano anterior ao do precatório → `ANO_ANTERIOR`. Os créditos processados antes disso
foram acertados pelo `recalcular_originario_provavel.py` (script de uma vez só, já rodado e removido do repositório). Em 08/10/2026: 808 créditos `SUCESSO_ANALISAR` ganharam o provável
(322 média, 486 baixa, nenhum alto: os guardados antes não têm o advogado em comum).

**Status:**

| Situação | Status |
|---|---|
| CPF + originário | `SUCESSO_PROCESSO_ORIGINARIO` |
| CPF, sem originário no Projudi | `SUCESSO_PROCESSO_CREDITO` (credor confirmado no próprio precatório) |
| CPF, originário empatado | `SUCESSO_ANALISAR CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO` (credor ligado) |
| CPF sem nome (coletiva) | `SUCESSO_ANALISAR CREDOR_SEM_NOME` (CPF e processos no metadata) |
| Só o nome (pagos, DJEN) | `SUCESSO_INCOMPLETO SUCESSO_SEM_CPF` (com ou sem originário) |
| Crédito só de honorários | `SUCESSO_INCOMPLETO SUCESSO_SEM_CPF … regra=HONORARIOS_ADVOGADO` |
| Só de honorários, advogado pessoa física credor (valor > 0, CPF de pessoa com OAB e nome do SGP) | `SUCESSO_PROCESSO_CREDITO CREDOR_ADVOGADO … regra=HONORARIOS_ADVOGADO` (CREDOR e ADVOGADO) |
| Honorários sem candidato; outra situação sem credor confirmado (1ª e 2ª vez) | volta para a fila em 60 dias (`CREDOR_NAO_IDENTIFICADO` / `FORA_DO_MURAKI … [sem_credor=N]`) |
| Idem, 3ª vez | `FALHA SEM_CREDOR` |
| Cancelado/indeferido; ente público | `FALHA` |

**Sem credor (decisão do usuário, 05/10/2026):** lead sem credor e sem candidato para revisar não fica em
`SUCESSO_ANALISAR` (lá ele nunca voltaria: o `fila_credor_repescar` só reabre FALHA). Volta para a fila em 60 dias — a
situação no SGP/Murakî e as publicações mudam — e, na 3ª vez, vira `FALHA SEM_CREDOR`. O contador vai no motivo da
fila (`[sem_credor=N]`) e sobrevive a erro passageiro. Os 414 que já estavam em `SUCESSO_ANALISAR` com esses motivos
foram devolvidos à fila pelo `reabrir_sem_credor.py` (script de uma vez só, já rodado e removido do repositório) (contam como a 1ª vez).

## Gravação

Igual ao TJMT: lote numa transação, um SAVEPOINT por crédito; `registrar_credor` do CREDOR com CPF (a fonte corrige
o CPF divergente do banco, só com CPF do Murakî); ADVOGADO com OAB do originário/DJEN e o beneficiário dos honorários
com o documento; capa recortada do originário (`registrar_capa`: só o credor, o polo passivo como REU e os advogados
do credor que estão no precatório; ação coletiva sem advogados; originário que já tem outros credores no banco não é
ligado, só o credor); `creditos.processo.metadata.capa_pje` com `fonte: projudi_tjrr`; metadata do crédito
(`registrar_credito`); capa antiga (`originarios.*`) e filas mensais do legado; `fila_credor_finalizar` com
`p_sistema => 'PROJUDI'`. Software `CONSULTA_PUBLICA_TJRR`, criado na 1ª rodada real.

## Saídas (`TJRR/saida/`, fora do git)

`fetch_TJRR.csv` (1 linha por crédito) e `fetch_credores_trocados.csv`; com `--com-desfazer`, também
`fetch_legado_backup.csv`, `desfazer_legado_*.sql` e `desfazer_fila_*.sql` (devolve ao RPA os leads que o robô
pegou); na simulação, os mesmos nomes com `_simulacao`.

## Banco (02/10/2026)

6.455 créditos (todos `PENDENTE` no `RPA_CREDOR_V1`): 3.659 pagos, ~2.280 não pagos vivos (2.203 "Requisitado"),
~520 cancelados/indeferidos, 195 só de honorários. 516 credores CONTATOS com CPF (507 créditos). Nenhum originário.

## Testes (02/10/2026, todos em simulação com ROLLBACK; banco conferido igual depois)

- **20 créditos variados** (6 não pagos com CPF, 5 do caso honorários, 2 só honorários, 5 pagos, 2 cancelados):
  20/20 gravados, 0 erro. Os não pagos com CPF saíram com originário; só honorários → `HONORARIOS_ADVOGADO`;
  cancelados → FALHA.
- **Empates de originário:** o detalhe do Projudi só traz as 20 movimentações mais novas; com a busca binária nas
  movimentações antigas, 3 de 5 empates viraram `SUCESSO_PROCESSO_ORIGINARIO` (VARA_ORIGEM + EXPEDICAO_PERTO).
  Processo distribuído depois da apresentação deixou de ser candidato.
- **40 primeiros da fila real** (todos NAO_PAGO, os mais antigos da ordem cronológica): 8,1 créditos/min com 3
  workers, mediana de 7 s por crédito, 6 captchas (1 Murakî, 5 Projudi), 0 resposta 429, 0 freada. 15 CPF +
  originário, 6 CPF sem originário, 6 analisar, 4 só nome, 9 FALHA "fora do Murakî" (eram Suspenso, Aguardando
  Baixa, Pago Parcialmente e sem situação: viraram a faixa OUTRA_SITUACAO; no reteste, 3 recuperados pelo CONTATOS
  confirmado no Projudi e 6 analisar, 0 FALHA). Nenhum credor removido; capas só com partes acrescentadas.
- **Estimativa da carga inteira** (6.455 créditos, 1 máquina, 3 workers): ~8/min nos não pagos, mais rápido nos
  cancelados e só honorários → ~10–14 h.
