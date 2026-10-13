# TJMT — `fetch_TJMT.py`

Robô de credor do TJMT (tribunal 111 em `creditos.tribunal`) pela **consulta pública, sem login e sem o token A3**.
Ele toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`) nos leads do TJMT. O RPA não resolve o TJMT: os
precatórios são **sigilosos** no PJe e 12 mil tentativas dele deram `PROCESSO_NAO_ENCONTRADO`.

A lista do TJMT (no banco) traz o nº do precatório (CNJ `….8.11.0000`), o ente, o valor requisitado, a data de envio
e a situação. Não traz o beneficiário nem o originário. Para cada precatório o robô:
1. pega no **DJEN** as **iniciais** do credor (`E. J. S.`) e os **advogados** (de um índice baixado em lote: ver
   "Escala");
2. lista, na **consulta processual do TJMT**, os processos desses advogados contra o ente e acha o autor com as
   mesmas iniciais, já com **nome, CPF/CNPJ completo** e o nº do originário;
3. confirma o originário pelo **DJEN do candidato** (espelho do precatório perto da data de envio, valor exato ou nº
   do precatório citado);
4. grava em lotes, numa transação por lote, com um SAVEPOINT por crédito.

## Como rodar

```bash
python TJMT/fetch_TJMT.py --simulacao                       # 20 créditos da ordem: faz tudo e dá ROLLBACK
python TJMT/fetch_TJMT.py --simulacao --limite 100          # 100 créditos da ordem, tudo desfeito
python TJMT/fetch_TJMT.py --simulacao --creditos 1224478,1225907   # só esses créditos, tudo desfeito
python TJMT/fetch_TJMT.py --limite 50                       # modo real: para depois de 50 créditos
python TJMT/fetch_TJMT.py                                   # modo real: roda até acabar a carga (Ctrl+C para)
python TJMT/fetch_TJMT.py --workers 4 --ritmo 4             # padrão: 4 threads, até 4 req/s à consulta do TJMT
```

**Rodar até acabar a carga:** com a fila vazia, o robô espera os créditos adiados por erro passageiro (30 min) e
continua; termina quando só sobram os sem publicação (voltam em 15 dias). Se uma rodada para (TJMT ou DJEN fora,
8 falhas técnicas seguidas, banco caiu, erro inesperado), começa outra depois de 5 min, até 30 vezes. Conexão de
escrita que cai no meio de um lote: reconecta e grava o lote de novo. O Postgres derruba sessão parada há mais de
15 min (`idle_session_timeout`): depois da espera pelos adiados o robô abre conexões novas, e a leitura do crédito
tenta de novo uma vez com conexão nova.

**Página quebrada na consulta do TJMT:** algumas listas (ex.: advogada VALDELICY MARIA MONTEIRO sem filtro de ente)
têm um registro cujo nome quebra a pesquisa do próprio servidor (HTTP 400 `Invalid pattern '*renato aparecido
ferreira'`). Não passa sozinho; o robô refaz a página em pedaços de 10 e de 1 e pula só esse registro (aviso no log
"registro N pulado").

- **`.env`:** `PG_*`. O lote de gravação (créditos por transação) é a constante `LOTE` no início do robô (20).
- **Reserva:** cada lead pego fica reservado também nas filas mensais do RPA antigo (`CREDOR_EM_ANDAMENTO` com o
  motivo `CONSULTA_PUBLICA_RESERVA: <worker>`), para o RPA com token A3 não o pegar; a gravação troca a marca pelo
  resultado e a devolução (erro passageiro, Ctrl+C) a solta. Ver `utils/legado.py`.
- **Ritmo:** DJEN a 0,8 s por consulta (sobe sozinho com 429; o limite é por IP e dividido com os outros robôs da
  máquina). A consulta do TJMT tem ritmo global que freia sozinho (403, 429, 502-504, timeout: metade do ritmo e
  60 s de pausa) e sobe 10% a cada 200 respostas boas.
- **Cache:** a lista de processos de cada advogado (+ ente) fica na memória (até 300 listas): o mesmo advogado
  aparece em muitos precatórios, e a 1ª lista de um advogado grande leva dezenas de páginas.
- **Simulação:** não reserva nada na fila; faz as consultas e as gravações e dá ROLLBACK no fim de cada lote.
- **Ctrl+C:** os créditos em andamento voltam para a fila (`fila_credor_liberar`) e o lote já processado é gravado.
- **Log:** terminal e `TJMT/saida/logs/fetch_TJMT_AAAAMMDD.log`.

## Escala

- **DJEN em lote:** na partida, o robô baixa tudo o que a Presidência do TJMT publicou (`siglaTribunal=TJMT&
  orgaoId=31259`, só PRECATÓRIO) mês a mês desde 01/2022 e monta um índice `{precatório: publicações}`. Em 01/10/2026:
  57.634 publicações de 28.012 precatórios. Cada mês fica em `saida/djen_presidencia/AAAA-MM.json`; a 1ª carga levou
  38 min, depois só o mês corrente e o anterior são baixados de novo. Isso troca 1 consulta por lead (~17 mil) por
  ~600 páginas. Precatório fora do índice: 1 consulta direta; sem nada, adiado sem gastar mais.
- **Proxies:** os `PROXY_01..05` do `.env` saem pela Alemanha: o DJEN responde 403 (bloqueio geográfico) e a
  consulta do TJMT recusa a conexão. Não usar. Para ir mais rápido, só uma 2ª saída no Brasil (outra máquina ou VM
  em região do Brasil) com `--proxies` ou rodando lá; o gargalo que sobra é o DJEN dos candidatos (1 a 8 por lead).
- **Velocidade medida:** 7 a 14 leads/min com 4 workers e o índice pronto (cresce com o cache de advogados);
  17,3 mil leads ≈ 20 a 40 h.

## Fila

- **Pega:** leads do TJMT em PENDENTE do RPA (software 2), os do RPA sem credor em qualquer status (menos
  EM_ANDAMENTO) e os PENDENTE do próprio robô, com `disponivel_em` vencido.
- **Ordem:** não pagos (Situação `Aguardando Pagamento`, `Pagamento Preferencial`, `Autuado`, `Provisionado`) →
  o resto (pago em parte, em quitação, suspenso...). Dentro da faixa: prioridade de campanha, sem credor antes de com
  credor, maior valor. Refeita a cada 30 minutos.
- **Reserva:** um lead por vez, com lease (`reservar`). Nessa hora ele passa para `CONSULTA_PUBLICA_TJMT` (criado
  na 1ª execução real). `saida/desfazer_fila_<rodada>.sql` devolve ao RPA, com o status de antes, cada lead pego.

## Fontes (todas públicas e anônimas)

| Fonte | Para quê | O que vem |
|---|---|---|
| **DJEN** `comunicaapi.pje.jus.br/api/v1/comunicacao?numeroProcesso=<precatório>` | Quem é o credor | Polo A só em **iniciais** (com as partículas: `E. J. D. S.`), às vezes o nome inteiro; advogados com nome e OAB. Texto: "Processo sigiloso" |
| **Consulta processual do TJMT** `hellsgate.tjmt.jus.br/consultaprocessual/ProcessosJudiciais/v2` | Achar o originário e o CPF | Pesquisa por `NomeOab` (nome do advogado), `parteNome`, `parteCpfCnpj`, `numeroUnico` (combináveis, sem acento, `Take` ≤ 60). Partes com **CPF/CNPJ completo**, papel, data de nascimento; advogados com CPF e OAB; classe, órgão, comarca, valor da causa. Não lista os precatórios sigilosos |
| **Consulta processual**, precatório antigo (`0…`, até 2019) | Credor dos antigos | Nome inteiro do credor e o advogado (sem CPF) |
| **DJEN** do candidato a originário | Evidência | Texto das decisões: "formulário (espelho) do precatório", "Expeça-se o precatório", valores |

Não servem: o portal `precatorios.tjmt.jus.br` (em manutenção em 01/10/2026; a API dele responde 401), o filtro de
OAB do DJEN (ignora o TJMT, que escreve a OAB como `12027-O`) e o detalhe do processo na consulta (pede token).

## Decisão

Candidato = processo de um advogado do precatório, com o ente no polo passivo (pesquisa `parteNome`; sem nenhum, o
advogado sozinho com um ente público no passivo), classe que gera precatório (fora criminal, recursos, execução
fiscal...) e uma parte do polo ativo que casa com o credor publicado. Casamento: nome inteiro > iniciais iguais >
iniciais com um `D` a mais no DJEN (`R. R. D. C.` = RAUL RAMOS CORTES); fica só o melhor nível.

Evidência no DJEN do candidato (até 3 por pessoa e 8 por crédito), por força: **nº do precatório** citado (4);
**valor requisitado** exato ou certidão do **espelho do precatório** entre 200 dias antes e 20 depois do envio (3);
"Expeça-se o precatório" nessa janela (2, às vezes é texto padrão); espelho/expedição fora da janela (1). Confirma
com força ≥ 2. Precatório com preferência por **idade**: candidato que, pela data de nascimento da consulta, tinha
menos de 60 anos no envio sai.

| Situação | Status | O que grava |
|---|---|---|
| 1 pessoa (ou a única entre várias) com evidência, com CPF/CNPJ | `SUCESSO_PROCESSO_ORIGINARIO` | originário, credor, capa recortada |
| idem, sem documento na consulta | `SUCESSO_INCOMPLETO` (`SUCESSO_SEM_CPF: …`) | originário e capa; nome no metadata |
| precatório antigo: 1 pessoa pelo nome inteiro, sem evidência, 1 processo só | `SUCESSO_PARTES_SEM_VALOR` | originário, credor, capa |
| credor certo (evidência ou nome inteiro), mas com 2+ processos igualmente prováveis | `SUCESSO_ANALISAR` (`CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO: …`) | credor (com CPF), sem originário |
| 1 pessoa só pelas iniciais, sem evidência | `SUCESSO_ANALISAR` (`CREDOR_SO_POR_INICIAIS: …`) | só o metadata (`candidato_analisar`: nome, documento, originário) |
| várias pessoas sem desempate | `SUCESSO_ANALISAR` (`CANDIDATOS_POR_INICIAIS: …`) | só o metadata (candidatos) |
| credor é ente público | `FALHA` (`REQTE_ORGAO_PUBLICO`) | metadata |
| nenhum candidato / sem advogado no DJEN (1ª e 2ª vez) | repete a busca na hora sem cache; vazio de novo, volta para a fila em 7 dias (`PROCESSO_NAO_ENCONTRADO: … [nao_encontrado=N]`) | nada |
| nenhum candidato / sem advogado no DJEN (3ª vez) | `FALHA` (`PROCESSO_NAO_ENCONTRADO … [nao_encontrado=3]`) | metadata |
| sem publicação no DJEN | volta para a fila em 15 dias (`AINDA_SEM_PUBLICACAO`) | nada |

### Não encontrado (desde 09/10/2026, regra do TJMA/TJRJ)
`PROCESSO_NAO_ENCONTRADO` não é final de cara: o robô repete a busca uma vez na hora (sem o cache das listas dos
advogados e do DJEN dos candidatos); se continuar vazio, o crédito volta para a fila em 7 dias
(`ADIAMENTO_NAO_ENCONTRADO`) com o contador `[nao_encontrado=N]` no motivo; só na 3ª vez
(`MAX_TENTATIVAS_NAO_ENCONTRADO`) grava `FALHA`. Não conta como falha técnica (não para a rodada).
Os `FALHA PROCESSO_NAO_ENCONTRADO` gravados antes (sem credor ligado) voltaram para a fila em 09/10/2026 pelo
`reabrir_nao_encontrado.py` (script de uma vez só, já rodado e removido do repositório); desfazer em `TJMT/saida/desfazer_reabrir_nao_encontrado_<data>.sql`.

Consulta pelo nº do precatório (hellsgate `numeroUnico`, com e sem `ExibirArquivados`), teste de 09/10/2026 em 20
`FALHA PROCESSO_NAO_ENCONTRADO` + 20 `CREDOR_SO_POR_INICIAIS`: só os 2 precatórios antigos (00…/01…, sistema legado)
vieram (1 com CPF do credor); os 38 do PJe (1…) não vêm (sigilosos). O robô já consulta o antigo pelo número
(`do_legado`); não há etapa nova.

Honorários (credor = sociedade de advogados, com CNPJ) seguem a mesma regra, com "(honorários: sociedade de
advogados)" no motivo.

### Honorários de advogado pessoa física (desde 08/10/2026)

Regra do TJMA/TJDFT (`advogado_credor`): o polo A do DJEN tem **uma pessoa só**, em iniciais de **3 letras ou mais**
que batem (com as partículas) com **exatamente um** advogado do precatório — ou o polo A só traz esse advogado com o
nome inteiro (antes virava `SEM_BENEFICIARIO "polo ativo vazio"`). O credor é o próprio advogado:

| Situação | Status | O que grava |
|---|---|---|
| com CPF (do processo dele na consulta, ou ao lado da OAB) | `SUCESSO_PROCESSO_CREDITO` (`CREDOR_ADVOGADO: … regra=HONORARIOS_ADVOGADO beneficiario=… oab=UFnnn`) | CREDOR com CPF + ADVOGADO com OAB |
| idem, com evidência num processo dele | `SUCESSO_PROCESSO_ORIGINARIO` (mesmo motivo) | + originário e capa |
| sem CPF | `SUCESSO_INCOMPLETO` (`SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO …`) | só ADVOGADO |

Outra pessoa com as mesmas iniciais nos processos dos advogados (parente: GIULLEVERSON/GIULLIANO SILVA QUINTEIRO DE
ALMEIDA, RONALDO/ROSIMEIRE QUEIROZ GARCIA) segue a decisão de sempre: com evidência, fica ela; sem, `SUCESSO_ANALISAR`.
Precatório antigo (legado) e sociedade de advogados: como antes. Polo A com 2+ pessoas (cessionário, ex.: J. R. R. =
JOSE RICARDO REZEK) não entra.

Os já processados: `religar_honorarios.py` (script de uma vez só, já rodado e removido do repositório). Rodado em 08/10/2026: 611 créditos ganharam o advogado como CREDOR com CPF, 24 ficaram só com
ADVOGADO (sem CPF), 273 que já tinham o advogado como credor ganharam só a marca; 55 de fora (46 outra pessoa com as
iniciais, 9 CPFs conflitantes). Desfazer: `TJMT/saida/desfazer_religar_honorarios_20261008_155727.sql`.

## Originário provável (desde 08/10/2026)

Quando o originário **não** é confirmado (`SUCESSO_ANALISAR`: `CREDOR_SO_POR_INICIAIS`, `CANDIDATOS_POR_INICIAIS`,
`CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO`), o robô grava `credito_fonte.metadata.originario_provavel`: o candidato mais
provável, a **confiança** (alta/media/baixa), os pontos, a margem para o 2º e até 5 candidatos com os sinais. A escala
é a comum dos robôs (`utils/originario_provavel.py`). **Decisão do usuário (08/10/2026): fica só no metadata**; não
liga `credito_originario` e não muda o status. Com o originário confirmado a chave não existe (e sai, se existia).

Os sinais saem do que o robô já guardou de cada candidato (`sinais_do_candidato`):

| Sinal | No TJMT |
|---|---|
| `CPF_CREDOR` | documento da parte = CPF do credor confirmado pelo robô (originário ambíguo) ou de um credor que o banco já tinha de outra origem (CONTATOS, LEGADO) |
| `NOME_CREDOR` / `INICIAIS_CREDOR` | casamento 3 (nome inteiro) / 1-2 (iniciais) |
| `CITA_PRECATORIO`, `VALOR` | evidência `NUM_PRECATORIO` / `VALOR` no DJEN do candidato |
| `EXPEDICAO_PERTO` | `ESPELHO_PERTO` ou `EXPEDICAO_PERTO` (as "longe" não contam) |
| `ENTE_PASSIVO` | achado na pesquisa com o ente (fonte `ADVOGADO`, ou `NOME` com o ente) |
| `ADVOGADO_EM_COMUM` | achado nos processos de um advogado do precatório (fonte `ADVOGADO`/`ADVOGADO_SEM_ENTE`) |
| `CLASSE_CUMPRIMENTO`, `VARA_FAZENDA` | classe de cumprimento/execução; órgão da Fazenda Pública (o órgão é guardado desde 08/10/2026) |
| `ANO_ANTERIOR` / `ANO_POSTERIOR` | ano do CNJ antes / depois do ano do precatório (o posterior sai) |

Créditos processados antes de 08/10/2026: acertados pelo `recalcular_originario_provavel.py` (script de uma vez só, já rodado e removido do repositório). Validação de 08/10/2026 nos
9.735 créditos com originário confirmado, escondendo a confirmação: com o credor certo e a evidência escondida
(o caso do `CREDOR_CONFIRMADO_ORIGINARIO_AMBIGUO`), nos 4.918 com 2+ candidatos, **alta acerta 90,8%**, media 91,2%,
baixa 39,2%; escondendo também o credor (o caso das iniciais), alta cai para 54,5% (112 casos, quase todos de
precatório antigo achado pelo nome) — por isso "alta" sem o CPF do credor merece conferência.

## Gravação

- **Credor:** `registrar_credor` (origem FONTE) e a regra "a fonte corrige o banco": vínculo de CREDOR da mesma
  pessoa com outro documento é apagado (com o INSERT que o recria em `desfazer_legado_*.sql`), e o CPF da capa antiga
  do precatório é trocado.
- **Data de nascimento** do credor (da consulta) em `creditos.pessoa.data_nascimento`, só quando está vazia.
- **Advogados do precatório** (DJEN): em todo lead processado, mesmo sem credor, `registrar_credor` papel ADVOGADO
  com a OAB (o banco normaliza `12027-O` para `12027`) e o CPF quando a consulta mostra a mesma OAB com o mesmo nome.
- **Capa do originário (`registrar_capa` → `creditos.processo`):** `classe_nome`, `classe_codigo` (ex. 12078),
  `orgao_julgador`, `grau`, `segredo_justica`, `sistema` (PJe). Partes recortadas: só o credor (papel AUTOR/
  EXEQUENTE...; RECONVINTE e outros viram AUTOR, com o texto em `papel_bruto`), o polo passivo (como `REU`) e os
  advogados do credor que também estão no precatório, com CPF; só acrescenta ao que o banco já tem. Gravar todos os
  autores faria o recálculo do banco ligar cada um ao precatório como credor; em processo coletivo (2+ autores ou
  2+ créditos ligados) vai sem advogados, que o recálculo espalharia para todos os créditos.
- **Ação coletiva que já está no banco:** se o originário já tem no banco outros credores (autores vindos de
  raspagens antigas) e nenhum outro crédito ligado, o robô liga só o credor e **não** liga o originário nem grava a
  capa (motivo começa com `ORIGINARIO_COLETIVO_NAO_LIGADO`). Ligar faria o `registrar_capa` → `recalcular_credores_do_processo`
  ligar todos os autores como credores deste crédito (aconteceu em 5 créditos na carga de 01–02/10/2026: 42 vínculos,
  desfeitos com o `corrigir_coletivos.py` (script de uma vez só, já rodado e removido do repositório), que também tira o precatório de `precatorio_relacionado` na capa antiga,
  senão a sincronização do legado religa o originário).
- **`creditos.processo.metadata`:** `capa_pje` no formato dos outros robôs (`fonte: consulta_tjmt`, `campos`:
  comarca, órgão, classe, data da distribuição, valor da causa, arquivado; `partes`) e `dataAjuizamento` (a data mais
  antiga do número: processo migrado para o PJe traz a da migração). Só acrescenta; backup do anterior.
- **Legado:** a mesma capa recortada em `originarios.*` (origem `TJMT`) e o status nas filas mensais
  `listas_primarias.processos_unificados_AAAA_MM` (de 2026_08 em diante), com backup e `desfazer_legado_*.sql`.
- **Metadata** (`credito_fonte` do software): decisão, candidatos com evidência (e o órgão, desde 08/10/2026),
  iniciais e advogados do DJEN, capa do originário, situação da lista e, sem originário confirmado,
  `originario_provavel`. O texto do DJEN não é guardado.

## Saídas (`TJMT/saida/`, fora do git)

`fetch_TJMT.csv` (1 linha por crédito; `_simulacao` na simulação), `fetch_credores_trocados.csv` e `logs/`; com
`--com-desfazer`, também `fetch_legado_backup.csv`, `desfazer_legado_<rodada>.sql` e `desfazer_fila_<rodada>.sql`
(desde 07/10/2026 o padrão é não escrever o desfazer; `--sem-desfazer`, que os ciclos do modo 5 passam, continua
aceito).

## Testes (01/10/2026)

- **Simulação nos 60 sorteados:** 34 originário, 3 nome inteiro, 9 analisar, 6 falha, 8 sem publicação; 0 remoções;
  91 advogados do precatório ligados (quase todos com CPF), 29 datas de nascimento, 37 capas com metadata.
- **Auditoria por um agente independente** (20 créditos novos, numa transação desfeita, conferidos contra a API do
  TJMT e o DJEN): credor, advogados, colunas da capa, partes, nascimento, metadata e legado corretos. Apontou 5
  defeitos, todos corrigidos: originário escolhido pela ordem quando a pessoa tem vários processos; data da migração
  no lugar do ajuizamento; papel RECONVINTE/ESPÓLIO virando OUTRO; CPF de advogado perdido por grafia do nome;
  parada no 1º originário com evidência fraca.
- **Gravação real de 50 leads** (COMMIT, 3,7 min): 18 originário, 9 analisar, 23 falha (17 delas são um lote de
  precatórios de 01/2026 do mesmo escritório, cujo originário não aparece na consulta pública: provável ação coletiva
  com substituídos). **2ª auditoria** (só leitura, contra as fontes): credor, originário, capa, evidência, nascimento,
  desfazer e ausência de efeito colateral corretos. Defeito achado e corrigido: a OAB da consulta vinha como MT quando
  escrita `147427 RJ` ou `107.016/RJ` (2 vínculos errados de advogado no crédito 1213273, apagados; o SQL que os
  recria está em `saida/desfazer_correcao_oab_1213273.sql`). Também: `papel_bruto` não acumula mais o prefixo do polo
  quando a capa é regravada, e o robô pesquisa até 6 advogados por precatório (o DJEN os lista em ordem qualquer).
- **Limites conhecidos:** precatório com mais de um beneficiário no DJEN (co-exequente, FIDC cessionário) liga só o
  credor achado; credor que só aparece em apelação (polo invertido) não é achado.

## Medição (01/10/2026, 60 leads sorteados, antes do robô)

Credor em 45 (75%), 44 com CPF/CNPJ: 32 com evidência no originário, 13 só pelas iniciais; 1 ambíguo; 6 sem
candidato; 8 sem publicação no DJEN. Nos 3 casos em que o banco já tinha CPF (CONTATOS), o da fonte bateu.
Tempo com 1 worker: média 37 s, mediana 28 s por lead.
