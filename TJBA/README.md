# TJBA — `fetch_TJBA.py`

Robô de credor do TJBA (tribunal 105 em `creditos.tribunal`) pela consulta pública, de ponta a ponta, no lugar do
modo credor do RPA antigo (`RPA_CREDOR_V1`). Para cada precatório **sem credor** acha o **processo originário**
(pelo DJEN e pelas pistas que já estão no banco), confirma no **PJe 1º grau público** que o beneficiário está no polo
ativo e o ente no passivo, pega o **CPF/CNPJ** do credor e grava o resultado no banco **crédito a crédito**, logo
depois de processar cada um.

Estado em 28/09/2026: 1ª execução real às 10:20 (software `CONSULTA_PUBLICA_TJBA`, id 6 em `creditos.software`),
que assumiu a fila do RPA; ~39,5 mil leads ativos no TJBA, ~33,8 mil sem credor.

## Como rodar

```bash
python TJBA/fetch_TJBA.py --simulacao             # 20 créditos (ou --limite): faz tudo e dá ROLLBACK em cada um
python TJBA/fetch_TJBA.py --simulacao --limite 5
python TJBA/fetch_TJBA.py                         # assume a fila do TJBA e processa até acabar (Ctrl+C para parar)
python TJBA/fetch_TJBA.py --limite 30             # modo real, para depois de 30 créditos
python TJBA/fetch_TJBA.py --workers 4             # 4 workers neste terminal (Ctrl+C para todos)
python TJBA/fetch_TJBA.py --workers 4 --simulacao --limite 5   # teste: 4 workers, 5 créditos cada, tudo desfeito
python TJBA/fetch_TJBA.py --worker 2              # só o 2º worker, em outro terminal
```

- `.env`: `PG_*`; `PROXY_01..05` só para os workers 2 em diante (ver abaixo).
- Abre uma janela do Google Chrome instalado (perfil próprio `TJBA/.chrome-profile-pje-tjba`). Não é headless.
- A simulação não reserva nada na fila e não mexe em `coleta_credor` fora da transação desfeita; só lê uma amostra
  que alterna status e motivo.
- Ctrl+C: desfaz o que estava em gravação (ROLLBACK) e o crédito em andamento volta para a fila
  (`fila_credor_liberar`); os créditos anteriores já tiveram COMMIT.
- Log: terminal e `TJBA/saida/logs/fetch_TJBA_AAAAMMDD.log` (`fetch_TJBA_w<N>_AAAAMMDD.log` nos outros workers).
  Cada linha diz de qual worker é (`fetch_TJBA_w1`, `fetch_TJBA_w2`...).

### Vários workers na mesma máquina (`--workers N` ou `--worker N`)

- **Num terminal só: `--workers N`.** O terminal vira o supervisor: sobe os workers 1..N como processos filhos (um a
  cada 10 s, para os Chromes e o captcha não começarem juntos), mostra as linhas de todos no mesmo terminal e espera
  todos acabarem (log do supervisor em `saida/logs/fetch_TJBA_workers_AAAAMMDD.log`). `--simulacao` e `--limite`
  valem para cada worker (`--limite 5` = até 5 créditos por worker). O **Ctrl+C** chega a todos (é o mesmo console):
  cada worker desfaz o que estava gravando, devolve o crédito em andamento para a fila e sai; um **2º Ctrl+C** mata
  os que ainda não saíram (o crédito deles volta para a fila quando o lease de 45 min expirar). Worker que para
  sozinho (fila vazia, falhas seguidas) não é reiniciado; o supervisor só registra e espera os outros.
- **Um terminal por worker: `--worker N`** (1 é o padrão), se preferir acompanhar cada um separado.

Nos dois casos cada worker é um processo com um número. A fila com lease garante que dois workers nunca pegam o
mesmo crédito; o resto é separado por worker:

| | worker 1 | worker N (2 em diante) |
|---|---|---|
| perfil do Chrome | `TJBA/.chrome-profile-pje-tjba` | `TJBA/.chrome-profile-pje-tjba-<N>` (novo, sem cookies) |
| saída para o DJEN | direta | o (N-1)º `PROXY_*` do `.env` (`--worker 2` -> `PROXY_01`); sem proxy para ele, direta |
| arquivos e log | nomes de sempre | sufixo `_w<N>` |
| `assumir_fila` | sim (na subida e a cada 30 min) | não: só pega da fila |
| simulação | 1ª fatia da amostra | N-ésima fatia (não repete créditos do worker 1) |

- O DJEN limita por IP (~1,3-2 consultas/s) e cada worker faz ~0,8/s: dois workers saindo direto ficam no limite.
  Por isso o worker N usa um proxy; os proxies precisam sair pelo Brasil (o DJEN bloqueia fora do país). Proxy que
  falha 3 vezes seguidas (conexão, 403/407) é desligado e o worker passa a sair direto (aviso no log).
- O PJe continua saindo direto em todos os workers (os proxies atuais não servem: pelo PJe dão 502). Em 29/09/2026 o
  PJe do TJBA ficou instável para qualquer IP (pesquisas que não voltam, 502, tela "SISTEMA TEMPORARIAMENTE
  INDISPONÍVEL"); os workers pausam juntos nesse caso (ver "PJe instável" abaixo). Suba os workers aos poucos (3 a 5)
  e acompanhe `CAPTCHA_REATIVADO`, `PESQUISA_SEM_RESPOSTA` e `PJe instável` no log.
- `saida/worker_<N>.lock`: um 2º processo com o mesmo `--worker` é recusado na subida (usaria o mesmo perfil e os
  mesmos arquivos). O Windows solta a trava sozinho se o processo morrer.
- Chrome que ficou aberto com o perfil do worker (worker anterior que caiu ou foi morto sem fechar o navegador) é
  fechado na subida (`matar_chrome_do_perfil`, aviso no log): sem isso o Chrome novo entregava a janela para o
  antigo, a porta de depuração não abria e o worker caía. A trava acima garante que o perfil não é de outro worker
  vivo.
- Worker que cai por erro inesperado registra o traceback no próprio log (`worker parou por erro inesperado`) e
  sai com código 1; o supervisor mostra o código de cada worker no fim.
- O Chrome sobe com `--disable-backgrounding-occluded-windows`, `--disable-renderer-backgrounding` e
  `--disable-background-timer-throttling`: a janela atrás das outras não é desacelerada (senão o captcha atrasa).
- Deadlock entre as gravações de dois workers (o mesmo originário em precatórios diferentes) desfaz a gravação do
  crédito que perdeu, que volta para a fila em 5 min (`ADIADO`, `CONFLITO_DE_LOCK`), sem virar FALHA.
- Cada Chrome ocupa ~300-500 MB de memória.

## Fluxo ponta a ponta

### 0. Preparação (`Rodada`)

- Conexão de escrita (transação manual) e conexão de leitura (readonly, autocommit: nada fica preso enquanto raspa).
- `id_do_software`: cadastra `CONSULTA_PUBLICA_TJBA` com `raspa_credor = true` (no modo real).
- Filas mensais do legado com permissão de UPDATE (`processos_unificados_2026_08` em diante) e o de-para
  `status_coleta.codigo` -> `codigo_legado` (ex.: `SUCESSO_PROCESSO_ORIGINARIO` -> `CREDOR_SUCESSO_atraves_originario`).
- Modo real: `fila_credor_expirar_leases()` e **`assumir_fila`**: as linhas do TJBA em `coleta_credor` que eram do
  RPA (software 2) e não estão em lease passam para o robô; as sem credor ficam `PENDENTE`. Grava
  `desfazer_fila_<rodada>.sql` (devolve tudo ao RPA, com o status de antes). Repete a cada 30 min.
- `Pje.abrir()`: sobe o Chrome com `--remote-debugging-port` e conecta pelo Playwright (`connect_over_cdp`); guarda
  as imagens do captcha que passam pela rede.

### 1. Fila

`fila_credor_pegar(p_tribunal='TJBA', p_software='CONSULTA_PUBLICA_TJBA')` reserva 1 crédito com lease de 45 min
(o teto de 15 min por crédito + 30 min de folga para a gravação). `SQL_CREDITO` lê o lead: beneficiários (`lista_item.beneficiario_nome` e `metadata.de_beneficiario`), ente,
valores (`valor_lista`, `valor_devido`), originários já ligados, motivos antigos do RPA e o último status.

### 2. Candidatos a originário (`processar`)

- Nomes a buscar: o beneficiário da lista sem aposto, "REP. POR", "E OUTROS"; espólio com inventariante vira dois
  nomes. Sem nome -> `SEM_BENEFICIARIO`; todos órgãos públicos -> `REQTE_ORGAO_PUBLICO`.
- Fontes de candidato (só CNJ do TJBA `8.05`, origem ≠ `0000`, diferente do precatório):
  - `LIGADO`: originário já em `credito_originario` (evidência forte);
  - pistas do RPA no `motivo_detalhe`: `…:OK:<id>` (capa antiga com valor conferido, forte),
    `…:PARTES_SEM_VALOR:<id>` e `…SEM_VINCULO:<cnj>`;
  - **DJEN** (`https://comunicaapi.pje.jus.br/api/v1/comunicacao`, API pública do CNJ): busca por `nomeParte` +
    `siglaTribunal=TJBA` (até 5 páginas de 100). Candidato = processo com o beneficiário no polo A (publicação em que
    ele aparece como **advogado** não conta: seria precatório de honorários), o ente no polo P e ano ≤ ano do
    precatório. Cumprimento/execução e os mais novos primeiro. Os 15 primeiros são abertos pelo número
    (`numeroProcesso`) para ler o texto: se citam o **valor** do precatório (formato brasileiro) ou o **número** do
    precatório, a evidência é forte; guarda também as OABs.
  - Ritmo do DJEN: 1,2 s entre consultas, 6 tentativas (30 s de espera em 429), cache em memória.
- Desempate prévio (`decisivo`): 1 candidato; ou 1 com evidência forte; ou 1 com OAB em comum com as publicações do
  precatório.

### 3. Confirmação

Na ordem (forte, ligado, pista do RPA, DJEN), cada candidato é confirmado:

1. pela capa que **já está no banco** (`partes_do_banco`): beneficiário no polo ativo com CPF/CNPJ válido e ente no
   passivo — sem abrir o PJe; ou
2. pelo **PJe 1º grau** (`https://consultapublicapje.tjba.jus.br/pje/ConsultaPublica/listView.seam`): lê a capa
   (`campos_da_capa`: classe, assunto, jurisdição, órgão julgador, data) e as partes das tabelas de polo ativo e
   passivo, virando as páginas (até 50; `completa = False` se passou disso). O HTML não é guardado em disco.
   - **O captcha é por pesquisa, não por sessão** (testado em 02/10/2026): o servidor confere o ticket e recusa o
     repetido, o vazio e o inventado ("A verificação de captcha não está correta"). Não dá para reaproveitar a
     sessão como no TJRR. Mas o **link do detalhe (`?ca=`) abre sem captcha, por HTTP puro** (sessão `requests`
     nova), e as páginas de partes também (o POST AJAX do paginador, montado do próprio HTML: `detalhe_http`).
   - Por isso, no 1º candidato que precisa do PJe, o robô faz **1 pesquisa pelo nome do beneficiário**
     (`pesquisar_nome`, 1 captcha): a 1ª página traz até 30 processos da pessoa (classe, número, "polo ativo X
     polo passivo") com o link de cada um. A 2ª página pede outro captcha, então nome comum (mais de 30) fica só
     com a 1ª (aviso no log). Espólio com inventariante: o 2º nome só é pesquisado se ainda faltar candidato.
   - Candidato com link conhecido abre por HTTP (até 30 por crédito, ~0,5 s cada); o que não veio na pesquisa por
     nome é pesquisado pelo número como antes (1 captcha cada, até 10 por crédito). Os links ficam na memória do
     worker (até 50 mil); link que não abre mais sai dela e o processo é pesquisado pelo número.
   - **Nenhum candidato confirmado (ou nenhum candidato no DJEN/RPA)**: os processos da pesquisa por nome com o ente
     no polo passivo da linha, de 1º grau do TJBA e não mais novos que o precatório, viram candidatos (fonte
     `PJE_NOME`; cumprimento/execução e os mais novos primeiro) e são conferidos por HTTP com a mesma regra
     (beneficiário no polo ativo, ente no passivo). A pesquisa por nome que trava aqui adia o crédito (`ADIADO`),
     não vira FALHA; no atalho do 1º candidato o robô só segue pelo número.
   - **Captcha Tencent** (slider): o robô baixa o fundo e a peça, acha o buraco com OpenCV (bordas Canny +
     `matchTemplate`) e arrasta o slider com aceleração, tremor e leve passada do ponto. Até 4 imagens por espera;
     depois disso `CAPTCHA_REATIVADO`.
   - A linha do log mostra os acessos ao PJe do crédito: `PJe 1 nome/0 nº/3 http` (pesquisas por nome, pesquisas
     por número, detalhes por HTTP); o mesmo vai em `coleta_credor_tentativa.detalhe.acessos_pje`.

Para quando o desempate prévio é confirmado. Passou de 15 min no crédito -> `TIMEOUT_PAGINA_PROCESSO`.

### 4. Decisão

| Situação | Status / motivo | Liga o originário? |
|---|---|---|
| 1 confirmado (ou desempate por forte/OAB), credor com CPF, evidência forte | `SUCESSO_PROCESSO_ORIGINARIO` | sim |
| idem, sem evidência forte | `SUCESSO_PARTES_SEM_VALOR` | sim |
| escolhido, mas o PJe não mostra o CPF | `FALHA` / `SEM_CPF_CREDOR` | sim |
| vários confirmados sem desempate | `SUCESSO_ANALISAR` / `CREDOR_COM_DOCUMENTO_SEM_VINCULO:<cnjs>` | não (grava as capas) |
| idem, e todos são a mesma pessoa (mesmo CPF/CNPJ válido) sem candidato por conferir | `SUCESSO_ANALISAR`, regra `CREDOR_UNICO` | não, mas **liga o credor** ao crédito (`registrar_credor`, sem processo) |
| 1 candidato claro que o PJe público não deixa ler (não achado, ou achado sem link: ver abaixo) | `FALHA` / `PROC_SEM_CAPA` | sim (sem capa) |
| idem, e os processos achados pelo nome (beneficiário no ativo, ente no passivo) têm todos o mesmo CPF/CNPJ válido | `SUCESSO_ANALISAR` / `PROC_SEM_CAPA_CREDOR_OUTRO_PROCESSO`, regra `CREDOR_OUTRO_PROCESSO` | sim (sem capa), e **liga o credor** sem processo |
| nenhum confirmado | `FALHA` / `PROCESSO_NAO_ENCONTRADO` | não |

**Processo achado sem link para o detalhe** (visto em 05/10/2026, ex.: 8031939-09.2021.8.05.0001). A pesquisa traz a
linha ("1 resultados encontrados", classe, número e "1ª parte do polo ativo X 1ª do passivo"), mas sem o "Ver
detalhes" e sem a última movimentação: o detalhe não é público e não abre por outro caminho (o id interno da linha
não serve no `DetalheProcessoConsultaPublica`). Antes o robô esperava o link por 90 s, dava `PESQUISA_SEM_RESPOSTA`,
pesquisava de novo e adiava o crédito, para sempre. Agora `Pje.sem_detalhe` reconhece a resposta na hora
(`'processosGridCount'` ≥ 1 sem link) e o candidato fica `pje = SEM_DETALHE`, com a `linha` guardada em
`candidatos`. Se ele é o candidato claro (DJEN, evidência forte, ou a linha mostra o beneficiário no ativo e o ente
no passivo: `linha_confere`), vira `PROC_SEM_CAPA`; o CPF pode vir de outros processos da pessoa (linha acima). Nesse
caso os processos achados pelo nome não disputam o originário com ele.

**Erro passageiro** (captcha, PJe/DJEN fora do ar, timeout, navegador caiu — `ErroTecnico`) ou inesperado: o crédito
volta para a fila em 30 min (`fila_credor_adiar`), vira linha `ADIADO` no log e **nunca vira FALHA**. 5 falhas
técnicas seguidas (fora a instabilidade do PJe) param o robô; navegador fechado -> reabre o Chrome.

**PJe instável** (`PjeInstavel`: `PESQUISA_SEM_RESPOSTA`, `PROCESSO_NAO_CARREGOU`, `PJE_INDISPONIVEL`). O PJe do TJBA
às vezes trava pesquisas ao acaso (o captcha sai, a pesquisa não volta) ou mostra a tela "SISTEMA TEMPORARIAMENTE
INDISPONÍVEL" / 502, para qualquer IP. O robô:

1. reconhece a tela de indisponível e o HTTP 5xx na hora (`RE_PJE_FORA`, `Pje.conferir_no_ar`), sem esperar os 90 s;
2. repete uma vez a pesquisa que não voltou (`Pje.consultar(tentativas=2)`: recarrega e pesquisa de novo), porque o
   mesmo processo costuma abrir na vez seguinte; a tela de indisponível não é repetida;
3. não conta isso como falha técnica: **3 créditos seguidos** com o PJe instável viram uma **pausa de todos os
   workers da máquina**. O worker que viu vira o dono da pausa: grava `saida/pausa_pje.txt` (até quando), espera
   10 min e testa o PJe com o processo-sonda `0510492-15.2019.8.05.0001` (duas pesquisas seguidas precisam abrir).
   Voltou: apaga o arquivo, refaz a conexão com o banco se ela caiu e todos retomam. Não voltou: nova pausa de 20 min,
   depois 30 min de novo e de novo, até somar 6 h — aí o worker para, como antes. Os outros workers veem o arquivo
   antes de pegar o próximo crédito e esperam sem reservar nada (um arquivo largado por um dono que morreu vale só até
   o fim marcado + 10 min). Ctrl+C durante a pausa sai normal e apaga a marcação.

Nenhum crédito fica preso na pausa: ela acontece entre um crédito e outro, e os créditos em que o PJe falhou voltaram
para a fila (`ADIADO`) como qualquer erro passageiro.

### 5. Gravação do crédito (`gravar_credito` -> `gravar`)

Logo depois de processado, o crédito é gravado numa **transação só dele** (COMMIT; ROLLBACK na simulação). Em ordem:

1. `fila_credor_registrar_originario` (liga o originário, confere o lease) — `credito_originario`, `processo`.
2. `registrar_credor(CREDOR, nome, CPF/CNPJ, processo, fonte)` se `documento_de_parte` aceita o documento — antes do
   `registrar_capa`, para o vínculo ficar com origem FONTE, que o recálculo das partes não apaga. No `CREDOR_UNICO`
   vai sem processo (`processo_id` nulo): o credor é certo, o originário não. Por isso não se liga originário nenhum
   — o recálculo do `registrar_capa` copiaria para o crédito também os advogados do processo, que podem ser de outra
   ação da mesma pessoa.
3. Para cada capa (a escolhida e as outras confirmadas no PJe): `travar_processo` (o mesmo advisory lock do
   `registrar_capa`) e `registrar_capa`. `partes_para_banco`: capa do banco é reenviada como está; PJe completo vale
   por inteiro (CPF que faltar vem do banco pelo mesmo nome); PJe cortado só acrescenta. Só advogados do polo ativo.
4. Capa do PJe também vai para o **legado** (`gravar_capa_legado`): `originarios.processos_originarios` (preenche só
   campo vazio; `precatorio_relacionado` quando é `SUCESSO_PROCESSO_ORIGINARIO` e o precatório ainda não está em
   outro originário), `originarios.partes_processuais` e `originarios.advogados` (trocados se o PJe veio completo).
5. `registrar_metadata`: `credito_fonte.metadata` do software ganha `motor` (resultado, regra, fontes, candidatos) e
   `capa_originario`, via `registrar_credito` (conferindo antes que o número bate com o crédito).
6. `atualizar_filas_mensais`: status legado, motivo e `numero_originario` nas linhas do precatório nas filas
   mensais (pula `CREDOR_EM_ANDAMENTO`).
7. `fila_credor_finalizar` (status, motivo, via, sistema PJE, processo, `detalhe` JSON com as partes e os credores
   antes/depois) — fecha a tentativa em `coleta_credor_tentativa` e tira o lease.

Erro na gravação desfaz a transação inteira do crédito: lease de outro worker -> `LEASE_PERDIDO`; deadlock com a
gravação de outro worker -> `ADIADO` (`fila_credor_adiar` em 5 min); outro erro -> `FALHA/PERSISTENCIA_CAPA` (com
traceback no log se não for erro do Postgres). A marcação na fila (`na_fila`) roda numa transação curta própria,
depois do ROLLBACK. Só depois do COMMIT o `Backup` escreve o SQL de desfazer do legado (fora do `try`: um erro de
disco ali não vira FALHA de um crédito já gravado). 5 gravações seguidas em FALHA param o robô (banco com problema).

### 6. Saídas (`TJBA/saida/`, sufixo `_w<N>` nos workers 2 em diante)

O resultado de cada crédito fica **no banco** (`coleta_credor_tentativa`: status, motivo e `detalhe` com regra,
fontes, candidatos, partes e credores antes/depois; `credito_fonte.metadata`) e **no log**, uma linha por crédito:

```
2026-09-28 14:27:17 | INFO    | fetch_TJBA_w1 | [3] 900769 8004491-93.2023.8.05.0000 -> FALHA (PROCESSO_NAO_ENCONTRADO)   | 1 s | COMMIT
2026-09-28 14:27:19 | INFO    | fetch_TJBA_w1 | [4] 921778 8045268-52.2025.8.05.0000 -> SUCESSO_PARTES_SEM_VALOR 0322231-47.2011.8.05.0001 CPF 05628083534 | 2 s | COMMIT
```

Credor que sai de um crédito vira `WARNING` no log. Em disco, só o que serve para desfazer, e só na execução real
(a simulação dá ROLLBACK e não grava arquivo): `desfazer_legado_<rodada>.sql` (tabelas antigas, pode rodar em qualquer
ordem) e `desfazer_fila_<rodada>.sql` (a tomada da fila do RPA pelo worker 1). Mais `logs/` e `worker_<N>.lock`.

## Tecnologias

- Python 3 (testado no 3.14), **Windows** (`winreg` acha o Chrome, `taskkill` fecha a árvore de processos).
- Google Chrome instalado controlado por **CDP** (`subprocess` + `--remote-debugging-port`) e **Playwright**
  (`connect_over_cdp`); usa **Patchright** no lugar se estiver instalado.
- **OpenCV** (`opencv-python-headless`) + **NumPy** para o captcha Tencent.
- `requests` para o DJEN; regex sobre o HTML do PJe (sem parser de HTML).
- `psycopg2` e `python-dotenv`. Postgres: `creditos.fila_credor_*`, `registrar_credito`, `registrar_capa`,
  `registrar_credor`, `documento_de_parte`; legado `listas_primarias.*` e `originarios.*`.

## Mapa do código

| Seção | Funções / classes |
|---|---|
| utilidades | `normal`, `chave_nome`, `nomes_para_buscar`, `chaves_ente`, `formatos_valor`, `valor_numerico`, `data_br`, `fmt_credores` |
| DJEN | `proxies_do_env`, `saida_djen`, `Djen` (`buscar`, `por_nome`, `por_numero`), `candidatos_djen`, `oabs_do_precatorio` |
| PJe | `localizar_chrome`, `porta_livre`, `esperar_cdp`, `partes_da_pagina`, `campos_da_capa`, `captcha_aberto`, `achar_buraco`, `arrastar_slider`, `Pje` (`abrir`, `fechar`, `reabrir`, `conferir_no_ar`, `esperar`, `consultar`, `pesquisar`, `ler_detalhe`) |
| banco: leitura | `conectar`, `SQL_CREDITO`, `ler_credito`, `pistas_do_rpa`, `credores_do_credito`, `confirmar` |
| decisão | `processar` |
| gravação de um crédito | `Backup`, `id_do_software`, `filas_antigas`, `partes_para_banco`, `capa_para_banco`, `travar_processo`, `gravar_capa_legado`, `atualizar_filas_mensais`, `registrar_metadata`, `gravar` |
| gravação de cada crédito | `desfazer_transacao`, `na_fila`, `marcar_falha`, `adiar_por_conflito`, `gravar_credito`, `registrar_credito` |
| fila | `assumir_fila`, `pegar`, `devolver`, `amostra_simulacao` |
| pausa por PJe instável | `pausa_ate`, `marcar_pausa`, `tirar_pausa`, `pje_voltou`, `reconectar_banco`, `pausar_por_instabilidade`, `pausa_de_outro`, `esperar_pausa_de_outro` |
| execução | `Rodada`, `proximo_credito`, `processar_credito`, `encerrar`, `travar_worker`, `definir_worker`, `sufixo_worker`, `ler_argumentos`, `main`, `rodar` |
| vários workers num terminal | `supervisionar`, `esperar_workers` |

## Cuidados

- A 1ª execução real **tira do RPA** a fila do TJBA (`assumir_fila`). Para voltar atrás, rode o
  `desfazer_fila_<rodada>.sql` gerado.
- Uma instância por perfil de Chrome: para rodar mais de uma, use `--worker N` (perfil próprio por worker; o mesmo
  número duas vezes é recusado).
- Queda de conexão no meio da gravação desfaz só o crédito em andamento; se nem a devolução para a fila der certo,
  ele volta sozinho quando o lease (45 min) expira.
