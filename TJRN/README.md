# TJRN — `esteira_TJRN.py` + `fetch_TJRN.py`

Consulta, no **PJe 1º grau público** do TJRN (tribunal 120 em `creditos.tribunal`), o processo originário dos
precatórios **sem credor** e registra num CSV o credor (polo ativo), o devedor (polo passivo), os advogados e a capa.

Estado em 28/09/2026: **só consulta — nada é gravado no banco** (a sessão com o banco é somente leitura). O CSV
`TJRN/saida/leads_TJRN.csv` (e `leads_TJRN_w<N>.csv` dos outros workers) é o único registro e também o controle da
esteira. ~57,6 mil leads ativos no TJRN; ~8,3 mil entram na esteira (sem credor e com originário no PJe 1º grau).

| Arquivo | Papel |
|---|---|
| `fetch_TJRN.py` | a consulta de um processo no PJe (Chrome + Buster) e a leitura do detalhe; também roda avulso para uma lista de números |
| `esteira_TJRN.py` | pega os leads no banco em lotes, chama o `fetch_TJRN` para cada originário e escreve o CSV |

## Como rodar

```bash
python TJRN/esteira_TJRN.py                         # lotes de 10 até acabar os leads (ou o site bloquear)
python TJRN/esteira_TJRN.py --lote 10 --limite 20   # para depois de 20 leads
python TJRN/esteira_TJRN.py --workers 3             # 3 workers neste terminal (Ctrl+C para todos)
python TJRN/esteira_TJRN.py --worker 2 --total 3    # só o worker 2 de 3, em outro terminal
python TJRN/fetch_TJRN.py 0826136-40.2019.8.20.5001 [outros números] [--csv TJRN/saida/lote.csv]
```

- Abre o Google Chrome instalado **com janela** (o Akamai barra o headless). **Não minimize a janela**: o Akamai barra
  janela minimizada.
- Perfil próprio `TJRN/.chrome-profile-pje-tjrn` (ou a pasta em `CHROME_PERFIL`): guarda os cookies do Akamai e tem
  a extensão **Buster** instalada pela Chrome Web Store. O Chrome 136+ não deixa automatizar o perfil padrão.
- `.env`: `PG_*` (só a esteira usa, para ler os leads).
- Log: terminal e `TJRN/saida/logs/esteira_TJRN_AAAAMMDD.log` (`esteira_TJRN_w<N>_...` nos outros workers,
  `esteira_TJRN_workers_...` do supervisor, `fetch_TJRN_...` no uso avulso). Cada linha diz de qual worker é
  (`esteira_TJRN_w1`, `fetch_TJRN_w2`...).
- **Ctrl+C** é um pedido de parada: o worker termina o lead em andamento (que entra no CSV), fecha o Chrome e sai; um
  2º Ctrl+C sai na hora (fecha o Chrome do perfil e encerra). O driver do Playwright e o Chrome sobem ignorando o
  Ctrl+C do console (`filhos_ignoram_ctrl_c`): o Playwright síncrono não aguenta um KeyboardInterrupt no meio de uma
  chamada, e o `close()` travava para sempre.

### Vários workers (`--workers N`), todos pelo IP da máquina

- **Divisão dos leads sem mexer no banco**: o worker `i` de `N` fica com os originários em que
  `mod(hashtext(numero_cnj) + 2^31, N) = i-1`. Os leads de um mesmo originário caem sempre no mesmo worker (a consulta
  continua sendo uma só) e nunca em dois; juntas, as fatias dão todos os leads (conferido com 2, 3 e 10 workers).
- **Arquivos por worker**: worker 1 = `TJRN/.chrome-profile-pje-tjrn` e `saida/leads_TJRN.csv` (os de sempre);
  worker `i` = `TJRN/.chrome-profile-pje-tjrn-<i>` e `saida/leads_TJRN_w<i>.csv`. Na subida, cada worker lê os CSVs de
  **todos** para saber o que já foi feito (`ja_feitos`): mudar o número de workers entre execuções não repete lead.
  `saida/worker_<i>.lock` recusa um 2º processo com o mesmo número.
- **Buster em perfil novo**: na 1ª vez, o worker novo abre a página do Buster na Chrome Web Store e **espera até
  15 min você clicar em "Usar no Chrome"** (aviso no log); nas próximas execuções o perfil já tem a extensão. Não dá
  para automatizar: o Chrome 137+ não aceita `--load-extension` e marca como corrompida (`DISABLE_CORRUPTED`) a
  extensão de um perfil copiado.
- `--workers N` sobe um worker a cada 15 s. `--lote` e `--limite` valem para cada worker.
- **Risco**: todos saem pelo mesmo IP. Os proxies do `.env` não servem: saem pela Alemanha, e o TJRN bloqueia acesso
  do exterior ("Acesso Bloqueado ... usuários localizados no exterior"). Mais workers aumentam a chance de o
  reCAPTCHA recusar o áudio ou o Akamai negar o acesso: suba aos poucos e acompanhe `bloqueio` no log.

## Fluxo ponta a ponta

### 1. Leitura dos leads (`proximos_leads`, sessão somente leitura)

`SQL_PROXIMOS`: créditos do TJRN ainda na lista (`saiu_da_lista_em IS NULL`), **sem** `CREDOR`/`CESSIONARIO` em
`creditos.credito_credor` (papéis 1 e 5), com originário em `creditos.credito_originario` cujo CNJ é do PJe 1º grau
do TJRN (`J=8`, `TR=20`, origem `5xxx`/`6xxx`: regex `^\d{13}820[56]\d{3}$`). Um originário por lead (o primeiro),
mais quantos leads vivos apontam para o mesmo originário. Uma conexão nova por lote (a esteira roda por horas e uma
conexão ociosa cai). Leads já concluídos no CSV entram no `pular`.

### 2. Consulta (`fetch_TJRN.consultar`)

URL: `https://pje1gconsulta.tjrn.jus.br/consultapublica/ConsultaPublica/listView.seam`.

1. Abre a página de pesquisa e espera o formulário (a verificação do Akamai, `bm-verify`, some sozinha; até 60 s).
   "Access Denied" -> `Bloqueado`.
2. Digita o número (o campo tem máscara) e clica em Pesquisar, levando o mouse em passos até o botão (o reCAPTCHA
   pontua o movimento).
3. **reCAPTCHA invisível**: se o desafio aparecer (iframe `bframe` visível), clica uma vez no botão do **Buster**
   (resolve pelo áudio). Se o desafio seguir na tela 10 s depois (ou o botão não aparecer), recarrega a página e
   tenta de novo com desafio novo (até 3 vezes). Google recusando o áudio ("consultas automáticas",
   `.rc-doscaptcha-body`) -> `Bloqueado`, que **interrompe a esteira**.
4. Sem resultado em 20 s (sem desafio na tela) -> "sem resultado".
5. Abre o detalhe pela URL do `openPopUp` (`DetalheProcessoConsultaPublica/listView.seam?ca=...`) e lê o texto com o
   Scrapling (`linhas_da_pagina`): a capa (`Número Processo`, `Data da Distribuição`, `Classe Judicial`, `Assunto`,
   `Jurisdição`, `Órgão Julgador`, `Endereço`) e as seções "Polo ativo" e "Polo passivo". Cada linha de parte é
   `NOME - OAB RN5644 - CPF: ***.974.194-** (PAPEL)`; quem tem OAB ou papel ADVOGADO vai para os advogados.
6. Não guarda HTML nem JSON: o resultado vai só para o CSV (na esteira, `leads_TJRN.csv` / `leads_TJRN_w<N>.csv`,
   um por worker; no uso avulso, o `--csv` do lote).

Robustez: 5 a 10 s de pausa entre consultas; se o Chrome fechar no meio, reabre e repete o mesmo processo (até 3 vezes
por execução); um originário é consultado **uma vez por execução**, mesmo com vários leads.

### 3. Registro (`registrar`)

Uma linha por lead em `leads_TJRN.csv` (acrescentando; se o arquivo estiver aberto no Excel, espera fechar):
`credito_id, precatorio, ente, originario, leads_vivos_no_originario, status, hora, duracao_s, desafio,
cliques_buster, tentativas, classe, data_distribuicao, orgao_julgador, jurisdicao, polo_ativo, polo_ativo_papel,
polo_ativo_documento, advogados_polo_ativo, polo_passivo, polo_passivo_papel, polo_passivo_documento,
advogados_polo_passivo` (várias partes na mesma célula, separadas por `|`, na mesma ordem nas colunas de nome, papel e
documento).

`status`: `ok`, `sem resultado`, `erro: ...` ou `bloqueio: ...`. Na próxima execução a esteira **pula** o lead cuja
última linha é `ok` ou `sem resultado`; `erro` e `bloqueio` voltam.

Outros arquivos em `saida/`: `lote_*.csv` (uso avulso do `fetch_TJRN`), `leads_TJRN.antes_tentativas.csv` e
`padrao_bloqueio.csv` (análises feitas à mão).

### 4. Banco: o que falta para gravar

Ainda não há gravação. Pontos para quando houver (mesmas funções do TJAL/TJBA):

- O PJe público do TJRN **mascara o CPF/CNPJ** (`***.340.134-**`). `creditos.documento_de_parte` recusa documento
  com `*`, então `registrar_credor` não aceita esse credor e `registrar_capa` gravaria as partes só pelo nome.
  O CPF completo precisa vir de outra fonte (ou de casar o nome com uma pessoa que o banco já tem com documento).
- O originário já está ligado no banco (é de onde a esteira parte): a gravação seria capa + partes
  (`registrar_capa`, só advogados do polo ativo), `credito_fonte.metadata` (`registrar_credito`, conferindo antes que
  o número bate com o crédito) e o status na fila (`fila_credor_finalizar`), seguindo o padrão de simulação com
  ROLLBACK, backup e SQL de desfazer dos outros robôs.

## Tecnologias

- Python 3 (testado no 3.14; precisa do 3.10+).
- **Playwright** com o Chrome instalado (`launch_persistent_context`, `channel="chrome"`, `headless=False`, sem
  `--enable-automation` e sem `--disable-extensions`, com `--disable-blink-features=AutomationControlled`).
- Extensão **Buster** (resolve o reCAPTCHA pelo áudio) instalada no perfil do robô.
- **Scrapling** (`scrapling.parser.Selector`) para o texto do detalhe; regex para as partes.
- `psycopg2` + `python-dotenv` (leitura dos leads).
- Proteções do site: **Akamai Bot Manager** e **reCAPTCHA v2 invisível**.

## Mapa do código

| Arquivo | Funções |
|---|---|
| `fetch_TJRN.py` — navegador e reCAPTCHA | `esperar`, `clicar`, `pesquisar`, `buster_funciona`, `garantir_buster`, `abrir_detalhe` |
| `fetch_TJRN.py` — leitura do detalhe | `secao`, `extrair_partes`, `linhas_da_pagina`, `extrair_capa` |
| `fetch_TJRN.py` — consulta | `filhos_ignoram_ctrl_c`, `abrir_chrome`, `consultar` |
| `fetch_TJRN.py` — CSV e execução | `linha_csv`, `gravar_csv`, `main` |
| `esteira_TJRN.py` | `SQL_PROXIMOS`, `definir_worker`, `proximos_leads`, `arquivos_de_leads`, `ja_feitos`, `registrar`, `ler_argumentos`, `main`, `supervisionar`, `consultar_originario`, `rodar` |
| `utils/workers.py` (comum com o TJBA) | `travar_worker`, `supervisionar`, `esperar_workers`, `ParadaSuave`, `matar_chrome_do_perfil` |

## Cuidados

- Bloqueio do Google/Akamai para a esteira na hora; os leads com `bloqueio` voltam na próxima execução.
- Para rodar mais de uma esteira use `--workers N` (ou `--worker i --total N` em cada terminal, com o mesmo `N`):
  duas esteiras sem isso pegariam os mesmos leads, e duas com o mesmo número são recusadas pela trava.
