# CPF_API — `completar_cpf.py`

Completa o **CPF do credor pelo nome**, usando a API interna de CPFs (a mesma do modo 5 do RPA_SISTEMAS), nos
créditos em que o robô do tribunal achou o credor na fonte pública mas **sem o CPF**. Não é robô de tribunal: só lê o
que os robôs já gravaram.

**Dentro dos robôs (07/10/2026):** o TJPI e o TJRJ consultam a API no próprio processamento, pelo `utils/cpf_robo.py`
(mesma regra, mesmo gate deste `liberacao.json`, mesmo cache `saida/cache_cpf_api.jsonl` e as mesmas marcas: status
`SUCESSO_API_TERCEIRO`, tentativa com `detalhe.software = 'CPF_API_NOME'` e `detalhe.robo`, `credito_credor.tentativa_id`
e `metadata.cpf_api`, com `via: "robo"`). O que o robô tenta e não aceita já fica com `metadata.cpf_api` e não volta
aqui. Este script continua para o estoque antigo e para os robôs que ainda não consultam. O limite da API (60/min)
continua dividido: o robô e este script na mesma máquina somam as consultas.

| Tribunal | O que os robôs deixaram | Créditos (06/10/2026) | Consultáveis | Nomes distintos |
|---|---|---|---|---|
| TJRJ | `SUCESSO_INCOMPLETO` `SUCESSO_SEM_CPF` (DCP não mostra CPF) | 41.565 | 37.842 | 33.521 |
| TJBA | `FALHA` `SEM_CPF_CREDOR` (PJe sem CPF do credor) | 976 | 859 | 807 |
| TJDFT | `SUCESSO_INCOMPLETO` `SUCESSO_SEM_CPF` (DJe só com o nome) | 852 | 783 | 779 |
| TJRR | `SUCESSO_INCOMPLETO` `SUCESSO_SEM_CPF` | 96 | 93 | 92 |
| TJMT | `SUCESSO_INCOMPLETO` `SUCESSO_SEM_CPF` | 31 | 20 | 20 |
| TJMA | `FALHA` `SEM_CPF_CREDOR` | 12 | 11 | 11 |
| TJPI | `SUCESSO_INCOMPLETO` `SUCESSO_SEM_CPF` (DJEN só com o nome; robô novo em 07/10/2026) | 0 | 0 | 0 |

Ficam de fora:
- honorários (motivo com `HONORARIOS`);
- crédito que já tem algum CREDOR ligado;
- no TJRJ, os `SUCESSO_ANALISAR`: são cessão ou herdeiro, e o autor pode não ser mais o credor;
- os nomes barrados pelos filtros (ver abaixo).

## Pré-requisito

`.env` da raiz, sem valores no `.env.shared`:
- `CPF_API_URL`: hoje `https://recall-0127-1.tail796cdb.ts.net`.
- `CPF_API_TOKEN`: Bearer. O token rotaciona: se a API devolver 401, o script relê o `.env` uma vez antes de parar.

**Limites da API** (documentação da base_cpfs, 06/10/2026):
- 60 consultas/min por token, em token bucket: rajada de 60, depois 1 por segundo.
- Até 200 linhas por página; o valor que valeu vem em `limite_aplicado`.
- O limite é **dividido com quem mais usar o mesmo token** (por exemplo, as rotinas do TJMG no RPA_SISTEMAS): não
  rodar em paralelo. Uma instância por máquina (`saida/worker_cpf_api.lock`), 1 consulta a cada 1,05 s.
- Toda requisição fica na auditoria da API, inclusive as recusadas.
- O token vai só no header `Authorization`: nunca em URL, log, print, repositório ou mensagem de erro.

## Como rodar

```bash
python -m utils.cpf_api --nome "MARIA DA SILVA" --forma          # sondagem: estrutura da resposta, sem CPF inteiro
python CPF_API/completar_cpf.py --medir 500                       # mede a regra em 500 nomes do gabarito de cada tribunal
python CPF_API/completar_cpf.py --medir 2287 --tribunal TJRJ      # gabarito inteiro de um tribunal
python CPF_API/completar_cpf.py --simulacao --tribunal TJMT       # faz tudo e dá ROLLBACK em cada lote
python CPF_API/completar_cpf.py --tribunal TJMT                   # GRAVA (avisa e espera 10 s: Ctrl+C cancela)
python CPF_API/completar_cpf.py --tribunal TJRJ --limite 2000     # grava em fatias
```

Opções:

| Opção | O que faz |
|---|---|
| `--creditos 1,2` | só esses créditos |
| `--lote 50` | créditos por transação |
| `--cache-dias 30` | validade do cache |
| `--renovar-cache` | consulta de novo nomes que já estão no cache |
| `--so-cache` | não chama a API |
| `--refazer` | tenta de novo os créditos já tentados nesta versão da regra |
| `--sem-espera` | grava sem os 10 s de aviso (é como o modo 5 chama) |
| `--sem-desfazer` | não escreve `desfazer_*.sql`/`backup_*.csv` (o modo 5 passa sempre, como nos outros tribunais do modo 5) |
| `--forcar` | grava sem a medição liberar (só por decisão do usuário) |

**A gravação só roda para tribunal liberado** em `CPF_API/liberacao.json`. O `--medir` escreve esse arquivo, que vai
junto com o código (é o que o modo 5 do RPA_SISTEMAS lê). Sem liberação, a gravação sai com rc 3. Liberar exige
(decisão do usuário de 06/10/2026):
- a mesma versão da regra (`REGRA_VERSAO` em `utils/cpf_api.py`);
- pelo menos **200 aceitos** na medição (`MIN_ACEITOS_LIBERAR`);
- **CPF errado em no máximo 1% dos aceitos** (`MAX_TAXA_ERRO`). O TJRJ tem teto de 2,5%, liberado pelo usuário em
  06/10/2026 (medido: 7 errados em 308 aceitos, 2,3%).

## No modo 5 do RPA_SISTEMAS

Uma cópia deste script e de `utils/cpf_api.py` fica em `RPA_SISTEMAS/COLETA/teste publico/`. O `ciclo_<trib>.py` do
TJBA, TJMA, TJMT e TJRJ roda o robô e depois `completar_cpf.py --tribunal <T> --limite <até 300> --sem-espera
--sem-desfazer` (no modo 5 não fica arquivo de desfazer, como nos outros tribunais do modo 5):
- a fila que o orquestrador conta é a do robô mais os créditos sem CPF **ainda não tentados nesta versão da regra**
  (`_SQL_MODO5_CPF` no `main.py`, igual ao estoque daqui);
- com a fila do robô vazia, só o CPF roda;
- sem `CPF_API_URL`/`CPF_API_TOKEN` no `COLETA/.env.local`, a etapa é pulada;
- o rc do completar não vira o do ciclo.

O TJAL fica de fora, porque o robô dele não guarda o nome do credor sem CPF.

**Ao mudar a regra:**
1. subir `REGRA_VERSAO`;
2. rodar `--medir 500`;
3. copiar `utils/cpf_api.py`, `completar_cpf.py` e `liberacao.json` para o RPA;
4. trocar a versão no `_SQL_MODO5_CPF` (o teste `test_modo5_ciclo_publico` confere).

Com a versão nova, todo o estoque volta a ser tentado.

## Regra de aceite (decisão do usuário de 06/10/2026)

`utils/cpf_api.py`, `Cliente.cpf_unico`. O `resolver()` do RPA **não** é usado, porque aceita um candidato único sem
conferir o nome e trata erro de rede como "não achou".

1. `GET /v1/cpfs?nome=<nome normalizado>&limit=200`, seguindo o `proximo_cursor` até o fim (até 30 páginas).
   - **Com CPF tarjado na fonte** (`metadata.credor.cpf_mascarado`, ex. `123.***.***-45`, capturado pelo TJRJ na
     certidão e no PJe) e pelo menos 3 dígitos iniciais visíveis, a consulta é `GET /v1/busca?nome=&cpf=<prefixo>`.
   - Em todos os casos, só fica o candidato cujo CPF bate com **todos** os dígitos visíveis da máscara.
2. Só conta o candidato cujo nome, normalizado, é **idêntico**. A normalização tira acento, deixa em maiúsculas,
   troca o que não é A-Z por espaço e deixa espaços simples.
3. **Aceita só com UM CPF assim no país inteiro**, com o 9º dígito na **região fiscal do tribunal**
   (`Adaptador.uf`: MT=1, RR=2, MA=3, BA=5, RJ=7, DF=1):
   - `NOME_UNICO_NO_PAIS`;
   - com a data de nascimento da fonte (TJRJ, TJMT), ela também tem de bater: `NOME_E_NASCIMENTO`;
   - com a máscara da fonte: `NOME_E_MASCARA`.
4. **Rejeita**:
   - `REGIAO_DIVERGE`: o único CPF é de outra região fiscal (é a regra "nome + região", que corta o erro pela metade e
     perde ~12% da cobertura);
   - `MASCARA_INVALIDA`: a máscara tem menos de 3 dígitos visíveis;
   - `SEM_CANDIDATO`;
   - `HOMONIMOS` (para na 1ª página que mostrar 2 CPFs);
   - `PAGINACAO_INCOMPLETA`;
   - `NASCIMENTO_DIVERGE`;
   - `NASCIMENTO_AUSENTE_NA_API`;
   - `CPF_INVALIDO`;
   - `CONSULTA_AMPLA_DEMAIS` (400 da API: o nome casa resultados demais, que na prática é homônimo; vai para o cache);
   - `CONSULTA_DEMORADA` (400 da API: passou de 15 s; fica fora do cache para tentar de novo outro dia).
5. A região fiscal filtra, mas não desempata: com 2 CPFs do mesmo nome, é `HOMONIMOS` mesmo se só um for da região.
6. **Erro da API nunca vira "não achou".**
   - Repetições: 429 até 3 vezes (2, 4 e 8 s); rede, timeout (30 s, porque a API desiste sozinha em 15 s) e 5xx até 4
     vezes.
   - O 400 nunca é repetido (também gasta cota). `cursor_invalido` é erro.
   - Nada é gravado nem guardado no cache por causa de um erro.
   - 5 erros seguidos pausam 5 min; 3 pausas ou um token recusado param a rodada. O token é recusado com 401
     (inválido, expirado ou revogado) ou com `403 sem_escopo`.

**Conferências na gravação** (o crédito não é gravado e fica como estava):
- o CPF já existe no banco com outro nome: `CONFLITO_NOME_CPF` (o mesmo critério do `corrigir_credor` dos robôs);
- o CPF já existe com outra data de nascimento: `CONFLITO_NASCIMENTO_CPF`;
- o crédito mudou na fila desde a leitura: `MUDOU_NA_FILA`;
- o crédito ganhou um credor nesse meio-tempo: `JA_TEM_CREDOR`.

## Filtros de nome

O nome barrado não é consultado e vai para o CSV como `PULADO` com o motivo:

| Motivo | Quando |
|---|---|
| `ESPOLIO` | no nome, no papel ou no beneficiário da lista (TJBA/TJMA) |
| `PAPEL_REPRESENTANTE` | administrador judicial, inventariante, curador, consórcio |
| `REPRESENTADO` | "rep. por", "representado" |
| `E_OUTROS` | "e outros" |
| `NOME_SOCIAL` | "registrado(a) civilmente como" |
| `SOCIEDADE_ADV` | sociedade de advogados |
| `ORGAO_PUBLICO` | ente público |
| `PJ_SEM_CNPJ` | LTDA, associação, sindicato… (a API é só de CPF) |
| `NOME_CURTO` | menos de 2 palavras que não sejam partícula |
| `NOME_ABREVIADO` | uma letra só, ou terminação JR/FO… |
| `NOME_NAO_CONFIRMADO` / `VARIOS_NOMES` / `CREDOR_NAO_POR_NOME` | TJBA/TJMA: o credor do robô não é uma única parte ativa sem CPF |

O aposto do fim, como "(MENOR)", é tirado antes da consulta.

## De onde vem o nome

- **TJRJ, TJDFT, TJMT, TJRR e TJPI:**
  - nome em `credito_fonte.metadata.credor.nome`, gravado pelo robô;
  - nascimento em `metadata.credor.data_nascimento` (`dd/mm/aaaa` no TJRJ, `aaaa-mm-dd` no TJMT);
  - originário em `metadata.motor.originario`.
- **TJBA e TJMA:** a **única** parte ATIVA sem CPF, em `coleta_credor_tentativa.detalhe.partes` da última tentativa,
  cujo nome bate com o beneficiário.
  - No TJBA, o beneficiário é o da lista (`lista_item`).
  - No TJMA, é o credor do DJEN (`metadata.motor.djen.credor`). Exige também que o robô tenha confirmado o credor
    pelo nome (`credor_por = NOME`).
- O originário só é ligado ao credor se já estiver em `credito_originario`.

## O que é gravado

Cada lote é uma transação, com um SAVEPOINT por crédito. Para cada crédito aceito:

1. `creditos.registrar_credor(CREDOR, nome da fonte, CPF, processo do originário)`.
   - Não mexe em `processo_parte` nem nas capas antigas: CPF da API não é dado do processo.
2. `credito_fonte.metadata.cpf_api` do software do robô: `{regra, aceito: true, regra_versao, consultado_em,
   n_candidatos, nascimento_conferido, mascara_conferida, nome_consultado}`. É um UPDATE só dessa chave; o resto do JSON
   fica.
3. `CREDOR_SUCESSO_API_TERCEIRO` nas filas do RPA antigo.
   - TJRJ, TJBA, TJMA, TJMT, TJRR e TJPI: as mensais de 2026_08 em diante com permissão de UPDATE. A 2026_10 não tem
     permissão e é pulada, como nos robôs.
   - TJDFT: a tabela antiga `processos_unificados`.
   - Só troca a linha que está vazia ou com o status que o robô gravou; não rebaixa o que outro robô já melhorou.
4. `fila_credor_finalizar`:
   - status `SUCESSO_API_TERCEIRO`, motivo `CPF_API: <regra> cnj=<originário>`, via `NOME`, worker `cpf_api_<trib>`;
   - `detalhe.software = 'CPF_API_NOME'`. `SUCESSO_API_TERCEIRO` também é usado pelo TJAL (SAPRE) e por outro
     sistema; o que separa os créditos deste script é esse campo.
5. `credito_credor.tentativa_id` = a tentativa do passo 4. É a **marca de que o CPF veio da API**, porque
   `credito_credor.origem` não tem um valor próprio para isso.

**Crédito tentado e não aceito** fica só com a marca `metadata.cpf_api = {regra, aceito: false, regra_versao, ...}`.
O status e o credor não mudam, e a marca entra no desfazer. Isso vale para:
- os rejeitados pela regra;
- os pulados pelo nome;
- `CONFLITO_NOME_CPF`, `CONFLITO_NASCIMENTO_CPF` e `CPF_INVALIDO`.

A rodada seguinte da mesma versão da regra não os consulta de novo (`--refazer` força), e é assim que a fila do
modo 5 esvazia. Erro da API, `OCUPADO` e erro de gravação não ganham a marca e voltam na próxima rodada.

Auditoria dos vínculos que vieram daqui:

```sql
SELECT k.credito_id, p.nome, t.detalhe->>'regra' AS regra, t.finalizada_em
  FROM creditos.credito_credor k
  JOIN creditos.pessoa p ON p.id = k.pessoa_id
  JOIN creditos.coleta_credor_tentativa t ON t.id = k.tentativa_id
 WHERE t.detalhe->>'software' = 'CPF_API_NOME';
```

**A fonte vence a API.** Os robôs não pegam de novo um `SUCESSO_API_TERCEIRO`. Se um robô reprocessar o crédito
(`--creditos`) e achar o CPF na fonte, o `corrigir_credor` dele apaga o vínculo da mesma pessoa com outro documento.
Isso vale para TJMA, TJMT, TJRJ, TJRR, TJDF e TJPI. **O TJBA não tem `corrigir_credor`**: lá podem ficar 2 CREDOR, e
reconciliar fica para depois.

## Saídas (`CPF_API/saida/`, fora do git)

O **CPF nunca vai** para o log, o CSV, o motivo ou o detalhe. Ele fica no banco e no cache; no CSV aparece só
mascarado, como `***.456.789-**`.

| Arquivo | O que é |
|---|---|
| `completar_cpf.csv` / `completar_cpf_simulacao.csv` | 1 linha por crédito: resultado (`GRAVADO`, `SIMULADO`, `REJEITADO`, `PULADO`, `ERRO_API`, `ERRO_GRAVACAO`), regra, candidatos, legado, credores antes e depois |
| `desfazer_<rodada>_<TRIB>.sql` + `backup_<rodada>_<TRIB>.csv` | só na gravação; cada crédito no seu `BEGIN…COMMIT` |
| `medicao_<rodada>.csv` | 1 linha por nome do gabarito: resultado, regra, palavras, faixa de ano do originário, se o CPF certo estava entre os candidatos |
| `medicao_resumo.json` | cobertura, precisão, erros e chamadas por tribunal (o resumo para ler; quem libera é o `CPF_API/liberacao.json`, que fica no git) |
| `cache_cpf_api.jsonl` | **tem CPF**; resposta decisiva de cada nome por 30 dias, para nome repetido custar 1 consulta e a rodada poder ser retomada |
| `logs/completar_cpf_AAAAMMDD.log` | log da rodada |

O desfazer de cada crédito, na ordem:
- devolve a fila e apaga a tentativa;
- volta o legado;
- tira a chave `cpf_api`;
- apaga o vínculo e, se mais nada aponta para ela, a pessoa criada.

**Retomada:** a rodada seguinte só vê o que falta, e o cache evita consultar de novo. Ctrl+C termina o crédito em
andamento e grava o lote.

## Medição (`--medir N`)

O **gabarito** são os créditos cujo CREDOR com CPF veio da própria fonte (`credito_credor.origem = 'FONTE'`), com o
nome como a fonte escreveu:
- **TJRJ, TJDFT, TJMT e TJRR:** `metadata.credor` com `cpf_encontrado`;
- **TJBA e TJMA:** a parte ATIVA da tentativa com o mesmo documento;
- **TJPI** (`Adaptador.gabarito = "LEGADO"`, `SQL_GABARITO_LEGADO`): o robô quase nunca vê CPF no DJEN, então o
  gabarito é o crédito ativo com um só CREDOR com CPF, ligado pela fonte do RPA (`origem` FONTE ou LEGADO: o PJe lido
  com A3 e a correção do legado de 05/10/2026, conferida no PDPJ 13/13), com o nome da pessoa.

A amostra tem um crédito por nome, com semente fixa (`--semente`), e passa pelos mesmos filtros de nome.

| Tribunal | Gabarito (créditos) | Nomes elegíveis |
|---|---|---|
| TJDFT | 13.497 | 12.921 |
| TJMT | 8.955 | 8.278 |
| TJMA | 9.054 | 8.451 |
| TJBA | 5.589 | 5.265 |
| TJRJ | 2.571 | 2.287 |
| TJRR | 626 | 535 |
| TJPI | 4.181 | 3.771 |

**TJPI, 07/10/2026 (regra 2026-10-06.2, 500 nomes):** aceitos 206 (cobertura 41,2%), acertos 204, **erros 2
(0,97%, liberado no limite de 1%)**; rejeitados: homônimos 210, sem candidato 67, região diferente 17. Os 2 erros
(créditos 409882 e 406046, `NOME_UNICO_NO_PAIS`) podem ser o CPF do gabarito fora da base da API (ver Sondagem).

**Classificação:**
- `ACERTO`: aceito e o CPF é o do gabarito;
- **`ERRO`**: aceito e o CPF é outro;
- `REJEITADO`, com a regra;
- `API_ERRO`: fica fora das contas.

O log traz também estratos por número de palavras do nome e por faixa de ano do originário. O gabarito é mais novo
(PJe) que o estoque (DCP/físico, no TJRJ), então o risco de homônimo pode ser maior no estoque.

**Custo:** cerca de 1 consulta por nome (homônimo para na 1ª página). 500 nomes por tribunal ≈ 9 min de API. O estoque
inteiro ≈ 35,2 mil nomes ≈ 10 h, das quais ~9,8 h no TJRJ.

## Sondagem (06/10/2026, 15 consultas)

Feita com `python -m utils.cpf_api --nome "<nome>" --forma` e nomes do gabarito:
- **Envelope:** `{"resultados": [{"cpf", "nome", "sexo", "nasc"}], "limite_aplicado": 200, "proximo_cursor": str|null,
  "request_id"}`. O `nasc` vem em `dd/mm/aaaa` ou vazio.
- **Nome que não existe:** 200 com `resultados` vazio (não 404).
- **Nome comum** ("MARIA DA SILVA"): 200 linhas idênticas e cursor, sem `consulta_ampla_demais`. O cliente para na 1ª
  página (HOMONIMOS), ou seja, 1 consulta.
- **Caixa e acento:** a busca não diferencia maiúsculas, e a base guarda os nomes sem acento. O nome normalizado que o
  cliente manda é o certo.
- **Gabarito:**
  - nos nomes únicos testados, o CPF certo veio sozinho;
  - no TJRJ, o nascimento da fonte bateu com o da API em 5 de 5 acertos (houve 1 caso divergente, que a regra
    rejeita).
- **Riscos vistos:**
  - um CPF do gabarito **não está na base da API**. É o caso perigoso: se houver um homônimo único, a regra o aceita.
    A medição mede isso;
  - outro está na base com uma palavra diferente no nome (grafia, nome de casada) e volta `SEM_CANDIDATO`, que é o
    lado seguro.
- Não se sabe ainda a chave do código de erro no corpo dos 400. O cliente procura `erro`, `error`, `codigo`, `code`,
  `detail`... e, sem achar, qualquer `palavra_com_underscore`.

## Testes feitos (06/10/2026, sem o token)

- **Funções puras:** normalização, datas, regra de aceite em todos os casos, filtros de nome.
- **Cliente contra uma API falsa local:**
  - paginação pelo cursor;
  - parada nos homônimos;
  - 429 repetido;
  - 400 `consulta_ampla_demais` vira rejeição guardada no cache; `consulta_demorada` vira rejeição sem cache;
    `cursor_invalido` vira erro;
  - 500 e rede viram erro, sem cache;
  - 401 relê o `.env`;
  - o token nunca vai na URL.
- **Gravação + desfazer nos 6 tribunais, numa transação desfeita (ROLLBACK) com CPF fictício:**
  - o status virou `SUCESSO_API_TERCEIRO`, o vínculo saiu marcado e o legado foi atualizado;
  - o SQL de desfazer devolveu fila, vínculos, tentativas, metadata e legado ao estado exato de antes, e a pessoa
    criada foi apagada;
  - o banco ficou sem nenhuma linha do script.
- **Linha de comando com a API falsa:**
  - `--simulacao` (TJMT, lotes de 5);
  - `--medir` (6 tribunais; os CPFs aleatórios da API falsa saíram como `ERRO` e nenhum tribunal foi liberado);
  - modo real sem medição foi recusado.

## Tecnologias

- Python 3. A API usa só a biblioteca padrão (`urllib`); o resto usa `psycopg2` e `python-dotenv`.
- `utils/cpf_api.py`: cliente, regra, cache.
- `utils/legado.py`: `Backup`/desfazer e filas antigas, uma cópia parametrizada das funções dos robôs (que continuam
  com as deles).
