# TJPI — `fetch_TJPI.py`

Robô de credor do TJPI (tribunal 118 em `creditos.tribunal`) pela **consulta pública, sem login e sem o token A3**.
Ele toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`) nos leads do TJPI, por decisão do usuário
(07/10/2026): só consulta pública. O CPF que a fonte não mostra fica para o `CPF_API/completar_cpf.py`.

No TJPI o número do crédito é o **processo do próprio precatório** no PJe 2º grau (`07xxxxx-xx.AAAA.8.18.0000`). A
lista não traz nome, CPF nem originário, e a consulta pública do PJe (1º e 2º grau) redireciona para o login do PDPJ
(gov.br). Para cada precatório o robô:
1. lê no **DJEN** o cabeçalho das publicações do precatório: `REQUERENTE: A, B, FUNDO … REQUERIDO: ESTADO DO PIAUI`;
2. classifica os requerentes e decide o credor;
3. procura o originário entre os CNJs citados e os processos do credor no DJEN, e só liga quando confirma;
4. grava em lotes, numa transação por lote, com um SAVEPOINT por crédito.

## Como rodar

```bash
python TJPI/fetch_TJPI.py --simulacao                       # 20 créditos da ordem: faz tudo e dá ROLLBACK
python TJPI/fetch_TJPI.py --simulacao --creditos 411392,409643   # só esses créditos, tudo desfeito
python TJPI/fetch_TJPI.py --limite 20                       # modo real: para depois de 20 créditos
python TJPI/fetch_TJPI.py                                   # modo real: roda até acabar a carga (Ctrl+C para)
python TJPI/fetch_TJPI.py --workers 3                       # padrão: 3 threads
```

O CPF que o DJEN não mostra o próprio robô busca na API de CPF, pelo nome (`utils/cpf_robo.py`), desde 07/10/2026.
`--sem-cpf-api` desliga; aí o `CPF_API/completar_cpf.py --tribunal TJPI` completa depois.

- **`.env`:** `PG_*`. O lote de gravação é a constante `LOTE` no início do robô (20).
- **Reserva:** cada lead pego fica reservado também nas filas mensais do RPA antigo (`CREDOR_EM_ANDAMENTO` com o
  motivo `CONSULTA_PUBLICA_RESERVA: <worker>`), para o RPA com token A3 não o pegar. Ver `utils/legado.py`.
- **Ritmo:** DJEN a 1,2 s por consulta (sobe sozinho com 429; o limite é por IP e dividido com os outros robôs da
  máquina). Na simulação de 07/10: ~22 créditos/min com 3 workers, 2 respostas 429 em 171 créditos.
- **Rodar até acabar a carga:** com a fila vazia, espera os adiados por erro passageiro (30 min) e continua, com
  conexões novas depois da espera (o Postgres derruba sessão parada há mais de 15 min).
- **Simulação:** não reserva nada na fila; faz as consultas e as gravações e dá ROLLBACK no fim de cada lote.
- **Log:** terminal e `TJPI/saida/logs/fetch_TJPI_AAAAMMDD.log`.

## Fila

- **Pega:** leads do TJPI em PENDENTE do RPA (software 2), os do RPA sem credor em qualquer status (menos
  EM_ANDAMENTO) e os PENDENTE do próprio robô, com `disponivel_em` vencido. Em 07/10/2026 eram só 4: o RPA com A3 e a
  correção do legado (`corrigir_legado_TJPI.py`, 05/10) deixaram ~99% dos ativos com credor e CPF. O robô serve para
  os leads novos que a lista trouxer (~1.000 precatórios/ano).
- **Ordem:** sem credor antes de com credor, prioridade de campanha, maior valor. Refeita a cada 30 minutos.
- **Reserva:** um lead por vez, com lease. Nessa hora ele passa para `CONSULTA_PUBLICA_TJPI` (criado na 1ª execução
  real). Com `--com-desfazer`, `saida/desfazer_fila_<rodada>.sql` devolve ao RPA, com o status de antes, cada lead
  pego.
- **Outros tribunais na lista:** 3 créditos da lista do TJPI têm número de outro tribunal (`8.10` TJMA, `8.07`
  TJDFT); o DJEN é consultado com a sigla tirada do número.

## Fontes (todas públicas e anônimas)

| Fonte | Para quê | O que vem |
|---|---|---|
| **DJEN** `comunicaapi.pje.jus.br/api/v1/comunicacao?numeroProcesso=<precatório>&siglaTribunal=TJPI` | Quem é o credor | Cabeçalho `REQUERENTE: … REQUERIDO: …` (a capa do precatório); advogados com OAB; às vezes `CPF` na decisão de pagamento |
| **DJEN** `nomeParte=<credor>` | Candidatos a originário | Os processos da pessoa no TJPI, com polos e advogados |
| **DJEN** do candidato a originário | Confirmação | Polos, advogados e o texto (nº do precatório, valor) |

Não servem (testado em 05 e 07/10/2026): a consulta pública do PJe (manda para o PDPJ), o PDPJ sem login (401), o
DataJud (sem partes e sem vínculo entre precatório e originário; a data de expedição não casa), o SAPRE e a lista
cronológica (só o número do precatório). O PDPJ com o login do advogado traz tudo, mas fica fora por decisão do
usuário (consultas autenticadas têm limite diário).

**Cuidado com o polo A do DJEN:** as decisões em massa da Presidência intimam fundos, escritórios e pessoas de outros
precatórios (ADAUTO FORTES ADVOGADOS, DOMUS OCTANTE FIDC, FJ CONSULTORIA, FLAVIA CEOLIN LOPES PIANA...). Por isso o
credor sai do cabeçalho; o polo A só vale sem cabeçalho e com uma única pessoa física. Os advogados só entram das
publicações em que o polo A não tem ninguém de fora do precatório. CPF/CNPJ do texto só vale quando vem logo depois
do nome do próprio credor (o nome não pode ser pedaço de um nome maior: `PAULO … DA COSTA` x `PAULO … DA COSTA
JUNIOR`).

Há cabeçalhos com o ente rotulado como requerente (`REQUERENTE: FULANO REQUERENTE: ESTADO DO PIAUI`): o ente público
é descartado.

## Decisão

| Situação | Status | O que grava |
|---|---|---|
| 1 pessoa física (ou só uma empresa), com CPF/CNPJ no texto | `SUCESSO_PROCESSO_CREDITO` (ou `SUCESSO_PROCESSO_ORIGINARIO` com originário) | credor (`registrar_credor`, a fonte corrige o banco), advogados, metadata |
| idem, sem documento no texto, CPF aceito pela API de CPF | `SUCESSO_API_TERCEIRO` (`CPF_API: <regra> …`) | credor com o CPF da API (marcas do `completar_cpf.py`), advogados, `metadata.cpf_api` |
| idem, sem CPF na API (homônimo, fora da base, outra região) | `SUCESSO_INCOMPLETO` (`SUCESSO_SEM_CPF: credor … no DJEN do precatório`) | advogados; nome em `metadata.credor` e a tentativa em `metadata.cpf_api` |
| 2+ pessoas físicas | `SUCESSO_ANALISAR` (`VARIOS_REQUERENTES: …`) | nomes só no metadata (`candidato_analisar`) |
| pessoa física + fundo/empresa | `SUCESSO_ANALISAR` (`CESSAO: …; cessionário(s): …`) | idem |
| só sociedade de advogados | `SUCESSO_INCOMPLETO` (`SUCESSO_SEM_CPF: honorários … regra=HONORARIOS_ADVOGADO`) | metadata |
| publicação sem nome de credor | volta para a fila em 15 dias; na 3ª vez `FALHA` (`SEM_CREDOR … [sem_credor=3]`) | nada / metadata |
| sem publicação no DJEN | volta para a fila em 15 dias (`AINDA_SEM_PUBLICACAO`) | nada |

**Originário** (só com credor único; o usuário decidiu ligar só quando confirmado). Candidatos: CNJs de 1º grau do
TJPI citados nas publicações do precatório e os processos do credor no DJEN pelo nome (credor no polo ativo e não
como advogado, não mais novos que o precatório, fora classes que não geram precatório). Até 6 por crédito são
conferidos no DJEN do próprio candidato:
- **confirma** com o ente no polo passivo **e** (o candidato citando o nº do precatório, **ou** o credor no polo ativo
  com o CNJ citado no DJEN do precatório ou o valor do precatório no texto);
- **OAB em comum** só reforça: sozinha ligou, na simulação de 07/10, o mandado de segurança de 2014 em vez do
  cumprimento de 2021 que o banco tem (o mesmo advogado leva os dois);
- 1 confirmado → liga com `fila_credor_registrar_originario` (sem capa: não há capa pública com partes); empate ou
  nenhum → candidatos só em `metadata.motor.candidatos`;
- ação coletiva que já tem outros credores no banco → não liga (`ORIGINARIO_COLETIVO_NAO_LIGADO`).

## Resultado das simulações (07/10/2026, ROLLBACK, banco conferido igual antes e depois)

100 créditos sorteados com credor e CPF no banco (gabarito) + os 71 com originário no banco:
- **Nome:** dos 100, 70 têm publicação no DJEN (nos de 2025–2026, 20 de 21; nos de 2023, 1 de 11). Dos 70, 50 saíram
  com credor único igual ao banco, 19 em `SUCESSO_ANALISAR` (cessão para FIDC ou vários requerentes, com o credor do
  banco entre os nomes) e 2 com nome diferente do banco (MARC FARLANE x KAMILLA JULIANA; BENIRIA x MARIA IVANDETE,
  os dois de 2025 e com CPF do banco da região 6, a conferir no banco).
- **CPF no texto:** nenhum dos credores únicos tinha CPF público.
- **API de CPF dentro do robô** (simulação de 07/10, 52 credores consultados): 28 aceitos, 24 iguais ao banco; os 3
  diferentes são os 2 créditos em que o DJEN mostra outra pessoa e 1 com o CPF do banco de outra região (410274). O
  único sem credor da amostra (412588) saiu `SUCESSO_API_TERCEIRO`.
- **Originário:** com a regra estrita, 1 ligado nos 71 pares conhecidos, igual ao banco (precatório citado + valor +
  OAB). Antes da regra estrita, a OAB sozinha tinha ligado 2 certos e 1 diferente do banco.

## Gravação

- **Credor com documento:** `registrar_credor` (origem FONTE) e a regra "a fonte corrige o banco": vínculo de CREDOR
  da mesma pessoa com outro documento é apagado (com o INSERT que o recria em `desfazer_legado_*.sql`), e o documento
  da capa antiga do precatório (`precatorios.partes_processuais`) é trocado.
- **Advogados do precatório** (DJEN): `registrar_credor` papel ADVOGADO pela OAB, em todo lead processado.
- **Metadata** (`credito_fonte.metadata` do software): `motor` (resultado, regra, evidência, candidatos, requerentes,
  requerido, advogados, nº de publicações), `credor` (nome, papel, `cpf_encontrado`) e `candidato_analisar`. O texto
  das publicações não é guardado.
- **Legado:** status nas filas mensais `listas_primarias.processos_unificados_AAAA_MM` (de 2026_08 em diante, as que
  o usuário pode gravar); backup e `desfazer_legado_*.sql` só com `--com-desfazer`.

## Saídas (`TJPI/saida/`)

- `fetch_TJPI.csv` (`_simulacao` na simulação): 1 linha por crédito (resultado, motivo, credor, requerentes do DJEN,
  candidatos a originário, credores antes e depois).
- `fetch_credores_trocados.csv`: créditos em que os credores ligados mudaram.
- Só com `--com-desfazer`: `desfazer_legado_<rodada>.sql` + `fetch_legado_backup.csv` e `desfazer_fila_<rodada>.sql`.
  Desde 07/10/2026 o padrão é não escrever o desfazer; `--sem-desfazer`, que os ciclos do modo 5 do RPA_SISTEMAS
  passam, continua aceito.

## Outros scripts da pasta

- `corrigir_legado_TJPI.py` (05/10/2026): ligou o credor que o RPA antigo leu do PJe com o papel errado (requerente
  gravado como ADVOGADO, com o nome emendado aos advogados e o CPF certo). Não consulta fonte nenhuma.
