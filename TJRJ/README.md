# TJRJ — `fetch_TJRJ.py`

Robô de credor do TJRJ (tribunal 119 em `creditos.tribunal`) pela **consulta pública, sem login e sem o token A3 de
advogado**. Ele toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`) nos leads do TJRJ. Para cada precatório:
1. consulta o **processo originário**, que já vem na lista do TJRJ;
2. decide quem é o **credor** do precatório;
3. grava o nome e, quando a fonte mostra, o **CPF**, os **advogados (OAB)** e as evidências.

A gravação é em lotes, numa transação por lote, com um SAVEPOINT por crédito.

Estado em 01/10/2026: o escopo do 1º grau (100.312 leads) foi todo processado entre 30/09 e 01/10. O software
`CONSULTA_PUBLICA_TJRJ` (id 10) foi criado na 1ª execução real. O resultado ficou assim:
- credor em 40.488, dos quais 2.562 com CPF;
- 56.384 em analisar;
- 3.440 em FALHA.

Em 01/10 entraram os originários do 2º grau (eJUD) e os que vêm sem máscara na lista, ~8,4 mil leads. Continuam
com o RPA os ~18,5 mil sem originário na lista.

## Como rodar

```bash
python TJRJ/fetch_TJRJ.py --simulacao                       # 20 créditos da ordem: faz tudo e dá ROLLBACK
python TJRJ/fetch_TJRJ.py --simulacao --limite 100          # 100 créditos da ordem, tudo desfeito
python TJRJ/fetch_TJRJ.py --simulacao --creditos 1160917,1192966   # só esses créditos, tudo desfeito
python TJRJ/fetch_TJRJ.py --limite 50                       # modo real: para depois de 50 créditos
python TJRJ/fetch_TJRJ.py                                   # modo real: processa a fila até acabar (Ctrl+C para)
python TJRJ/fetch_TJRJ.py --workers 6 --ritmo 6             # padrão: 6 threads, até 6 req/s ao TJRJ no total
python TJRJ/fetch_TJRJ.py --ritmo 12                        # mais rápido (12 req/s rodou 2 h sem freada em 01/10)
python TJRJ/fetch_TJRJ.py --chromes 2                       # Chromes para o eJUD 2º grau (padrão 1; mais Chromes = mais recusa do reCAPTCHA)
python TJRJ/fetch_TJRJ.py --chromes 0                       # sem o eJUD: os leads do 2º grau ficam para o RPA
```

### Ritmo e bloqueio
O robô não tem pausa fixa por worker. Um **ritmo global** (`--ritmo`, requisições por segundo somando todos os
workers) vale para o TJRJ (DCP, eJUD e portal). O PJe tem um ritmo próprio, de até 2 req/s. O ritmo se ajusta sozinho:
- **freia** quando o servidor responde 403, 429, 502, 503 ou 504, ou quando dá timeout ou erro de conexão: o ritmo
  cai pela metade (mínimo 0,5 req/s) e **todos os workers param 60 s**;
- **sobe** 10% a cada 200 respostas boas seguidas, até o `--ritmo`.

O log de cada lote mostra o ritmo atual e quantas vezes freou, por exemplo `ritmo TJRJ 6.0 req/s (0 freada(s))`.
Com 12 falhas técnicas seguidas, o robô para: o TJRJ caiu ou está bloqueando.

Enquanto a thread principal grava um lote, os workers seguem com a fila (até 2× workers créditos em voo).

- **`.env`:** `PG_*` e `LOTE_GRAVACAO_TJRJ` (créditos por transação, inteiro maior que zero). Nada de proxy.
- **Chrome:** o DCP, o PJe e o portal são HTTP puro. Só o eJUD 2º grau usa o **Chrome instalado**, por CDP, com o
  `patchright` (ou o `playwright`):
  - cada Chrome tem perfil próprio, `TJRJ/.chrome-profile-ejud-N`, fora do git;
  - os Chromes só abrem quando aparece o 1º lead do 2º grau e fecham no fim;
  - um Chrome que sobrou de uma execução anterior com o mesmo perfil é fechado na partida.
- **Simulação:** não reserva nada na fila; faz as consultas e as gravações e dá ROLLBACK no fim de cada lote.
- **Ctrl+C:** os créditos em andamento voltam para a fila (`fila_credor_liberar`) e o lote já processado é gravado.
- **Log:** terminal e `TJRJ/saida/logs/fetch_TJRJ_AAAAMMDD.log`.

## Fila

- **Pega:** leads do TJRJ em PENDENTE do RPA (software 2) ou do próprio robô, fora de lease, com `disponivel_em`
  vencido.
- **Ordem:** **T1** (não pagos na fila cronológica: `Pago=False`, `Ativo`, `OrdemPagamento>0`) → **T2** (outros não
  pagos) → **T3** (pagos). Dentro de cada faixa, maior valor primeiro. A ordem é refeita a cada 30 minutos.
- **Escopo:** originário CNJ do TJRJ na lista, com ou sem máscara (`0001110-61.2015.8.19.0080` ou
  `00011106120158190080`). O 2º grau (`.8.19.0000`) só entra com o eJUD ligado (`--chromes` maior que 0).
- **Fica intocado para o RPA:**
  - leads **sem originário** na lista: ~18,4 mil, quase todos pagos antigos. Nenhuma fonte pública dá o originário
    deles: a página do precatório no eJUD diz só o ente e "Informações sigilosas";
  - originário de outro tribunal (16 leads): as fontes do robô são do TJRJ.
- **Entram também** (desde 02/10/2026):
  - os que saíram da lista e têm originário (33);
  - os 195 de número malformado na carga legada (ex.: `2022008-79.5202.0.04.3682`). Todos têm originário válido. O
    robô usa o número da lista (`NumeroPrecatorio`, ex.: `2022.00879-5`) para o DCP, o eJUD e o portal, e o crédito
    continua o mesmo. Nenhum tem um "gêmeo" com o número certo no banco, e a conferência antes do
    `registrar_credito` impede criar outro crédito.
- **Reserva:** um lead por vez, com lease (`reservar`). Nessa hora ele sai do software 2 e passa para
  `CONSULTA_PUBLICA_TJRJ`, então o RPA (`fila_credor_pegar`) e o espelho do legado (`espelhar_status_credor_legado`)
  não o tocam mais. `saida/desfazer_fila_<rodada>.sql` devolve ao RPA, com o status de antes, cada lead que o robô
  pegou.

## Fontes (todas públicas e anônimas)

| Fonte | Para quê | O que vem |
|---|---|---|
| **Lista do TJRJ** (`lista_item.metadata`, já no banco) | Entrada | Originário, ente, `ValorHistorico` (valor bruto), prioridade, pago |
| **DCP**: `www3.tjrj.jus.br/consultaprocessual/api` | Originários `0…` (~88%) | Partes e papéis, advogados com OAB, **precatórios vinculados** ao processo, certidões nos movimentos (beneficiário, valor bruto, data de nascimento e, raramente, o CPF). **Não mostra CPF nas partes** |
| **PJe 1º grau**: `tjrj.pje.jus.br/pje/ConsultaPublica` | Originários `08…` (~5%) | Polo ativo com **CPF completo** e advogados com OAB |
| **eJUD 2º grau**: `ejud/ConsultaProcesso.aspx`, no Chrome | Originários `.8.19.0000` | Personagens (autor/exequente, réu), advogados com OAB, **precatórios autuados** no processo, segredo, processo principal. **Sem CPF** |
| **eJUD**: página do precatório | Só em ação coletiva | O advogado daquele precatório |
| **Portal de precatórios**: `api/precatorios/detalhes` | Quando o credor foi definido | `PossuiCessao` e `PossuiHerdeiro` |

- **DCP:** hoje não tem reCAPTCHA (`security/recuperar-site-key` devolve vazio). O robô confere isso na partida e a
  cada 30 minutos; se voltar, ele para.
- **PJe:** tem `if (false)` no reCAPTCHA; se isso mudar, o crédito é adiado com `CAPTCHA_REATIVADO`.
- **eJUD 2º grau** (uso liberado pelo usuário em 01/10/2026). O caminho até os dados:
  1. o DCP (`por-numeracao-unica`) dá o número antigo do processo no eJUD;
  2. o robô abre `ConsultaProcesso.aspx?N=<número>` no Chrome;
  3. a própria página roda o **reCAPTCHA v3 invisível**: o Google dá uma nota ao navegador, sem desafio na tela;
  4. só com nota de pelo menos 0,3 a página busca os dados em `WS/ConsultaEjud.asmx/DadosProcesso_1`, e o robô lê
     esse JSON.

  **O robô não resolve nem pula o captcha** e não chama o serviço por fora da página. Se a nota vier baixa, o crédito
  é adiado (`CAPTCHA_EJUD`) e aquele Chrome descansa 30 min. Com 3 erros seguidos, ele fecha e abre de novo.
  - **Ritmo conservador** (padrão desde 02/10/2026): 1 Chrome e 5 s entre páginas, ~9 páginas por minuto. Em 01/10,
    2 Chromes a ~1 página/s cada levaram a nota a 0,1 depois de ~45 min.
  - **A recusa não para o robô:** ela não conta como falha técnica. Com todos os Chromes em pausa, o robô para de
    pegar leads, grava o lote pronto, espera a pausa acabar e continua sozinho.
  - Resposta atrasada de uma página abandonada é descartada: o pedido de dados precisa trazer o número do processo
    da página atual. Se mesmo assim vierem dados de outro processo, o crédito é adiado e nunca vira FALHA.
- **Originários migrados para o eproc:** continuam com as partes e todo o histórico no DCP até a migração.
- O CNJ do precatório (`…8.19.0801`) é calculado a partir do número antigo (conferido com o eJUD) e fica no metadata.

## Decisão do credor e status

| Situação | Status em `coleta_credor` | Motivo (só códigos, sem nome nem CPF) |
|---|---|---|
| Credor definido **com CPF** (PJe, ou certidão ou texto do DCP com o CPF logo depois do nome dele) | `SUCESSO_PROCESSO_ORIGINARIO` | `cnj=… regra=… fontes=…` |
| Credor definido **só com nome** | `SUCESSO_INCOMPLETO` | `SUCESSO_SEM_CPF: cnj=… regra=… fontes=…` |
| Ação coletiva sem certidão que decida, herdeiro ou habilitado sem certidão, PJe com mais de um requerente | `SUCESSO_ANALISAR` | `CREDOR_POLO_ATIVO_SEM_VINCULO: …` |
| Autor único, mas o precatório vale menos da metade do maior precatório do mesmo originário (possível honorários do advogado) | `SUCESSO_ANALISAR` | `POSSIVEL_HONORARIOS: … razao=0.10` (até 35%) ou `PRECATORIO_MENOR_DO_PROCESSO: …` (35–50%) |
| Autor único com vários precatórios, mas sem valor dos outros para comparar | `SUCESSO_ANALISAR` | `PRECATORIOS_SEM_VALOR_PARA_COMPARAR: …` |
| Cessão ou herdeiro no precatório (portal) | `SUCESSO_ANALISAR` | `CESSAO_OU_HERDEIRO_NO_PRECATORIO: …` |
| O precatório não está nos vinculados do originário (DCP) ou nos precatórios autuados (eJUD 2º grau) | `SUCESSO_ANALISAR` | `VINCULO_NAO_CONFIRMADO: …` |
| Polo ativo só com ente público (ex.: execução fiscal) | `FALHA` | `REQTE_ORGAO_PUBLICO: …` |
| Segredo de justiça | `FALHA` | `SEGREDO_DE_JUSTICA: …` |
| Originário não achado | `FALHA` | `PROCESSO_NAO_ENCONTRADO: …` (reprocessável) |
| Erro técnico (timeout, 5xx, captcha) | volta para a fila em 30 min (`fila_credor_adiar`) | `PESQUISA_SEM_RESPOSTA…`, `CAPTCHA_REATIVADO…`, `CAPTCHA_EJUD…`, `TIMEOUT_EJUD…` |

**Quem conta como ente público:**
- os nomes do `RE_ORGAO_PUBLICO` (Estado, município, prefeitura, autarquia...);
- o próprio ente devedor do precatório.

O nome **da cidade** do município devedor só conta quando a parte se chama exatamente assim. Com o Município de Carmo
como devedor, "CARMO" é o ente, mas "MARIA DO CARMO" é uma pessoa. Até 01/10/2026 o nome da cidade contava em
qualquer posição, o que podia descartar uma pessoa ("JOSE MESQUITA", "MARIA DO CARMO") como se fosse o ente. A
estimativa é de menos de 20 leads afetados no 1º grau. Associação, sindicato, federação, cooperativa e clube nunca
contam como ente público, mesmo com o nome do ente no nome.

**Regras (`regra`)** para definir o credor:
- **`CERTIDAO_VALOR`**: a certidão de expedição do precatório tem valor bruto igual ao `ValorHistorico` da lista
  (±R$ 1,00). O credor é o beneficiário dela, e a data de nascimento vem junto quando está na certidão.
- **`AUTOR_UNICO`**: o originário tem um só autor pessoa física, sem herdeiro, habilitado ou sucessor.
- **`AUTOR_UNICO_VARIOS_PREC`**: autor único, mas o originário tem mais de um precatório (no DCP ou no banco). O
  credor só é o autor quando este precatório é o **maior** do processo, ou vale pelo menos **50%** do maior
  (principal, complementar, parcela). Até 35% do maior tem cara de honorários do advogado (10–30% do principal); de
  35% a 50% é zona de dúvida. Nos dois casos o lead vai para `SUCESSO_ANALISAR` sem gravar o autor como credor.
  - Os valores vêm da lista, para todos os precatórios ligados ao originário em `credito_originario`.
  - A razão calculada fica em `metadata.flags.razao_maior_precatorio`.
  - Medição de 30/09/2026: 31% dos precatórios "pequenos" do DCP valem exatamente 5%, 10%, 15%, 20%, 25% ou 30% de
    outro precatório do mesmo processo (por acaso seriam ~7%).
- **`HONORARIOS_ADVOGADO`**: o beneficiário da certidão é advogado do processo. O status é só nome, e ele continua
  como advogado.

O CPF que aparece nos movimentos só conta quando vem logo depois do nome do credor, sem perito, advogado, patrono,
curador ou procurador no meio. Nas medições, os outros CPFs do texto eram de peritos e advogados.

A lista unificada (`vw_processos_unificados`) mostra `status_coleta_lead` = `codigo_legado` do status (ex.:
`CREDOR_SUCESSO_INCOMPLETO`) e `motivo_coleta_lead` = o motivo (ex.: `SUCESSO_SEM_CPF: …`).

## O que grava

Por crédito, dentro do SAVEPOINT:
1. **Credor com CPF:** `creditos.registrar_credor` (papel CREDOR, origem FONTE), **antes** da capa, porque o
   recálculo das partes não apaga vínculo FONTE. Depois vem a regra "a fonte corrige o banco": vínculo de CREDOR da
   mesma pessoa com outro documento é apagado, e o INSERT que o recria fica no `desfazer_legado_*.sql`.
2. **Capa do originário** (`creditos.registrar_capa`), só quando o credor foi definido. **Só acrescenta:** reenvia
   as partes que o banco já tem e soma as novas, porque a função troca o conjunto inteiro e recalcula os credores de
   todos os precatórios do processo. O que vai:
   - o credor, com o nome e o CPF quando houver. Sem CPF ele vira parte só com nome e **não entra em
     `credito_credor`**, porque o banco exige CPF/CNPJ ou OAB;
   - o polo passivo, **sempre com papel REU**, com o texto da fonte em `papel_bruto`. O banco tira o papel do
     texto sem olhar o polo, e um "EXEQUENTE" ou "BENEFICIÁRIO" listado no passivo do PJe viraria CREDOR. Até
     01/10/2026 isso ligou 2 vínculos errados;
   - os advogados do polo ativo com OAB. **Na ação coletiva não vai nenhum advogado**: o recálculo ligaria cada um a
     todos os precatórios do processo. O advogado daquele precatório (eJUD) fica só no metadata;
   - o grau: `G2` no originário do 2º grau e `G1` nos demais.
3. **`credito_fonte.metadata`** do software (via `registrar_credito`, com conferência do número antes): o credor
   (nome, regra, papel, data de nascimento, se achou CPF), os advogados, os autores (nos casos de analisar), o CNJ do
   precatório, as flags (cessão, herdeiro, migrado para o eproc) e os dados da lista.
4. **Legado, como o TJMA:**
   - capa antiga em `originarios.processos_originarios`, `partes_processuais` e `advogados`, só acrescentando e com o
     mesmo recorte;
   - status e motivo nas filas mensais `listas_primarias.processos_unificados_2026_08/09`, para o RPA antigo não
     reprocessar com A3.

   Cada linha tocada tem o "antes" em `saida/desfazer_legado_<rodada>.sql` e `fetch_legado_backup.csv`.
5. **`creditos.fila_credor_finalizar`** com status, motivo, `via=ORIGINARIO`, sistema e detalhe sem documento. O
   sistema é PJE ou EPROC; o DCP e o eJUD não têm código em `sistema_processual`, e no CSV e no metadata aparecem
   como `DCP` e `EJUD`.

O CPF nunca vai para log, CSV, motivo nem detalhe da fila, e o HTML do PJe não é guardado no disco.

## Saídas (`TJRJ/saida/`, fora do git)

| Arquivo | Conteúdo |
|---|---|
| `fetch_TJRJ.csv` (`_simulacao`) | 1 linha por crédito: faixa, originário, sistema, resultado, motivo, regra, fontes, credor, `cpf_encontrado`, nascimento, autores, precatórios do originário, advogados, o que mudou no banco e no legado, credores antes e depois (sem documento) |
| `fetch_credores_trocados.csv` | Créditos cujos credores mudaram |
| `desfazer_fila_<rodada>.sql` | Devolve ao RPA os leads que o robô pegou |
| `desfazer_legado_<rodada>.sql` e `fetch_legado_backup.csv` | Desfaz as mudanças no legado e os vínculos de credor apagados |

## Depois do robô: mandar o resto para o A3

Os leads em `SUCESSO_INCOMPLETO` (credor só com nome) e `SUCESSO_ANALISAR` ficam com o software do robô. Se for
decidido buscar o CPF deles com o A3, a função existente devolve os leads para a fila do RPA:

```sql
SELECT creditos.fila_credor_repescar('TJRJ', 'SUCESSO_INCOMPLETO', NULL, false, now(), 'CONSULTA_PUBLICA_TJRJ');
```

Confira a assinatura e o efeito da função antes de usar.

## Medições que embasam as regras (30/09/2026)

- **PJe:** 41 de 43 leads com nome e CPF.
- **DCP (580 leads):** nome definido em ~53%, ação coletiva ambígua em ~46%, CPF do credor em ~0,3%.
- **Servidor:** o DCP gasta ~2 s por lead (numeração 0,07 s, partes 0,3 s, movimentos 1 s, portal 0,5 s). Com 4
  consultas simultâneas, a 8,2 req/s por 1 minuto, não deu erro nem lentidão.
- **Velocidade:** com 6 workers e ritmo 6, foram **77 leads/min** na simulação de 120 leads, sem nenhuma freada. Nesse
  ritmo, os ~45 mil leads da T1 levam **~10 h** e o escopo inteiro (~100 mil), ~22 h. Antes (pausa fixa, 2 workers)
  eram 11 a 19 leads/min. Na rodada real de 01/10, com ritmo 12, foram 156 a 175 leads/min sem freada.

## Medições do eJUD 2º grau (01/10/2026)

- **Amostra:** 40 originários do 2º grau, de leads não pagos.
  - 40 de 40 páginas carregaram, em 1,7 s cada (mediana). A nota do reCAPTCHA foi 0,9 em 39 e 0,7 em 1.
  - 35 são cumprimentos de sentença individuais de ações coletivas do Órgão Especial, com 1 exequente.
  - Em 40 de 40, o precatório do lead está entre os precatórios autuados no processo.
  - Nenhum em segredo. A API do DCP acha o processo (tipo 2), mas não devolve as partes.
- **Simulação de 40 leads na ordem da fila, com 2 Chromes:** 43 leads/min.
  - 21 com credor só com nome (20 AUTOR_UNICO e 1 AUTOR_UNICO_VARIOS_PREC);
  - 17 em analisar (vários autores);
  - 2 de órgão público. Um deles era uma associação, o que levou à correção da regra de ente público acima.
- **Tamanho:** os ~8,4 mil leads do 2º grau estão em ~4,6 mil processos. Com 2 Chromes, devem levar ~3 h.
