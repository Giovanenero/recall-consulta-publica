# TJMA — `fetch_TJMA.py`

Robô de credor do TJMA (tribunal 110 em `creditos.tribunal`, software `CONSULTA_PUBLICA_TJMA`) pela **consulta
pública, sem navegador, sem login e sem o token A3**. Toma o lugar do modo credor do RPA antigo (`RPA_CREDOR_V1`) nos
leads do TJMA. A lista do TJMA não traz o beneficiário (Res. CNJ 303) e o PJe 2º grau público não abre precatório,
então o robô:
1. lê o **DJEN** do precatório (nome ou iniciais do credor, advogados com OAB, CNJs de 1º grau citados);
2. acha e confirma o **processo originário** no **PJe 1º grau público** (HTTP puro; CPF completo do polo ativo);
3. nos antigos (`00...`) que não achou, consulta o **Jurisconsult** do TJMA (credor com CPF e advogados);
4. grava credor, CPF, originário, advogados e as evidências no banco, em lotes de `LOTE` (20) créditos, numa transação
   por lote e um SAVEPOINT por crédito.

Cobertura em 09/10/2026 (linha `[cobertura]` que o robô loga no fim de cada rodada): 22.381 créditos ativos na lista;
credor com CPF/CNPJ 66,1%; originário 29,5%; credor + CPF + originário 29,4%; advogado com OAB 80,8%.

## Como rodar

```bash
python TJMA/fetch_TJMA.py                              # modo real: a fila de disponíveis do TJMA até acabar (Ctrl+C para)
python TJMA/fetch_TJMA.py --simulacao                  # 20 créditos da ordem: faz tudo e dá ROLLBACK; não mexe na fila
python TJMA/fetch_TJMA.py --simulacao --creditos 268230,268175   # só esses créditos, tudo desfeito
python TJMA/fetch_TJMA.py --limite 30                  # para depois de 30 créditos
python TJMA/fetch_TJMA.py --iniciais --workers 2       # rota das iniciais (abaixo)
```

Opções:

| Opção | O que faz |
|---|---|
| `--simulacao` | 20 créditos (ou `--limite`) da ordem do robô; faz tudo e desfaz no banco (ROLLBACK); nada é reservado |
| `--limite N` | para depois de N créditos |
| `--workers N` | threads raspando ao mesmo tempo (padrão 6); um processo só |
| `--creditos a,b,c` | só na simulação: esses ids no lugar dos primeiros da fila |
| `--proxies` | soma `PROXY_01..05` do `.env` como saídas do DJEN (precisam sair pelo Brasil) |
| `--com-desfazer` | escreve `desfazer_*.sql` e o backup `.csv` (o padrão é não escrever) |
| `--sem-desfazer` | não escreve (já é o padrão; o ciclo do modo 5 do RPA passa) |
| `--iniciais` | rota das iniciais: a fila passa a ser só os FALHA `SEM_BENEFICIARIO` do robô |
| `--sem-jurisconsult` | desliga o fallback dos antigos no Jurisconsult (não abre o Chrome) |

Dependências além do `requirements.txt` comum: o fallback do Jurisconsult usa `ddddocr` (OCR do captcha de imagem),
`patchright` e o Chrome instalado (`pip install ddddocr patchright`). Sem eles, o robô loga um aviso uma vez e segue
sem o Jurisconsult.

Erro passageiro (DJEN/PJe fora, timeout, captcha) devolve o crédito para a fila em 30 min (`fila_credor_adiar`) e
nunca vira FALHA; 8 falhas técnicas seguidas param o robô (o DJEN às vezes responde 503 por horas).

## Fila

Ordem própria: não pagos primeiro (lista vigente e PDFs de pagos do hotsite do TJMA), e dentro de cada faixa
campanha, sem credor antes de com credor e maior valor. Pega os PENDENTE do RPA (`software_id = 2`, ou sem credor
fora ADVOGADO) e os PENDENTE do próprio software, com `disponivel_em <= now()`. Cada crédito é reservado com lease
(`reservar`), também nas filas mensais do RPA antigo (`CREDOR_EM_ANDAMENTO` com a marca do robô), e só nessa hora passa
do RPA para o software próprio. A ordem é remontada a cada 30 min (entram os adiados que venceram).

## Esteira por lead (`processar`)

1. Precatório que não é do TJMA (`.8.10.` fora) -> FALHA `CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO`.
2. **DJEN do precatório**: credor pelo rótulo `CREDOR:` (em iniciais, o destinatário com as mesmas iniciais), OABs,
   CNJs citados, cessão. Os advogados do lado do credor (`utils/advogados_djen.py`) vão para o crédito como ADVOGADO em
   todo lead gravado.
   - credor órgão público -> FALHA `REQTE_ORGAO_PUBLICO`;
   - credor **sociedade de advogados** -> honorários: SUCESSO_INCOMPLETO `SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO
     beneficiario=<sociedade> doc=sim|nao (sociedade de advogados)`, sem CREDOR; nome (e CNPJ, se houver) em
     `metadata.honorarios`; com CNPJ, a sociedade entra como ADVOGADO (desde 09/10/2026; antes era FALHA
     `SEM_CPF_CREDOR`);
   - sem publicação nenhuma -> volta para a fila em 15 dias (`AINDA_SEM_PUBLICACAO`).
3. **Originário conhecido** (ligado no banco, citado no DJEN, pista do RPA): abre direto pelo número; confirma pelas
   partes do banco ou pelo detalhe do PJe (credor no ativo, ente no passivo).
4. Nenhum confirmou: **pelo nome** do credor no PJe 1º grau (até 30 resultados; saturou, corta pela data de autuação
   e soma a busca pelo nome no DJEN). Credor só em iniciais e elas são de um advogado do precatório: o credor é o
   próprio advogado (`CREDOR_ADVOGADO ... regra=HONORARIOS_ADVOGADO`, CPF pela OAB no banco ou no PJe).
5. **Decide**: 1 confirmado -> liga (`SUCESSO_PROCESSO_ORIGINARIO` com evidência forte, senão
   `SUCESSO_PARTES_SEM_VALOR`); vários -> desempata por CNJ citado, OAB em comum ou DJEN do candidato citando o
   precatório/valor; sem desempate -> `SUCESSO_ANALISAR`. Sem originário, o **originário provável** (alta/média/baixa,
   `utils/originario_provavel.py`) vai só em `metadata.originario_provavel`.
6. **Processo não encontrado** (desde 09/10/2026): repete a busca uma vez na hora (sem o cache do DJEN); vazio de novo,
   o crédito volta para a fila em 7 dias com `[nao_encontrado=N]` no motivo; só na 3ª vez grava FALHA
   `PROCESSO_NAO_ENCONTRADO`. O adiamento não conta como falha técnica.
7. **Jurisconsult** (desde 09/10/2026; `--sem-jurisconsult` desliga): precatório antigo (`numero_norm` `00...`) que
   terminou em `PROCESSO_NAO_ENCONTRADO` (depois da 2ª busca) ou sem publicação no DJEN é consultado no Jurisconsult
   (Themis SG, seção "Jurisconsult" do `fetch_TJMA.py`) antes de adiar ou gravar FALHA. Credor com CPF/CNPJ (até 3, sem conflito de
   nome no banco) -> `SUCESSO_PROCESSO_CREDITO` com credor e advogados pela OAB (fonte `JURISCONSULT_TJMA`, sem
   originário); só o nome ou não achado -> segue o caminho de antes, com o resumo em `metadata.jurisconsult` e os
   advogados ligados. O Chrome (perfil `TJMA/.chrome-profile-jurisconsult`, criado sozinho) só abre no 1º antigo que
   precisar: um por processo, numa thread própria que atende os workers em fila, 20 s entre consultas. Sem `ddddocr`,
   desafio visível do Google, Chrome que não abre ou 5 falhas seguidas: desliga até o fim da execução, sem derrubar a
   rodada. Crédito com `metadata.jurisconsult` da versão atual não é consultado de novo. Na simulação, tudo com ROLLBACK.

### Rota das iniciais (`--iniciais`)

Desligada por padrão (~13-30 consultas ao DJEN por crédito; o limite do DJEN é por IP). Para o FALHA
`SEM_BENEFICIARIO` cujo DJEN só traz as iniciais (`CREDOR: M. R. A. E.`): lê as publicações do TJMA de até 2
advogados do credor pela OAB, junta os autores com as mesmas iniciais e confere no PJe 1º grau. Pessoa única com o
nº/valor do precatório no DJEN do originário -> SUCESSO normal; só pelas iniciais -> SUCESSO_ANALISAR
`CREDOR_SO_POR_INICIAIS` (candidato em `metadata.candidato_analisar`); escritório -> regra de honorários acima.
Ninguém ou 2+ pessoas: continua FALHA com `| rota das iniciais: ...` no motivo (cada crédito passa uma vez).

## O que grava

`fila_credor_registrar_originario`, `registrar_credor` (CREDOR com CPF, ADVOGADO pela OAB), `registrar_capa` (só a
capa do originário escolhido vinda do PJe; na coletiva, só o credor), `registrar_credito` (metadata: `motor`,
`capa_originario`, `pagamento`, `candidato_analisar`, `originario_provavel`, `honorarios`, `jurisconsult`), a capa e as filas
mensais do legado e o status na fila (`fila_credor_finalizar`). O texto das publicações do DJEN não é gravado.

## Saídas (`TJMA/saida/`, fora do git)

`fetch_TJMA.csv` (1 linha por crédito), `fetch_credores_trocados.csv`, `logs/`, `pje1g_html/`, `pdf_tjma/` e, com
`--com-desfazer`, `desfazer_legado_*.sql`, `desfazer_fila_*.sql` e `fetch_legado_backup.csv`.

## Scripts avulsos

```bash
python TJMA/reabrir_nao_encontrado.py [--aplicar]       # FALHA PROCESSO_NAO_ENCONTRADO sem credor -> PENDENTE (o robô passa os antigos no Jurisconsult)
python TJMA/reabrir_sociedade_advogados.py [--aplicar]  # FALHA SEM_CPF_CREDOR de escritório sem credor -> PENDENTE
```

Os `reabrir_*` só mostram sem `--aplicar`; com ele gravam e escrevem o desfazer em `TJMA/saida/desfazer_*.sql`.

## Mapa do código

| Parte | Funções |
|---|---|
| Configuração | constantes no topo (`LOTE`, `WORKERS`, `TETO_CREDITO`, `ADIAMENTO*`, `*_NAO_ENCONTRADO`) |
| DJEN | `Djen` (relógio por saída, cache em memória), `credor_do_djen`, `advogado_credor`, `candidatos_djen_nome` |
| PJe 1º grau | `Pje` (pesquisa, detalhe), `partes_da_pagina`, `campos_da_capa`, `cpf_do_advogado_no_pje` |
| Pagamentos | `pagamentos_tjma`, `numeros_do_pdf`, `situacao_pagamento`, `ordenar_escopo` |
| Decisão | `processar`, `confirmar`, `honorarios_sociedade`, `rota_iniciais`, `decidir_por_iniciais` |
| Originário provável | `sinais_do_candidato`, `originario_provavel` |
| Gravação | `gravar`, `registrar_metadata`, `corrigir_credor`, `gravar_capa_legado`, `atualizar_filas_mensais`, `Backup` |
| Lote e fila | `gravar_lote`, `descarregar`, `reservar`, `pegar`, `devolver` |
| Jurisconsult | `jurisconsult_cabe`, `fallback_jurisconsult`, `gravar_jurisconsult_adiado`; cliente na seção Jurisconsult (`Jurisconsult`, `ServicoJurisconsult`, `ler_jurisconsult`) |
| Execução | `Rodada`, `proximo_credito`, `processar_credito` (repete o não encontrado), `tratar`, `encerrar`, `medir_cobertura`, `main` |

## Cuidados

- O DJEN limita por IP e é dividido com os outros robôs; `--proxies` só com saídas no Brasil.
- A sessão do Postgres parada cai em 15 min (`idle_session_timeout`).
- No RPA_SISTEMAS (modo 5) roda a cópia em `COLETA/teste publico/TJMA/` (este README vai como `README_origem.md`).
