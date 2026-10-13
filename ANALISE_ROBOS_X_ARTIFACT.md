# Robôs do modo 5 x artifact "Ordem Cronológica dos TJs"

Atualizado em 13/10/2026 (primeira versão: 08/10). Compara os 8 robôs que rodam no modo 5 do RPA_SISTEMAS com o
que o artifact "Ordem Cronológica dos TJs" (fichas de 21 a 30/09/2026) pede para cada tribunal.

- **Fora da análise:** baixar a lista da ordem cronológica. Isso é trabalho de outro setor.
- **Números:** medidos no banco em 13/10 com `python utils/cobertura.py` (só leitura). Contam os precatórios que
  estão hoje na lista do tribunal. Cada robô também escreve essa medição no log, no fim de cada rodada (`[cobertura]`).
- **Teste de 13/10:** os 8 robôs rodaram em simulação com 5 créditos cada (3 já resolvidos e 2 em aberto). Nenhum
  erro; nos já resolvidos o robô chegou ao mesmo credor, CPF e originário que estavam no banco.

## Termos usados

| Termo | O que é |
|---|---|
| Credor | Quem tem o dinheiro a receber no precatório. |
| Originário | O processo de 1º grau que gerou o precatório. É dele que sai a íntegra para a análise jurídica. |
| Originário provável | Candidato a originário sem confirmação. Fica só no `credito_fonte.metadata.originario_provavel`, com a confiança (alta, média, baixa) e os outros candidatos. Não é ligado ao crédito (decisão de 08/10). |
| OAB | Número do advogado na Ordem dos Advogados. |
| DJEN | Diário de Justiça Eletrônico Nacional. Publica intimações com nome das partes e dos advogados, sem CPF. |
| Consulta pública | Site do tribunal que mostra o processo sem login. Alguns trazem o CPF. |
| Honorários | Precatório cujo credor é o próprio advogado ou o escritório. O artifact pede para marcar, não descartar. |
| Análise manual | O robô achou candidatos, mas não teve certeza. Grava os candidatos e deixa para uma pessoa decidir. |

## Resumo

| Tribunal | Na lista | Credor com CPF | Originário | Os dois | Só provável | Advogado com OAB | Segue o artifact? |
|---|---|---|---|---|---|---|---|
| TJAL | 9.622 | 95,9% | 97,7% | 95,4% | 0% | 97,7% | ✅ Sim |
| TJPI | 7.029 | 98,8% | 0,9% | 0,9% | 0% | 88,5% | ✅ Sim (a fonte não dá originário) |
| TJMT | 17.333 | 66,8% | 56,2% | 56,0% | 21,0% | 87,5% | 🟡 Por outro caminho, melhor |
| TJMA | 22.381 | 66,1% | 29,5% | 29,4% | 30,2% | 80,8% | ✅ Sim, e mais (Jurisconsult) |
| TJRR | 7.198 | 36,4% | 50,5% | 29,0% | 11,2% | 65,4% | 🟡 Por fontes que a ficha não conhecia |
| TJBA | 39.822 | 40,0% | 19,8% | 17,0% | 13,7% | 39,0% | ✅ Sim, e mais (CPF do PJe) |
| TJDFT | 37.200 | 48,5% | 12,2% | 12,2% | 8,8% | 63,6% | 🟡 Sem o DataJud e a SAPRE (não funcionam) |
| TJRJ | 127.378 | 21,2% | 85,5% | 21,2% | 0% | 26,6% | ✅ Sim, e mais (DJERJ, PJe, API de CPF) |

Nos não pagos (só TJRJ, TJRR e TJPI têm no banco a situação de pago): TJRJ 60.647 com credor com CPF 36,2% e
originário 98,9%; TJRR 2.701 com 52,3% e 43,1%; TJPI 4.154 com 98,0% e 1,3%.

Desde 08/10 o advogado com OAB subiu muito (TJMA 19% → 81%, TJBA 19% → 39%, TJAL 79% → 98%), e o credor com CPF do
TJRJ foi de 2% para 21% (relação do DJERJ e API de CPF).

## O que todos os robôs fazem hoje

Pedido pelo artifact (etapa comum):
- Só cumprimento de sentença contra a Fazenda, contra o mesmo ente; advogado em comum; publicação que cita o número
  do precatório = confirmado.
- **Originário provável com confiança** (`utils/originario_provavel.py`): no metadata, sem ligar ao crédito.
- **Honorários marcados**, inclusive de advogado pessoa física.
- **Repetir a busca antes de gravar "não encontrado"** (volta para a fila e só vira FALHA na 3ª vez).
- **Medição de cobertura** (`utils/cobertura.py`): total, não pagos e por ano orçamentário.

A mais (o artifact não pede):
- A fonte corrige o banco: se a fonte mostra o CPF certo da mesma pessoa, o CPF errado do banco é trocado.
- Proteção em ação coletiva: não transforma todos os autores do processo em credores do precatório de um só.
- Filtro de órgão público e de procurador do ente: prefeitura, estado ou procurador não entram como credor nem como
  advogado.
- Atualização das tabelas antigas (legado) e reserva no modo 5 (dois robôs não pegam o mesmo precatório).
- API de CPF dentro do robô (TJPI, TJRJ, TJDFT, TJRR), com a regra de segurança de `utils/cpf_robo.py`.

Diferença pequena: o artifact pede cumprimento de ano **anterior** ao do precatório; os robôs excluem só o ano
**posterior** (o mesmo ano acontece, e a consulta pública confirma as partes depois).

## Por tribunal

### TJAL — ✅

- **Artifact:** API SAPRE dá credor, CPF e originário; e-SAJ 1º grau dá o advogado e a OAB.
- **Robô:** o mesmo, e mais: e-SAJ 2º grau como segunda fonte do originário, marca de pago, prioridade, justiça
  gratuita, OAB do 1º grau pelas movimentações, procurador do ente fora dos advogados (13/10).
- **Diverge:** não.

### TJPI — ✅

- **Artifact:** DJEN pelo número do precatório dá nome e advogado; "sem rota pública" para o originário (PJe exige login
  gov.br).
- **Robô:** DJEN (credor pelo cabeçalho "REQUERENTE"), API de CPF, originário confirmado só quando a publicação cita.
- **Diverge:** não. O originário fica em 1% porque a fonte não dá.

### TJMT — 🟡 por outro caminho

- **Artifact:** iniciais → serviço de CPF → DJEN pelo nome; API do TJMT pelo número do precatório; motor DataJud.
- **Robô:** DJEN (iniciais e advogados) → API do TJMT pelos processos do advogado → originário escolhido pelas partes,
  ente e valor → CPF e nascimento do originário.
- **Por que diverge:**
  - API pelo número do precatório: 2 de 40 (o precatório é sigiloso ou não está no PJe).
  - Serviço de CPF: precisa do nome completo; só temos as iniciais. O CPF já vem do originário.
  - DataJud: não funciona (seção abaixo).

### TJMA — ✅

- **Artifact:** DJEN dá o nome; PJe 1º grau dá o CPF; originário pela etapa comum; motor DataJud; Jurisconsult "não
  testado".
- **Robô:** DJEN, PJe, rota das iniciais, advogado do DJEN, **Jurisconsult** (CPF dos precatórios antigos que não estão
  no PJe; liberado pelo jurídico em 09/10), sociedade de advogados como honorários.
- **Por que diverge:** só no DataJud (não funciona).

### TJRR — 🟡 por fontes que a ficha não conhecia

- **Artifact:** DJEN dá o credor em 35% e a OAB em ~100%; "nenhuma consulta pública mostra o precatório"; sem CPF.
- **Robô:** **Murakî** (CPF do não pago), **Projudi** pesquisado pelo CPF (originário e OAB), SGP (nome do pago), DJEN em
  todos os créditos com a guarda do SGP.
- **Por que diverge:** a ficha não tinha rota para CPF e originário; essas fontes dão.

### TJBA — ✅

- **Artifact:** nome da lista; DJEN pelo nome e pelo advogado; **não automatizar o PJe** por causa do captcha.
- **Robô:** DJEN como pedido, advogado do DJEN gravado, honorários, e o **PJe com o captcha resolvido**, que confirma as
  partes e dá o CPF.
- **Por que diverge:** a ficha diz que não há rota para o CPF. O PJe é essa rota.

### TJDFT — 🟡 sem o DataJud e a SAPRE

- **Artifact:** motor DataJud (classe 12078); API do PJe dá CPF e OAB; SAPRE (entidade + CPF) confirma.
- **Robô:** **DJe do TJDFT** (nome e CPF até 2021), DJEN, API do PJe, API de CPF, honorários.
- **Por que diverge:**
  - DataJud: só 21% dos originários estão lá.
  - SAPRE: reCAPTCHA a cada busca e não mostra o originário.
  - Consulta do PJe por nome: não mostra processos arquivados.

### TJRJ — ✅

- **Artifact:** originário da lista; API DCP dá nome, advogado e vínculo; sem CPF; DJEN para quem não tem originário.
- **Robô:** DCP, e mais: PJe (CPF quando o originário é `08…`), eJUD 2º grau, **relação do DJERJ** (+16 mil CPFs),
  API de CPF, não rebaixa o status de quem já tem credor com CPF (13/10).
- **Por que diverge:** DJEN para os sem originário achou 0 de 76 (são pagos de 1997 a 2012).
- **O que falta (não pagos sem credor com CPF, ~38,7 mil):** 17.970 ações coletivas sem fonte que diga qual autor é o
  credor; 13.482 com nome sem CPF (a API recusou 10.165); 3.270 possíveis honorários; o resto é órgão público, segredo
  de justiça ou análise manual. A página do precatório no eJUD dá só o advogado ("Informações sigilosas") e o DJEN
  pelo CNJ do precatório não tem publicações.

## Fontes do artifact que não foram usadas

| Fonte | Tribunais | Resultado do teste | Vale implementar? |
|---|---|---|---|
| Motor DataJud | TJMT, TJMA, TJDFT, TJRJ | Originário com expedição perto da data: 8% (TJMT/TJMA); 21% dos originários na base (TJDFT); prova real 0 acertos | Não para achar. Talvez para desempatar candidatos (não testado) |
| SAPRE (entidade + CPF) | TJDFT | Captcha a cada busca; não dá originário | Não |
| API pelo número do precatório | TJMT | 2 de 40 | Não |
| Serviço de CPF pelas iniciais | TJMT | A API precisa do nome completo | Não |
| Lista SRP (nome abreviado, pago) | TJMT | Endereço antigo fora do ar (503); portal novo não mapeado | Baixo ganho (é dado de lista) |
| DJEN para quem não tem originário | TJRJ | 0 de 76 | Não (a relação do DJERJ cobre) |

## DataJud: não funciona do jeito que o artifact propõe

Testado em 08/10/2026 no TJDFT, no TJMT e no TJMA.

**O que o artifact propõe.** Quando o tribunal não dá o originário:
1. pegar no DataJud os processos de cumprimento de sentença contra a Fazenda;
2. achar neles o movimento de "expedição de precatório" perto da data do precatório;
3. abrir os candidatos na consulta pública para confirmar o credor.

O DataJud é uma base pública do CNJ. Tem número, classe, vara e movimentações, mas **não tem nome nem CPF das partes**
e não tem campo que ligue o precatório ao originário.

**Calibragem (400 precatórios com o originário conhecido, por tribunal):**

| | TJDFT | TJMT | TJMA |
|---|---|---|---|
| Processos de cumprimento contra a Fazenda no DataJud | 95.820 | 616.225 | — |
| Originário certo está no DataJud | **83 de 400 (21%)** | 397 de 400 | 395 de 400 |
| Originário tem a marca de expedição | 7 de 400 | 81 de 400 (20%) | 78 de 400 (20%) |
| Marca perto da data do precatório | 2 de 400 | 32 de 400 (8%) | 33 de 400 (8%) |

**Prova de conceito:** no TJMT cada precatório sem originário teve de 3 a 116 candidatos, abertos na consulta pública
(até 40 por precatório), e nenhum era o originário certo. O DataJud ainda deu vários erros 504 e 429.

**Por que não funciona:** no TJDFT a maioria dos originários nem está lá; no TJMT e no TJMA a marca de expedição quase
nunca é lançada, então o filtro deixa o originário certo de fora. Sem o filtro sobram centenas de candidatos por
precatório, cada um uma consulta ao tribunal (no TJMA seriam ~2,7 milhões de consultas).

**Onde ainda pode ajudar:** só como desempate, quando o robô já tem 2 a 5 candidatos (os ~20 mil "só provável"):
pelo número, o DataJud mostra classe, ano, vara e movimentos e descarta os que não servem. Não testado.

## Pendências

- Feito em 13/10: limpeza de 2 vínculos de procurador do ente no TJAL, 113 nomes com "REPRESENTA:"/"PROCURADOR:"
  colado e 1 procuradora do Estado no TJRR (desfazer em `TJAL/saida` e `TJRR/saida`).
- Ainda para rodar: `TJMA/reabrir_sociedade_advogados.py --aplicar` (394 FALHA), `TJRJ/reabrir_nao_encontrado.py
  --aplicar` (73) e o `TJRJ/gravar_relacao_djerj.py` de novo (98 que deram deadlock ou foram pulados).
- Nas máquinas: ddddocr e a extensão Buster para o Jurisconsult (TJMA); atualizar a RECALL-0162 (TJDFT antigo) e
  reabrir os 3.581 créditos do TJDFT (`python TJDF/reabrir_data_migracao.py --com-desfazer`).
- Para avisar: a regra do banco que liga todo o polo ativo nos coletivos (TJBA 15 créditos desde 08/10) e os 813
  credores do TJPI com nome colado ("FULANA (REQUERENTE) BELTRANO"), vindos do robô antigo.
- Melhorias possíveis: TJRJ honorários pelo advogado da página do precatório (até 3.270); TJRJ peças do precatório
  (ofício requisitório, se abrir sem login); TJMA DataJud como desempate (teste de 200 casos).
