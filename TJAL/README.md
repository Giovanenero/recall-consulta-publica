# TJAL — `fetch_TJAL.py`

Enriquece os precatórios do TJAL (tribunal 102 em `creditos.tribunal`) pela consulta pública, no lugar do modo
credor do RPA antigo (RPA_SISTEMAS). Para cada lead da lista busca o **credor com CPF/CNPJ** na API do SAPRE e a
**capa** (partes, advogados com OAB, situação) do precatório e do originário no e-SAJ, e atualiza o banco.
**Não cria crédito**: só atualiza leads que já existem. O valor do precatório é ignorado.

Estado: em produção (software `CONSULTA_PUBLICA_TJAL`, id 4 em `creditos.software`). ~9,7 mil leads ativos.

## Como rodar

```bash
python TJAL/fetch_TJAL.py --simulacao   # faz tudo e dá ROLLBACK em cada lead: só gera o resumo
python TJAL/fetch_TJAL.py               # GRAVA (avisa e espera 10 s: Ctrl+C cancela)
```

- **`--fila [--limite N]`** (modo 5 do RPA_SISTEMAS): só os leads em FALHA/PENDENTE que o robô não raspou nos
  últimos 7 dias (`DIAS_FILA`), com número CNJ do TJAL que bate com o crédito, do mais prioritário e de maior valor
  para o menor. Gravando, cada lead é **reservado** antes da raspagem: `coleta_credor` em EM_ANDAMENTO com lease do
  processo (`consulta_publica_tjal:<máquina>:<pid>`, 3 h), sem trocar o software, e as linhas do precatório nas filas
  mensais em `CREDOR_EM_ANDAMENTO` com a marca do robô (o RPA com token A3 e outra máquina não o pegam; lead que o
  RPA já está processando fica de fora). No fim, ou no Ctrl+C, o lead que não virou SUCESSO volta ao status de antes.
  Se o robô cair, o lease vence em 3 h e o banco devolve o lead como PENDENTE. A fila tem pasta de rodada própria
  (`*_fila-gravacao`, `*_fila-simulacao`).
- Sem `--fila`, roda **todos** os leads do TJAL de uma vez. Uma rodada completa leva horas: o 1º grau do e-SAJ
  só aceita uma consulta a cada ~1,6 s.
- **Retomada**: a raspagem guarda um checkpoint (`_progresso.jsonl`) na pasta da execução. Se for interrompida
  (Ctrl+C, queda), rodar o mesmo comando continua de onde parou; leads que deram erro são raspados de novo. O banco só
  é tocado depois que a raspagem inteira termina.
- Ctrl+C durante a atualização do banco desfaz só o lead em andamento; os anteriores já tiveram COMMIT.
- Log: terminal e `TJAL/saida/logs/fetch_TJAL_AAAAMMDD.log`.

## Fluxo ponta a ponta

### 1. Leitura dos leads (`carregar_leads`, sessão somente leitura)

`SQL_LEADS`: uma linha por crédito de `creditos.credito` com `tribunal_id = 102`, ainda na lista
(`saiu_da_lista_em IS NULL`) e com linha na fila `creditos.coleta_credor`. Traz o que o banco já sabe, para comparar:
status/motivo da coleta, ente, devedor como está na lista, originários ligados, credor (nome e documento),
classe/órgão do processo, quantos advogados (e com OAB) e se o devedor tem CNPJ.

### 2. Consulta e processamento (`raspar_leads` -> `processar_lead`, 4 threads)

Filtros iniciais: número que não é CNJ -> `FALHA/NUMERO_NAO_CNJ`; CNJ de outro tribunal (J.TR diferente de `8.02`)
-> `FALHA/CREDITO_ORIGINADO_EM_TRIBUNAL_DISTINTO`.

1. **SAPRE** (`https://precatorios.tjal.jus.br/api/sapre/precatorios`, JSON, sem login)
   - `MapaEntidades` baixa uma vez as entidades (`GET entidades` e `GET entidades-devedoras/{id}`) e casa o nome do
     devedor da lista com a entidade da API (nome igual > palavra inteira > nome parecido; tenta também a sigla antes
     do " - ").
   - `buscar_precatorio`: `POST por-entidade?page=1` com o número do precatório para cada entidade candidata,
     conferindo os 20 dígitos do CNJ; último recurso: `GET por-nome-requerente` com o nome do credor do banco.
   - `resumo_sapre` fica com: credor (nome + CPF/CNPJ), devedor (nome + CNPJ), processos de conhecimento e de
     execução, data de cadastro, natureza (ALIMENTAR/COMUM) e comarca.
2. **e-SAJ** (`https://www2.tjal.jus.br`, HTML, sem login)
   - Capa do precatório no 2º grau (`cposg5/search.do`); se não existir lá, no 1º grau (`cpopg/search.do`). Se a
     busca cair na tela "Selecione o processo" (incidentes), abre o principal (`show.do`).
   - Originário, nesta ordem: processo de conhecimento/execução do SAPRE > "Números de 1ª Instância" da capa do
     precatório > originário já ligado no banco. Cada originário é raspado uma vez só (`CacheOriginarios`), porque
     ações coletivas geram muitos precatórios do mesmo processo.
   - `extrair_capa`: classe, assunto, área, órgão, juiz/relator, foro, distribuição, situação, etiquetas (prioridade,
     justiça gratuita, 100% digital, liminar), última movimentação. No 2º grau infere `precatorio_pago`
     (`SIM`/`PARCIAL`/`NAO`) pelas movimentações de pagamento/alvará e pelo arquivamento.
   - `partes_e_advogados`: tabela `tableTodasPartes`; o polo vem da classe da linha ou do rótulo (Reqte, Credor,
     Exeqte... = ativo; Reqdo, Devedor, Executado... = passivo). No 2º grau a OAB vem de um `<input hidden>`.
   - Ritmo (`Ritmo`, compartilhado entre as threads): 1,6 s entre consultas ao 1º grau (senão o e-SAJ bloqueia o IP
     por ~1 min com "múltiplas consultas simultâneas"); bloqueio detectado -> pausa de 45 s e repete (até 6 vezes).
     HTTP: até 4 tentativas com espera crescente para queda de conexão, timeout (75 s), 5xx e 429.
3. **Prioridade da API** (`aplicar_prioridade_api`): o SAPRE decide o credor. A parte do polo ativo que é o credor
   da API recebe o nome e o CPF dela; outro "credor" que só o e-SAJ mostra continua como parte, mas com papel `OUTRO`;
   credor da API que a capa não mostra entra no polo ativo. CPF do credor e CNPJ do devedor vêm do SAPRE.
4. **Status sugerido** (`status_sugerido`), nos códigos de `status_coleta`/`motivo_coleta`:
   `FALHA/SEGREDO_DE_JUSTICA`, `FALHA/PROCESSO_NAO_ENCONTRADO`, `FALHA/REQTE_ORGAO_PUBLICO`,
   `FALHA/SEM_CPF_CREDOR` (API sem CPF), `SUCESSO_PROCESSO_CREDITO` (credor na capa do precatório),
   `SUCESSO_PROCESSO_ORIGINARIO` (na capa do originário) ou `SUCESSO_API_TERCEIRO` (credor e CPF só pela API).
5. Cada lead vira uma linha de comparação banco x fontes (`preenche_cpf_credor`, `diverge_nome_credor`,
   `preenche_originario`, `preenche_oab`...). Um lead com erro não derruba a execução: vira `ERRO_DESCONHECIDO` e é
   raspado de novo na retomada.

### 3. Gravação no banco (`atualizar_banco` -> `atualizar_lead`)

Uma conexão de escrita; **cada lead é uma transação** (COMMIT na gravação, ROLLBACK na simulação) e cada passo roda
num `SAVEPOINT` (erro do Postgres desfaz só o passo). Documento da API com dígito verificador errado nunca é gravado.

| Passo | O que faz | Onde |
|---|---|---|
| `garantir_software` | cadastra `CONSULTA_PUBLICA_TJAL` se faltar (`raspa_credor = false`) | `creditos.software` |
| `situacao_e_originario` (`registrar_fonte`) | confere que o número bate com o crédito e chama `registrar_credito(PRECATORIO, TJAL, origem RASPAGEM)` com os originários (SAPRE + raspagem), `etapa` = status sugerido, `situacao` (`PAGO`, `PAGO_PARCIAL`, `ARQUIVADO`, `SUSPENSO`, `EM_ANDAMENTO`) e `metadata` (id SAPRE, natureza, capas) | `credito_fonte`, `credito_originario`, `processo` |
| `partes_precatorio` / `partes_originario` (`registrar_capa`) | partes e advogados de cada capa. Só advogados do lado do credor (o procurador do ente viraria "advogado do lead"). Como a função troca o conjunto de partes, `juntar_com_banco` completa CPF/OAB faltantes pelo banco (mesmo nome) e devolve as partes que o banco tinha e a raspagem não trouxe — menos o credor que a API desmente. Pula a capa do precatório se o credor é órgão público | `processo`, `processo_parte`, `pessoa`, `pessoa_oab`, recálculo de `credito_credor` |
| `status` (`melhorar_status`) | só `FALHA`/`PENDENTE` -> `SUCESSO_*`, e só com CPF/CNPJ válido da API: `fila_credor_finalizar` (worker `consulta_publica_tjal`, sistema ESAJ, via `PROCESSO_CREDITO`/`ORIGINARIO`/`CONSULTA_PUBLICA`) | `coleta_credor`, `coleta_credor_tentativa` |
| `fila_antiga_AAAA_MM` (`atualizar_fila_antiga`) | em cada fila mensal do RPA (de 2026_08 em diante, com permissão de UPDATE): credor da API em `requerentes` (se era outra pessoa) e status legado `CREDOR_SUCESSO_*` (se estava vazio ou `CREDOR_FALHA*`). Linha `CREDOR_EM_ANDAMENTO` não é tocada | `listas_primarias.processos_unificados_AAAA_MM` |
| `capa_antiga_precatorio` (`atualizar_capa_antiga`) | capa antiga do precatório: credor da API (nome + CPF; tira outros "requerentes"), OAB dos advogados onde está vazia e CNPJ do devedor se vazio. Sem isso o espelho das capas antigas traria o credor errado de volta | `precatorios.processos_precatorios`, `partes_processuais`, `advogados` |

Casos para conferência humana vão para `revisao.csv`: `CREDOR_ORGAO_PUBLICO`, `DOCUMENTO_INVALIDO_API`,
`API_E_ESAJ_DISCORDAM`, `CONFLITO_NOME_CPF` (o CPF da API já está no banco com outro nome), `DOIS_CREDORES`.

### 4. Saídas

Pasta `TJAL/saida/AAAAMMDD_HHMM_<gravacao|simulacao>/`: `processos.csv`, `partes.csv`, `advogados.csv`,
`leads_enriquecimento.csv` (escrito por último: marca a raspagem como completa), `acoes.csv`,
`credores_trocados.csv`, `revisao.csv`, `backup_*.csv` e, na gravação com `--com-desfazer`,
`desfazer_tabelas_antigas.sql` (devolve filas mensais e capa antiga ao estado anterior). **No fim da execução os CSVs e
o checkpoint são apagados**; fica só o SQL de desfazer, quando pedido. Desde 07/10/2026 o padrão é não escrever o
desfazer; `--sem-desfazer`, que os ciclos do modo 5 passam, continua aceito. O resumo (contagens por status, por ação
e por fila) sai no log.

## Tecnologias

- Python 3 (testado no 3.14), `requests` (uma `Session` por thread), `BeautifulSoup` (`html.parser`),
  `concurrent.futures.ThreadPoolExecutor` (4 workers), `difflib.SequenceMatcher` (nomes parecidos),
  `psycopg2` (Postgres), `python-dotenv`.
- Sem navegador: SAPRE é API JSON e o e-SAJ público responde HTML direto.
- Postgres: funções `creditos.registrar_credito`, `registrar_capa`, `fila_credor_finalizar`, `chave_texto`,
  `cnj_normalizar`; tabelas legadas `listas_primarias.*` e `precatorios.*`.

## Mapa do código

| Seção | Funções |
|---|---|
| utilitários | `numero_base`, `normal`, `chave`, `nome_chave`, `mesmo_nome`, `mesma_pessoa_grafia`, `nome_compacto_igual`, `orgao_publico` |
| HTTP | `requisitar` (retentativas) |
| banco (leitura) | `SQL_LEADS`, `carregar_leads` |
| SAPRE | `sapre`, `MapaEntidades`, `buscar_precatorio`, `resumo_sapre` |
| e-SAJ | `Ritmo`, `consultar`, `abrir`, `rotulos`, `numeros_primeira_instancia`, `partes_e_advogados`, `movimentacoes`, `extrair_capa`, `raspar_capa` |
| raspagem | `aplicar_prioridade_api`, `CacheOriginarios`, `status_sugerido`, `linhas_das_capas`, `processar_lead`, `raspar_leads` |
| gravação | `registrar_fonte`, `juntar_com_banco`, `registrar_capa`, `melhorar_status`, `atualizar_fila_antiga`, `atualizar_capa_antiga`, `sql_para_desfazer`, `atualizar_lead`, `atualizar_banco` |
| execução | `pasta_da_execucao`, `limpar_pasta`, `executar`, `main` |

## Cuidados

- Sempre rode `--simulacao` antes de uma gravação e confira o resumo (principalmente `leads_com_credor_trocado` e
  `revisao::*`).
- Não rode duas instâncias ao mesmo tempo: o limite do e-SAJ 1º grau é por IP e o ritmo só é controlado dentro do
  processo.
- Filas mensais sem permissão de UPDATE e o schema `precatorios` sem permissão são pulados (aparece no log e no
  resumo), não dão erro.
