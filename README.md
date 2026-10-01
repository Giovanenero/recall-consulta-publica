# recall-consulta-publica

Robôs que buscam, nas **consultas públicas** dos tribunais (sem login), o **credor** (nome + CPF/CNPJ) e o
**processo originário** dos precatórios que estão nas listas de ordem cronológica, e gravam o resultado no Postgres
da Recall (schema `creditos` e tabelas legadas). Cada tribunal tem uma pasta com o robô e um `README.md` próprio:

| Pasta | Robô | Fontes consultadas | Grava no banco? | Documentação |
|---|---|---|---|---|
| `TJAL/` | `fetch_TJAL.py` | API do SAPRE + e-SAJ (1º e 2º grau) | Sim | [TJAL/README.md](TJAL/README.md) |
| `TJBA/` | `fetch_TJBA.py` | API do DJEN + PJe 1º grau (captcha Tencent) | Sim | [TJBA/README.md](TJBA/README.md) |
| `TJRN/` | `esteira_TJRN.py` + `fetch_TJRN.py` | PJe 1º grau (Akamai + reCAPTCHA) | **Não** (só CSV) | [TJRN/README.md](TJRN/README.md) |
| `TJMA/` | `fetch_TJMA.py` | — | — | arquivo vazio (ainda não começou) |
| `TJRJ/` | `fetch_TJRJ.py` | API da consulta processual (DCP) + PJe 1º grau + eJUD 2º grau (Chrome por CDP) + portal de precatórios, sem A3 | Sim | [TJRJ/README.md](TJRJ/README.md) |
| `TJMT/` | `fetch_TJMT.py` | API do DJEN (iniciais do credor e advogados) + API da consulta processual do TJMT (processos do advogado, com CPF), sem A3 | Sim | [TJMT/README.md](TJMT/README.md) |
| `utils/` | código comum | — | — | abaixo |

`utils/cloudflare.py` é um teste à parte (Turnstile do eproc da JFRJ) e não é usado pelos robôs.

## Convenções comuns

- **Execução**: `python <PASTA>/<robo>.py` a partir da raiz ou de dentro da pasta. Cada robô põe a raiz do projeto no
  `sys.path` para importar `utils/`.
- **Simulação x gravação**: TJAL e TJBA têm `--simulacao`, que faz tudo (inclusive as escritas no banco) e dá
  `ROLLBACK` no fim — use sempre antes de uma gravação real. Sem a flag, o robô grava (`COMMIT`).
- **Credenciais**: `.env` na raiz (modelo sem valores em `.env.shared`): `PG_HOST`, `PG_PORT`, `PG_DATABASE`,
  `PG_USER`, `PG_PASSWORD`; `LOTE_GRAVACAO_TJMA` (TJMA); `LOTE_GRAVACAO_TJRJ` (TJRJ); `LOTE_GRAVACAO_TJMT` (TJMT). `PROXY_01..05` (`host:porta:usuário:senha`, saindo pelo Brasil): saídas extras para o
  DJEN no TJMA (`--proxies`) e nos workers 2 em diante do TJBA (`--workers N` num terminal só, ou `--worker N`).
- **Log padrão** (`utils/log.py`): mesma linha no terminal e em `<PASTA>/saida/logs/<robo>_AAAAMMDD.log`:
  ```
  2026-09-28 09:48:47 | INFO    | fetch_TJBA | [5] 921778 8045268-52.2025.8.05.0000 -> SUCESSO_PARTES_SEM_VALOR ...
  ```
  `INFO` = andamento; `WARNING` = algo que volta a ser tentado (adiado, bloqueio passageiro, Chrome reaberto);
  `ERROR` = o robô parou ou uma gravação falhou. Erro inesperado leva o traceback para o log.
- **Saídas** em `<PASTA>/saida/` (fora do git: têm CPF/CNPJ). CSV sempre com `;` e UTF-8 com BOM (abre direto no
  Excel). Perfis do Chrome em `<PASTA>/.chrome-profile-*` (fora do git: cookies e sessão).
- **Nunca cria crédito**: os robôs só enriquecem créditos que já existem em `creditos.credito`; antes de chamar
  `creditos.registrar_credito` conferem que o número normalizado bate com o crédito (senão a função criaria outro).
- **Tabelas legadas**: toda mudança nelas gera backup do "antes" e um `desfazer_*.sql` que devolve o estado anterior.

## utils/

| Módulo | O que tem |
|---|---|
| `log.py` | `configurar_log(script, pasta_logs)`: liga terminal + arquivo no formato acima |
| `banco.py` | `conectar(aplicacao, escrita=False, worker="")` (leitura = sessão readonly em autocommit; escrita = transação manual), `como_dicts`, `partes_do_banco`, `chave_texto_lote`, `filas_antigas`, `id_do_software` |
| `texto.py` | `so_digitos`, `formatar_cnj`, `documento_valido` (dígito verificador de CPF/CNPJ) |
| `arquivos.py` | `gravar_csv` (sobrescreve) e `anexar_csv` (acrescenta; cabeçalho só em arquivo novo; arquivo que já existe segue o cabeçalho dele) |
| `workers.py` | vários workers num terminal: `supervisionar` (`--workers N`), `travar_worker` (um processo por número), `ParadaSuave` (Ctrl+C vira pedido de parada) e `matar_chrome_do_perfil` (Chrome órfão de um perfil) |

A comparação de nomes (`normal`, `chave`, `mesmo_nome`...) fica em cada robô: cada tribunal escreve os nomes de um
jeito e as regras não são iguais.

## Banco: o que os robôs usam

- `creditos.credito` (o precatório = "lead"), `creditos.lista_item` (linha da lista cronológica: beneficiário, valor,
  ente), `creditos.coleta_credor` (fila do modo credor: status, lease, software dono) e
  `creditos.coleta_credor_tentativa` (histórico de cada tentativa).
- `creditos.processo` / `processo_parte` / `pessoa` / `pessoa_oab` (capa e partes), `creditos.credito_originario`
  (precatório -> originário), `creditos.credito_credor` (credor/advogado ligado ao crédito),
  `creditos.credito_fonte` (o que cada software sabe do crédito: etapa, situação, `metadata` JSON).
- Funções de escrita: `registrar_credito` (fonte, originários, metadata; cria o crédito se não existir — por isso a
  conferência antes), `registrar_capa` (troca o conjunto de partes do processo e recalcula os credores dos créditos
  ligados), `registrar_credor`, `fila_credor_pegar` / `_finalizar` / `_adiar` / `_liberar` /
  `_registrar_originario` / `_expirar_leases`.
- Legado: `listas_primarias.processos_unificados_AAAA_MM` (filas mensais do RPA antigo; os robôs mexem de
  `2026_08` em diante), `precatorios.*` (capa antiga do precatório, TJAL) e `originarios.*` (capa antiga do
  originário, TJBA).
- Status (`creditos.status_coleta`): `PENDENTE`, `EM_ANDAMENTO`, `SUCESSO_PROCESSO_CREDITO` (credor achado na capa
  do precatório), `SUCESSO_PROCESSO_ORIGINARIO` (no originário, com evidência forte), `SUCESSO_PARTES_SEM_VALOR`,
  `SUCESSO_ANALISAR`, `SUCESSO_API_TERCEIRO`, `FALHA`. Motivos em `creditos.motivo_coleta` (`SEM_CPF_CREDOR`,
  `PROCESSO_NAO_ENCONTRADO`, `REQTE_ORGAO_PUBLICO`, `SEGREDO_DE_JUSTICA`, `PROC_SEM_CAPA`, `CAPTCHA_REATIVADO`...).

## Glossário

- **Precatório**: requisição de pagamento contra o poder público; na lista cronológica aparece com o número do
  processo do precatório, o ente devedor, o beneficiário e o valor.
- **Originário**: o processo de conhecimento/execução (1º grau) que gerou o precatório. É lá que aparecem as partes
  com CPF/CNPJ.
- **Credor / beneficiário**: quem tem a receber (polo ativo). **Ente / devedor**: estado, município, autarquia
  (polo passivo).
- **Capa**: cabeçalho do processo na consulta pública (classe, assunto, órgão julgador, partes e advogados).
- **e-SAJ / PJe**: sistemas processuais dos tribunais (TJAL usa e-SAJ; TJBA e TJRN, PJe).
- **SAPRE**: sistema de precatórios do TJAL (API pública com credor e CPF/CNPJ).
- **DJEN**: Diário de Justiça Eletrônico Nacional (API pública do CNJ com as publicações e as partes por polo).
- **Lease**: reserva temporária de um crédito na fila para um worker (outro worker não pega enquanto vale).
