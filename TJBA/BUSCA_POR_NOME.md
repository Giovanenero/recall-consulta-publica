# TJBA — achar o originário sem depender do DJEN

Investigação de 30/09 e 01/10/2026. Nada aqui altera o `fetch_TJBA.py`: são
**scripts de leitura** e as medições que justificam (ou não) mexer nele depois.

> **Se for ler uma coisa só, leia "O número que importa" e "Como medir sem se
> enganar".** O resto é o caminho até lá.

---

## O problema, em números

Base do TJBA: **40.378 créditos**.

| campo | temos |
|---|---|
| Número do precatório, ente devedor, ano, valor | 100% |
| **Processo originário** | **18%** (7.256) |
| **Credor** | **18%** (7.224) |
| **CPF/CNPJ** | **16%** (6.586) |
| Advogado | 18% |

Os quatro últimos andam juntos de propósito: **não são quatro problemas, é um**.
Com o originário, o credor vem em 99,6% dos casos, o advogado em 99% e o CPF em
91%. Achar o originário é a tarefa inteira.

Estado da fila (software `CONSULTA_PUBLICA_TJBA`):

```
PENDENTE                      17.069    nunca tentados
FALHA                         10.240
SUCESSO_PARTES_SEM_VALOR       7.413
SUCESSO_ANALISAR               5.103    achou, não desempatou, espera humano
SUCESSO_PROCESSO_ORIGINARIO      553
```

Taxa real do robô sobre o que já processou: **56% de sucesso**.

Dentro das 10.240 falhas:

```
6.244  PROCESSO_NAO_ENCONTRADO
1.915  SESSAO_EXPIRADA_SEM_RELOGIN   <- técnico: recupera só reprocessando
1.034  SEM_CPF_CREDOR
  536  SEM_BENEFICIARIO
```

E dentro das 6.244: **4.380** sem candidato no DJEN nem pista, **1.008** não
achadas no PJe, **857** com candidato recusado pela regra.

---

## Por que o robô perde esses casos

```
nome do credor (banco)
   -> DJEN procura processos dessa pessoa      <- o gargalo
   -> candidatos
   -> PJe pelo NÚMERO confirma
   -> CPF
```

O DJEN cobre de meados de 2025 em diante. **Originário anterior não existe lá**,
e o robô encerra como "nenhum candidato".

Uma hipótese testada e descartada: achei que faltasse o nome do credor. Não
falta — `requerentes` está preenchido em 40 de 40 da amostra. O nome está lá; o
que falta é onde procurar com ele.

---

## A rota nova: perguntar ao tribunal, não ao diário

A tela de pesquisa do PJe público aceita muito mais do que o robô usa:

```
Processo · Processo referência · Nome da Parte · Nome do advogado
Classe judicial · CPF CNPJ · OAB · Data de autuação
```

O robô só preenche **Processo**. Os campos `fPP:dnp:nomeParte` e
`fPP:dpDec:documentoParte` nunca são tocados — a única ocorrência de `nomeParte`
no `fetch_TJBA.py` (linha 365) é do **DJEN**.

```
nome do credor (banco)
   -> PJe procura PELO NOME      <- acervo inteiro, sem recorte de data
   -> escolhe o originário
   -> CPF
```

O DJEN não some: continua ótimo para desempatar.

### O que o detalhe devolve

```
A. M. S. S.                     REQUERENTE  ATIVO    CPF  098.***.***-15
C. T. G. S.                     ADVOGADO    ATIVO    OAB BA 36025
SECRETARIA DA SAÚDE DA BAHIA    REQUERIDO   PASSIVO  CNPJ 00.***.***/0001-**
classe: CUMPRIMENTO DE SENTENÇA CONTRA A FAZENDA PÚBLICA · autuado 15/03/2013
```

CPF completo, sem máscara — do credor e do advogado. Contraste com o TJSP, onde
nenhuma via pública expõe documento (art. 12, §3º da Resolução CNJ 303/2019).

A **lista** de resultados é pobre: só Processo e Última movimentação. Todo o
valor está no detalhe, e abrir detalhe é o que custa tempo.

---

## O número que importa

Medido contra GABARITO — créditos que já têm originário conhecido no banco, para
comparar a escolha com a resposta certa.

```
decisões tomadas     98
acertos              98
FALSO POSITIVO        0

PRECISÃO    100%   (quando decide, acerta)
COBERTURA    51%   (resolve metade; na outra metade, cala)
```

**A precisão é o ativo, não a cobertura.** Crédito ligado ao processo errado
traz o CPF de outra pessoa e entra na base com cara de dado bom. Não decidir
custa um lead; decidir errado contamina a base. Qualquer mudança futura precisa
manter o zero.

### Como ele decide

Nenhum sinal decide sozinho, porque nenhum prova sozinho:

- o **nome** não basta: existe homônimo, e a mesma pessoa costuma ter várias
  ações contra a Fazenda;
- a **OAB** não basta: o mesmo advogado pode ter entrado com DUAS ações
  diferentes para o MESMO cliente;
- o **estrutural** (credor no ativo, ente no passivo) não basta: casa com
  qualquer ação dela contra ente público.

Daí a pontuação com corroboração:

| sinal | peso |
|---|---|
| valor do precatório ou o próprio número citados na publicação | 4 |
| advogado em comum com o precatório | 2 |
| credor no polo ativo + ente no passivo | 1 |
| ação anterior ao precatório | 1 |

Aceita com **3 pontos** — ou a prova dura, ou dois indícios independentes. E
exige **vantagem**: o primeiro tem de estar à frente do segundo; empate no topo
significa dois candidatos igualmente plausíveis, e aí não se escolhe.

Candidato único exige 2: com um só na mesa não há com quem confundir, então o
risco de *escolher errado* não existe.

O que decidiu, na prática:

```
oab + estrutura + data          30
oab + data                      12
forte + oab + estrutura + data   6
forte + estrutura + data         2
```

**A OAB aparece em quase tudo.** A evidência que o robô usa hoje — valor ou
número citados — resolveu só 8 de 50: é rara demais para carregar sozinha.

---

## Como medir sem se enganar

Três medições deram errado antes de uma dar certo, e **os três erros inflavam o
resultado**. Vale mais que o número final:

**1. População errada.** Medi a rota nova sobre o RESÍDUO — os créditos onde o
robô já tinha tentado e desistido. É o grupo mais difícil por construção. Deu
15%. Na fila de verdade, o mesmo código deu 86%. Por isso
`medir_busca_por_nome.py` recebe o grupo como argumento (`residuo` | `pendente`).

**2. Sem gabarito.** Os "86%" contavam como acerto qualquer processo da pessoa
contra a Fazenda, sem verificar se era o processo CERTO. Com gabarito, o acerto
real era 32% — em 57% dos casos ele pegava outra ação da mesma pessoa.

**3. Paralelismo disfarçado de ausência.** Com 6 buscas simultâneas o PJe
devolve o formulário em branco, que parece "não encontrado". Deu 22% onde era
97%. Instabilidade **nunca** entra no denominador.

---

## Busca por CPF

Para os ~6.586 créditos onde o documento já está no banco. Medido em 20 casos
com gabarito, comparando as duas buscas no MESMO crédito:

```
CPF trouxe o certo    20/20 = 100%
NOME trouxe o certo   20/20 = 100%
candidatos por busca:  CPF 3,4  x  NOME 4,0
```

Mesmo acerto, **menos candidatos** — e candidato a menos é empate a menos, que é
onde o critério se cala. Os casos individuais mostram melhor que a média:

```
MARIA MARTINS LIMA   CPF  2  x  NOME 10
EDMILSON CHELLES     CPF  3  x  NOME  5
```

Ressalva: rodou em créditos que já têm CPF, ou seja, os que o robô já resolveu —
população mais fácil. O resultado válido aqui não é a taxa, é a **comparação**.

Conclusão: onde há documento, o CPF deve ser o primeiro caminho; o nome fica
para os outros 84%.

---

## O que foi testado e NÃO serve

| fonte | veredito |
|---|---|
| **DJEN pelo número do precatório** | 55% têm publicação, mas **0%** traz "Processo de Origem" (diferente do TJSP). Só 25% trazem nome completo de pessoa — o resto vem `A. M. D. S. E. S.`; os nomes cheios são empresas e órgãos |
| **DataJud (CNJ)** | classe, órgão e movimentos, **sem partes e sem origem** |
| **PJe 1º grau, pelo número do precatório** | 12 de 12 `NAO_ENCONTRADO` |
| **PJe 2º grau** (`pje2g.tjba.jus.br`) | 12 de 12 `NAO_ENCONTRADO`, inclusive um precatório que o robô resolveu. Precatório do TJBA tramita no Núcleo Auxiliar de Conciliação e Precatórios, fora da consulta aberta |
| **Lista Unificada** (`listaprecatorios.tjba.jus.br`) | reCAPTCHA, e filtra por devedor/número/natureza/ordem — **sem nome e sem CPF**. A API é `listaprecatoriosws.tjba.jus.br`; só `/api/entidade-devedora/` responde aberto (416 entidades) |

Registrado para ninguém refazer.

---

## O que falta para virar fluxo

- **Não está no robô.** Tudo vive nestes scripts; nenhuma linha do
  `fetch_TJBA.py` foi alterada.
- **Não grava.** Todos são só leitura.
- **49% sem resposta.** Dos 98 casos, 43 tinham o certo entre os candidatos e o
  critério calou. É o estoque a atacar — sem criar risco, porque já estão lá.
  O caminho mais promissor é refinar a **data**: hoje só compara o ano.
- **Velocidade.** ~40 s por caso (navegador + captcha). Para 33 mil créditos
  isso é mês, não semana. O robô já tem workers e proxy, mas ninguém dimensionou
  com a rota nova.
- **8.503 originários na lista antiga**, dos quais só 466 viraram vínculo. Pode
  ser dado de graça parado — não medido.
- **1.915 falhas por sessão expirada**: recupera só reprocessando.

---

## Os scripts

Todos **só leem**. Importam a classe `Pje` do `fetch_TJBA`, então herdam o
captcha, o perfil do Chrome e o tratamento de instabilidade.

| script | o que faz |
|---|---|
| `teste_busca_por_nome.py` | prova de conceito: pesquisa nomes e lista candidatos |
| `teste_campos_por_nome.py` | mostra QUAIS campos a busca e o detalhe devolvem |
| `medir_busca_por_nome.py` | taxa de acerto por critério estrutural (sem gabarito) |
| **`validar_busca_por_nome.py`** | **com GABARITO**: mede o falso positivo da busca por nome |
| **`validar_combinado.py`** | **a rota completa**: DJEN + nome + pontuação. É o que vale |
| `teste_busca_por_cpf.py` | compara busca por CPF x por nome, no mesmo crédito |
| `teste_precatorio_no_pje.py` | precatório no PJe 1º grau (deu 0%) |
| `teste_pje_2grau.py` | idem no 2º grau, trocando só `BASE`/`URL` (deu 0%) |

```bash
python TJBA/validar_combinado.py 100        # a medição que importa
python TJBA/teste_busca_por_cpf.py 20       # CPF x nome
python TJBA/medir_busca_por_nome.py 60 pendente
```

O `validar_combinado.py` gera **`saida/conferencia_manual.csv`**: uma linha por
caso com o que escolheu, a resposta conhecida, se bateu e a evidência usada —
com o número formatado, pronto para colar na consulta pública e conferir à mão.
O resultado não deve depender de acreditar no script.

Saídas em `TJBA/saida/` (fora do git: têm CPF).
