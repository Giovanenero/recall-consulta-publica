# TJBA — achar o originário pesquisando o PJe por NOME

Investigação de 30/09/2026 sobre os créditos do TJBA que ficam sem credor.
Nada aqui altera o `fetch_TJBA.py`: são **testes de leitura** e a medição que
justifica (ou não) mexer nele depois.

---

## O problema, em números

Base do TJBA: **40.378 créditos**.

| campo | temos |
|---|---|
| Número do precatório, ente devedor, ano, natureza, data | 100% |
| Valor | 97,7% |
| **Processo originário** | **17,8%** (7.204) |
| **Credor** | **17,8%** (7.173) |
| **CPF/CNPJ** | **16,2%** (6.535) |
| Advogado | 17,7% |

Os quatro últimos andam juntos de propósito: **não são quatro problemas, é um**.
Assim que o originário aparece, o credor vem em 99,6% dos casos, o advogado em
99% e o CPF em 91%. Achar o originário é a tarefa inteira.

O robô já passou uma vez pela base:

```
SUCESSO   13.069   56%
FALHA     10.240   44%
PENDENTE  17.069
```

E as falhas:

```
6.244  PROCESSO_NAO_ENCONTRADO
1.915  SESSAO_EXPIRADA_SEM_RELOGIN   <- técnico: recupera só reprocessando
1.034  SEM_CPF_CREDOR
  536  SEM_BENEFICIARIO
```

Dentro das 6.244: **4.380** sem candidato no DJEN nem pista, **1.008** não
achadas no PJe, **857** com candidato recusado pela regra.

---

## Por que o robô perde esses casos

O fluxo de hoje é:

```
nome do credor (banco)
   -> DJEN procura processos dessa pessoa      <- o gargalo
   -> candidatos
   -> PJe pelo NÚMERO confirma
   -> CPF
```

O DJEN cobre de meados de 2025 em diante. **Originário anterior a isso não
existe lá**, e o robô encerra como "nenhum candidato".

Uma hipótese testada e descartada: achei que faltasse o nome do credor. Não
falta — `requerentes` está preenchido em 40 de 40 da amostra. O nome está lá; o
que falta é onde procurar com ele.

---

## A rota nova

A tela de pesquisa do PJe público aceita muito mais do que o robô usa:

```
Processo · Processo referência · Nome da Parte · Nome do advogado
Classe judicial · CPF CNPJ · OAB · Data de autuação
```

O robô só preenche **Processo**. O campo `fPP:dnp:nomeParte` e o
`fPP:dpDec:documentoParte` nunca são tocados (conferido por busca no código: a
única ocorrência de `nomeParte` no `fetch_TJBA.py` é a do **DJEN**, na linha
365).

A rota proposta inverte a ordem:

```
nome do credor (banco)
   -> PJe procura PELO NOME      <- acervo inteiro, sem recorte de data
   -> escolhe o originário
   -> CPF
```

O DJEN não some: continua bom para desempatar quando aparecem vários
candidatos.

### O que o detalhe devolve

Tudo o que o robô procura, numa página só:

```
A. M. S. S.   REQUERENTE  ATIVO    CPF  098.***.***-15
C. T. G. S.      ADVOGADO    ATIVO    OAB BA 36025
SECRETARIA DA SAÚDE DA BAHIA    REQUERIDO   PASSIVO  CNPJ 00.***.***/0001-**
ESTADO DA BAHIA                 REQUERIDO   PASSIVO  CNPJ 00.***.***/0001-**

classe: CUMPRIMENTO DE SENTENÇA CONTRA A FAZENDA PÚBLICA · autuado 15/03/2013
```

CPF **completo, sem máscara** — do credor e do advogado. Vale registrar o
contraste com o TJSP, onde nenhuma via pública expõe documento (vedação do
art. 12, §3º da Resolução CNJ 303/2019). Na Bahia o PJe mostra.

A lista de resultados, porém, é pobre: só **Processo** e **Última
movimentação**. Todo o valor está no detalhe, e abrir detalhe é o que custa
tempo.

---

## A medição

Critério de ACERTO, deliberadamente apertado. Um caso só conta quando existe um
processo que:

- é de 1º grau do TJBA (foro != 0000) e não é o próprio precatório;
- não é mais novo que o precatório;
- tem o **credor no polo ATIVO** e não como advogado;
- tem **ente público no polo PASSIVO**;
- e traz o CPF/CNPJ do credor.

Falha de rede do PJe conta em `INSTAVEL` e **fica fora do denominador**.
Misturar instabilidade com "não encontrado" é o erro que faz uma rota boa
parecer ruim.

### O resultado — e o erro de método que quase o enterrou

A primeira medição deu **15%**. Estava errada: foi feita sobre o **resíduo** —
os 4.380 créditos onde o robô **já tinha tentado e desistido**. Por construção é
o grupo mais difícil da base. Comparar isso com a taxa geral do robô é comparar
populações diferentes.

Refeita sobre a fila de verdade (créditos nunca tentados, `status_id = 1`), a
mesma rota e o mesmo código:

| população | acerto |
|---|---|
| resíduo (o robô já desistiu) | 2/13 = **15%** |
| fila de verdade (nunca tentados) | 30/35 = **86%** |

Comparando com a mesma população:

```
robô hoje (DJEN + PJe por número)   56%
busca por nome no PJe               86%
```

Exemplos de acerto, com o originário e o documento:

```
C. C. X.        8003646-40.2018.8.05.0193   CPF 128.***.***-44
M. C. S.        8001145-39.2019.8.05.0271   CPF 726.***.***-25
C. R. S.   0000012-42.1986.8.05.0114
P. P. S.        0700012-16.1967.8.05.0001   CPF 541.***.***-68
```

**1986 e 1967** — décadas fora do alcance do DJEN. É exatamente o buraco que a
rota fecha.

### Limites do número

- **35 casos respondidos.** A margem é larga; o valor real está em algum ponto
  entre ~70% e ~95%. Antes de mexer no robô, vale rodar ~100.
- **Empresa não funciona.** Os poucos erros são todos pessoa jurídica —
  `CLARO S.A.` trouxe 30 processos, `ARCELORMITTAL` idem, e nenhum critério
  simples separa qual é o certo. Pessoa física acertou quase tudo.
- **O PJe do TJBA anda instável** (pesquisa que não volta em 90 s). Perdeu-se de
  12 a 15% da amostra por isso, em dias diferentes.

---

## O que foi testado e NÃO serve

| fonte | veredito |
|---|---|
| **DJEN pelo número do precatório** | 55% têm publicação, mas **0%** traz "Processo de Origem" (diferente do TJSP). E só 25% trazem nome completo de pessoa — o resto vem `A. M. D. S. E. S.`; os nomes cheios são empresas e órgãos |
| **DataJud (CNJ)** | devolve o precatório com classe, órgão e movimentos, **sem partes e sem origem**. O único CNJ no retorno é o do próprio precatório |
| **PJe 1º grau, pelo número do precatório** | 12 de 12 `NAO_ENCONTRADO` |
| **PJe 2º grau** (`pje2g.tjba.jus.br`) | 12 de 12 `NAO_ENCONTRADO`, inclusive um precatório que o próprio robô resolveu. Precatório do TJBA tramita no Núcleo Auxiliar de Conciliação e Precatórios, fora da consulta aberta |
| **Lista Unificada** (`listaprecatorios.tjba.jus.br`) | atrás de reCAPTCHA, e os filtros são devedor / número / natureza / ordem / superpreferência — **sem nome e sem CPF**. Não traz credor. A API é `listaprecatoriosws.tjba.jus.br`; só `/api/entidade-devedora/` responde aberto (416 entidades) |

Registrado para ninguém refazer.

---

## Ainda não testado

**Busca por CPF no PJe** (`fPP:dpDec:documentoParte`). Para os **6.535**
créditos onde já temos o documento, ela é exata — sem risco de homônimo, que é
justamente o que derruba os casos de empresa. É o próximo passo óbvio.

**Originário que já está na lista antiga.** `numero_originario` está preenchido
em 8.503 das 45.556 linhas do TJBA (19%), e — ao contrário do TJPR — **sem
máscara e sem repetir o próprio precatório**: zero em ambas as verificações. Os
números são plausíveis, de 1º grau e foro real. Vale medir quantos desses ainda
não foram aproveitados: seria dado de graça, já no banco.

---

## Os scripts

Todos **só leem**. Nenhum grava no banco, nenhum altera o `fetch_TJBA.py`; eles
importam a classe `Pje` dele, então herdam o captcha, o perfil do Chrome e o
tratamento de instabilidade.

| script | o que faz |
|---|---|
| `teste_busca_por_nome.py` | prova de conceito: pesquisa 5 nomes e lista os candidatos |
| `teste_campos_por_nome.py` | mostra QUAIS campos a busca e o detalhe devolvem |
| `medir_busca_por_nome.py` | **a medição**: taxa de acerto com critério apertado |
| `teste_precatorio_no_pje.py` | testa o precatório no PJe de 1º grau (deu 0%) |
| `teste_pje_2grau.py` | idem no 2º grau, trocando só `BASE`/`URL` (deu 0%) |

```bash
python TJBA/medir_busca_por_nome.py 60 pendente   # fila de verdade
python TJBA/medir_busca_por_nome.py 25 residuo    # o que o robô já descartou
```

O segundo argumento existe justamente para não repetir o erro de medir a rota
no grupo errado.

Saídas em `TJBA/saida/` (fora do git: têm CPF).
