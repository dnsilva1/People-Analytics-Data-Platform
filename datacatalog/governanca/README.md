# Data Catalog & Governança — People Analytics

## 1. Objetivo

Documentar os metadados das tabelas da camada Gold do projeto People Analytics Data Platform, facilitando a compreensão, a descoberta e o uso dos dados para análises e tomada de decisão.

## 2. Ambiente

- **Catálogo:** `people_analytics`
- **Schemas do projeto:** `bronze`, `silver`, `gold` e `monitoring`
- **Tecnologia:** Databricks e Unity Catalog
- **Escopo desta etapa:** documentação das tabelas e colunas da camada Gold.

## 3. Tabelas documentadas

### Dimensões

| Tabela | Finalidade |
|---|---|
| `gold.dim_calendario` | Atributos temporais para análises por dia, mês, trimestre e ano. |
| `gold.dim_colaborador` | Atributos cadastrais, demográficos, profissionais e salariais dos colaboradores. |

### Tabelas fato

| Tabela | Finalidade |
|---|---|
| `gold.fato_absenteismo` | Análise das ocorrências de absenteísmo. |
| `gold.fato_ferias` | Análise dos períodos de férias dos colaboradores. |
| `gold.fato_turnover` | Análise dos registros de desligamento. |

## 4. Resultado da documentação

- 5 tabelas Gold documentadas.
- 47 colunas documentadas.
- Comentários registrados no Unity Catalog.
- Regras de negócio relevantes revisadas com base nos notebooks de transformação.

## 5. Regras de negócio documentadas

### Dimensão colaborador

- **Tempo de empresa:** calculado em anos completos, utilizando a diferença em meses entre a data de admissão e a data atual do processamento.
- **Faixa etária:** classificação baseada na idade, com categorias de 18–24, 25–34, 35–44, 45–54 e 55 anos ou mais. Idades nulas são classificadas como "Não informado".
- **Faixa salarial:** classificação baseada no salário, com categorias de até R$ 2.000, de R$ 2.000 a menos de R$ 3.000, de R$ 3.000 a menos de R$ 6.000, de R$ 6.000 a menos de R$ 9.000 e de R$ 9.000 ou mais. Salários nulos são classificados como "Não informado".

### Dimensão calendário

- Trimestre numerado de 1 a 4.
- Semestre numerado de 1 a 2.
- Datas de início e fim de mês calculadas a partir da data de referência.
- Dia da semana seguindo a convenção da função `dayofweek` do Spark: domingo = 1 até sábado = 7.
- Ano e mês disponibilizados no formato `yyyy-MM`.

### Férias

- Duração calculada pela diferença entre as datas final e inicial, acrescida de um dia, incluindo as duas datas.
- Ano e mês de férias derivados da data de início.

### Absenteísmo

- A quantidade de dias afastados é recebida dos dados de origem.
- A fonte específica e a regra de preenchimento devem ser confirmadas na documentação da origem.

## 6. Limitações e pontos de atenção

- A documentação foi aplicada às cinco tabelas da camada Gold; não representa a documentação completa de todas as tabelas do catálogo.
- O tempo de empresa depende da data de execução do processamento e pode variar ao longo do tempo.
- As convenções de formatação dos nomes de meses e dias devem ser verificadas no ambiente de execução.
- As regras de negócio devem ser revisadas sempre que os notebooks de transformação forem alterados.

## 7. Próximas etapas

- Revisar a propriedade e as permissões dos objetos do Unity Catalog.
- Avaliar a classificação e o tratamento dos dados.
- Documentar a linhagem e os mecanismos de governança aplicáveis ao projeto.
- Desenvolver a camada semântica para consumo analítico.

## 8. Notebooks relacionados

- `01_diagnostico_catalogo`
- `02_metadata_documentacao`
