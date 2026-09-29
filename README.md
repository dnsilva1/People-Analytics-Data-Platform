# 📊 People Analytics Data Platform

Projeto de Engenharia de Dados e Analytics desenvolvido com Databricks, Python, PySpark, SQL e Power BI, utilizando arquitetura Medallion (Bronze, Silver e Gold) aplicada ao contexto de People Analytics.

O projeto simula uma plataforma moderna de dados para Recursos Humanos, contemplando ingestão, tratamento, Data Quality, modelagem analítica, orquestração, automação e disponibilização de dados para Business Intelligence.

Este projeto faz parte da minha evolução profissional para Analytics Engineering e Engenharia de Dados, aplicando conceitos de Data Lakehouse, Engenharia de Dados, Data Quality, modelagem dimensional, orquestração e Business Intelligence.

---

# 🎯 Objetivo do Projeto

O objetivo deste projeto é simular uma plataforma moderna de dados para Recursos Humanos, permitindo a ingestão, tratamento, transformação e disponibilização de indicadores estratégicos para apoio à tomada de decisão.

A solução foi construída utilizando uma arquitetura Medalhão (Bronze, Silver e Gold), amplamente utilizada em projetos modernos de Engenharia de Dados.

---

# 🚀 Tecnologias Utilizadas

- Databricks
- Python
- Pandas
- PySpark
- Apache Spark
- Delta lake
- Jobs e Pipelines - Orquestração
- SQL
- Power BI
- GitHub

### Tecnologias que poderão ser incorporadas em versões futuras

- Unity Catalog
- Azure Data Lake Storage
- Azure Data Factory
- Microsoft Fabric
- Apache Airflow
- APIs
- Machine Learning
- IA aplicada a People Analytics
- Monitoramento avançado

---

# 🏗️ Arquitetura da Solução

```text
Arquivos CSV
       ↓
Bronze Layer
(Dados Brutos)

       ↓

Silver Layer
(Dados Tratados + Data Quality + DQ)

       ↓

Gold Layer
(Modelo Analítico + Quality Gate)

       ↓

Power BI
(Dashboard Executivo)
```

### Fluxo do Projeto

1. Geração de dados fictícios de People Analytics;
2. Ingestão dos dados no Databricks;
3. Armazenamento dos dados brutos na camada Bronze;
4. Tratamento, limpeza e padronização na camada Silver;
5. Criação de métricas e indicadores na camada Gold;
6. Consumo dos dados pelo Power BI;
7. Disponibilização dos dashboards para análise executiva.

---

# 📂 Estrutura do Projeto

```text
people-analytics-data-platform/

├── datasets/
│
├── notebooks
│   ├── bronze
│   │   ├── 01_bronze_ingestao_tabelas.py ✅
│   │   └── README.md
│   │
│   ├── silver/data quality/dqs
│   │   ├── 01_silver_colaboradores.py ✅
│   │   ├── 02_silver_ferias.py ✅
│   │   ├── 03_silver_absenteismo.py ✅
│   │   └── 04_silver_turnover.py ✅
│   │       
│   ├── gold_kpis
│   │   ├── 00_gold_setup.py ✅
│   │   ├── 01_gold_dim_calendario.py ✅
│   │   ├── 02_gold_dim_colaborador.py ✅
│   │   ├── 03_gold_fato_ferias.py ✅
│   │   ├── 04_gold_fato_absenteismo.py ✅
│   │   ├── 05_gold_fato_turnover.py ✅
│   │   └── 06_gold_validacao.py ✅
│   │
│   ├── pipelines_automaticos
│   │   ├──
│   │   └── README.md
│   │ 
│   ├── analytics_model/
│   │   ├── camada_semantica/
│   │   └──
│   │
│   ├── datacatalog/govenança
│   │   ├── governanca/
│   │   └──
│   │
├── dashboard/
│   ├── dashboard.pbix
│   └── screenshots/
│
├── architecture/
│   └── architecture.png
│
└── README.md
```

---

# 📁 Dataset

O projeto utiliza uma base fictícia de People Analytics criada para simular cenários corporativos reais.

Arquivos utilizados:

- colaboradores.csv
- absenteismo.csv
- turnover.csv
- ferias.csv
- (outros arquivos que forem criados posteriormente)

### Principais informações simuladas

- Dados cadastrais dos colaboradores;
- Estrutura organizacional;
- Histórico de admissões;
- Histórico de desligamentos;
- Controle de férias;
- Indicadores de absenteísmo;
- Informações salariais;
- Dados para análises de People Analytics.

---

# 🥉 Camada Bronze

Objetivo:

Realizar a ingestão automatizada dos dados brutos provenientes do arquivo de origem, preservando integralmente seu conteúdo e estrutura. Durante o processo, são adicionados metadados para garantir rastreabilidade, auditoria e governança, armazenando cada conjunto de dados como uma tabela Delta independente na camada Bronze.

Atividades realizadas:

- Upload do arquivo de origem (.xlsx);
- Leitura automática de todas as abas do arquivo utilizando Pandas;
- Conversão dos dados para DataFrames PySpark;
- Inclusão de metadados para rastreabilidade da carga;
- Validação da estrutura, quantidade e da qualidade dos dados de cada tabela;
- Escrita das tabelas no formato Delta Lake;
- Persistência dos dados brutos na camada Bronze.

Metadados adicionados

- dt_ingestao
- nome_arquivo
- sistema_origem
- usuario_responsavel
- identificador_carga
- id_carga


Notebook:

- 01_bronze_ingestao_tabelas

Fluxo 

```text
Arquivo Excel
      │
      ▼
Leitura de todas as abas (Pandas)
      │
      ▼
Conversão para DataFrames PySpark
      │
      ▼
Inclusão de metadados
      │
      ▼
Validações iniciais
      │
      ▼
Gravação em Delta Lake
      │
      ▼
Camada Bronze
├── colaboradores
├── ferias
├── absenteismo
└── turnover
```

### Status: ✅ Concluído

---

# 🥈 Camada Silver

Objetivo:

Realizar tratamento, limpeza, padronização, criação de novos dados derivados e validação com Data Quality a partir da camada bronze. Durante o processo, são adicionados metadados do processamento para garantir rastreabilidade, auditoria e governança, armazenando cada conjunto de dados como uma tabela Delta na camada Silver.

Atividades realizadas:

- Tratamento de valores nulos;
- Padronização de formatos;
- Conversão de tipos de dados;
- Aplicação e validação de regras de negócio;
- Identificação e remoção de inconsistências;
- Data Quality
- Deduplicação após as validações de qualidade
- Persistência das tabelas Silver

Notebook:

- 01_silver_colaboradores
- 02_silver_ferias
- 03_silver_absenteismo
- 04_silver_turnover

 ```text
Leitura Bronze
      ↓
Metadados de processamento
      ↓
Diagnóstico
      ↓
Padronização
      ↓
Tratamento
      ↓
Conversão de tipos
      ↓
Colunas derivadas
      ↓
Data Quality
      ↓
Deduplicação
      ↓
Gravação Silver
├── colaboradores
├── ferias
├── absenteismo
└── turnover
``` 

### Status: ✅ Concluído

---

# 🔎 Data Quality

A camada Silver possui mecanismos estruturados de Data Quality, permitindo identificar e registrar inconsistências antes da aplicação de tratamentos como deduplicação.

As validações contemplam regras relacionadas a:

- Campos obrigatórios
- Unicidade de identificadores
- Validade de valores
- Datas
- Regras de negócio
- Integridade dos dados
  
### Estrutura de monitoramento

 ```text
people_analytics.monitoring
│
├── dq_absenteismo
├── dq_absenteismo_erros
├── dq_colaboradores
├── dq_colaboradores_erros
├── dq_ferias
├── dq_ferias_erros
├── dq_turnover
└── dq_turnover_erros
 ```

As tabelas de DQ armazenam informações sobre as regras executadas, status, severidade, divergências e rastreabilidade da carga.

### Status: ✅ Concluído

---

# 🥇 Camada Gold

### Objetivo:
Construir uma camada analítica estruturada, preparada para consumo por ferramentas de Business Intelligence e análises de People Analytics.

A camada Gold utiliza conceitos de modelagem dimensional, separando dimensões e fatos.

### Dimensão
- dim_calendario
- dim_colaborador

### Fatos
- fato_ferias
- fato_absenteismo
- fato_turnover

### Notebooks

- 00_gold_setup.py
- 01_gold_dim_calendario.py
- 02_gold_dim_colaborador.py
- 03_gold_fato_ferias.py
- 04_gold_fato_absenteismo.py
- 05_gold_fato_turnover.py
- 06_gold_validacao.py

### Modelo

 ```text
                    dim_calendario
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
       fato_ferias  fato_absenteismo  fato_turnover
             │            │            │
             └────────────┼────────────┘
                          │
                          ▼
                   dim_colaborador
```

### Tabelas Gold

```text
people_analytics.gold
│
├── dim_calendario
├── dim_colaborador
├── fato_ferias
├── fato_absenteismo
└── fato_turnover
```

### Status: ✅ Concluído

---

# 🛡️ Quality Gate — Gold

Foi implementado um **Quality Gate automatizado** para validar a camada Gold antes de sua aprovação para consumo.

O notebook 06_gold_validacao.py realiza validações como:

- Existência das tabelas Gold
- Integridade da dimensão calendário
- Duplicidade de chaves
- Valores nulos em identificadores
- Integridade referencial das tabelas fato
- Integridade das datas
- Reconciliação de quantidade de registros entre Silver e Gold
- Consolidação dos resultados das validações

### Regra de aprovação

Quando todas as validações são aprovadas:

STATUS FINAL: APROVADO

Caso alguma validação falhe, o notebook utiliza raise Exception, fazendo com que a Task seja marcada como **FAILED**.

Dessa forma, a execução da pipeline não é considerada aprovada quando os dados não passam pelo Quality Gate.

### Status: ✅ Concluído

---

### 🔄 Orquestração e Automação

A plataforma possui uma pipeline automatizada utilizando **Databricks Jobs**, responsável por coordenar a execução das camadas Bronze, Silver e Gold.

### Tasks

```text
01_bronze_ingestao
        │
        ├──→ 02_silver_colaboradores
        │          │
        │          └──→ 07_gold_dim_colaborador
        │
        ├──→ 03_silver_ferias
        │          │
        │          └──→ 08_gold_fato_ferias
        │
        ├──→ 04_silver_absenteismo
        │          │
        │          └──→ 09_gold_fato_absenteismo
        │
        └──→ 05_silver_turnover
                   │
                   └──→ 10_gold_fato_turnover

06_gold_dim_calendario
        │
        └──────────────────────────────┐
                                       ▼
                              11_gold_validacao
```

As dependências entre as Tasks garantem que cada etapa seja executada somente após suas respectivas dependências serem concluídas.

### Quality Gate na orquestração

```text
CSV
     ↓
Databricks Bronze
     ↓
Databricks Silver
     ↓
Gold
     ↓
Quality Gate
     ↓
┌───────────────┐
│ Todas PASS?   │
└───────┬───────┘
        │
   ┌────┴────┐
   ▼         ▼
 SUCCESS    FAILED
```

Em caso de falha no Quality Gate, a Task 11_gold_validacao é marcada como **FAILED**, fazendo com que a execução geral do Job seja considerada reprovada.

### Agendamento

A pipeline foi configurada para execução automática:

- Frequência: Diária
- Horário: 02:00
- Fuso: America/Sao_Paulo

A primeira execução completa da pipeline foi realizada com sucesso e as execuções foram verificadas quanto à ocorrência de duplicidades.

### Status: ✅ Concluído

---

# 📈 Indicadores Desenvolvidos

### People Analytics

- Headcount
- Turnover
- Absenteísmo
- Tempo médio de empresa
- Distribuição por departamento
- Distribuição por cargo
- Distribuição por gênero
- Colaboradores em férias

### Financeiro

- Folha salarial
- Custo médio por colaborador
- Indicadores de savings
- ROI de iniciativas (caso implementado futuramente)

### Indicadores adicionais

(Incluir conforme forem sendo desenvolvidos)

---

# 📊 Dashboard Power BI

O dashboard foi desenvolvido para fornecer uma visão estratégica dos principais indicadores de Recursos Humanos.

### Visões previstas

#### Visão Executiva

- Headcount
- Turnover
- Absenteísmo
- Principais indicadores

#### People Analytics

- Distribuição de colaboradores
- Diversidade
- Tempo de empresa
- Movimentações

#### Financeiro

- Custos
- Folha salarial
- Saving
- ROI

### Screenshots

(Adicionar imagens dos dashboards após conclusão)

---

# 🖼️ Arquitetura Visual

(Adicionar imagem da arquitetura do projeto)

Exemplo:

architecture/architecture.png

---

# 📚 Conceitos Aplicados

Durante o desenvolvimento deste projeto foram aplicados conceitos de:

- Engenharia de Dados
- Analytics Engineering
- Arquitetura Medallion
- ETL / ELT
- Data Lakehouse
- Delta Lake
- PySpark
- SQL
- Data Quality
- Quality Gate
- Modelagem dimensional
- Orquestração de pipelines
- Automação de processos
- Business Intelligence
- People Analytics
- Git / GitHub

---

# 🎓 Aprendizados

Este projeto está sendo utilizado para aprofundamento prático em:

- Integração de Dados
- Databricks
- PySpark
- Apache Spark
- Arquitetura Medallion
- Delta Lake
- Data Quality
- Engenharia de Dados
- Analytics Engineering
- Modelagem analítica
- Orquestração
- Automação
- Business Intelligence
- Power BI
- Cloud Computing

---

# 🚧 Próximas Evoluções

### ETAPA 8 — Data Catalog & Governança
- Unity Catalog
- Organização de schemas
- Catálogo de dados
- Governança
- Controle de acesso
- Permissões
- Linhagem
- Metadados
- 
### ETAPA 9 — Modelo Semântico
- Camada semântica
- Regras de negócio
- Definição dos KPIs
- Estrutura preparada para consumo analítico

### ETAPA 10 — Power BI
- Modelo de dados
- Medidas
- Dashboards
- Indicadores executivos
- Análises de People Analytics

### Evoluções futuras
- Integração com Azure Storage
- Integração com Microsoft Fabric
- Integração com APIs
- Integração com bancos de dados
- Ingestão incremental
- Monitoramento
- Streaming de dados
- Machine Learning aplicado a RH
- IA aplicada a People Analytics
- Analytics preditivo
- Analytics prescritivo

---

# 👨‍💻 Autor

## Davy Nascimento Silva

Especialista em Analytics & BI | Analytics Engineer | Engenharia de Dados | People Analytics | Power BI | SQL

LinkedIn:
https://www.linkedin.com/in/davy-nascimento-silva-9aa61117a

---

# ⭐ Objetivo Profissional

Este projeto faz parte da minha jornada de evolução para Analytics Engineering e Engenharia de Dados, combinando minha experiência de mais de 8 anos em Analytics, Business Intelligence e People Analytics com arquiteturas modernas de dados, cloud computing e automação.

