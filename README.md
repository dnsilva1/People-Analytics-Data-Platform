# 📊 People Analytics Data Platform

Projeto de Engenharia de Dados e Analytics desenvolvido utilizando Databricks, PySpark, SQL e Power BI para implementação de uma arquitetura Medalhão (Bronze, Silver e Gold) aplicada ao contexto de People Analytics.

Este projeto foi desenvolvido como parte da minha evolução profissional para Analytics Engineering e Engenharia de Dados, aplicando conceitos de Data Lakehouse, processamento de dados, modelagem analítica e Business Intelligence.

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
- SQL
- Power BI
- GitHub

### Tecnologias que poderão ser incorporadas em versões futuras

- Azure Data Lake Storage
- Azure Data Factory
- Microsoft Fabric
- Apache Airflow
- Databricks Workflows
- Azure DevOps

---

# 🏗️ Arquitetura da Solução

```text
Arquivos CSV
       ↓
Bronze Layer
(Dados Brutos)

       ↓

Silver Layer
(Dados Tratados)

       ↓

Gold Layer
(KPIs e Métricas)

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
│   ├── 02_silver_transformation
│   ├── 03_gold_kpis
│   └── 04_analytics_model
│
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
├── absenteismo
├── turnover
└── ferias
```

(Status: Concluido)

---

# 🥈 Camada Silver

Objetivo:

Realizar tratamento, limpeza e padronização dos dados.

Atividades realizadas:

- Tratamento de valores nulos;
- Padronização de formatos;
- Conversão de tipos de dados;
- Validação de regras de negócio;
- Remoção de inconsistências;
- -Data Quality
- Integração entre tabelas.

Notebook:

- 01_silver_colaboradores
- 02_silver_ferias
- 03_silver_absenteismo
- 04_silver_turnover

 ```text
Leitura Bronze
      ↓
Metadados
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
Validação
      ↓
Gravação Silver
├── colaboradores
├── absenteismo
├── turnover
└── ferias
``` 

(Status: Em desenvolvimento)

---

# 🥇 Camada Gold

Objetivo:

Disponibilizar dados consolidados para consumo analítico.

Atividades realizadas:

- Construção de KPIs;
- Agregações;
- Cálculos de indicadores;
- Tabelas analíticas;
- Estruturas voltadas para Business Intelligence.

Notebook:

- 03_gold_kpis

(Status: Em desenvolvimento)

---

# 📐 Modelagem Analítica

Estrutura desenvolvida para otimizar o consumo dos dados pelo Power BI.

Tabelas previstas:

### Dimensões

- dim_colaborador
- dim_departamento
- dim_cargo
- (outras dimensões criadas posteriormente)

### Fatos

- fato_absenteismo
- fato_turnover
- fato_ferias
- (outras tabelas fato criadas posteriormente)

Notebook:

- 04_analytics_model

(Status: Em desenvolvimento)

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

# 🔄 Pipeline de Dados

```text
CSV
 ↓
Databricks Bronze
 ↓
Databricks Silver
 ↓
Databricks Gold
 ↓
Power BI
 ↓
Dashboards Executivos RH
```

---

# 📚 Conceitos Aplicados

Durante o desenvolvimento deste projeto foram aplicados conceitos de:

- Engenharia de Dados
- Analytics Engineering
- Arquitetura Medalhão
- ETL / ELT
- Data Lakehouse
- Modelagem Analítica
- Data Quality
- PySpark
- SQL
- Business Intelligence
- People Analytics

---

# 🎓 Aprendizados

Este projeto está sendo utilizado para aprofundamento prático em:

- Databricks
- PySpark
- Arquitetura Medalhão
- Engenharia de Dados
- Analytics Engineering
- Integração de Dados
- Power BI
- Cloud Computing

---

# 🚧 Próximas Evoluções

- Integração com Azure Storage
- Integração com Microsoft Fabric
- Automação de cargas
- Orquestração de pipelines
- Data Quality automatizada
- Monitoramento
- Integração com APIs
- Streaming de dados
- Machine Learning aplicado a RH

---

# 👨‍💻 Autor

## Davy Nascimento Silva

Especialista em Analytics & BI | Analytics Engineer | Engenharia de Dados | People Analytics | Power BI | SQL

LinkedIn:
https://www.linkedin.com/in/davy-nascimento-silva-9aa61117a

---

# ⭐ Objetivo Profissional

Este projeto faz parte da minha jornada de evolução para Analytics Engineering e Engenharia de Dados, combinando minha experiência de mais de 8 anos em Analytics, Business Intelligence e People Analytics com arquiteturas modernas de dados, cloud computing e automação.

