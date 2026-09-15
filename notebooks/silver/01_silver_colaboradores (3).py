# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Silver
# Notebook: 01_silver_colaboradores
#
# Objetivo:
# Ler a tabela Bronze de colaboradores,
# aplicar tratamentos, padronizações,
# validações e gravar na camada Silver.
# ==========================================

# COMMAND ----------

##Importar Bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Leitura da tabela Bronze

df = spark.table("people_analytics.bronze.colaboradores")

display(df)

df.printSchema()

# COMMAND ----------

# Adição dos metadados de processamento

df = (
    df
    .withColumn(
        "dt_processamento",
        current_timestamp()
    )
    .withColumn(
        "camada_origem",
        lit("bronze")
    )
)

# COMMAND ----------

#Analise inicial dos dados

display(df.describe()) #Validação geral dos dados

# Análise da distribuição por gênero
display(
    df
        .groupBy("genero")
        .count()
        .orderBy("count", ascending=False)
) 

# Análise da distribuição por cargo
display(
    df
        .groupBy("cargo")
        .count()
        .orderBy("count", ascending=False)
) 

# Análise da distribuição por departamento
display(
    df
        .groupBy("departamento")
        .count()
        .orderBy("count", ascending=False)
) 

# Análise da distribuição por status
display(
    df
        .groupBy("status")
        .count()
        .orderBy("count", ascending=False)
) 


# Análise de valores nulos

display(
    df.select(
        [
            sum(
                col(c).isNull().cast("int")
            ).alias(c)
            for c in df.columns
        ]
    )
)


# Análise de duplicidades

display(
    df
        .groupBy("id_colaborador")
        .count()
        .filter(col("count") > 1)
        .orderBy("count", ascending=False)
)



# COMMAND ----------

#Padronização dos nomes das colunas

novos_nomes = [
    c.lower()
     .replace(" ", "_")
     .replace("ç","c")
     .replace("ã","a")
     .replace("á","a")
     .replace("é","e")
    for c in df.columns
]

df = df.toDF(*novos_nomes)

# COMMAND ----------

#Padronização dos dados - Eliminar espaços em branco

colunas_texto = [
    "nome",
    "genero",
    "cargo",
    "departamento",
    "status"
]

for coluna in colunas_texto:

    df = df.withColumn(
        coluna,
        trim(col(coluna))
    )

# COMMAND ----------

#Tratamento de valores nulos

df = (
    df
    .fillna({
        "departamento":"Não informado",
        "cargo":"Não informado",
        "status":"Não informado"
    })
)

# COMMAND ----------

#Conversão de tipos

df = (
    df
    .withColumn(
        "idade",
        col("idade").cast(IntegerType())
    )
    .withColumn(
        "salario",
        col("salario").cast(DecimalType(12,2))
    )
)

# COMMAND ----------

#Criação de novas colunas

#Tempo de empresa
df = df.withColumn(
    "tempo_empresa_anos",
    floor(
        months_between(
            current_date(),
            col("data_admissao")
        )/12
    )
)

#Ano de admissão
df = df.withColumn(
    "ano_admissao",
    year("data_admissao")
)

#Mês de admissão
df = df.withColumn(
    "mes_admissao",
    month("data_admissao")
)

#Faixa etária
df = (
    df
    .withColumn(
        "faixa_etaria",
        when(col("idade").isNull(), "Não informado")
        .when(col("idade") < 25,"18-24")
        .when(col("idade") < 35,"25-34")
        .when(col("idade") < 45,"35-44")
        .when(col("idade") < 55,"45-54")
        .otherwise("55+")
    )
)

#Faixa salarial
df = (
    df
    .withColumn(
        "faixa_salarial",
        when(col("salario").isNull(), "Não informado")
        .when(col("salario") < 2000,"Até 2 mil")
        .when(col("salario") < 3000,"2 a 3 mil")
        .when(col("salario") < 6000,"3 a 6 mil")
        .when(col("salario") < 9000,"6 a 9 mil")
        .otherwise("Acima de 9 mil")
    )
)



# COMMAND ----------

# ==========================================
# BLOCO 12 - DATA QUALITY
# ==========================================


# ==========================================
# 12.1 - Quantidade total de registros
# ==========================================

qtd_total = df.count()

print("==========================================")
print("DATA QUALITY")
print("==========================================")
print(f"Total de registros analisados: {qtd_total}")


# ==========================================
# 12.2 - Definição das regras
# ==========================================

regras = [

    {
        "regra": "id_colaborador_obrigatorio",
        "severidade": "CRITICO"
    },

    {
        "regra": "nome_obrigatorio",
        "severidade": "ALTO"
    },

    {
        "regra": "data_admissao_obrigatoria",
        "severidade": "ALTO"
    },

    {
        "regra": "idade_valida",
        "severidade": "MEDIO"
    },

    {
        "regra": "salario_valido",
        "severidade": "ALTO"
    },

    {
        "regra": "data_admissao_nao_futura",
        "severidade": "MEDIO"
    },

    {
        "regra": "status_valido",
        "severidade": "MEDIO"
    },

    {
        "regra": "id_colaborador_unico",
        "severidade": "CRITICO"
    }
]


# ==========================================
# 12.3 - Execução das regras
# ==========================================

erro_id = df.filter(
    col("id_colaborador").isNull()
).count()

erro_nome = df.filter(
    col("nome").isNull() |
    (trim(col("nome")) == "")
).count()

erro_data_admissao = df.filter(
    col("data_admissao").isNull()
).count()

erro_idade = df.filter(
    col("idade").isNull() |
    (col("idade") <= 0) |
    (col("idade") > 100)
).count()

erro_salario = df.filter(
    col("salario").isNull() |
    (col("salario") <= 0)
).count()

erro_data_futura = df.filter(
    col("data_admissao") > current_date()
).count()

erro_status = df.filter(
    col("status").isNull() |
    (~col("status").isin(
        ["Ativo", "Inativo", "Não informado"]
    ))
).count()

erro_duplicidade = (
    df
    .groupBy("id_colaborador")
    .count()
    .filter(col("count") > 1)
    .count()
)


# ==========================================
# 12.4 - Percentual de erro
# ==========================================

def percentual_erro(qtd_erro, qtd_total):

    if qtd_total == 0:
        return 0.0

    return (qtd_erro / qtd_total) * 100


percentual_id = percentual_erro(erro_id, qtd_total)

percentual_nome = percentual_erro(
    erro_nome,
    qtd_total
)

percentual_data_admissao = percentual_erro(
    erro_data_admissao,
    qtd_total
)

percentual_idade = percentual_erro(
    erro_idade,
    qtd_total
)

percentual_salario = percentual_erro(
    erro_salario,
    qtd_total
)

percentual_data_futura = percentual_erro(
    erro_data_futura,
    qtd_total
)

percentual_status = percentual_erro(
    erro_status,
    qtd_total
)

percentual_duplicidade = percentual_erro(
    erro_duplicidade,
    qtd_total
)


# ==========================================
# 12.5 - Status
# ==========================================

def status_qualidade(qtd_erro):

    if qtd_erro == 0:
        return "OK"

    return "ALERTA"


status_id = status_qualidade(erro_id)

status_nome = status_qualidade(erro_nome)

status_data_admissao = status_qualidade(
    erro_data_admissao
)

status_idade = status_qualidade(
    erro_idade
)

status_salario = status_qualidade(
    erro_salario
)

status_data_futura = status_qualidade(
    erro_data_futura
)

status_status = status_qualidade(
    erro_status
)

status_duplicidade = status_qualidade(
    erro_duplicidade
)


# ==========================================
# 12.6 - Severidade
# ==========================================

severidade_id = "CRITICO"

severidade_nome = "ALTO"

severidade_data_admissao = "ALTO"

severidade_idade = "MEDIO"

severidade_salario = "ALTO"

severidade_data_futura = "MEDIO"

severidade_status = "MEDIO"

severidade_duplicidade = "CRITICO"


# ==========================================
# 12.7 - Criação do DataFrame de Data Quality
# ==========================================

resultado_dq = [

    (
        "id_colaborador_obrigatorio",
        severidade_id,
        erro_id,
        percentual_id,
        status_id
    ),

    (
        "nome_obrigatorio",
        severidade_nome,
        erro_nome,
        percentual_nome,
        status_nome
    ),

    (
        "data_admissao_obrigatoria",
        severidade_data_admissao,
        erro_data_admissao,
        percentual_data_admissao,
        status_data_admissao
    ),

    (
        "idade_valida",
        severidade_idade,
        erro_idade,
        percentual_idade,
        status_idade
    ),

    (
        "salario_valido",
        severidade_salario,
        erro_salario,
        percentual_salario,
        status_salario
    ),

    (
        "data_admissao_nao_futura",
        severidade_data_futura,
        erro_data_futura,
        percentual_data_futura,
        status_data_futura
    ),

    (
        "status_valido",
        severidade_status,
        erro_status,
        percentual_status,
        status_status
    ),

    (
        "id_colaborador_unico",
        severidade_duplicidade,
        erro_duplicidade,
        percentual_duplicidade,
        status_duplicidade
    )
]


df_dq = spark.createDataFrame(
    resultado_dq,
    [
        "regra",
        "severidade",
        "registros_com_erro",
        "percentual_erro",
        "status"
    ]
)


# ==========================================
# Metadados do Data Quality
# ==========================================

df_dq = (
    df_dq

    .withColumn(
        "qtd_total_registros",
        lit(qtd_total)
    )

    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.colaboradores")
    )

    .withColumn(
        "camada_origem",
        lit("silver")
    )

    .withColumn(
        "dt_processamento",
        current_timestamp()
    )
)


# ==========================================
# Visualização do resultado
# ==========================================

display(
    df_dq
)


# ==========================================
# 12.8 - Persistência do histórico
# ==========================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS people_analytics.monitoring
""")


(
    df_dq.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "people_analytics.monitoring.dq_colaboradores"
    )
)


# ==========================================
# Validação da persistência
# ==========================================

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.monitoring.dq_colaboradores
        ORDER BY dt_processamento DESC
    """)
)

# COMMAND ----------

# ==========================================
# Tratamento de duplicidades
# Regra:
# Para registros com o mesmo id_colaborador,
# manter aquele com a data_admissao mais recente.
# ==========================================

from pyspark.sql.window import Window

window_deduplicacao = Window.partitionBy(
    "id_colaborador"
).orderBy(
    col("data_admissao").desc_nulls_last()
)

df = (
    df
    .withColumn(
        "rn",
        row_number().over(window_deduplicacao)
    )
    .filter(
        col("rn") == 1
    )
    .drop("rn")
)

# COMMAND ----------

#Escrita da Camada Silver

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(
          "people_analytics.silver.colaboradores"
      )
)

# COMMAND ----------

#Validação da gravação na Camada Silver

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.silver.colaboradores
    """)
)

display(
    spark.sql("""
        SELECT COUNT(*) AS quantidade
        FROM people_analytics.silver.colaboradores
    """)
)