# Databricks notebook source
# Databricks notebook source

# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 01_gold_dim_calendario
#
# Objetivo:
# Criar a dimensão calendário para suporte
# às análises temporais da plataforma.
# ==========================================

from pyspark.sql.functions import *

# COMMAND ----------

# ==========================================
# 1. Identificação do período dos dados
# ==========================================

df_colaboradores = spark.table(
    "people_analytics.silver.colaboradores"
)

df_ferias = spark.table(
    "people_analytics.silver.ferias"
)

df_absenteismo = spark.table(
    "people_analytics.silver.absenteismo"
)

df_turnover = spark.table(
    "people_analytics.silver.turnover"
)

# COMMAND ----------

# ==========================================
# 2. Identificação das datas mínima e máxima
# ==========================================

datas = [

    df_colaboradores.select(
        min("data_admissao").alias("data_min"),
        max("data_admissao").alias("data_max")
    ),

    df_ferias.select(
        min("data_inicio").alias("data_min"),
        max("data_fim").alias("data_max")
    ),

    df_absenteismo.select(
        min("data_falta").alias("data_min"),
        max("data_falta").alias("data_max")
    ),

    df_turnover.select(
        min("data_desligamento").alias("data_min"),
        max("data_desligamento").alias("data_max")
    )
]

df_datas = datas[0]

for df_data in datas[1:]:
    df_datas = df_datas.unionByName(df_data)


periodo = df_datas.select(
    min("data_min").alias("data_min"),
    max("data_max").alias("data_max")
).collect()[0]


data_min = periodo["data_min"]
data_max = periodo["data_max"]


print(f"Data mínima encontrada: {data_min}")
print(f"Data máxima encontrada: {data_max}")

# COMMAND ----------

# ==========================================
# 3. Criação da dimensão calendário
# ==========================================

df_calendario = (
    spark.range(1)
    .select(
        explode(
            sequence(
                to_date(lit(data_min)),
                to_date(lit(data_max)),
                expr("interval 1 day")
            )
        ).alias("data")
    )
)

# COMMAND ----------

# ==========================================
# 4. Criação dos atributos do calendário
# ==========================================

df_calendario = (
    df_calendario

    .withColumn(
        "ano",
        year("data")
    )

    .withColumn(
        "mes",
        month("data")
    )

    .withColumn(
        "nome_mes",
        date_format("data", "MMMM")
    )

    .withColumn(
        "ano_mes",
        date_format("data", "yyyy-MM")
    )

    .withColumn(
        "trimestre",
        quarter("data")
    )

    .withColumn(
        "semestre",
        when(
            month("data") <= 6,
            1
        ).otherwise(2)
    )

    .withColumn(
        "inicio_mes",
        trunc("data", "month")
    )

    .withColumn(
        "fim_mes",
        last_day("data")
    )

    .withColumn(
        "dia",
        dayofmonth("data")
    )

    .withColumn(
        "dia_semana",
        dayofweek("data")
    )

    .withColumn(
        "nome_dia_semana",
        date_format("data", "EEEE")
    )
)

# COMMAND ----------

# ==========================================
# 5. Organização das colunas
# ==========================================

df_calendario = df_calendario.select(
    "data",
    "ano",
    "mes",
    "nome_mes",
    "ano_mes",
    "trimestre",
    "semestre",
    "inicio_mes",
    "fim_mes",
    "dia",
    "dia_semana",
    "nome_dia_semana"
)

# COMMAND ----------

# ==========================================
# 6. Validação visual
# ==========================================

display(
    df_calendario.orderBy("data")
)

# COMMAND ----------

# ==========================================
# 7. Escrita da dimensão calendário
# ==========================================

(
    df_calendario.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "people_analytics.gold.dim_calendario"
    )
)

# COMMAND ----------

# ==========================================
# 8. Validação da gravação
# ==========================================

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.gold.dim_calendario
        ORDER BY data
        LIMIT 20
    """)
)


display(
    spark.sql("""
        SELECT
            MIN(data) AS data_minima,
            MAX(data) AS data_maxima,
            COUNT(*) AS quantidade_dias
        FROM people_analytics.gold.dim_calendario
    """)
)