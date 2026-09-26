# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 03_gold_fato_ferias
#
# Objetivo:
# Criar a tabela fato de férias para suportar
# análises de People Analytics.
#
# Granularidade:
# 1 registro = 1 período de férias
# ==========================================

# COMMAND ----------

# ==========================================
# 1. IMPORTAÇÃO DAS BIBLIOTECAS
# ==========================================

from pyspark.sql.functions import *

# COMMAND ----------

# ==========================================
# 2. LEITURA DA TABELA SILVER
# ==========================================

df = spark.table(
    "people_analytics.silver.ferias"
)

display(df)

df.printSchema()

# COMMAND ----------

# ==========================================
# 3. SELEÇÃO DOS CAMPOS ANALÍTICOS
# ==========================================

df_fato_ferias = df.select(
    "id_ferias",
    "id_colaborador",
    "data_inicio",
    "data_fim",
    "dias_ferias",
    "ano_ferias",
    "mes_ferias"
)

display(df_fato_ferias)

# COMMAND ----------

# ==========================================
# 4. VALIDAÇÃO DA CHAVE DA FATO
# ==========================================

duplicidades = (
    df_fato_ferias
    .groupBy("id_ferias")
    .count()
    .filter(col("count") > 1)
)

print("Duplicidades encontradas:")
display(duplicidades)

# COMMAND ----------

# ==========================================
# 5. VALIDAÇÃO DE CHAVE NULA
# ==========================================

print("Registros com id_ferias nulo:")

display(
    df_fato_ferias
    .filter(
        col("id_ferias").isNull()
    )
)

# COMMAND ----------

# ==========================================
# 6. VALIDAÇÃO DE REFERÊNCIA
# ==========================================

print("Registros de férias sem colaborador correspondente:")

df_colaboradores = spark.table(
    "people_analytics.gold.dim_colaborador"
).select(
    "id_colaborador"
)

df_sem_colaborador = (
    df_fato_ferias.alias("f")
    .join(
        df_colaboradores.alias("d"),
        col("f.id_colaborador") == col("d.id_colaborador"),
        "left"
    )
    .filter(
        col("d.id_colaborador").isNull()
    )
    .select(
        "f.*"
    )
)

display(df_sem_colaborador)

# COMMAND ----------

# ==========================================
# 7. VALIDAÇÃO DOS REGISTROS
# ==========================================

qtd_registros = df_fato_ferias.count()

qtd_ferias_distintas = (
    df_fato_ferias
    .select("id_ferias")
    .distinct()
    .count()
)

print(f"Quantidade de registros: {qtd_registros}")
print(f"Quantidade de férias distintas: {qtd_ferias_distintas}")

# COMMAND ----------

# ==========================================
# 8. GRAVAÇÃO DA TABELA GOLD
# ==========================================

(
    df_fato_ferias
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "people_analytics.gold.fato_ferias"
    )
)

# COMMAND ----------

# ==========================================
# 9. VALIDAÇÃO DA TABELA GOLD
# ==========================================

print("Tabela Gold - fato_ferias:")

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.gold.fato_ferias
        ORDER BY data_inicio, id_ferias
    """)
)

# COMMAND ----------

# ==========================================
# 10. VALIDAÇÃO RESUMIDA
# ==========================================

display(
    spark.sql("""
        SELECT
            COUNT(*) AS quantidade_registros,
            COUNT(DISTINCT id_ferias) AS ferias_distintas,
            COUNT(DISTINCT id_colaborador) AS colaboradores_com_ferias,
            SUM(dias_ferias) AS total_dias_ferias,
            MIN(data_inicio) AS primeira_ferias,
            MAX(data_fim) AS ultima_ferias
        FROM people_analytics.gold.fato_ferias
    """)
)

# COMMAND ----------

# ==========================================
# 11. DISTRIBUIÇÃO POR ANO
# ==========================================

display(
    spark.sql("""
        SELECT
            ano_ferias,
            COUNT(*) AS quantidade_ferias,
            COUNT(DISTINCT id_colaborador) AS colaboradores,
            SUM(dias_ferias) AS total_dias_ferias
        FROM people_analytics.gold.fato_ferias
        GROUP BY ano_ferias
        ORDER BY ano_ferias
    """)
)

# COMMAND ----------

# ==========================================
# 12. DISTRIBUIÇÃO POR MÊS
# ==========================================

display(
    spark.sql("""
        SELECT
            ano_ferias,
            mes_ferias,
            COUNT(*) AS quantidade_ferias,
            SUM(dias_ferias) AS total_dias_ferias
        FROM people_analytics.gold.fato_ferias
        GROUP BY
            ano_ferias,
            mes_ferias
        ORDER BY
            ano_ferias,
            mes_ferias
    """)
)