# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 05_gold_fato_turnover
#
# Objetivo:
# Criar a tabela fato de turnover para
# suportar análises de People Analytics.
#
# Granularidade:
# 1 registro = 1 desligamento de colaborador
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
    "people_analytics.silver.turnover"
)

display(df)

df.printSchema()

# COMMAND ----------

# ==========================================
# 3. SELEÇÃO DOS CAMPOS ANALÍTICOS
# ==========================================

df_fato_turnover = df.select(
    "id_turnover",
    "id_colaborador",
    "data_desligamento",
    "motivo",
    "mes_desligamento",
    "ano_desligamento",
    "tipo_desligamento"
)

display(df_fato_turnover)

# COMMAND ----------

# ==========================================
# 4. VALIDAÇÃO DA CHAVE DA FATO
# ==========================================

duplicidades = (
    df_fato_turnover
    .groupBy("id_turnover")
    .count()
    .filter(col("count") > 1)
)

print("Duplicidades encontradas:")

display(duplicidades)

# COMMAND ----------

# ==========================================
# 5. VALIDAÇÃO DE CHAVE NULA
# ==========================================

print("Registros com id_turnover nulo:")

display(
    df_fato_turnover
    .filter(
        col("id_turnover").isNull()
    )
)

# COMMAND ----------

# ==========================================
# 6. VALIDAÇÃO DE REFERÊNCIA
# ==========================================

print("Registros de turnover sem colaborador correspondente:")

df_colaboradores = (
    spark.table(
        "people_analytics.gold.dim_colaborador"
    )
    .select(
        "id_colaborador"
    )
)

df_sem_colaborador = (
    df_fato_turnover.alias("f")
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

qtd_registros = df_fato_turnover.count()

qtd_turnovers_distintos = (
    df_fato_turnover
    .select("id_turnover")
    .distinct()
    .count()
)

print(f"Quantidade de registros: {qtd_registros}")
print(
    f"Quantidade de turnovers distintos: "
    f"{qtd_turnovers_distintos}"
)

# COMMAND ----------

# ==========================================
# 8. GRAVAÇÃO DA TABELA GOLD
# ==========================================

(
    df_fato_turnover
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "people_analytics.gold.fato_turnover"
    )
)

# COMMAND ----------

# ==========================================
# 9. VALIDAÇÃO DA TABELA GOLD
# ==========================================

print("Tabela Gold - fato_turnover:")

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.gold.fato_turnover
        ORDER BY data_desligamento, id_turnover
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
            COUNT(DISTINCT id_turnover) AS turnovers_distintos,
            COUNT(DISTINCT id_colaborador) AS colaboradores_desligados,
            MIN(data_desligamento) AS primeiro_desligamento,
            MAX(data_desligamento) AS ultimo_desligamento
        FROM people_analytics.gold.fato_turnover
    """)
)

# COMMAND ----------

# ==========================================
# 11. DISTRIBUIÇÃO POR TIPO DE DESLIGAMENTO
# ==========================================

display(
    spark.sql("""
        SELECT
            tipo_desligamento,
            COUNT(*) AS quantidade_desligamentos,
            COUNT(DISTINCT id_colaborador) AS colaboradores
        FROM people_analytics.gold.fato_turnover
        GROUP BY tipo_desligamento
        ORDER BY quantidade_desligamentos DESC
    """)
)

# COMMAND ----------

# ==========================================
# 12. DISTRIBUIÇÃO POR MOTIVO
# ==========================================

display(
    spark.sql("""
        SELECT
            motivo,
            COUNT(*) AS quantidade_desligamentos,
            COUNT(DISTINCT id_colaborador) AS colaboradores
        FROM people_analytics.gold.fato_turnover
        GROUP BY motivo
        ORDER BY quantidade_desligamentos DESC
    """)
)

# COMMAND ----------

# ==========================================
# 13. DISTRIBUIÇÃO POR ANO
# ==========================================

display(
    spark.sql("""
        SELECT
            ano_desligamento,
            COUNT(*) AS quantidade_desligamentos,
            COUNT(DISTINCT id_colaborador) AS colaboradores
        FROM people_analytics.gold.fato_turnover
        GROUP BY ano_desligamento
        ORDER BY ano_desligamento
    """)
)

# COMMAND ----------

# ==========================================
# 14. DISTRIBUIÇÃO POR MÊS
# ==========================================

display(
    spark.sql("""
        SELECT
            ano_desligamento,
            mes_desligamento,
            COUNT(*) AS quantidade_desligamentos,
            COUNT(DISTINCT id_colaborador) AS colaboradores
        FROM people_analytics.gold.fato_turnover
        GROUP BY
            ano_desligamento,
            mes_desligamento
        ORDER BY
            ano_desligamento,
            mes_desligamento
    """)
)