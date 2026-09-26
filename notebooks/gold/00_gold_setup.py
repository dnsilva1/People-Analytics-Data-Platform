# Databricks notebook source
# Databricks notebook source

# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 00_gold_setup
#
# Objetivo:
# Preparar a camada Gold e validar as
# tabelas de origem da camada Silver.
# ==========================================


# COMMAND ----------

# ==========================================
# 1. Criação do schema Gold
# ==========================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS people_analytics.gold
""")


# COMMAND ----------

# ==========================================
# 2. Validação das tabelas Silver
# ==========================================

tabelas_silver = [
    "people_analytics.silver.colaboradores",
    "people_analytics.silver.ferias",
    "people_analytics.silver.absenteismo",
    "people_analytics.silver.turnover"
]

for tabela in tabelas_silver:

    print("=" * 70)
    print(f"Tabela: {tabela}")

    df = spark.table(tabela)

    print(f"Quantidade de registros: {df.count()}")

    df.printSchema()


# COMMAND ----------

# ==========================================
# 3. Validação do schema Gold
# ==========================================

display(
    spark.sql("""
        SHOW TABLES IN people_analytics.gold
    """)
)