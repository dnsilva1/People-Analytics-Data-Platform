# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 04_gold_fato_absenteismo
#
# Objetivo:
# Criar a tabela fato de absenteísmo para
# suportar análises de People Analytics.
#
# Granularidade:
# 1 registro = 1 ocorrência de absenteísmo
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
    "people_analytics.silver.absenteismo"
)

display(df)

df.printSchema()


# COMMAND ----------

# ==========================================
# 3. SELEÇÃO DOS CAMPOS ANALÍTICOS
# ==========================================

df_fato_absenteismo = df.select(
    "id_absenteismo",
    "id_colaborador",
    "data_falta",
    "tipo_falta",
    "dias_afastados",
    "dt_retorno",
    "mes_falta"
)

display(df_fato_absenteismo)

# COMMAND ----------

# ==========================================
# 4. VALIDAÇÃO DA CHAVE DA FATO
# ==========================================

duplicidades = (
    df_fato_absenteismo
    .groupBy("id_absenteismo")
    .count()
    .filter(col("count") > 1)
)

print("Duplicidades encontradas:")

display(duplicidades)

# COMMAND ----------

# ==========================================
# 5. VALIDAÇÃO DE CHAVE NULA
# ==========================================

print("Registros com id_absenteismo nulo:")

display(
    df_fato_absenteismo
    .filter(
        col("id_absenteismo").isNull()
    )
)

# COMMAND ----------

# ==========================================
# 6. VALIDAÇÃO DE REFERÊNCIA
# ==========================================

print("Registros de absenteísmo sem colaborador correspondente:")

df_colaboradores = (
    spark.table(
        "people_analytics.gold.dim_colaborador"
    )
    .select(
        "id_colaborador"
    )
)

df_sem_colaborador = (
    df_fato_absenteismo.alias("f")
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

qtd_registros = df_fato_absenteismo.count()

qtd_absenteismos_distintos = (
    df_fato_absenteismo
    .select("id_absenteismo")
    .distinct()
    .count()
)

print(f"Quantidade de registros: {qtd_registros}")
print(
    f"Quantidade de absenteísmos distintos: "
    f"{qtd_absenteismos_distintos}"
)

# COMMAND ----------

# ==========================================
# 8. GRAVAÇÃO DA TABELA GOLD
# ==========================================

(
    df_fato_absenteismo
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "people_analytics.gold.fato_absenteismo"
    )
)


# COMMAND ----------

# ==========================================
# 9. VALIDAÇÃO DA TABELA GOLD
# ==========================================

print("Tabela Gold - fato_absenteismo:")

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.gold.fato_absenteismo
        ORDER BY data_falta, id_absenteismo
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
            COUNT(DISTINCT id_absenteismo) AS absenteismos_distintos,
            COUNT(DISTINCT id_colaborador) AS colaboradores_com_absenteismo,
            SUM(dias_afastados) AS total_dias_afastados,
            MIN(data_falta) AS primeira_ocorrencia,
            MAX(data_falta) AS ultima_ocorrencia
        FROM people_analytics.gold.fato_absenteismo
    """)
)

# COMMAND ----------

# ==========================================
# 11. DISTRIBUIÇÃO POR TIPO DE FALTA
# ==========================================

display(
    spark.sql("""
        SELECT
            tipo_falta,
            COUNT(*) AS quantidade_ocorrencias,
            COUNT(DISTINCT id_colaborador) AS colaboradores,
            SUM(dias_afastados) AS total_dias_afastados
        FROM people_analytics.gold.fato_absenteismo
        GROUP BY tipo_falta
        ORDER BY quantidade_ocorrencias DESC
    """)
)

# COMMAND ----------

# ==========================================
# 12. DISTRIBUIÇÃO POR MÊS
# ==========================================

display(
    spark.sql("""
        SELECT
            YEAR(data_falta) AS ano,
            mes_falta,
            COUNT(*) AS quantidade_ocorrencias,
            COUNT(DISTINCT id_colaborador) AS colaboradores,
            SUM(dias_afastados) AS total_dias_afastados
        FROM people_analytics.gold.fato_absenteismo
        GROUP BY
            YEAR(data_falta),
            mes_falta
        ORDER BY
            ano,
            mes_falta
    """)
)