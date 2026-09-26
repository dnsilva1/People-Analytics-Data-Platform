# Databricks notebook source
# Databricks notebook source

# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 02_gold_dim_colaborador
#
# Objetivo:
# Criar a dimensão de colaboradores para
# suportar as análises de People Analytics.
#
# Granularidade:
# 1 registro = 1 colaborador
# ==========================================


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# ==========================================
# 1. Leitura da camada Silver
# ==========================================

df = spark.table(
    "people_analytics.silver.colaboradores"
)

display(df)

df.printSchema() #validar que é o mesmo schema da Silver

# COMMAND ----------

# ==========================================
# 2. Seleção dos atributos da dimensão
# ==========================================

df_dim_colaborador = df.select(
    "id_colaborador",
    "nome",
    "genero",
    "idade",
    "faixa_etaria",
    "cargo",
    "departamento",
    "status",
    "salario",
    "faixa_salarial",
    "data_admissao",
    "tempo_empresa_anos",
    "ano_admissao",
    "mes_admissao"
)

# COMMAND ----------

# ==========================================
# 3. Validação da granularidade
# ==========================================

duplicidades = (
    df_dim_colaborador
    .groupBy("id_colaborador")
    .count()
    .filter(col("count") > 1)
)

display(duplicidades)

# COMMAND ----------

# ==========================================
# 4. Validação da chave
# ==========================================

display(
    df_dim_colaborador
    .filter(col("id_colaborador").isNull())
)

# COMMAND ----------

# ==========================================
# 5. Validações gerais
# ==========================================

print(
    f"Quantidade de registros: {df_dim_colaborador.count()}"
)

print(
    f"Quantidade de colaboradores distintos: "
    f"{df_dim_colaborador.select('id_colaborador').distinct().count()}"
)

# COMMAND ----------

# ==========================================
# 6. Escrita da dimensão
# ==========================================

(
    df_dim_colaborador.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "people_analytics.gold.dim_colaborador"
    )
)

# COMMAND ----------

# ==========================================
# 7. Validação da gravação
# ==========================================

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.gold.dim_colaborador
        ORDER BY id_colaborador
    """)
)

display(
    spark.sql("""
        SELECT
            COUNT(*) AS quantidade_registros,
            COUNT(DISTINCT id_colaborador) AS colaboradores_distintos,
            MIN(data_admissao) AS primeira_admissao,
            MAX(data_admissao) AS ultima_admissao
        FROM people_analytics.gold.dim_colaborador
    """)
)

display(
    spark.sql("""
        SELECT
            departamento,
            COUNT(*) AS quantidade_colaboradores
        FROM people_analytics.gold.dim_colaborador
        GROUP BY departamento
        ORDER BY quantidade_colaboradores DESC
    """)
)

display(
    spark.sql("""
        SELECT
            status,
            COUNT(*) AS quantidade_colaboradores
        FROM people_analytics.gold.dim_colaborador
        GROUP BY status
        ORDER BY quantidade_colaboradores DESC
    """)
)

display(
    spark.sql("""
        SELECT
            faixa_etaria,
            COUNT(*) AS quantidade_colaboradores
        FROM people_analytics.gold.dim_colaborador
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria
    """)
)

