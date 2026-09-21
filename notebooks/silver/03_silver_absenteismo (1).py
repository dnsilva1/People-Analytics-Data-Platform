# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Silver
# Notebook: 03_silver_absenteismo
#
# Objetivo:
# Ler a tabela Bronze de absenteismo,
# aplicar tratamentos, padronizações,
# validações e gravar na camada Silver.
# ==========================================

# COMMAND ----------

##Importar Bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Leitura da tabela Bronze

df = spark.table("people_analytics.bronze.absenteismo")

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

#mais avançado, futuramente quando estiver automatizado, inserir os campos abaixo no lugar de responsável
#job_name
#pipeline_name
#execution_id

# COMMAND ----------

#Analise inicial dos dados

display(df.describe()) #Validação geral dos dados

#Validação de dados duplicados - id_colaborador
display(
    df
        .groupBy("id_colaborador")
        .count()
        .orderBy("count", ascending=False)
)



#Validação de dados duplicados - id_colaborador e data_falta
display(
    df
        .groupBy("id_colaborador","data_falta","tipo_falta")
        .count()
        .orderBy("count", ascending=False)
) 

#Validação de dados duplicados - id_absenteismo
display(
    df
        .groupBy("id_absenteismo")
        .count()
        .orderBy("count", ascending=False)
) 


# Análise de valores nulos

from pyspark.sql.functions import col, sum

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

#Validações 

print("Quantidade de registros:")

print(df.count())

display(df)

# COMMAND ----------

#Criação de novas colunas

#criar coluna dt_retorno - data de retorno ao trabalho

df = df.withColumn(
    "dt_retorno",
    date_add(
        col("data_falta"),
        col("dias_afastados").cast("int")
    )
)


#Mês de falta
df = df.withColumn(
    "mes_falta",
    month("data_falta")
)



display(df)







# COMMAND ----------

# ==========================================
# 12. DATA QUALITY
# ==========================================


# ------------------------------------------
# 12.1 Quantidade total de registros
# ------------------------------------------

qtd_total = df.count()

print(f"Quantidade total de registros analisados: {qtd_total}")


# ------------------------------------------
# 12.2 Definição das regras de Data Quality
# ------------------------------------------

regras = [
    {
        "regra": "id_absenteismo_obrigatorio",
        "descricao": "O identificador do absenteísmo não pode ser nulo.",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_absenteismo_unico",
        "descricao": "Cada registro de absenteísmo deve possuir um identificador único.",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_colaborador_obrigatorio",
        "descricao": "O identificador do colaborador não pode ser nulo.",
        "severidade": "CRITICO"
    },
    {
        "regra": "data_falta_obrigatoria",
        "descricao": "A data da falta não pode ser nula.",
        "severidade": "ALTO"
    },
    {
        "regra": "tipo_falta_obrigatorio",
        "descricao": "O tipo da falta não pode ser nulo ou vazio.",
        "severidade": "ALTO"
    },
    {
        "regra": "dias_afastados_obrigatorio",
        "descricao": "A quantidade de dias afastados não pode ser nula.",
        "severidade": "ALTO"
    },
    {
        "regra": "dias_afastados_valido",
        "descricao": "A quantidade de dias afastados deve ser maior que zero.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_falta_nao_futura",
        "descricao": "A data da falta não pode ser posterior à data atual.",
        "severidade": "MEDIO"
    }
]


# ------------------------------------------
# 12.3 Execução das regras de Data Quality
# ------------------------------------------


# Regra: id_absenteismo obrigatório
erro_id_absenteismo = df.filter(
    col("id_absenteismo").isNull()
).count()


# Regra: id_absenteismo único
duplicados_id_absenteismo = (
    df
    .groupBy("id_absenteismo")
    .count()
    .filter(
        (col("count") > 1) &
        col("id_absenteismo").isNotNull()
    )
)

erro_id_absenteismo_unico = duplicados_id_absenteismo.count()


# Regra: id_colaborador obrigatório
erro_id_colaborador = df.filter(
    col("id_colaborador").isNull()
).count()


# Regra: data_falta obrigatória
erro_data_falta = df.filter(
    col("data_falta").isNull()
).count()


# Regra: tipo_falta obrigatório
erro_tipo_falta = df.filter(
    col("tipo_falta").isNull() |
    (trim(col("tipo_falta")) == "")
).count()


# Regra: dias_afastados obrigatório
erro_dias_afastados = df.filter(
    col("dias_afastados").isNull()
).count()


# Regra: dias_afastados válido
erro_dias_afastados_valido = df.filter(
    col("dias_afastados").isNotNull() &
    (col("dias_afastados") <= 0)
).count()


# Regra: data_falta não pode ser futura
erro_data_falta_futura = df.filter(
    col("data_falta") > current_date()
).count()


# ------------------------------------------
# 12.4 Criação dos resultados de Data Quality
# ------------------------------------------

resultados_dq = [
    (
        "id_absenteismo_obrigatorio",
        "CRITICO",
        erro_id_absenteismo
    ),
    (
        "id_absenteismo_unico",
        "CRITICO",
        erro_id_absenteismo_unico
    ),
    (
        "id_colaborador_obrigatorio",
        "CRITICO",
        erro_id_colaborador
    ),
    (
        "data_falta_obrigatoria",
        "ALTO",
        erro_data_falta
    ),
    (
        "tipo_falta_obrigatorio",
        "ALTO",
        erro_tipo_falta
    ),
    (
        "dias_afastados_obrigatorio",
        "ALTO",
        erro_dias_afastados
    ),
    (
        "dias_afastados_valido",
        "ALTO",
        erro_dias_afastados_valido
    ),
    (
        "data_falta_nao_futura",
        "MEDIO",
        erro_data_falta_futura
    )
]


# ------------------------------------------
# 12.5 Criação do resumo de Data Quality
# ------------------------------------------

df_dq = spark.createDataFrame(
    resultados_dq,
    [
        "regra",
        "severidade",
        "registros_com_erro"
    ]
)


# Calcula percentual de erro
df_dq = df_dq.withColumn(
    "percentual_erro",
    round(
        (col("registros_com_erro") / lit(qtd_total)) * 100,
        2
    )
)


# Define status
df_dq = df_dq.withColumn(
    "status",
    when(
        col("registros_com_erro") == 0,
        "OK"
    ).otherwise(
        "ALERTA"
    )
)


# ------------------------------------------
# 12.6 Adição dos metadados do resumo DQ
# ------------------------------------------

df_dq = (
    df_dq
    .withColumn(
        "qtd_total_registros",
        lit(qtd_total)
    )
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.absenteismo")
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


# ------------------------------------------
# 12.7 Exibição do resumo DQ
# ------------------------------------------

display(
    df_dq
)


# ==========================================
# 12.8 Detalhamento dos registros com erro
# ==========================================

# Objetivo:
# Identificar quais registros violaram cada regra
# de Data Quality, preservando o registro e o
# detalhe da divergência para investigação.


# ------------------------------------------
# Regra: id_absenteismo obrigatório
# ------------------------------------------

df_erro_id_absenteismo = (
    df
    .filter(col("id_absenteismo").isNull())
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_absenteismo_obrigatorio").alias("regra"),
        lit("id_absenteismo").alias("campo"),
        col("id_absenteismo").cast("string").alias("valor"),
        lit("Identificador do absenteísmo não informado").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: id_absenteismo único
# ------------------------------------------

ids_absenteismo_duplicados = (
    df
    .groupBy("id_absenteismo")
    .count()
    .filter(
        (col("count") > 1) &
        col("id_absenteismo").isNotNull()
    )
    .select("id_absenteismo")
)


df_erro_id_absenteismo_unico = (
    df
    .join(
        ids_absenteismo_duplicados,
        on="id_absenteismo",
        how="inner"
    )
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_absenteismo_unico").alias("regra"),
        lit("id_absenteismo").alias("campo"),
        col("id_absenteismo").cast("string").alias("valor"),
        lit("Identificador do absenteísmo possui registros duplicados").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: id_colaborador obrigatório
# ------------------------------------------

df_erro_id_colaborador = (
    df
    .filter(col("id_colaborador").isNull())
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_colaborador_obrigatorio").alias("regra"),
        lit("id_colaborador").alias("campo"),
        col("id_colaborador").cast("string").alias("valor"),
        lit("Identificador do colaborador não informado").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_falta obrigatória
# ------------------------------------------

df_erro_data_falta = (
    df
    .filter(col("data_falta").isNull())
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_falta_obrigatoria").alias("regra"),
        lit("data_falta").alias("campo"),
        col("data_falta").cast("string").alias("valor"),
        lit("Data da falta não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: tipo_falta obrigatório
# ------------------------------------------

df_erro_tipo_falta = (
    df
    .filter(
        col("tipo_falta").isNull() |
        (trim(col("tipo_falta")) == "")
    )
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("tipo_falta_obrigatorio").alias("regra"),
        lit("tipo_falta").alias("campo"),
        col("tipo_falta").cast("string").alias("valor"),
        lit("Tipo de falta não informado").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: dias_afastados obrigatório
# ------------------------------------------

df_erro_dias_afastados = (
    df
    .filter(col("dias_afastados").isNull())
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("dias_afastados_obrigatorio").alias("regra"),
        lit("dias_afastados").alias("campo"),
        col("dias_afastados").cast("string").alias("valor"),
        lit("Quantidade de dias afastados não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: dias_afastados válido
# ------------------------------------------

df_erro_dias_afastados_valido = (
    df
    .filter(
        col("dias_afastados").isNotNull() &
        (col("dias_afastados") <= 0)
    )
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("dias_afastados_valido").alias("regra"),
        lit("dias_afastados").alias("campo"),
        col("dias_afastados").cast("string").alias("valor"),
        lit("Quantidade de dias afastados deve ser maior que zero").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_falta não pode ser futura
# ------------------------------------------

df_erro_data_falta_futura = (
    df
    .filter(
        col("data_falta") > current_date()
    )
    .select(
        col("id_absenteismo"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_falta_nao_futura").alias("regra"),
        lit("data_falta").alias("campo"),
        col("data_falta").cast("string").alias("valor"),
        lit("Data da falta posterior à data atual").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# 12.9 União dos registros com erro
# ------------------------------------------

df_dq_erros = (
    df_erro_id_absenteismo
    .unionByName(df_erro_id_absenteismo_unico)
    .unionByName(df_erro_id_colaborador)
    .unionByName(df_erro_data_falta)
    .unionByName(df_erro_tipo_falta)
    .unionByName(df_erro_dias_afastados)
    .unionByName(df_erro_dias_afastados_valido)
    .unionByName(df_erro_data_falta_futura)
)


# ------------------------------------------
# 12.10 Adição dos metadados dos erros
# ------------------------------------------

df_dq_erros = (
    df_dq_erros
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.absenteismo")
    )
    .withColumn(
        "camada_origem",
        lit("silver")
    )
    .withColumn(
        "dt_execucao",
        current_timestamp()
    )
)


# ------------------------------------------
# 12.11 Exibição dos registros com erro
# ------------------------------------------

display(
    df_dq_erros
)


# ==========================================
# 12.12 Persistência do histórico de DQ
# ==========================================

# Cria o schema de monitoramento caso não exista
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS people_analytics.monitoring
""")


# ------------------------------------------
# Salva o resumo das regras
# ------------------------------------------

(
    df_dq
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(
        "people_analytics.monitoring.dq_absenteismo"
    )
)


# ------------------------------------------
# Salva o detalhamento dos erros
# ------------------------------------------

(
    df_dq_erros
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(
        "people_analytics.monitoring.dq_absenteismo_erros"
    )
)


# ==========================================
# 12.13 Validação da persistência
# ==========================================

print("Resumo de Data Quality:")

display(
    spark.table(
        "people_analytics.monitoring.dq_absenteismo"
    )
)


print("Detalhamento dos registros com erro:")

display(
    spark.table(
        "people_analytics.monitoring.dq_absenteismo_erros"
    )
)

# COMMAND ----------

# ==========================================
# Tratamento de duplicidades
#
# Regra de negócio:
# Quando houver mais de um registro para o mesmo
# id_absenteismo, deve ser mantido o registro
# correspondente à primeira data do atestado.
#
# Critérios de desempate:
# 1. Menor data_falta
# 2. Maior dt_ingestao
# 3. Maior id_carga
# ==========================================

from pyspark.sql.window import Window

window_deduplicacao = Window.partitionBy(
    "id_absenteismo"
).orderBy(
    col("data_falta").asc_nulls_last(),
    col("dt_ingestao").desc_nulls_last(),
    col("id_carga").desc_nulls_last()
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

#Validações 

print("Quantidade de registros:")

print(df.count())

display(df)

# COMMAND ----------

#Escrita da Camada Silver

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(
          "people_analytics.silver.absenteismo" #sempre trocar o nome tabela destino
)
)

# COMMAND ----------

#Validação da gravação na Camada Silver

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.silver.absenteismo
    """)
)

display(
    spark.sql("""
        SELECT COUNT(*) AS quantidade
        FROM people_analytics.silver.absenteismo
    """)
)