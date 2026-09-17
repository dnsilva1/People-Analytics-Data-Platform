# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Silver
# Notebook: 02_silver_ferias
#
# Objetivo:
# Ler a tabela Bronze de ferias,
# aplicar tratamentos, padronizações,
# validações e gravar na camada Silver.
# ==========================================

# COMMAND ----------

##Importar Bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Leitura da tabela Bronze

df = spark.table("people_analytics.bronze.ferias")

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

#Análise da distribuição por id_colaborador
display(
    df
        .groupBy("id_colaborador")
        .count()
        .orderBy("count", ascending=False)
)



#Análise da distribuição por id_colaborador e data_inicio e data_fim
display(
    df
        .groupBy("id_colaborador","data_inicio","data_fim")
        .count()
        .orderBy("count", ascending=False)
) 

#Análise da distribuição por id_ferias
display(
    df
        .groupBy("id_ferias")
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

#Criação de novas colunas

#dias de férias
df = df.withColumn(
    "dias_ferias",
    datediff(
        col("data_fim"),
        col("data_inicio")
    )+1
)


#Ano ferias
df = df.withColumn(
    "ano_ferias",
    year("data_inicio")
)

#Mês de ferias
df = df.withColumn(
    "mes_ferias",
    month("data_inicio")
)

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
        "regra": "id_ferias_obrigatorio",
        "descricao": "O identificador das férias não pode ser nulo.",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_ferias_unico",
        "descricao": "Cada registro de férias deve possuir um identificador único.",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_colaborador_obrigatorio",
        "descricao": "O identificador do colaborador não pode ser nulo.",
        "severidade": "CRITICO"
    },
    {
        "regra": "data_inicio_obrigatoria",
        "descricao": "A data de início das férias não pode ser nula.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_fim_obrigatoria",
        "descricao": "A data de término das férias não pode ser nula.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_fim_maior_data_inicio",
        "descricao": "A data de término deve ser igual ou posterior à data de início.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_inicio_nao_futura",
        "descricao": "A data de início das férias não pode ser posterior à data atual.",
        "severidade": "MEDIO"
    }
]


# ------------------------------------------
# 12.3 Execução das regras de Data Quality
# ------------------------------------------


# Regra: id_ferias obrigatório
erro_id_ferias = df.filter(
    col("id_ferias").isNull()
).count()


# Regra: id_ferias único
duplicados_id_ferias = (
    df
    .groupBy("id_ferias")
    .count()
    .filter(
        (col("count") > 1) &
        col("id_ferias").isNotNull()
    )
)

erro_id_ferias_unico = duplicados_id_ferias.count()


# Regra: id_colaborador obrigatório
erro_id_colaborador = df.filter(
    col("id_colaborador").isNull()
).count()


# Regra: data_inicio obrigatória
erro_data_inicio = df.filter(
    col("data_inicio").isNull()
).count()


# Regra: data_fim obrigatória
erro_data_fim = df.filter(
    col("data_fim").isNull()
).count()


# Regra: data_fim maior ou igual à data_inicio
erro_periodo_ferias = df.filter(
    col("data_inicio").isNotNull() &
    col("data_fim").isNotNull() &
    (col("data_fim") < col("data_inicio"))
).count()


# Regra: data_inicio não pode ser futura
erro_data_inicio_futura = df.filter(
    col("data_inicio") > current_date()
).count()


# ------------------------------------------
# 12.4 Função para calcular percentual de erro
# ------------------------------------------

def calcular_percentual(qtd_erro, qtd_total):
    
    if qtd_total == 0:
        return 0.0
    
    return round((qtd_erro / qtd_total) * 100, 2)


# ------------------------------------------
# 12.5 Função para definir status da regra
# ------------------------------------------

def definir_status(qtd_erro):
    
    if qtd_erro == 0:
        return "OK"
    
    return "ALERTA"


# ------------------------------------------
# 12.6 Criação dos resultados de Data Quality
# ------------------------------------------

resultados_dq = [
    (
        "id_ferias_obrigatorio",
        "CRITICO",
        erro_id_ferias
    ),
    (
        "id_ferias_unico",
        "CRITICO",
        erro_id_ferias_unico
    ),
    (
        "id_colaborador_obrigatorio",
        "CRITICO",
        erro_id_colaborador
    ),
    (
        "data_inicio_obrigatoria",
        "ALTO",
        erro_data_inicio
    ),
    (
        "data_fim_obrigatoria",
        "ALTO",
        erro_data_fim
    ),
    (
        "data_fim_maior_data_inicio",
        "ALTO",
        erro_periodo_ferias
    ),
    (
        "data_inicio_nao_futura",
        "MEDIO",
        erro_data_inicio_futura
    )
]


# ------------------------------------------
# 12.7 Criação do resumo de Data Quality
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
# 12.8 Adição dos metadados do resumo DQ
# ------------------------------------------

df_dq = (
    df_dq
    .withColumn(
        "qtd_total_registros",
        lit(qtd_total)
    )
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.ferias")
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
# 12.9 Exibição do resumo DQ
# ------------------------------------------

display(
    df_dq
)


# ==========================================
# 12.10 Detalhamento dos registros com erro
# ==========================================

# Objetivo:
# Identificar quais registros violaram cada regra
# de Data Quality, preservando o registro e o
# detalhe da divergência para investigação.


# ------------------------------------------
# Regra: id_ferias obrigatório
# ------------------------------------------

df_erro_id_ferias = (
    df
    .filter(col("id_ferias").isNull())
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_ferias_obrigatorio").alias("regra"),
        lit("id_ferias").alias("campo"),
        col("id_ferias").cast("string").alias("valor"),
        lit("Identificador das férias não informado").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: id_ferias único
# ------------------------------------------

ids_ferias_duplicados = (
    df
    .groupBy("id_ferias")
    .count()
    .filter(
        (col("count") > 1) &
        col("id_ferias").isNotNull()
    )
    .select("id_ferias")
)


df_erro_id_ferias_unico = (
    df
    .join(
        ids_ferias_duplicados,
        on="id_ferias",
        how="inner"
    )
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_ferias_unico").alias("regra"),
        lit("id_ferias").alias("campo"),
        col("id_ferias").cast("string").alias("valor"),
        lit("Identificador das férias possui registros duplicados").alias("divergencia"),
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
        col("id_ferias"),
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
# Regra: data_inicio obrigatória
# ------------------------------------------

df_erro_data_inicio = (
    df
    .filter(col("data_inicio").isNull())
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_inicio_obrigatoria").alias("regra"),
        lit("data_inicio").alias("campo"),
        col("data_inicio").cast("string").alias("valor"),
        lit("Data de início das férias não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_fim obrigatória
# ------------------------------------------

df_erro_data_fim = (
    df
    .filter(col("data_fim").isNull())
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_fim_obrigatoria").alias("regra"),
        lit("data_fim").alias("campo"),
        col("data_fim").cast("string").alias("valor"),
        lit("Data de término das férias não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_fim maior ou igual à data_inicio
# ------------------------------------------

df_erro_periodo_ferias = (
    df
    .filter(
        col("data_inicio").isNotNull() &
        col("data_fim").isNotNull() &
        (col("data_fim") < col("data_inicio"))
    )
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_fim_maior_data_inicio").alias("regra"),
        lit("data_inicio,data_fim").alias("campo"),
        concat(
            col("data_inicio").cast("string"),
            lit(" | "),
            col("data_fim").cast("string")
        ).alias("valor"),
        lit("Data de término anterior à data de início das férias").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_inicio não pode ser futura
# ------------------------------------------

df_erro_data_inicio_futura = (
    df
    .filter(
        col("data_inicio") > current_date()
    )
    .select(
        col("id_ferias"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_inicio_nao_futura").alias("regra"),
        lit("data_inicio").alias("campo"),
        col("data_inicio").cast("string").alias("valor"),
        lit("Data de início das férias posterior à data atual").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# 12.11 União dos registros com erro
# ------------------------------------------

df_dq_erros = (
    df_erro_id_ferias
    .unionByName(df_erro_id_ferias_unico)
    .unionByName(df_erro_id_colaborador)
    .unionByName(df_erro_data_inicio)
    .unionByName(df_erro_data_fim)
    .unionByName(df_erro_periodo_ferias)
    .unionByName(df_erro_data_inicio_futura)
)


# ------------------------------------------
# 12.12 Adição dos metadados dos erros
# ------------------------------------------

df_dq_erros = (
    df_dq_erros
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.ferias")
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
# 12.13 Exibição dos registros com erro
# ------------------------------------------

display(
    df_dq_erros
)


# ==========================================
# 12.14 Persistência do histórico de DQ
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
        "people_analytics.monitoring.dq_ferias"
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
        "people_analytics.monitoring.dq_ferias_erros"
    )
)


# ==========================================
# 12.15 Validação da persistência
# ==========================================

print("Resumo de Data Quality:")
display(
    spark.table(
        "people_analytics.monitoring.dq_ferias"
    )
)


print("Detalhamento dos registros com erro:")
display(
    spark.table(
        "people_analytics.monitoring.dq_ferias_erros"
    )
)

# COMMAND ----------

# ==========================================
# Tratamento de duplicidades
# Regra:
# Para registros com o mesmo id_ferias,
# manter aquele com a data_inicio mais recente.
# ==========================================


from pyspark.sql.window import Window

window_deduplicacao = Window.partitionBy(
    "id_ferias"
).orderBy(
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
          "people_analytics.silver.ferias"
      )
)

# COMMAND ----------

#Validação da gravação na Camada Silver

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.silver.ferias
    """)
)

display(
    spark.sql("""
        SELECT COUNT(*) AS quantidade
        FROM people_analytics.silver.ferias
    """)
)