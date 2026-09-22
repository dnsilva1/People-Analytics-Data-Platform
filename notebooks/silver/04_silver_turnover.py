# Databricks notebook source
# ==========================================
# Projeto: People Analytics
# Camada: Silver
# Notebook: 04_silver_turnover
#
# Objetivo:
# Ler a tabela Bronze de turnover,
# aplicar tratamentos, padronizações,
# validações e gravar na camada Silver.
# ==========================================

# COMMAND ----------

##Importar Bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Leitura da tabela Bronze

df = spark.table("people_analytics.bronze.turnover")

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



#Validação de dados duplicados - id_colaborador e data_desligamento
display(
    df
        .groupBy("id_colaborador", "data_desligamento")
        .count()
        .orderBy("count", ascending=False)
) 

#Validação de dados duplicados - id_turnover
display(
    df
        .groupBy("id_turnover")
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


#Validação de dados duplicados - motivo
display(
    df
        .groupBy("motivo")
        .count()
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

#Validações 

print("Quantidade de registros:")

print(df.count())

display(df)

# COMMAND ----------

#Criação de novas colunas

#criar coluna dt_retorno - data de retorno ao trabalho


#Mês de desligamento
df = df.withColumn(
    "mes_desligamento",
    month("data_desligamento")
)

#Ano de desligamento
df = df.withColumn(
    "ano_desligamento",
    year("data_desligamento")
)

#tipo de desligamento voluntario ou involuntario
df = df.withColumn(
    "tipo_desligamento",
    when(
        col("motivo") == "Pedido de Demissão",
        "Voluntario"
    ).otherwise("Involuntario")
)

display(df)







# COMMAND ----------

# ==========================================
# DATA QUALITY — TURNOVER
# ==========================================

from pyspark.sql.functions import (
    col,
    when,
    lit,
    current_timestamp,
    current_date,
    trim,
    sum
)


# ==========================================
# 1. Definição das regras de Data Quality
# ==========================================

regras_dq = [
    {
        "regra": "id_turnover_obrigatorio",
        "campo": "id_turnover",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_turnover_unico",
        "campo": "id_turnover",
        "severidade": "CRITICO"
    },
    {
        "regra": "id_colaborador_obrigatorio",
        "campo": "id_colaborador",
        "severidade": "CRITICO"
    },
    {
        "regra": "data_desligamento_obrigatoria",
        "campo": "data_desligamento",
        "severidade": "ALTO"
    },
    {
        "regra": "motivo_obrigatorio",
        "campo": "motivo",
        "severidade": "ALTO"
    },
    {
        "regra": "data_desligamento_nao_futura",
        "campo": "data_desligamento",
        "severidade": "MEDIO"
    },
    {
        "regra": "motivo_nao_vazio",
        "campo": "motivo",
        "severidade": "ALTO"
    }
]


# ==========================================
# 2. Execução das regras
# ==========================================

resultados_dq = []


# ------------------------------------------
# Regra 1 — id_turnover obrigatório
# ------------------------------------------

qtd_erro = df.filter(
    col("id_turnover").isNull()
).count()

resultados_dq.append(
    (
        "id_turnover_obrigatorio",
        "id_turnover",
        qtd_erro,
        "CRITICO"
    )
)


# ------------------------------------------
# Regra 2 — id_turnover único
# ------------------------------------------

qtd_erro = (
    df.groupBy("id_turnover")
      .count()
      .filter(
          (col("id_turnover").isNotNull()) &
          (col("count") > 1)
      )
      .agg(
          sum(col("count") - 1).alias("qtd_erro")
      )
      .collect()[0]["qtd_erro"]
)

qtd_erro = qtd_erro if qtd_erro is not None else 0

resultados_dq.append(
    (
        "id_turnover_unico",
        "id_turnover",
        qtd_erro,
        "CRITICO"
    )
)


# ------------------------------------------
# Regra 3 — id_colaborador obrigatório
# ------------------------------------------

qtd_erro = df.filter(
    col("id_colaborador").isNull()
).count()

resultados_dq.append(
    (
        "id_colaborador_obrigatorio",
        "id_colaborador",
        qtd_erro,
        "CRITICO"
    )
)


# ------------------------------------------
# Regra 4 — data_desligamento obrigatória
# ------------------------------------------

qtd_erro = df.filter(
    col("data_desligamento").isNull()
).count()

resultados_dq.append(
    (
        "data_desligamento_obrigatoria",
        "data_desligamento",
        qtd_erro,
        "ALTO"
    )
)


# ------------------------------------------
# Regra 5 — motivo obrigatório
# ------------------------------------------

qtd_erro = df.filter(
    col("motivo").isNull()
).count()

resultados_dq.append(
    (
        "motivo_obrigatorio",
        "motivo",
        qtd_erro,
        "ALTO"
    )
)


# ------------------------------------------
# Regra 6 — data_desligamento não futura
# ------------------------------------------

qtd_erro = df.filter(
    col("data_desligamento") > current_date()
).count()

resultados_dq.append(
    (
        "data_desligamento_nao_futura",
        "data_desligamento",
        qtd_erro,
        "MEDIO"
    )
)


# ------------------------------------------
# Regra 7 — motivo não vazio
# ------------------------------------------

qtd_erro = df.filter(
    col("motivo").isNotNull() &
    (trim(col("motivo")) == "")
).count()

resultados_dq.append(
    (
        "motivo_nao_vazio",
        "motivo",
        qtd_erro,
        "ALTO"
    )
)


# ==========================================
# 3. Criação do DataFrame de resumo
# ==========================================

schema_dq = [
    "regra",
    "campo",
    "qtd_erros",
    "severidade"
]

df_dq = spark.createDataFrame(
    resultados_dq,
    schema=schema_dq
)


# ==========================================
# 4. Métricas complementares do resumo
# ==========================================

qtd_total_registros = df.count()

df_dq = (
    df_dq
    .withColumn(
        "qtd_total_registros",
        lit(qtd_total_registros)
    )
    .withColumn(
        "percentual_erro",
        when(
            col("qtd_total_registros") > 0,
            (col("qtd_erros") / col("qtd_total_registros")) * 100
        ).otherwise(0)
    )
    .withColumn(
        "status",
        when(
            col("qtd_erros") > 0,
            lit("ERRO")
        ).otherwise(
            lit("OK")
        )
    )
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.turnover")
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


# ==========================================
# 5. Data Quality — Erros detalhados
# ==========================================

dq_erros = []


# ------------------------------------------
# Regra 1 — id_turnover obrigatório
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("id_turnover").isNull()
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_turnover_obrigatorio").alias("regra"),
        lit("id_turnover").alias("campo"),
        lit(None).cast("string").alias("valor"),
        lit("Campo obrigatório não preenchido").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 2 — id_turnover único
# ------------------------------------------

ids_duplicados = (
    df.groupBy("id_turnover")
      .count()
      .filter(
          (col("id_turnover").isNotNull()) &
          (col("count") > 1)
      )
      .select("id_turnover")
)

dq_erros.append(
    df.join(
        ids_duplicados,
        on="id_turnover",
        how="inner"
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_turnover_unico").alias("regra"),
        lit("id_turnover").alias("campo"),
        col("id_turnover").cast("string").alias("valor"),
        lit("ID de turnover duplicado").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 3 — id_colaborador obrigatório
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("id_colaborador").isNull()
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("id_colaborador_obrigatorio").alias("regra"),
        lit("id_colaborador").alias("campo"),
        lit(None).cast("string").alias("valor"),
        lit("Campo obrigatório não preenchido").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 4 — data_desligamento obrigatória
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("data_desligamento").isNull()
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_desligamento_obrigatoria").alias("regra"),
        lit("data_desligamento").alias("campo"),
        lit(None).cast("string").alias("valor"),
        lit("Data de desligamento não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 5 — motivo obrigatório
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("motivo").isNull()
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("motivo_obrigatorio").alias("regra"),
        lit("motivo").alias("campo"),
        lit(None).cast("string").alias("valor"),
        lit("Motivo não informado").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 6 — data_desligamento não futura
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("data_desligamento") > current_date()
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("data_desligamento_nao_futura").alias("regra"),
        lit("data_desligamento").alias("campo"),
        col("data_desligamento").cast("string").alias("valor"),
        lit("Data de desligamento está no futuro").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# Regra 7 — motivo não vazio
# ------------------------------------------

dq_erros.append(
    df.filter(
        col("motivo").isNotNull() &
        (trim(col("motivo")) == "")
    )
    .select(
        col("id_turnover"),
        col("id_colaborador"),
        col("id_carga"),
        lit("motivo_nao_vazio").alias("regra"),
        lit("motivo").alias("campo"),
        col("motivo").cast("string").alias("valor"),
        lit("Motivo preenchido, porém vazio ou contendo apenas espaços").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ==========================================
# 6. União dos erros detalhados
# ==========================================

df_dq_erros = dq_erros[0]

for df_erro in dq_erros[1:]:
    df_dq_erros = df_dq_erros.unionByName(df_erro)


# ==========================================
# 7. Metadados dos erros detalhados
# ==========================================

df_dq_erros = (
    df_dq_erros
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.turnover")
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


# ==========================================
# 8. Visualização do resumo
# ==========================================

display(
    df_dq.orderBy(
        col("qtd_erros").desc()
    )
)


# ==========================================
# 9. Visualização dos erros detalhados
# ==========================================

display(
    df_dq_erros.orderBy(
        col("severidade"),
        col("regra"),
        col("id_turnover")
    )
)


# ==========================================
# 10. Persistência do histórico de DQ
# ==========================================

(
    df_dq.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(
        "people_analytics.monitoring.dq_turnover"
    )
)


(
    df_dq_erros.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(
        "people_analytics.monitoring.dq_turnover_erros"
    )
)


# ==========================================
# 11. Validação das tabelas de monitoramento
# ==========================================

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.monitoring.dq_turnover
        ORDER BY dt_execucao DESC, qtd_erros DESC
    """)
)


display(
    spark.sql("""
        SELECT *
        FROM people_analytics.monitoring.dq_turnover_erros
        ORDER BY dt_execucao DESC, regra
    """)
)

# COMMAND ----------

# ==========================================
# Tratamento de duplicidades
#
# Regra de negócio:
# Quando houver mais de um registro para o mesmo
# id_turnover, deve ser mantido o registro
# correspondente à ultima data de desligamento.
#
# ==========================================

from pyspark.sql.window import Window

window_deduplicacao = Window.partitionBy(
    "id_turnover"
).orderBy(
    col("data_desligamento").desc_nulls_last()
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

display(
    df.groupBy("tipo_desligamento")
      .count()
      .orderBy("count", ascending=False)
)

display(
    df.groupBy("ano_desligamento")
      .count()
      .orderBy("count", ascending=False)
)

# COMMAND ----------

#Escrita da Camada Silver

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(
          "people_analytics.silver.turnover" #sempre trocar o nome tabela destino
)
)

# COMMAND ----------

#Validação da gravação na Camada Silver

display(
    spark.sql("""
        SELECT *
        FROM people_analytics.silver.turnover
    """)
)

display(
    spark.sql("""
        SELECT COUNT(*) AS quantidade
        FROM people_analytics.silver.turnover
    """)
)
