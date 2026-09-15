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
        "regra": "id_colaborador_obrigatorio",
        "descricao": "O identificador do colaborador não pode ser nulo.",
        "severidade": "CRITICO"
    },
    {
        "regra": "nome_obrigatorio",
        "descricao": "O nome do colaborador não pode ser nulo ou vazio.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_admissao_obrigatoria",
        "descricao": "A data de admissão não pode ser nula.",
        "severidade": "ALTO"
    },
    {
        "regra": "idade_valida",
        "descricao": "A idade deve estar entre 1 e 100 anos.",
        "severidade": "MEDIO"
    },
    {
        "regra": "salario_valido",
        "descricao": "O salário não pode ser nulo ou menor ou igual a zero.",
        "severidade": "ALTO"
    },
    {
        "regra": "data_admissao_nao_futura",
        "descricao": "A data de admissão não pode ser maior que a data atual.",
        "severidade": "MEDIO"
    },
    {
        "regra": "status_valido",
        "descricao": "O status deve possuir um valor válido.",
        "severidade": "MEDIO"
    },
    {
        "regra": "id_colaborador_unico",
        "descricao": "Cada colaborador deve possuir um único registro.",
        "severidade": "CRITICO"
    }
]


# ------------------------------------------
# 12.3 Execução das regras de Data Quality
# ------------------------------------------

# Regra: id_colaborador obrigatório
erro_id = df.filter(
    col("id_colaborador").isNull()
).count()


# Regra: nome obrigatório
erro_nome = df.filter(
    col("nome").isNull() |
    (trim(col("nome")) == "")
).count()


# Regra: data_admissao obrigatória
erro_data_admissao = df.filter(
    col("data_admissao").isNull()
).count()


# Regra: idade válida
erro_idade = df.filter(
    col("idade").isNull() |
    (col("idade") <= 0) |
    (col("idade") > 100)
).count()


# Regra: salário válido
erro_salario = df.filter(
    col("salario").isNull() |
    (col("salario") <= 0)
).count()


# Regra: data de admissão não pode ser futura
erro_data_futura = df.filter(
    col("data_admissao") > current_date()
).count()


# Regra: status válido
status_validos = [
    "Ativo",
    "Inativo",
    "Não informado"
]

erro_status = df.filter(
    col("status").isNull() |
    (~col("status").isin(status_validos))
).count()


# Regra: id_colaborador único
duplicados = (
    df
    .groupBy("id_colaborador")
    .count()
    .filter(col("count") > 1)
)

erro_duplicidade = duplicados.count()


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
# 12.6 Definição das quantidades de erro
# ------------------------------------------

resultados_dq = [
    (
        "id_colaborador_obrigatorio",
        "CRITICO",
        erro_id
    ),
    (
        "nome_obrigatorio",
        "ALTO",
        erro_nome
    ),
    (
        "data_admissao_obrigatoria",
        "ALTO",
        erro_data_admissao
    ),
    (
        "idade_valida",
        "MEDIO",
        erro_idade
    ),
    (
        "salario_valido",
        "ALTO",
        erro_salario
    ),
    (
        "data_admissao_nao_futura",
        "MEDIO",
        erro_data_futura
    ),
    (
        "status_valido",
        "MEDIO",
        erro_status
    ),
    (
        "id_colaborador_unico",
        "CRITICO",
        erro_duplicidade
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
# Regra: id_colaborador obrigatório
# ------------------------------------------

df_erro_id = (
    df
    .filter(col("id_colaborador").isNull())
    .select(
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
# Regra: nome obrigatório
# ------------------------------------------

df_erro_nome = (
    df
    .filter(
        col("nome").isNull() |
        (trim(col("nome")) == "")
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("nome_obrigatorio").alias("regra"),
        lit("nome").alias("campo"),
        col("nome").cast("string").alias("valor"),
        lit("Nome do colaborador não informado").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_admissao obrigatória
# ------------------------------------------

df_erro_data_admissao = (
    df
    .filter(col("data_admissao").isNull())
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("data_admissao_obrigatoria").alias("regra"),
        lit("data_admissao").alias("campo"),
        col("data_admissao").cast("string").alias("valor"),
        lit("Data de admissão não informada").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: idade válida
# ------------------------------------------

df_erro_idade = (
    df
    .filter(
        col("idade").isNull() |
        (col("idade") <= 0) |
        (col("idade") > 100)
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("idade_valida").alias("regra"),
        lit("idade").alias("campo"),
        col("idade").cast("string").alias("valor"),
        lit("Idade inválida ou não informada").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: salário válido
# ------------------------------------------

df_erro_salario = (
    df
    .filter(
        col("salario").isNull() |
        (col("salario") <= 0)
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("salario_valido").alias("regra"),
        lit("salario").alias("campo"),
        col("salario").cast("string").alias("valor"),
        lit("Salário inválido ou não informado").alias("divergencia"),
        lit("ALTO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: data_admissao não futura
# ------------------------------------------

df_erro_data_futura = (
    df
    .filter(
        col("data_admissao") > current_date()
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("data_admissao_nao_futura").alias("regra"),
        lit("data_admissao").alias("campo"),
        col("data_admissao").cast("string").alias("valor"),
        lit("Data de admissão posterior à data atual").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: status válido
# ------------------------------------------

df_erro_status = (
    df
    .filter(
        col("status").isNull() |
        (~col("status").isin(status_validos))
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("status_valido").alias("regra"),
        lit("status").alias("campo"),
        col("status").cast("string").alias("valor"),
        lit("Status inválido ou não informado").alias("divergencia"),
        lit("MEDIO").alias("severidade")
    )
)


# ------------------------------------------
# Regra: id_colaborador único
# ------------------------------------------

ids_duplicados = (
    df
    .groupBy("id_colaborador")
    .count()
    .filter(
        (col("count") > 1) &
        col("id_colaborador").isNotNull()
    )
    .select("id_colaborador")
)


df_erro_duplicidade = (
    df
    .join(
        ids_duplicados,
        on="id_colaborador",
        how="inner"
    )
    .select(
        col("id_colaborador"),
        col("id_carga"),
        lit("id_colaborador_unico").alias("regra"),
        lit("id_colaborador").alias("campo"),
        col("id_colaborador").cast("string").alias("valor"),
        lit("Identificador do colaborador possui registros duplicados").alias("divergencia"),
        lit("CRITICO").alias("severidade")
    )
)


# ------------------------------------------
# 12.11 União dos registros com erro
# ------------------------------------------

df_dq_erros = (
    df_erro_id
    .unionByName(df_erro_nome)
    .unionByName(df_erro_data_admissao)
    .unionByName(df_erro_idade)
    .unionByName(df_erro_salario)
    .unionByName(df_erro_data_futura)
    .unionByName(df_erro_status)
    .unionByName(df_erro_duplicidade)
)


# ------------------------------------------
# 12.12 Adição dos metadados dos erros
# ------------------------------------------

df_dq_erros = (
    df_dq_erros
    .withColumn(
        "tabela_origem",
        lit("people_analytics.silver.colaboradores")
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
        "people_analytics.monitoring.dq_colaboradores"
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
        "people_analytics.monitoring.dq_colaboradores_erros"
    )
)


# ==========================================
# 12.15 Validação da persistência
# ==========================================

print("Resumo de Data Quality:")
display(
    spark.table(
        "people_analytics.monitoring.dq_colaboradores"
    )
)


print("Detalhamento dos registros com erro:")
display(
    spark.table(
        "people_analytics.monitoring.dq_colaboradores_erros"
    )
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