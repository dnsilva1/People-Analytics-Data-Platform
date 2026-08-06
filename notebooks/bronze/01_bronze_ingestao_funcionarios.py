# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC # People Analytics Data Platform
# MAGIC
# MAGIC ## Camada Bronze
# MAGIC
# MAGIC ### Notebook: 01_bronze_ingestao_funcionarios
# MAGIC
# MAGIC **Objetivo**
# MAGIC
# MAGIC Realizar a ingestão dos dados de funcionários a partir de um arquivo Excel armazenado em um Volume do Unity Catalog, adicionando metadados técnicos e persistindo os dados em uma tabela Delta na camada Bronze.
# MAGIC

# COMMAND ----------

##Importação das bibliotecas

import uuid #importar biblioteca uuid para gerar identificadores unicos
import pandas as pd #importar biblioteca pandas para manipular dados

from pyspark.sql.functions import (
    current_timestamp,
    lit,
    monotonically_increasing_id
)#importar biblioteca pyspark.sql.functions para manipular dados

# COMMAND ----------

# Configurações do projeto/Notebook

VOLUME_PATH = "/Volumes/people_analytics/bronze/landing" #definir caminho do volume

ARQUIVO = "People_Analytics_Dataset (1).xlsx" #definir arquivo

ID_LOTE = str(uuid.uuid4()) #gerar identificador único

USUARIO = "davy" #definir usuário

SISTEMA_ORIGEM = "excel" #definir sistema de origem

# COMMAND ----------

#Leitura do arquivo

arquivo = f"{VOLUME_PATH}/{ARQUIVO}" #definir arquivo

df_pd = pd.read_excel(arquivo) #ler arquivo

df = spark.createDataFrame(df_pd) #criar dataframe

# COMMAND ----------

#Validação inicial

print(f"Quantidade de registros: {df.count()}") #visualizar quantidade de linhas

df.printSchema() #visualizar schema

display(df) #visualizar dataframe

# COMMAND ----------

#Inclusão de metadados para rastreabilidade

df = (
    df
    .withColumn("dt_ingestao", current_timestamp()) #Data de ingestão
    .withColumn("nome_arquivo", lit(ARQUIVO)) #Nome do arquivo
    .withColumn("sistema_origem", lit(SISTEMA_ORIGEM)) #Sistema de origem
    .withColumn("usuario_responsavel", lit(USUARIO)) #Usuário responsável
    .withColumn("identificador_carga", lit(ID_LOTE)) #Identificador unico da carga
    .withColumn("id_carga", monotonically_increasing_id()) #Identificador incremental
)

# COMMAND ----------

#Validação após inclusão dos metadados

display(df)

print(f"Quantidade de registros: {df.count()}")

# COMMAND ----------

#Escrita da tabela Bronze

(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("people_analytics.bronze.funcionarios")
) #gravar dados na bronze em formato delta lakes - Parquet

# COMMAND ----------

#Validação da tabela gravada

df_bronze = spark.table("people_analytics.bronze.funcionarios")

display(df_bronze)

print(f"Total de registros na Bronze: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Consulta SQL (opcional) 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM people_analytics.bronze.funcionarios
# MAGIC LIMIT 100;

# COMMAND ----------

#validação formato spark (opcional)

spark.sql("""
    SELECT *
    FROM people_analytics.bronze.funcionarios
    LIMIT 100
""").display() #formato spark

# COMMAND ----------

# MAGIC %md
# MAGIC