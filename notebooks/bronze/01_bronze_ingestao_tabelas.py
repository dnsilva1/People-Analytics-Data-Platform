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
# MAGIC Realizar a ingestão dos dados de todas as abas (colaboradores, absenteismo, turnover e ferias) a partir de um arquivo Excel armazenado em um Volume do Unity Catalog, adicionando metadados técnicos e criando cada tabela Delta na camada Bronze.
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

arquivo = f"{VOLUME_PATH}/{ARQUIVO}" #caminho do arquivo completo

# COMMAND ----------

#Leitura de cada aba do arquivo excel

abas = pd.read_excel(
    arquivo,
    sheet_name=None
)

# COMMAND ----------

# Conversão das abas para  data frame Spark

dfs = {}

for nome_aba, df_pd in abas.items():

    dfs[nome_aba] = spark.createDataFrame(df_pd)

# COMMAND ----------

#Inclusão de metadados para rastreabilidade

for nome_aba, df in dfs.items():

    df = (
        df
        .withColumn("dt_ingestao", current_timestamp()) #Data de ingestão
        .withColumn("nome_arquivo", lit(ARQUIVO)) #Nome do arquivo
        .withColumn("nome_aba", lit(nome_aba)) #Nome da aba
        .withColumn("sistema_origem", lit("Excel")) #Sistema de origem
        .withColumn("usuario_responsavel", lit("davy")) #Usuário responsável
        .withColumn("identificador_carga", lit(ID_LOTE)) #Identificador unico da carga
        .withColumn("id_carga", monotonically_increasing_id()) #Identificador incremental
    )

    dfs[nome_aba] = df

# COMMAND ----------

#Validação

for nome_aba, df in dfs.items():

    print("=" * 60)
    print(f"Tabela: {nome_aba}")

    display(df)

    df.printSchema()

    print(f"Quantidade de registros: {df.count()}")

# COMMAND ----------

#Escrita da tabela Bronze

for nome_aba, df in dfs.items():

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(
              f"people_analytics.bronze.{nome_aba}"
          )
    )

    print(f"Tabela {nome_aba} gravada com sucesso.")

# COMMAND ----------

#Validação das tabelas gravadas

for nome_aba in dfs.keys():

    print("=" * 60)

    print(f"Validando {nome_aba}")

    display(
        spark.sql(
            f"""
            SELECT *
            FROM people_analytics.bronze.{nome_aba}
            """
        )
    )

    display(
        spark.sql(
            f"""
            SELECT COUNT(*) AS quantidade
            FROM people_analytics.bronze.{nome_aba}
            """
        )
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Consulta SQL (opcional) 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM people_analytics.bronze.colaboradores
# MAGIC LIMIT 100;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM people_analytics.bronze.absenteismo
# MAGIC LIMIT 100;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM people_analytics.bronze.turnover
# MAGIC LIMIT 100;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM people_analytics.bronze.ferias
# MAGIC LIMIT 100;

# COMMAND ----------

#validação formato spark (opcional)

spark.sql("""
    SELECT *
    FROM people_analytics.bronze.colaboradores
    LIMIT 100
""").display() #formato spark

# COMMAND ----------

# MAGIC %md
# MAGIC