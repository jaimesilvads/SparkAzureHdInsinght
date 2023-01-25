# Importamdo as bibliotecas
import os.path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# sessão Spark
spark = SparkSession \
    .builder \
    .appName("job-1-spark-hdinsight") \
    .config("spark.sql.warehouse.dir", os.path.abspath('spark-warehouse')) \
    .getOrCreate()

# definindo o método de logging da aplicação
spark.sparkContext.setLogLevel("ERROR")

# lendo os dados do Data Lake
df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema", "True")\
    .csv("abfs://landing@jaimedatalake.dfs.core.windows.net/*.csv")

# imprime os dados lidos da raw
print("\nImprime os dados lidos da lading:")
print(df.show())

# imprime o schema do dataframe
print("\nImprime o schema do dataframe lido da raw:")
print(df.printSchema())

# converte para formato parquet
print("\nEscrevendo os dados lidos da raw para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("abfs://processing@jaimedatalake.dfs.core.windows.net/df-formatado.parquet")

# lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
    .load("abfs://processing@jaimedatalake.dfs.core.windows.net/df-formatado.parquet")

# imprime os dados lidos em parquet
print("\nImprime os dados lidos em parquet da processing zone")
print(df_parquet.show())

# cria uma view para trabalhar com sql
df_parquet.createOrReplaceTempView("Dados_SQL")

# processa os dados conforme regra de negócio
df_sql = spark.sql("SELECT BNF_CODE as Bnf_code \
                        ,SUM(ACT_COST) as Soma_custo \
                        ,SUM(QUANTITY) as Soma_Quantidade \
                        ,SUM(ITEMS) as Soma_items \
                        ,AVG(ACT_COST) as Media_Custo \
                       FROM Dados_SQL \
                       GROUP BY bnf_code")

# imprime o resultado do dataframe criado
print("\n ========= Imprime dados agregados =========\n")
print(df_result.show())

# converte para formato parquet
print("\nEscrevendo os dados Agregados na Curated Zone...")

# converte os dados processados para parquet e escreve na curated zone
df_sql.write.format("parquet")\
    .mode("overwrite")\
    .save("abfs://curated@jaimedatalake.dfs.core.windows.net/df-Dados-Agregados.parquet")


spark.stop()
