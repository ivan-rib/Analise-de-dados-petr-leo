
from pyspark.sql import SparkSession
import csv
from pyspark.sql.functions import regexp_replace
import pandas as pd

# conexão com spark session
spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

#lendo arquivo csv com pyspark
terra_primeiro_tri = spark.read.format("csv").option("header", "True").option("delimiter", ",").option("inferSchema", "true").option("encoding", "UTF-8").load(r"./producao_petroleo_gas_nacional_producao-terra-2018-4trim.csv")

# tratando dados com pyspark, transfomando separado numerico de virgula para ponto
terra = terra_primeiro_tri.withColumn('Produção de Óleo (m³)', regexp_replace('Produção de Óleo (m³)', ',', '.'))
terra = terra.withColumn('Produção de Gás Associado (Mm³)', regexp_replace('Produção de Gás Associado (Mm³)', ',', '.'))
terra = terra.withColumn('Produção de Água (m³)', regexp_replace('Produção de Água (m³)', ',', '.'))
terra = terra.withColumn('Injeção de Outros Fluidos (m³)', regexp_replace('Injeção de Outros Fluidos (m³)', ',', '.'))
terra = terra.withColumn('Injeção de Polímeros (m³)', regexp_replace('Injeção de Polímeros (m³)', ',', '.'))
terra = terra.withColumn('Injeção de Vapor de Água (t)', regexp_replace('Injeção de Vapor de Água (t)', ',', '.'))
terra = terra.withColumn('Injeção de Nitrogênio (Mm³)', regexp_replace('Injeção de Nitrogênio (Mm³)', ',', '.'))
terra = terra.withColumn('Injeção de Gás Carbônico (Mm³)', regexp_replace('Injeção de Gás Carbônico (Mm³)', ',', '.'))
terra = terra.withColumn('Injeção de Água para Descarte (m³)', regexp_replace('Injeção de Água para Descarte (m³)', ',', '.'))
terra = terra.withColumn('Injeção de Água para Recuperação Secundária (m³)', regexp_replace('Injeção de Água para Recuperação Secundária (m³)', ',', '.'))
terra = terra.withColumn('Injeção de Gás (Mm³)', regexp_replace('Injeção de Gás (Mm³)', ',', '.'))
terra = terra.withColumn('Produção de Gás Não Associado (Mm³)', regexp_replace('Produção de Gás Não Associado (Mm³)', ',', '.'))
terra = terra.withColumn('Produção de Condensado (m³)', regexp_replace('Produção de Condensado (m³)', ',', '.'))

# tratando dados com pyspark, transfomando separado numerico de virgula para ponto
terra.repartition(1).write.option("header", "false").option("encoding", "UTF-8").csv("producao_petroleo_gas_nacional_producao-terra-2018-4trim_tratado.csv")

