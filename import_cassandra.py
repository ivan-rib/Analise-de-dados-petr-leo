from msilib import Table
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
# from pyspark.sql.functions import col
from pyspark.sql.types import *
import pandas as pd

# conexão com o cassandra atrvés da spark session
spark = SparkSession.builder\
    .appName('AtividadeFinal')\
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0")\
    .config("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.cassandra.connection.host","34.145.126.113") \
    .config("spark.cassandra.connection.port","9042") \
    .config("spark.cassandra.output.batch.grouping.buffer.size", "3000") \
    .config("spark.cassandra.output.concurrent.writes", "1500") \
    .config("cassandra.output.throughput_mb_per_sec", "128") \
    .config("spark.cassandra.output.batch.size.bytes", "2056") \
    .config("cassandra.connection.keep_alive_ms", "30000") \
    .config("spark.sql.caseSensitive", "True")\
    .getOrCreate()
    
#buscando tabela panorama_geral_petr_distribuidora no cassandra
data = spark.read.format("org.apache.spark.sql.cassandra")\
    .options(table = 'panorama_geral_petr_distribuidora', keyspace = 'atividade_final').load()    


print(data.count())
# keyspace = 'atividade_final'
# table = 'panorama_geral_petr_distribuidora'

# df_read = 'select * from '

# def readData(df, table:str):    
#         df.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .option("spark.cassandra.connection.host","34.145.126.113") \
#         .option("spark.cassandra.connection.port","9042") \
#         .option("keyspace", keyspace) \
#         .option("table", table) \
#         .load ()
            
  
# readData(df_read, 'panorama_geral_petr_distribuidora')



# saveData(df_read, 'panorama_geral_petr_distribuidora')
    
