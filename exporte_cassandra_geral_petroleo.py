from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
    
#lendo parquet no cassandra e adicionando a coluna "id_reserva do tipo uuid" no keyspace "atividade_final"   
df_inserir_cassandra = spark.read.option('encoding', 'UTF-8').parquet(r"C:\Users\elihe\Desktop\atividade_final\scripts\parquet_sem_reserva_petroleo_geral")
df_cassandra_comID = df_inserir_cassandra.withColumn("id_reserva", expr("uuid()"))
keyspace = 'atividade_final'

#função de inserção no cassandra
def saveData(df, table:str):
        df.write\
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode('append') \
        .save()

#inserindo parquet na tabela  "panorama_geral_petr_reserva"        
saveData(df_cassandra_comID, "panorama_geral_petr")
