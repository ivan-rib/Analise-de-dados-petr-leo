from modules.conector_mysql import interface_db
from pyspark.sql import SparkSession
import pandas as pd

# conexão com o spark session
spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
# conexão com o mysql
bancosql = interface_db("root", 'Petroleo2022', '35.199.106.39', 'atividade_final')

#buscando a tabela panorama_geral_petroleo no mysql
query = "SELECT * FROM panorama_geral_petroleo"
dados = bancosql.selecionar(query)


## -------------------------------------------------
## retirando todas as linhas com RESERVAS no tipo de operação
## -------------------------------------------------

dados_petr = pd.DataFrame(dados, columns=['id','bloco','regiao','pais','volume','ano','tipo_operacao'])
dados_petr= dados_petr.drop(['id','bloco'], axis= 1)
dados_retirados = dados_petr.loc[(dados_petr['tipo_operacao'] == 'RESERVAS' )]
dados_final = dados_petr.drop(dados_retirados.index)
# print(dados_final.shape)

# importando tabela para o formato csv
dados_final.to_csv('dados_panorama_geral_petroleo_sem_reserva.csv', index= False)

# lendo arquivo csv gerado pelo pandas, utilizando o pyspark
dados_csv = spark.read.format("csv")\
	.option("header","true")\
	.option("delimiter",",")\
	.option("inferSchema","true")\
	.load(r"C:\Users\elihe\Desktop\atividade_final\scripts\dados_panorama_geral_petroleo_sem_reserva.csv")
 
# gravando arquivo no formato parquet
dados_csv.write.parquet(r"C:\Users\elihe\Desktop\atividade_final\scripts\parquet_sem_reserva")

# dados_final = spark.write(r"C:\spark\atividade_cluster")