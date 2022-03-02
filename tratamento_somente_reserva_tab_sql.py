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


## -------------------------------------------------------
## deixando somente as linhas com RESERVAS no tipo de operação
## -------------------------------------------------------

# retirando as linha com o tipo_operação "REFINO"
dados_petr = pd.DataFrame(dados, columns=['id','bloco','regiao','pais','volume','ano','tipo_operacao'])
dados_petr= dados_petr.drop(['id','bloco'], axis= 1)
dados_retirados = dados_petr.loc[(dados_petr['tipo_operacao'] == 'REFINO' )]
dados_final = dados_petr.drop(dados_retirados.index)
dados_s_refino = dados_final
# print(dados_final.tail())


# retirando as linha com o tipo_operação "PRODUÇÃO"
dados_s_p = dados_final.loc[(dados_final['tipo_operacao'] == 'PRODUÇÃO' )]
dados_retirados1 = dados_final.drop(dados_s_p.index)
dados_s_producao = dados_retirados1
# print(dados_s_producao.head(50))

# retirando as linha com o tipo_operação "CONSUMO"
dados_retirados2 = dados_retirados1.loc[(dados_retirados1['tipo_operacao'] == 'CONSUMO' )]
dados_retirados3 = dados_retirados1.drop(dados_retirados2.index)
dados_total_reserva = dados_retirados3
# print(dados_total_reserva.shape)

# importando tabela para o formato csv
dados_total_reserva.to_csv('dados_panorama_geral_petroleo_somente_reserva.csv', index= False)

# lendo arquivo csv gerado pelo pandas, utilizando o pyspark
dados_csv = spark.read.format("csv")\
	.option("header","true")\
	.option("delimiter",",")\
	.option("inferSchema","true")\
	.load(r"C:\Users\elihe\Desktop\atividade_final\scripts\dados_panorama_geral_petroleo_somente_reserva.csv")
 
# gravando arquivo no formato parquet
dados_csv.write.parquet(r"C:\Users\elihe\Desktop\atividade_final\scripts\parquet_somente_reserva")