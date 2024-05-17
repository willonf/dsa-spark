import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1-Script31').getOrCreate()
df_large = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')], ['id', 'value'])
df_small = spark.createDataFrame([(1, 'X'), (2, 'Y')], ['id', 'value'])  # Dependendo do tamanho do DF, o spark já coloca como broadcast

df_joined = df_large.join(broadcast(df_small), on='id')  # Para garantir o broadcast, utiliza-se a função broacast
df_joined.show()
