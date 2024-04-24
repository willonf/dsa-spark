import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, MapType, StructField, StringType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [('Eduardo', {'categoria': 'black', 'nivel': 'brown'}),
             ('Sofia', {'categoria': 'brown', 'nivel': None}),
             ('Gabriel', {'categoria': 'red', 'nivel': 'black'}),
             ('Fernanda', {'categoria': 'grey', 'nivel': 'grey'}),
             ('Carlos', {'categoria': 'brown', 'nivel': ''})]

schema = StructType([
    StructField('nome', StringType(), True),
    StructField('classificacao', MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(data=dados_dsa, schema=schema)
df.printSchema()

df.show(truncate=False)

df2 = (df.withColumn('categoria', df.classificacao.getItem('categoria'))
       .withColumn('nivel', df.classificacao.getItem('nivel'))
       .drop('classificacao'))
df2.show(truncate=False)

df3 = (df.withColumn('categoria', df.classificacao['categoria'])
       .withColumn('nivel', df.classificacao['nivel'])
       .drop('classificacao'))
df3.show(truncate=False)

# Outra maneira
niveisDF = df.select(f.explode(f.map_keys(df.classificacao))).distinct()
niveisDF.show(truncate=False)

niveisList = niveisDF.rdd.map(lambda x: x[0]).collect()
niveisCol = list(map(lambda x: f.col('classificacao').getItem(x).alias(str(x)), niveisList))

df.select(df.nome, *niveisCol).show(truncate=False)
