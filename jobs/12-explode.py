import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, MapType, StructField, StringType, ArrayType, IntegerType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [('Eduardo', [1, 2, 3], {'categoria': 'black', 'nivel': 'brown'}),
             ('Sofia', [4, 5, 6], {'categoria': 'brown', 'nivel': None}),
             ('Gabriel', [7, 8, 9], {'categoria': 'red', 'nivel': 'black'}),
             ('Fernanda', [10, 11, 12], {'categoria': 'grey', 'nivel': 'grey'}),
             ('Nulice', [10, 11, 12], None),
             ('Nulan', None, {'categoria': 'grey', 'nivel': 'grey'}),
             ('Carlos', [12, 14, 15], {'categoria': 'brown', 'nivel': ''})]

schema = StructType([
    StructField('nome', StringType(), True),
    StructField('codes', ArrayType(IntegerType()), True),
    StructField('classificacao', MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(data=dados_dsa, schema=schema)
df.printSchema()
df.show(truncate=False)

df2 = df.select(df.nome, f.explode(f.col('codes')).alias('codes'))
df2.printSchema()
df2.show(truncate=False)

df3 = df.select(df.nome, f.explode(f.col('classificacao')).alias('chave', 'valor'))
df3.printSchema()
df3.show(truncate=False)

# Incluindo valores NULL
df4 = df.select(df.nome, f.explode_outer(f.col('codes')).alias('codes'))
df4.printSchema()
df4.show(truncate=False)

df5 = df.select(df.nome, f.explode_outer(f.col('classificacao')).alias('chave', 'valor'))
df5.printSchema()
df5.show(truncate=False)

# Adicionando posição
df6 = df.select(df.nome, f.posexplode(f.col('classificacao')))
df6.printSchema()
df6.show(truncate=False)

df7 = df.select(df.nome, f.posexplode_outer(f.col('classificacao')))
df7.printSchema()
df7.show(truncate=False)
