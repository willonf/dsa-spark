import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, ArrayType, StructField, StringType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [("Patricia,Freitas", ["Python", "Rust", "C++"], ["Scala", "Ruby"], "RJ", "SP"),
             ("Fernanda,Oliveira,", ["Java", "Python", "C++"], ["PHP", "Perl"], "MG", "RS"),
             ("Carlos,Souza", ["PHP", "Java"], ["Ruby", "Python"], "ES", "SC")]

arrayCol = ArrayType(StringType(), False)

schema = StructType([
    StructField("nome", StringType(), True),
    StructField("linguagemMaisUsada", ArrayType(StringType()), True),
    StructField("linguagemMenosUsada", ArrayType(StringType()), True),
    StructField("estadoAnterior", StringType(), True),
    StructField("estadoAtual", StringType(), True)
])

df = spark.createDataFrame(dados_dsa, schema)
df.printSchema()
df.show(truncate=False)

df.select(df.nome, f.explode(df.linguagemMenosUsada)).show()
df.select(f.split(str=df.nome, pattern=",").alias('nomeAsArray')).show(truncate=False)
df.select(df.nome, df.linguagemMaisUsada, f.array_contains(col=df.linguagemMaisUsada, value='Python').alias('use_python')).show(truncate=False)

df = df.withColumn('linguagemMaisUsada', f.concat_ws(',', f.col('linguagemMaisUsada')))
df.show()
df.printSchema()
