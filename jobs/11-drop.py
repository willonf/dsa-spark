import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, ArrayType, StructField, StringType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [(("Patricia", "Freitas"), ["Python", "Rust", "C++"], ["Scala", "Ruby"], "RJ", None),
             (("Fernanda", "Oliveira"), ["Java", "Python", "C++"], ["PHP", "Perl"], "MG", "RS"),
             (("Willon", "Ferreira"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", "RS"),
             (("Jon", "Carvalho"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", None),
             (("Joana", "Carvalho"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", "RS"),
             (("Nulice", "Nula"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", "RS"),
             (("Nulice", "Nula"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", "RS"),
             (("Nulice", "Nula"), ["C", "Python", "Typescript"], ["PHP", "Perl"], "AM", "RS"),
             (None, None, None, None, None),
             (('', ''), [''], [''], '', ''),
             (("Carlos", "Souza"), ["PHP", "Java"], ["Ruby", "Python"], "ES", "SC")]

schema = StructType([
    StructField(name="nome", nullable=True, dataType=StructType([
        StructField(name='primeiroNome', nullable=True, dataType=StringType()),
        StructField(name='ultimoNome', nullable=True, dataType=StringType()),
    ])),
    StructField(name="linguagemMaisUsada", nullable=True, dataType=ArrayType(StringType())),
    StructField(name="linguagemMenosUsada", nullable=True, dataType=ArrayType(StringType())),
    StructField(name="estadoAnterior", nullable=True, dataType=StringType()),
    StructField(name="estadoAtual", nullable=True, dataType=StringType())
])

df = spark.createDataFrame(dados_dsa, schema)

df.drop('estadoAtual').show(truncate=False)

columns_to_drop = ('estadoAnterior', 'linguagemMenosUsada')
df.drop(*columns_to_drop).show(truncate=False)

df.dropna(how='any').show(truncate=False)
df.dropna(how='all').show(truncate=False)

df.dropDuplicates().show(truncate=False)
