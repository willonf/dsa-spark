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
             (("Carlos", "Souza"), ["PHP", "Java"], ["Ruby", "Python"], "ES", "SC")]

arrayCol = ArrayType(StringType(), False)

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
# df.printSchema()
# df.show(truncate=False)
#
# df.filter(f.col('estadoAnterior') == 'AM').show(truncate=False)
# df.filter(df.estadoAtual == 'AM').show(truncate=False)
# df.filter("estadoAtual == 'AM'").show(truncate=False)
#
# df.filter(f.col('estadoAnterior') != 'AM').show(truncate=False)
# df.filter(df.estadoAnterior != 'AM').show(truncate=False)
# df.filter(~(df.estadoAnterior == 'AM')).show(truncate=False)
# df.filter("estadoAnterior <> 'AM'").show(truncate=False)
#
# lista_estados = ['AM', 'RJ']
#
# df.filter(df.estadoAnterior.isin(lista_estados)).show(truncate=False)
# df.filter((df.estadoAnterior == 'AM') | (df.estadoAtual == 'RS')).show(truncate=False)
#
# df.filter(df.nome.primeiroNome.startswith('W')).show(truncate=False)
# df.filter(df.nome.primeiroNome.endswith('a')).show(truncate=False)
# df.filter(df.nome.primeiroNome == 'Willon').show(truncate=False)

# df.filter(df.nome.primeiroNome.like('W%n')).show(truncate=False)
# df.filter(df.nome.ultimoNome.rlike('(?i).*Carvalho$')).show(truncate=False)

df.filter('estadoAtual is NULL').show(truncate=False)
df.filter(df.estadoAtual.isNull()).show(truncate=False)
df.filter(df.estadoAtual.isNotNull()).show(truncate=False)

df.createOrReplaceTempView("dados_dsa")

spark.sql('SELECT * FROM dados_dsa WHERE estadoAtual is NULL').show(truncate=False)

# Deletando registos onde o valor da coluna estado é NULO
df.show(truncate=False)
df.na.drop(subset=['estadoAtual']).show(truncate=False)
