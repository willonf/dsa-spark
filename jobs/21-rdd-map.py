import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')
spark = SparkSession.builder.appName('DSAProjeto2-Script32').getOrCreate()

dados_dsa = [('Carlos', 'Estrela', 'M', 30), ('Tatiana', 'Moraes', 'F', 41), ('Renato', 'Carvalho', 'M', 62)]

colunas = ["primeiro_nome", "sobrenome", "genero", "salario"]

df = spark.createDataFrame(data=dados_dsa, schema=colunas)

df.show()

dsa_rdd = df.rdd.map(lambda x: (x[0] + " " + x[1], x[2], x[3] * 2))
dsa_rdd.toDF(['nome', 'genero', 'novo_salario']).show()

dsa_rdd = df.rdd.map(lambda x: (x['primeiro_nome'] + " " + x['sobrenome'], x['genero'], x['salario'] * 2))
dsa_rdd.toDF(['nome', 'genero', 'novo_salario']).show()

dsa_rdd = df.rdd.map(lambda x: (x.primeiro_nome + " " + x.sobrenome, x.genero, x.salario * 2))
dsa_rdd.toDF(['nome', 'genero', 'novo_salario']).show()


def dsa_func(x):
    nome = f'{x.primeiro_nome} {x.sobrenome}'
    genero = x.genero.lower()
    salario = x.salario * 2
    return (nome, genero, salario)


dsa_rdd = df.rdd.map(lambda x: dsa_func(x))
dsa_rdd.toDF(['nome', 'genero', 'novo_salario']).show()

df.rdd.map(dsa_func).toDF().show()
