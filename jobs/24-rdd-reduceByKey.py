import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('DSAProjeto2-Script35').getOrCreate()

dsa_dados = [('Cientista', 1),
             ('Arquiteto', 1),
             ('Arquiteto', 1),
             ('Analista', 1),
             ('Gerente', 1),
             ('Engenheiro', 1),
             ('Cientista', 1),
             ('Engenheiro', 1),
             ('Analista', 1),
             ('Cientista', 1),
             ('Engenheiro', 1),
             ('Cientista', 1),
             ('Engenheiro', 1)]

rdd = spark.sparkContext.parallelize(dsa_dados)
rdd.toDF().show()


def counter(x, y):
    print(x, y)
    return x + y


rdd2 = rdd.reduceByKey(lambda x, y: counter(x, y))
for elem in rdd2.collect():
    print(elem)
