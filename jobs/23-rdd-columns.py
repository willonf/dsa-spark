import os

from pyspark.sql import SparkSession

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script34').getOrCreate()

# Lista de tuplas contendo nomes de departamentos e seus respectivos IDs
dept = [("Vendas", 10),
        ("Marketing", 20),
        ("RH", 30),
        ("Engenharia de Dados", 40)]

rdd = spark.sparkContext.parallelize(dept)
rdd.toDF().show()

deptColumns = ["dept_name", "dept_id"]

rdd.toDF(deptColumns).show()
