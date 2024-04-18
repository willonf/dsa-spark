import os

from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nTestando nível de log:\n')

spark = SparkSession.builder.appName('script_1').getOrCreate()

data = [('FCD', 'Data Science', 6), ('FED', 'Engenharia de dados', 5), ('FADA', 'Analytics', 4)]

columns = ['nome', 'categoria', 'num_cursos']

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)
