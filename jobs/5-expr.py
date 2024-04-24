import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

df_users = spark.read.json('data/usuarios.json')
df_users.show(truncate=False)
df_users.withColumn('NomeCompleto', f.expr("nome || ' ' || cidade")).show(truncate=False)

df_users = df_users.withColumn('genero',
                               f.expr(
                                   "CASE WHEN genero = 'M' THEN 'Masculino'" + "WHEN genero = 'F' THEN 'Feminino' ELSE 'desconhecido'"
                               ))
