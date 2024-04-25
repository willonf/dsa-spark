import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, MapType, StructField, StringType, ArrayType, IntegerType

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [('Eduardo', [[1, 2, 3], [1, 7, 3]], {'categoria': 'black', 'nivel': 'brown'}),
             ('Sofia', [[4, 5, 6], [4, 5, 10]], {'categoria': 'brown', 'nivel': None}),
             ('Gabriel', [[7, 8, 9], [70, 89, 9]], {'categoria': 'red', 'nivel': 'black'}),
             ('Fernanda', [[10, 11, 12], [102, 11, 12]], {'categoria': 'grey', 'nivel': 'grey'}),
             ('Nulice', [[10, 121, 12], [10, 121, 112]], None),
             ('Nulan', None, {'categoria': 'grey', 'nivel': 'grey'}),
             ('Carlos', [[132, 14, 15], [132, 124, 155]], {'categoria': 'brown', 'nivel': ''})]

schema = StructType([
    StructField('nome', StringType(), True),
    StructField('codes', ArrayType(ArrayType(IntegerType())), True),
    StructField('classificacao', MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(data=dados_dsa, schema=schema)
df.printSchema()
df.show(truncate=False)

df.select('nome', f.explode(f.col('codes'))).show(truncate=False)
df.select('nome', f.flatten(f.col('codes'))).show(truncate=False)

results = df.collect()
print(results)
print(results[0])
