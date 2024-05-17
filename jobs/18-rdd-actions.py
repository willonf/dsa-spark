import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

dados_dsa = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 30), ("F", 60)]
inputRDD = spark.sparkContext.parallelize(dados_dsa)
listRdd = spark.sparkContext.parallelize([8, 3, 6, 7, 5, 3, 2, 2, 4, 6, 2, 4, 7, 4, 1])

print(f'Count: {listRdd.count()}')
print(f'Approximate count with timeout: {listRdd.countApprox(1000)}')
print(f'Approximate distinct count: {listRdd.countApproxDistinct()}')
print(f'Count by value: {listRdd.countByValue()}')
print(f'First: {listRdd.first()}')
print(f'2 highest elements: {listRdd.top(2)}')
print(f'2 highest elements: {inputRDD.top(2)}')
print(f'2 first elements: {listRdd.take(2)}')
print(f'2 first elements (ordered): {listRdd.takeOrdered(2)}')
print(f'Max element: {listRdd.max()}')
print(f'Min element: {listRdd.min()}')

