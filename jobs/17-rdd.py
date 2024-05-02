import os

from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()
listRDD = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f'RDD count: {listRDD.count()}')

seqOP = (lambda current, value: current + value)
combOP = (lambda current, value: current + 1)
agg = listRDD.aggregate(0, seqOP, combOP)
print(agg)

seqOP2 = (lambda current, value: (current[0] + value, current[1] + 1))
combOP2 = (lambda current, value: (current[0] + value[0], current[1] + value[1]))
agg2 = listRDD.aggregate((0, 0), seqOP2, combOP2)
print(agg2)

# Agregação em árvore. Mais eficiente para agregação em grandes RDD's
agg2 = listRDD.treeAggregate(0, seqOP, combOP)
print(agg2)

agg2 = listRDD.fold(0, seqOP)
print(agg2)

reducedRDD = listRDD.reduce(seqOP)
print(reducedRDD)
