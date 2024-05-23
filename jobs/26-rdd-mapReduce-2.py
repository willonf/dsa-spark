import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('DSAProjeto2-Script36').getOrCreate()

rdd = spark.sparkContext.textFile("/opt/spark/data/dataset1.txt")

rdd = rdd.flatMap(lambda record: record.split(' '))
print(rdd.collect())
print('\n----------')

rdd = rdd.map

