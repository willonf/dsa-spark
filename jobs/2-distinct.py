import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

df = spark.read.csv('data/orders.csv', header=True)
df.show(truncate=False)
df.printSchema()
print(df.count())

districts = df.distinct()
districts.show(truncate=False)
print(districts.count())

dropped = df.dropDuplicates(subset=['customer_id'])
dropped.show(truncate=False)
print(dropped.count())
