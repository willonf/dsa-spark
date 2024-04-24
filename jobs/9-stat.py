# Coletando amostras

import os

from pyspark.sql import SparkSession, functions as f

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = 'order_id INT, customer_id INT, payment FLOAT, order_date DATE, delivery_date DATE'
df = spark.read.schema(schema).csv('data/orders.csv', header=True)
df.printSchema()
df.show(5, truncate=False)

df.agg({'payment': 'sum'}).show()
df.agg(f.sum('payment')).show()
df.agg(f.min('payment')).show()
df.agg(f.stddev('payment')).show()
