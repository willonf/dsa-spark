import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = 'order_id INTEGER, customer_id INTEGER, payment FLOAT, order_date DATE, delivery_date DATE'
df = spark.read.schema(schema).csv('data/orders.csv', header=True)
df.printSchema()

df.show()

df2 = (df.withColumn('day', f.split(df.order_date, '-').getItem(2))
       .withColumn('month', f.split(df.order_date, '-').getItem(1))
       .withColumn('year', f.split(df.order_date, '-').getItem(0)))
df2.show(truncate=False)

split_col = f.split(df.order_date, '-')

df3 = (df.withColumn('day', split_col.getItem(2))
       .withColumn('month', split_col.getItem(1))
       .withColumn('year', split_col.getItem(0)))
df3.show(truncate=False)

df4 = (df.withColumn('day', f.split(df.order_date, '-', 1))
       .withColumn('month', f.split(df.order_date, '-', 2))
       .withColumn('year', f.split(df.order_date, '-', 3))
       .withColumn('x', f.split(df.order_date, '-', 4)))
df4.show(truncate=False)
