import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = 'order_id INT, customer_id INT, payment FLOAT, order_date DATE, delivery_date DATE'
df = spark.read.schema(schema).csv('data/orders.csv', header=True)
df.printSchema()

df.show()

df = df.withColumn('increment', f.lit(1))

df.select(f.col('delivery_date'),
          f.col('increment'),
          f.add_months(f.col('delivery_date'), 1).alias('inc_date')).show()

# df.createOrReplaceTempView('dsa_temp')
# spark.sql("SELECT order_date, increment, ADD_MONTHS(TO_DATE(order_date, 'yyyy-MM-dd'), CAST(increment as INT)) AS inc_date from dsa_temp").show()
