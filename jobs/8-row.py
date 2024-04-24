# Coletando amostras

import os

from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = 'order_id INT, customer_id INT, payment FLOAT, order_date DATE, delivery_date DATE'
df = spark.read.schema(schema).csv('data/orders.csv', header=True)
df.printSchema()

# 6% dos dados, sem repetição
df_sample = df.sample(seed=42, fraction=0.1, withReplacement=False)
print(df_sample.count())
df_sample.show(truncate=False)

# Amostragem estratificada
df_stratified = df.sampleBy(col='order_id', seed=42, fractions={1: 0.1, 2: 0.2, 3: 0.3, 4: 0.4})
df_stratified.show(truncate=False)
