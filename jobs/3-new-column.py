import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

df = spark.read.csv('data/sales.csv', header=True)
df.printSchema()

df = df.withColumn('discount', f.lit(0.10))
df = df.withColumn('value_discounted', df.total_price * (1 - f.col('discount')))
df.show()

df = df.withColumn(colName='code', col=f.concat_ws('-', 'sales_id', 'order_id', 'product_id'))
df.show()

df = df.withColumn(colName='current_date', col=f.current_date())
df.show()

df = df.withColumn('discount_delivery',
                   f.when((df.quantity == 1), 0.1)
                   .when((df.quantity == 2), 0.15)
                   .otherwise(0.25))
df.show(truncate=False)

df.select('code',
          'discount_delivery',
          f.lit('XPTO').alias('legacy_code'),
          f.current_date().alias('current_date')
          ).show()

df.createOrReplaceTempView('dsa_temp')

spark.sql('select * from dsa_temp').show()
spark.sql('select sales_id, total_price, current_date as today_date from dsa_temp').show()
