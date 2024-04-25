import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = 'customer_id INT, customer_name STRING, gender STRING, age INTEGER, home_address STRING, zip_code STRING, city STRING, state STRING, country STRING'
df = spark.read.schema(schema).csv('data/customers.csv', header=True)
df.printSchema()

df.show()

df = df.withColumn('increment', f.lit(1))

df2 = df.groupby('gender').pivot('country').count()
df2.show(n=10, truncate=False)

unpivot_expr = "stack(3, 'Australia', Australia) as (Pais,Total)"

unpivot_df = df2.select('gender', f.expr(unpivot_expr))
unpivot_df.show(truncate=False)
