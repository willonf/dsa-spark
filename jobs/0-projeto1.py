import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder.appName('projeto1').getOrCreate()

schema = types.StructType([
    types.StructField('nome', types.StringType(), True),
    types.StructField('idade', types.IntegerType(), True),
    types.StructField('email', types.StringType(), True),
    types.StructField('salario', types.IntegerType(), True),
    types.StructField('cidade', types.StringType(), True)
])

df = spark.read.schema(schema).json('data/usuarios.json')
df.show()

df = df.drop('email')

df = df.filter(
    (col('idade') > 35) &
    (col('cidade') == 'Natal') &
    (col('salario') < 7000)
)

df.printSchema()
df.show()

if df.rdd.isEmpty():
    print('Nenhum dado encontrado no arquivo .json. Verifique o arquivo')
else:
    df = df.withColumn('nome', regexp_replace(col('nome'), '@', ''))

    sqlite_db_path = os.path.abspath('data/usuarios.db')

    sqlite_url = 'jdbc:sqlite://' + sqlite_db_path
    properties = {'driver': 'org.sqlite.JDBC'}


    try:
        spark.read.jdbc(url=sqlite_url, table='dsa_usuarios', properties=properties)
        write_mode = 'append'
    except:
        write_mode = 'overwrite'
    print(sqlite_url)
    
    df.write.jdbc(url=sqlite_url, table='dsa_usuarios', mode=write_mode, properties=properties)
    print(f'Dados gravados! Modo: {write_mode}')
