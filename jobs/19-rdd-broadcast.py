import os

from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

spark = SparkSession.builder.appName('projeto1-Script31').getOrCreate()

cidades = {"RJ": "Cabo Frio", "SP": "Indaiatuba", "MG": "Contagem"}
broadcast_variable = spark.sparkContext.broadcast(cidades)

dados_dsa = [("Marcelo", "Andrade", "BRA", "SP"),
             ("Isabel", "Figueiredo", "BRA", "RJ"),
             ("Renato", "Carvalho", "BRA", "MG"),
             ("Bianca", "Serra", "BRA", "MG")]

rdd = spark.sparkContext.parallelize(dados_dsa)


def dsa_converte_dados(code):
    return broadcast_variable.value[code]


resultado = rdd.map(lambda x: (x[0], x[1], x[2], dsa_converte_dados(x[3]))).collect()
print(resultado)

# Variáveis broadcast são um mecanismo oferecido pelo Apacha Spark para compartilhar
# eficientemente dados imutáveis em todos os nós do cluster.
# Em vez de enviar esses dados junto com as tarefas, o spark envia uma cópia desses dados
# para cada nó worker apenas uma vez, tornando-os disponíveis como variável local em cada máquina.
# Isso reduz significamente o custo de comunicação e a quantidade de dados transferidos durante o
# processamento distribuído.

# As variáveis broadcast são especialmente úteis quando uma grande quantidade de dados precisa ser acessada
# por múltiplas tarefas distribuídas em vários nós, como em operações de join, lookup ou outros cálculos
# que requerem acesso a dados compartilhados (como tabelas de dimensão em processamentos de data warehouse).

# você pode usar variáveis broadcast com DataFrames no Apache Spark, embora o mecanismo seja um pouco diferente
# do uso direto de variáveis broadcast com RDDs. Em vez de usar o método broadcast() explicitamente como faria com RDDs,
# o Spark já otimiza operações de join em DataFrames de forma automática, especialmente em situações onde uma tabela
# pequena é usada repetidamente em joins com tabelas maiores. Este processo é conhecido como broadcast join.

# Quando você realiza uma operação de join entre dois DataFrames e um deles é significativamente menor que o outro,
# o Spark pode decidir automaticamente utilizar um broadcast join. Isso significa que ele enviará automaticamente o
# DataFrame menor para todos os nós de processamento, permitindo que o join seja realizado localmente em cada nó
# sem a necessidade de shuffles dispendiosos de rede, que são típicos em joins normais de grandes DataFrames distribuídos.
