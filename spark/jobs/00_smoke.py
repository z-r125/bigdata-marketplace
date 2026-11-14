from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
fmt = "org.apache.spark.sql.cassandra"
opts = {"keyspace": "smoke", "table": "ping"}

# Ecriture : 100 lignes depuis Spark vers Cassandra
rows = [(i, f"msg-{i}") for i in range(1, 101)]
spark.createDataFrame(rows, ["id", "msg"]).write.format(fmt).options(**opts).mode("append").save()

# Lecture : relecture depuis Cassandra
df = spark.read.format(fmt).options(**opts).load()
n = df.count()
df.orderBy("id").show(5)
print(f"SMOKE TEST OK - {n} lignes ecrites puis relues via le connecteur")
spark.stop()