from pyspark.sql import SparkSession

# Kreiranje Spark sesije
spark = SparkSession.builder.appName("CassandraIntegration") \
    .config("spark.cassandra.connection.host", "172.23.0.2") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Definisanje DataFrame-a koji treba upisati u Cassandra bazu podataka
data = [("1", "nik")]

# Kreiranje PySpark DataFrame-a
df = spark.createDataFrame(data, ["id", "name"])

# Upisivanje DataFrame-a u Cassandra bazu podataka
df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="tab_4", keyspace="ks4").save()

# Zatvaranje Spark sesije
spark.stop()
