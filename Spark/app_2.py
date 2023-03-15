from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json

spark = SparkSession.builder.appName("PySpark_Kafka_Consumer").getOrCreate()
    #.config("spark.cassandra.connection.host", "cassandra") \
    #.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "topic3") \
  .option("startingOffsets", "earliest") \
  .option("maxOffsetsPerTrigger", 1) \
  .load()

parsed_df =  df.selectExpr("CAST(value AS STRING)")

split_col = split(parsed_df['value'], ',')
parsed_df = parsed_df.withColumn("user", split_col.getItem(0))
parsed_df = parsed_df.withColumn("check_in_time", split_col.getItem(1))
parsed_df = parsed_df.withColumn("latitude", split_col.getItem(2))
parsed_df = parsed_df.withColumn("longitude", split_col.getItem(3))
parsed_df = parsed_df.withColumn("location_id", split_col.getItem(4))
parsed_df = parsed_df.withColumn("time_spent", split_col.getItem(5))
parsed_df = parsed_df.withColumn("time_stamp", split_col.getItem(6))

parsed_df = parsed_df.where(parsed_df.user != 'user')
parsed_df = parsed_df.withColumn("check_in_time", to_timestamp(parsed_df["check_in_time"], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
parsed_df = parsed_df.withColumn("time_stamp", from_utc_timestamp(parsed_df["time_stamp"], "UTC"))

# Izbacivanje slogova kod kojih nedostaje locaiton_id
parsed_df = parsed_df.filter((col("location_id") != "0") & (col("location_id").isNotNull()))

# UPIS U INFLUXDB 

def write_to_influxdb(df, epoch_id):
    # Inicijalizacija InfluxDB klijenta
    token = "Xl99hGe7tyPW-wWrgpZRo8vOxA6bK2nR-X3MEoqkigZqnSG1vSqpKOoBmLZdWpWbYKKMKNEfqAAX4FMoKhd5ug=="
    org = "brightkite-org"
    bucket = "brightkite-bucket"
    client = InfluxDBClient(url="http://influxdb:8086", token=token)

    # Kreiranje instance WriteApi klase
    write_api = client.write_api(write_options=SYNCHRONOUS)

    df = df\
        .withColumn("avg_time_spent", col("avg_time_spent").cast("double")) \
        .withColumn("min_time_spent", col("min_time_spent").cast("double")) \
        .withColumn("max_time_spent", col("max_time_spent").cast("double"))

    for row in df.collect():
        point = Point("statistics_3") \
            .tag("window", row.window) \
            .tag("location_id", row.location_id) \
            .tag("user", row.user) \
            .field("avg_time_spent", row.avg_time_spent) \
            .field("min_time_spent", row.min_time_spent) \
            .field("max_time_spent", row.max_time_spent)
        write_api.write(bucket=bucket, org=org, record=point)

def write_to_influxdb_2(df, epoch_id):
    # Inicijalizacija InfluxDB klijenta
    token = "Xl99hGe7tyPW-wWrgpZRo8vOxA6bK2nR-X3MEoqkigZqnSG1vSqpKOoBmLZdWpWbYKKMKNEfqAAX4FMoKhd5ug=="
    org = "brightkite-org"
    bucket = "brightkite-bucket"
    client = InfluxDBClient(url="http://influxdb:8086", token=token)

    # Kreiranje instance WriteApi klase
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for row in df.collect():
        point = Point("dist_users") \
            .tag("window", row.window) \
            .tag("location_id", row.location_id) \
            .field("distinct_users", row.distinct_users)
        write_api.write(bucket=bucket, org=org, record=point)

def write_to_influxdb_3(df, epoch_id):
    N = 2
    # Inicijalizacija InfluxDB klijenta
    token = "Xl99hGe7tyPW-wWrgpZRo8vOxA6bK2nR-X3MEoqkigZqnSG1vSqpKOoBmLZdWpWbYKKMKNEfqAAX4FMoKhd5ug=="
    org = "brightkite-org"
    bucket = "brightkite-bucket"
    client = InfluxDBClient(url="http://influxdb:8086", token=token)

    # Kreiranje instance WriteApi klase
    write_api = client.write_api(write_options=SYNCHRONOUS)

    df = df.orderBy(col("num_users").desc()).limit(N)

    for row in df.collect():
        point = Point("top_n_locations") \
            .tag("location_id", row.location_id) \
            .field("num_users", row.num_users)
        write_api.write(bucket=bucket, org=org, record=point)

# Prosecno, maksimalno i minimalno vreme koje je svaki korisnik proveo na odredjenoj lokaciji
windowed_df_1 = parsed_df.groupBy(window(parsed_df.time_stamp, "10 seconds"), parsed_df.user, parsed_df.location_id) \
    .agg(avg(parsed_df.time_spent).alias("avg_time_spent"),
         min(parsed_df.time_spent).alias("min_time_spent"),
         max(parsed_df.time_spent).alias("max_time_spent"))

#windowed_df_1.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="statistics", keyspace="spark").save()          

stream_1 = windowed_df_1.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influxdb) \
    .trigger(processingTime="10 seconds") \
    .start()

# Broj korisnika koji je posetio svaku od lokacija
windowed_df_2 = parsed_df.groupBy(window(parsed_df.time_stamp, "10 seconds"), parsed_df.location_id) \
    .agg(approx_count_distinct(parsed_df.user).alias("distinct_users"))

#windowed_df_2.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="location_user_counts", keyspace="spark").save() 

stream_2 = windowed_df_2.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influxdb_2) \
    .trigger(processingTime="10 seconds") \
    .start()

# Top N lokacija
windowed_df_3 = parsed_df.groupBy(window(parsed_df.time_stamp, "10 seconds"), parsed_df.location_id) \
    .agg(approx_count_distinct(parsed_df.user).alias("num_users"))

stream_3 = windowed_df_3.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influxdb_3) \
    .trigger(processingTime="10 seconds") \
    .start()

stream_3.awaitTermination()

#windowed_df_3.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="top_n_locations", keyspace="spark").save()

