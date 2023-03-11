from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark = SparkSession.builder.appName("PySpark_Kafka_Consumer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

df = spark \
  .read \
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
# parsed_df = parsed_df.withColumn("time_stamp", to_timestamp(split_col.getItem(6) / 1000))
parsed_df = parsed_df.withColumn("time_stamp", from_utc_timestamp(parsed_df["time_stamp"], "UTC"))


# filter out rows with "0" or "null" location_id
parsed_df = parsed_df.filter((col("location_id") != "0") & (col("location_id").isNotNull()))

# Prosecno, maksimalno i minimalno vreme koje je svaki korisnik proveo na odredjenoj lokaciji, za vremenski prozor od 24h
windowed_df_1 = parsed_df.groupBy(window(parsed_df.time_stamp, "10 seconds"), parsed_df.user, parsed_df.location_id) \
    .agg(avg(parsed_df.time_spent).alias("avg_time_spent"),
         min(parsed_df.time_spent).alias("min_time_spent"),
         max(parsed_df.time_spent).alias("max_time_spent"))

windowed_df_1.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="statistics", keyspace="spark").save() 
windowed_df_1.show()

# Broj korisnika koji je posetio svaku od lokacija, za vremenski prozor od 24h
windowed_df_2 = parsed_df.groupBy(window(parsed_df.time_stamp, "10 seconds"), parsed_df.location_id) \
    .agg(countDistinct(parsed_df.user).alias("distinct_users"))

windowed_df_2.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="location_user_counts", keyspace="spark").save() 
windowed_df_2.show()

# Top N najpopularnijih lokacija
N = 2

windowed_df = parsed_df.groupBy(
    window(parsed_df.time_stamp, "10 seconds").alias('time_window'), 
    parsed_df.location_id
).agg(countDistinct(parsed_df.user).alias("num_users"))

windowed_df_3 = windowed_df.groupBy(
    'time_window'
).agg(
    collect_list(struct('num_users', 'location_id')).alias('location_list')
).select(
    'time_window',
    expr('sort_array(location_list) as sorted_list')
).select(
    'time_window',
    expr('reverse(sorted_list) as reversed_list')
).select(
    'time_window',
    expr('slice(reversed_list, 1, {}) as top_locations'.format(N))
).select(
    'time_window',
    explode('top_locations').alias('top_location')
).select(
    'time_window',
    'top_location.location_id',
    'top_location.num_users',
).orderBy('num_users', 'time_window', ascending=False)

windowed_df_3.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="top_n_locations", keyspace="spark").save() 

windowed_df_3.show()

