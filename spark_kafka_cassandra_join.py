from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Define foreachbatch function to save stream data to cassandra table
def write_to_cassandra(target_df, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .mode("append") \
        .save()
    target_df.show()

if __name__ == "__main__":

# Create spark session
    spark = SparkSession \
        .builder \
        .appName("Stream Table Join") \
        .master("yarn") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

# Define schema for incoming data
    login_schema = StructType([
        StructField("created_time", StringType()),
        StructField("login_id", StringType())
    ])

# Read data from kafka topic
    kafka_source_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "logins") \
        .option("startingOffsets", "earliest") \
        .load()

# Deserialization
    value_df = kafka_source_df.select(from_json(col("value").cast("string"), login_schema).alias("value"))

# Convert "created_time" data to timestamp
    login_df = value_df.select("value.*") \
        .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss"))

#Read current cassandra table

    user_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .load()

# Define join statements
    join_expr = login_df.login_id == user_df.login_id
    join_type = "inner"

# Join new data from kafka (streaming data) with existing data from cassandra (static data)
    joined_df = login_df.join(user_df, join_expr, join_type) \
        .drop(login_df.login_id)

# Create output column to update last login record in cassandra db
    output_df = joined_df.select(col("login_id"), col("user_name"),
                                 col("created_time").alias("last_login"))

# Write joined data into cassandra table
    output_query = output_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "Cassandra/chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    output_query.awaitTermination()
