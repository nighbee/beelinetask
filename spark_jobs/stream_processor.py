import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum, lit, when, expr, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

KAFKA_TOPIC = "telecom_events"
KAFKA_SERVER = "kafka:29092"
SPARK_MASTER_URL = "spark://spark-master:7077"

POSTGRES_SERVER = "postgres:5432"
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_SERVER}/{POSTGRES_DB}"

def write_aggregates_to_postgres(batch_df, epoch_id):
    print(f"Processing batch ID: {epoch_id}")
    df_events_agg = batch_df.groupBy(window(col("timestamp"), "5 minutes"), col("event_type"), col("event_subtype")) \
        .agg(
            count("*").alias("event_count"),
            sum("data_mb").alias("total_data_mb"),
            sum("amount").alias("total_amount")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("event_type_agg").alias("metric_type"),
            when(col("event_subtype").isNotNull(), expr("concat(event_type, '_', event_subtype)"))
                .otherwise(col("event_type")).alias("metric_key"),
            when(col("event_type") == "data_session", col("total_data_mb") / 1024)
                .when(col("event_type") == "balance_recharge", col("total_amount"))
                .otherwise(col("event_count")).alias("metric_value")
        )
    df_region_agg = batch_df.groupBy(window(col("timestamp"), "10 minutes"), col("region")) \
        .agg(count("*").alias("event_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("region_agg").alias("metric_type"),
            col("region").alias("metric_key"),
            col("event_count").alias("metric_value")
        )
    final_metrics_df = df_events_agg.unionByName(df_region_agg, allowMissingColumns=True)
    if not final_metrics_df.rdd.isEmpty():
        final_metrics_df.repartition(1).write \
            .jdbc(url=POSTGRES_URL, table="real_time_metrics", mode="append", properties=POSTGRES_PROPERTIES)

def write_anomalies_to_postgres(batch_df, epoch_id):
    print(f"Processing anomalies batch ID: {epoch_id}")
    anomalies_to_write = batch_df.select(
        to_json(struct("*")).alias("event_data"),
        lit("invalid_data").alias("reason")
    )
    if not anomalies_to_write.rdd.isEmpty():
        anomalies_to_write.repartition(1).write \
            .jdbc(url=POSTGRES_URL, table="anomalies", mode="append", properties=POSTGRES_PROPERTIES)

def main():
    spark = SparkSession.builder \
        .appName("TelecomStreamProcessor") \
        .master(SPARK_MASTER_URL) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("msisdn", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_subtype", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("data_mb", FloatType(), True),
        StructField("amount", FloatType(), True),
        StructField("region", StringType(), True),
        StructField("cell_tower_id", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    df_events = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    df_events = df_events.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    valid_event_types = ['call', 'sms', 'data_session', 'balance_recharge']
    validity_condition = (
        (col("msisdn").isNotNull()) & (col("msisdn") != "") &
        (col("event_type").isin(valid_event_types)) &
        (col("duration_seconds").isNull() | (col("duration_seconds") >= 0)) &
        (col("data_mb").isNull() | (col("data_mb") >= 0)) &
        (col("amount").isNull() | (col("amount") >= 0))
    )
    df_valid = df_events.filter(validity_condition)
    df_anomalies = df_events.filter(~validity_condition)
    query_valid = df_valid.withWatermark("timestamp", "1 minute") \
        .writeStream \
        .foreachBatch(write_aggregates_to_postgres) \
        .outputMode("update") \
        .trigger(processingTime='30 seconds') \
        .start()
    query_anomalies = df_anomalies.writeStream \
        .foreachBatch(write_anomalies_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()
    query_valid.awaitTermination()
    query_anomalies.awaitTermination()

if __name__ == "__main__":
    main()



"""

docker compose exec -u 0 spark-master spark-submit `  --master spark://spark-master:7077 ` --total-executor-cores 4  --conf spark.jars.ivy=/tmp/.ivy2 `  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.postgresql:postgresql:42.6.0 `  /opt/bitnamilegacy/spark/jobs/stream_processor.py

"""