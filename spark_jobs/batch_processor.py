import os 
import sys 
from datetime import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, lit 


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


def main(date_arg): 
    print(f"Starting batch processing for {date_arg} at {datetime.now()}")

    spark = SparkSession.builder \
        .appName(f"TelecomBatchProcessor-{date_arg}") \
        .master(SPARK_MASTER_URL) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_source = spark.read \
        .jdbc(url=POSTGRES_URL, table="real_time_metrics", properties=POSTGRES_PROPERTIES)
    
    # filter needed day
    df_today = df_source.filter(to_date(col("window_start")) == lit(date_arg))
    print(f"Found {df_today.count()} metric entries for {date_arg}")

    # count events by type 
    df_total_events = df_today \
        .filter(col("metric_type") == "region_agg") \
        .groupBy(to_date(col("window_start")).alias("stat_date")) \
        .agg(sum("metric_value").alias("total_events")) \
        .select(
            col("stat_date"),
            lit("total_events_count").alias("metric_type"),
            lit("all_regions").alias("metric_key"),
            col("total_events").alias("metric_value")
    )

    # metrics by region 
    df_region_metrics = df_today \
        .filter(col("metric_type") == "region_agg") \
        .groupBy(to_date(col("window_start")).alias("stat_date"), col("metric_key").alias("region")) \
        .agg(sum("metric_value").alias("event_count")) \
        .select(
            col("stat_date"),
            lit("region_total_events").alias("metric_type"),
            col("region").alias("metric_key"),
            col("event_count").alias("metric_value")
        )

    # aggregate all metrics
    df_final_stats = df_total_events.unionByName(df_region_metrics)

    delete_query = f"DELETE FROM daily_stats WHERE stat_date = '{date_arg}'"
    
    try:
        spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "(SELECT 1) AS dummy_table") \
            .option("query", delete_query) \
            .load()
        print(f"Deleted old stats for {date_arg}")
    except Exception as e:
        print(f"Could not delete old stats (maybe table is empty): {e}")


    # writing into table 
    df_final_stats.write \
        .jdbc(url=POSTGRES_URL, table="daily_stats", mode="append", properties=POSTGRES_PROPERTIES)
        
    print(f"Successfully wrote new stats for {date_arg}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: batch_processor.py <YYYY-MM-DD>", file=sys.stderr)
        sys.exit(-1)
    
    # validation of date format 
    try:
        datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except ValueError:
        print("Error: Date format must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(-1)
        
    main(sys.argv[1])

"""
PUT data is this: 
$TODAY = (Get-Date).ToString("yyyy-MM-dd")

docker-compose exec -u root spark-master spark-submit `
  --master spark://spark-master:7077 `
  --packages org.postgresql:postgresql:42.6.0 `
  /opt/bitnamilegacy/spark/jobs/batch_processor.py $TODAY

"""