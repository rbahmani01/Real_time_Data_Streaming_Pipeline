from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "sensor_raw"

# MinIO "silver" paths – bucket name: silver
SILVER_PATH = "s3a://silver/house_sensors_raw"
CHECKPOINT_PATH = "s3a://silver/_checkpoints/kafka_to_silver"


def configure_s3(spark: SparkSession):
    """
    Configure Hadoop to talk to MinIO via s3a://.
    """
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", "http://minio:9000")
    hconf.set("fs.s3a.access.key", "minioadmin")
    hconf.set("fs.s3a.secret.key", "minioadmin")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToSilverMinio")
        .getOrCreate()
    )

    # To fix the time zone
    spark.conf.set("spark.sql.session.timeZone", "Europe/Berlin")

    configure_s3(spark)
    spark.sparkContext.setLogLevel("WARN")

    # Schema of your JSON events from producer.py
    schema = StructType([
        StructField("house_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pv_panel_kw", DoubleType(), True),
        StructField("ev_charging_kw", DoubleType(), True),
        StructField("power_kw", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
    ])

    # 1) Read stream from Kafka
    df_raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC)
            # only new data
            .option("startingOffsets", "latest")
            .load()
    )

    # 2) Parse JSON and make a clean "silver" schema
    df = (
        df_raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .withColumn("json", from_json(col("json_str"), schema))
            .select("json.*")
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .dropna(subset=["house_id", "event_time"])
            .withColumn("dt", to_date(col("event_time")))   # partition key 1
            .withColumn("hour", hour(col("event_time")))    # partition key 2
    )

    # 3) Write streaming parquet to MinIO
    query = (
        df.writeStream
          .format("parquet")
          .outputMode("append")
          .partitionBy("dt", "hour")    
          .option("path", SILVER_PATH)
          .option("checkpointLocation", CHECKPOINT_PATH)
          .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
