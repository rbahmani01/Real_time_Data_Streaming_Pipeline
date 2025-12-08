from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    avg,
    max as spark_max,
    to_date,
    hour,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    IntegerType,
)

SILVER_PATH = "s3a://silver/house_sensors_raw"

GOLD_1MIN_PATH = "s3a://gold/house_sensors_agg_1min_partitioned"
GOLD_15MIN_PATH = "s3a://gold/house_sensors_agg_15min_partitioned"

CHECKPOINT_1MIN = "s3a://gold/_checkpoints/house_sensors_agg_1min_partitioned"
CHECKPOINT_15MIN = "s3a://gold/_checkpoints/house_sensors_agg_15min_partitioned"

# TimescaleDB config 
TS_URL = "jdbc:postgresql://timescaledb:5432/sensordb"
TS_USER = "sensor"
TS_PASSWORD = "sensor"

TS_TABLE_1MIN = "house_sensors_agg_1min"
TS_TABLE_15MIN = "house_sensors_agg_15min"

def configure_s3(spark):
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.endpoint", "http://minio:9000")
    h.set("fs.s3a.access.key", "minioadmin")
    h.set("fs.s3a.secret.key", "minioadmin")
    h.set("fs.s3a.path.style.access", "true")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def write_batch_to_timescale(df, batch_id, table_name):
    """
    Called by foreachBatch for each micro-batch.
    Writes the batch DataFrame into TimescaleDB table via JDBC.
    """
    if df.rdd.isEmpty():
        return

    # Only keep the columns that exist in TimescaleDB tables
    base_cols = [
        "house_id",
        "window_start",
        "window_end",
        "avg_power_kw",
        "max_power_kw",
        "avg_pv_panel_kw",
        "avg_ev_charging_kw",
    ]
    df_ts = df.select(*base_cols)

    (
        df_ts.write
            .format("jdbc")
            .option("url", TS_URL)
            .option("dbtable", table_name)
            .option("user", TS_USER)
            .option("password", TS_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
    )


def make_ts_batch_writer(table_name):
    def _inner(df, batch_id):
        write_batch_to_timescale(df, batch_id, table_name)
    return _inner


def main():
    spark = (
        SparkSession.builder
        .appName("SilverToGoldMinio")
        .getOrCreate()
    )


    # Local business time
    spark.conf.set("spark.sql.session.timeZone", "Europe/Berlin")
    # fewer small files
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    configure_s3(spark)
    spark.sparkContext.setLogLevel("WARN")

    # Base schema of Silver (partition cols dt/hour are inferred)
    schema = StructType([
        StructField("house_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pv_panel_kw", DoubleType(), True),
        StructField("ev_charging_kw", DoubleType(), True),
        StructField("power_kw", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("dt", DateType(), True),
        StructField("hour", IntegerType(), True),
    ])

    # Read the streaming Silver layer
    df = (
        spark.readStream
            .format("parquet")
            .schema(schema)
            .load(SILVER_PATH)
            .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # ---------------- 1-MINUTE AGG ----------------
    agg_1min = (
        df
          .withWatermark("event_time", "2 minutes")
          .groupBy(
              col("house_id"),
              window(col("event_time"), "1 minute")
          )
          .agg(
              avg("power_kw").alias("avg_power_kw"),
              spark_max("power_kw").alias("max_power_kw"),
              avg("pv_panel_kw").alias("avg_pv_panel_kw"),
              avg("ev_charging_kw").alias("avg_ev_charging_kw"),
          )
          .select(
              "house_id",
              col("window.start").alias("window_start"),
              col("window.end").alias("window_end"),
              "avg_power_kw",
              "max_power_kw",
              "avg_pv_panel_kw",
              "avg_ev_charging_kw",
          )
          .withColumn("event_date", to_date(col("window_start")))
          .withColumn("hour", hour(col("window_start")))
    )

    agg_1min_out = agg_1min.repartition(
        4,
        col("event_date"),
        col("hour"),
    )

    q1 = (
        agg_1min_out.writeStream
            .format("parquet")
            .outputMode("append")
            .partitionBy("event_date", "hour")
            .option("path", GOLD_1MIN_PATH)
            .option("checkpointLocation", CHECKPOINT_1MIN)
            .trigger(processingTime="1 minute")
            .start()
    )

    # stream 1-min aggregates to TimescaleDB
    q1_ts = (
        agg_1min_out.writeStream
            .outputMode("append")
            .foreachBatch(make_ts_batch_writer(TS_TABLE_1MIN))
            .option("checkpointLocation", CHECKPOINT_1MIN + "_ts")
            .trigger(processingTime="1 minute")
            .start()
    )

    # ---------------- 15-MINUTE AGG ----------------
    agg_15min = (
        df
          .withWatermark("event_time", "2 minutes")
          .groupBy(
              col("house_id"),
              window(col("event_time"), "15 minutes")
          )
          .agg(
              avg("power_kw").alias("avg_power_kw"),
              spark_max("power_kw").alias("max_power_kw"),
              avg("pv_panel_kw").alias("avg_pv_panel_kw"),
              avg("ev_charging_kw").alias("avg_ev_charging_kw"),
          )
          .select(
              "house_id",
              col("window.start").alias("window_start"),
              col("window.end").alias("window_end"),
              "avg_power_kw",
              "max_power_kw",
              "avg_pv_panel_kw",
              "avg_ev_charging_kw",
          )
          .withColumn("event_date", to_date(col("window_start")))
          .withColumn("hour", hour(col("window_start")))
    )

    agg_15min_out = agg_15min.repartition(
        4,
        col("event_date"),
        col("hour"),
    )

    q15 = (
        agg_15min_out.writeStream
            .format("parquet")
            .outputMode("append")
            .partitionBy("event_date", "hour")
            .option("path", GOLD_15MIN_PATH)
            .option("checkpointLocation", CHECKPOINT_15MIN)
            .trigger(processingTime="1 minute")   # same trigger, but 15m windows
            .start()
    )

    # stream 15-min aggregates to TimescaleDB
    q15_ts = (
        agg_15min_out.writeStream
            .outputMode("append")
            .foreachBatch(make_ts_batch_writer(TS_TABLE_15MIN))
            .option("checkpointLocation", CHECKPOINT_15MIN + "_ts")
            .trigger(processingTime="1 minute")
            .start()
    )

    # wait for all
    spark.streams.awaitAnyTermination()



if __name__ == "__main__":
    main()
