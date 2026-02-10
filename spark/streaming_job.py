"""
Spark Structured Streaming Job – Market Anomaly Detection
==========================================================

This job:
  1. Reads raw stock & crypto ticks from Kafka topics.
  2. Parses JSON, enforces schemas, normalizes into a unified format.
  3. Computes windowed aggregations (5-min windows) via foreachBatch.
  4. Engineers features: price return %, rolling volatility, volume delta.
  5. Detects anomalies using the Z-score method.
  6. Writes clean aggregated data → Parquet data lake & PostgreSQL.
  7. Writes anomalies → PostgreSQL (for Grafana dashboards).

Submit with:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,\\
               org.postgresql:postgresql:42.7.1 \\
               streaming_job.py
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# Ensure the script directory is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schemas import RAW_STOCK_SCHEMA, RAW_CRYPTO_SCHEMA

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "marketdb")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "marketuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpass")
PARQUET_PATH      = os.getenv("PARQUET_PATH", "/opt/spark-data/clean_market_data")
CHECKPOINT_PATH   = os.getenv("CHECKPOINT_PATH", "/opt/spark-data/checkpoints")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# Anomaly detection threshold
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "3.0"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("streaming_job")

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session():
    """Build a SparkSession configured for Kafka + PostgreSQL."""
    return (
        SparkSession.builder
        .appName("MarketAnomalyDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Ingest raw data from Kafka
# ---------------------------------------------------------------------------

def read_kafka_stream(spark, topic):
    """Subscribe to a Kafka topic and return a streaming DataFrame of raw JSON."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(key AS STRING) AS msg_key",
                     "CAST(value AS STRING) AS json_value",
                     "topic",
                     "timestamp AS kafka_timestamp")
    )


# ---------------------------------------------------------------------------
# 2. Parse & normalize
# ---------------------------------------------------------------------------

def parse_stock_stream(raw_df):
    """Parse raw stock JSON and normalize to unified tick format."""
    return (
        raw_df
        .select(
            F.from_json(F.col("json_value"), RAW_STOCK_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time",
                     F.coalesce(
                         F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                         F.col("kafka_timestamp"),
                     ))
        .withColumn("price", F.col("close"))
        .drop("timestamp")
        .filter(F.col("symbol").isNotNull() & F.col("event_time").isNotNull())
    )


def parse_crypto_stream(raw_df):
    """Parse raw crypto JSON and normalize to unified tick format."""
    return (
        raw_df
        .select(
            F.from_json(F.col("json_value"), RAW_CRYPTO_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time",
                     F.coalesce(
                         F.to_timestamp(F.col("timestamp")),
                         F.col("kafka_timestamp"),
                     ))
        .withColumn("price",
                     F.coalesce(F.col("price"), F.col("close")))
        .withColumn("volume",
                     F.coalesce(F.col("volume"), F.col("volume_24h"), F.lit(0.0)))
        .drop("timestamp", "market_cap", "volume_24h", "change_24h_pct")
        .filter(F.col("symbol").isNotNull() & F.col("event_time").isNotNull())
    )


# ---------------------------------------------------------------------------
# 3. Windowed aggregations (applied inside foreachBatch on static DF)
# ---------------------------------------------------------------------------

def compute_window_aggregations(batch_df, window_duration="5 minutes"):
    """
    Compute windowed aggregations on a BATCH (static) DataFrame.

    Features engineered:
      - price_return_pct  : (close - open) / open * 100
      - rolling_volatility: stddev of price within the window
      - volume_delta      : max_volume - min_volume
    """
    windowed = (
        batch_df
        .groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("symbol"),
            F.col("asset_type"),
        )
        .agg(
            F.first("price").alias("open_price"),
            F.last("price").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("price_stddev"),
            F.sum("volume").alias("total_volume"),
            F.max("volume").alias("max_volume"),
            F.min("volume").alias("min_volume"),
            F.count("*").alias("record_count"),
        )
        .withColumn("price_return_pct",
                     F.when(F.col("open_price") != 0,
                            (F.col("close_price") - F.col("open_price"))
                            / F.col("open_price") * 100)
                     .otherwise(0.0))
        .withColumn("rolling_volatility",
                     F.coalesce(F.col("price_stddev"), F.lit(0.0)))
        .withColumn("volume_delta",
                     F.col("max_volume") - F.col("min_volume"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window", "price_stddev", "max_volume", "min_volume")
    )
    return windowed


# ---------------------------------------------------------------------------
# 4. Z-score anomaly detection (on batch DataFrames)
# ---------------------------------------------------------------------------

def detect_anomalies(windowed_df, threshold=Z_SCORE_THRESHOLD):
    """
    Apply Z-score anomaly detection on a BATCH (static) DataFrame.
    Uses Window functions per symbol to compute rolling statistics.
    """
    row_count = windowed_df.count()
    if row_count < 2:
        return None

    # Rolling look-back window per symbol for z-score computation
    symbol_window = (
        Window.partitionBy("symbol", "asset_type")
        .orderBy("window_start")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    enriched = windowed_df
    for feature in ["price_return_pct", "total_volume", "rolling_volatility"]:
        mean_col = f"{feature}_mean"
        std_col  = f"{feature}_std"
        z_col    = f"{feature}_z"

        enriched = (
            enriched
            .withColumn(mean_col, F.avg(feature).over(symbol_window))
            .withColumn(std_col,  F.stddev(feature).over(symbol_window))
            .withColumn(z_col,
                         F.when(
                             (F.col(std_col).isNotNull()) & (F.col(std_col) > 0),
                             (F.col(feature) - F.col(mean_col)) / F.col(std_col)
                         ).otherwise(0.0))
        )

    # Also flag rows by simple percentage thresholds (for early batches
    # where rolling stats are null because there's no history yet)
    enriched = enriched.withColumn(
        "_pct_anomaly",
        (F.abs(F.col("price_return_pct")) > 3.0) |
        (F.col("rolling_volatility") > F.col("avg_price") * 0.02)
    )

    # Collect anomaly rows
    anomaly_rows = []
    for feature, anomaly_label in [
        ("price_return_pct",   "price_spike"),
        ("total_volume",       "volume_surge"),
        ("rolling_volatility", "volatility_burst"),
    ]:
        z_col    = f"{feature}_z"
        mean_col = f"{feature}_mean"
        std_col  = f"{feature}_std"

        anomaly = (
            enriched
            .filter(
                (F.abs(F.col(z_col)) > threshold) |
                ((F.col(z_col) == 0.0) & F.col("_pct_anomaly"))
            )
            .select(
                F.col("symbol"),
                F.col("asset_type"),
                F.lit(anomaly_label).alias("anomaly_type"),
                F.current_timestamp().alias("detected_at"),
                F.col("window_start"),
                F.col("window_end"),
                F.col(feature).alias("current_value"),
                F.coalesce(F.col(mean_col), F.lit(0.0)).alias("mean_value"),
                F.coalesce(F.col(std_col),  F.lit(1.0)).alias("std_value"),
                F.col(z_col).alias("z_score"),
                F.col("close_price").alias("price"),
                F.col("total_volume").alias("volume"),
            )
            .withColumn("severity",
                         F.when(F.abs(F.col("z_score")) > 5, "high")
                          .when(F.abs(F.col("z_score")) > 4, "medium")
                          .otherwise("low"))
        )
        anomaly_rows.append(anomaly)

    result = anomaly_rows[0]
    for df in anomaly_rows[1:]:
        result = result.unionByName(df)

    return result


# ---------------------------------------------------------------------------
# 5. The unified foreachBatch processor
# ---------------------------------------------------------------------------

def process_batch(batch_df, batch_id):
    """
    Process one micro-batch of raw tick data:
      1. Compute windowed aggregations (static DataFrame → Window ops OK)
      2. Write clean data to PostgreSQL + Parquet
      3. Detect anomalies and write to PostgreSQL
    """
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d is empty, skipping", batch_id)
        return

    batch_df.persist()
    tick_count = batch_df.count()
    logger.info("=== Batch %d: %d raw ticks ===", batch_id, tick_count)

    try:
        # --- 1. Window aggregation ---
        windowed = compute_window_aggregations(batch_df, "5 minutes")
        windowed.persist()
        agg_count = windowed.count()
        logger.info("  Aggregated into %d window rows", agg_count)

        if agg_count == 0:
            logger.info("  No aggregation results, skipping sinks")
            windowed.unpersist()
            return

        # --- 2. Write clean data to PostgreSQL ---
        clean_df = (
            windowed
            .withColumnRenamed("total_volume", "volume")
            .select("symbol", "asset_type", "window_start", "window_end",
                     "open_price", "close_price", "high_price", "low_price",
                     "volume", "price_return_pct", "rolling_volatility",
                     "volume_delta", "record_count")
        )
        clean_df.write.jdbc(
            url=JDBC_URL, table="clean_market_data",
            mode="append", properties=JDBC_PROPERTIES,
        )
        logger.info("  Wrote %d clean rows to PostgreSQL", agg_count)

        # --- 3. Write to Parquet data lake ---
        (
            windowed
            .withColumn("date", F.to_date(F.col("window_start")))
            .write.mode("append")
            .partitionBy("date", "asset_type")
            .parquet(PARQUET_PATH)
        )
        logger.info("  Wrote Parquet to %s", PARQUET_PATH)

        # --- 4. Anomaly detection ---
        anomalies = detect_anomalies(windowed)
        if anomalies is not None:
            anomaly_count = anomalies.count()
            if anomaly_count > 0:
                anomalies.write.jdbc(
                    url=JDBC_URL, table="market_anomalies",
                    mode="append", properties=JDBC_PROPERTIES,
                )
                logger.info("  Wrote %d anomalies to PostgreSQL", anomaly_count)
            else:
                logger.info("  No anomalies detected")
        else:
            logger.info("  Too few rows for anomaly detection")

        windowed.unpersist()

    except Exception as e:
        logger.error("  Error in batch %d: %s", batch_id, e, exc_info=True)
    finally:
        batch_df.unpersist()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("=" * 60)
    logger.info("  Market Anomaly Detection – Spark Streaming Job")
    logger.info("=" * 60)
    logger.info("Kafka           : %s", KAFKA_BOOTSTRAP)
    logger.info("PostgreSQL      : %s", JDBC_URL)
    logger.info("Parquet path    : %s", PARQUET_PATH)
    logger.info("Z-score threshold: %.1f", Z_SCORE_THRESHOLD)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ---- Read raw streams from Kafka ----
    raw_stock  = read_kafka_stream(spark, "raw_stock_ticks")
    raw_crypto = read_kafka_stream(spark, "raw_crypto_ticks")

    # ---- Parse & normalize ----
    stock_ticks  = parse_stock_stream(raw_stock)
    crypto_ticks = parse_crypto_stream(raw_crypto)

    # ---- Merge into unified stream ----
    common_cols = ["symbol", "asset_type", "event_time", "price",
                   "open", "high", "low", "close", "volume",
                   "kafka_timestamp"]

    stock_aligned = (
        stock_ticks
        .select(*[F.col(c) if c in stock_ticks.columns
                   else F.lit(None).cast(DoubleType()).alias(c)
                   for c in common_cols])
    )
    crypto_aligned = (
        crypto_ticks
        .select(*[F.col(c) if c in crypto_ticks.columns
                   else F.lit(None).cast(DoubleType()).alias(c)
                   for c in common_cols])
    )

    unified = stock_aligned.unionByName(crypto_aligned)

    # ---- Single streaming query with foreachBatch ----
    # All heavy processing (aggregation, anomaly detection, sinks)
    # happens inside process_batch on static DataFrames.
    query = (
        unified.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/main")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Awaiting termination …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
