"""
Spark Structured Streaming Schemas
-----------------------------------
Defines the JSON schemas for raw stock ticks, raw crypto ticks,
and the output schemas for clean data and anomalies.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    LongType,
)

# ---------------------------------------------------------------------------
# Schema for raw stock ticks (from Alpha Vantage via Kafka)
# ---------------------------------------------------------------------------
RAW_STOCK_SCHEMA = StructType([
    StructField("symbol",      StringType(),  nullable=False),
    StructField("asset_type",  StringType(),  nullable=False),
    StructField("timestamp",   StringType(),  nullable=False),
    StructField("open",        DoubleType(),  nullable=True),
    StructField("high",        DoubleType(),  nullable=True),
    StructField("low",         DoubleType(),  nullable=True),
    StructField("close",       DoubleType(),  nullable=True),
    StructField("volume",      DoubleType(),  nullable=True),
    StructField("ingested_at", StringType(),  nullable=True),
])

# ---------------------------------------------------------------------------
# Schema for raw crypto ticks (from CoinGecko via Kafka)
# ---------------------------------------------------------------------------
RAW_CRYPTO_SCHEMA = StructType([
    StructField("symbol",         StringType(),  nullable=False),
    StructField("asset_type",     StringType(),  nullable=False),
    StructField("timestamp",      StringType(),  nullable=False),
    StructField("price",          DoubleType(),  nullable=True),
    StructField("open",           DoubleType(),  nullable=True),
    StructField("high",           DoubleType(),  nullable=True),
    StructField("low",            DoubleType(),  nullable=True),
    StructField("close",          DoubleType(),  nullable=True),
    StructField("volume",         DoubleType(),  nullable=True),
    StructField("market_cap",     DoubleType(),  nullable=True),
    StructField("volume_24h",     DoubleType(),  nullable=True),
    StructField("change_24h_pct", DoubleType(),  nullable=True),
    StructField("ingested_at",    StringType(),  nullable=True),
])

# ---------------------------------------------------------------------------
# Unified schema after normalization (used in the streaming pipeline)
# ---------------------------------------------------------------------------
UNIFIED_TICK_SCHEMA = StructType([
    StructField("symbol",      StringType(),    nullable=False),
    StructField("asset_type",  StringType(),    nullable=False),
    StructField("event_time",  TimestampType(), nullable=False),
    StructField("price",       DoubleType(),    nullable=True),
    StructField("open",        DoubleType(),    nullable=True),
    StructField("high",        DoubleType(),    nullable=True),
    StructField("low",         DoubleType(),    nullable=True),
    StructField("close",       DoubleType(),    nullable=True),
    StructField("volume",      DoubleType(),    nullable=True),
    StructField("ingested_at", TimestampType(), nullable=True),
])

# ---------------------------------------------------------------------------
# Output schema: clean market data (written to Parquet data lake)
# ---------------------------------------------------------------------------
CLEAN_DATA_SCHEMA = StructType([
    StructField("symbol",              StringType(),    nullable=False),
    StructField("asset_type",          StringType(),    nullable=False),
    StructField("window_start",        TimestampType(), nullable=False),
    StructField("window_end",          TimestampType(), nullable=False),
    StructField("open_price",          DoubleType(),    nullable=True),
    StructField("close_price",         DoubleType(),    nullable=True),
    StructField("high_price",          DoubleType(),    nullable=True),
    StructField("low_price",           DoubleType(),    nullable=True),
    StructField("volume",              DoubleType(),    nullable=True),
    StructField("price_return_pct",    DoubleType(),    nullable=True),
    StructField("rolling_volatility",  DoubleType(),    nullable=True),
    StructField("volume_delta",        DoubleType(),    nullable=True),
    StructField("record_count",        IntegerType(),   nullable=True),
])

# ---------------------------------------------------------------------------
# Output schema: detected anomalies (written to PostgreSQL)
# ---------------------------------------------------------------------------
ANOMALY_SCHEMA = StructType([
    StructField("symbol",         StringType(),    nullable=False),
    StructField("asset_type",     StringType(),    nullable=False),
    StructField("anomaly_type",   StringType(),    nullable=False),
    StructField("detected_at",    TimestampType(), nullable=False),
    StructField("window_start",   TimestampType(), nullable=False),
    StructField("window_end",     TimestampType(), nullable=False),
    StructField("current_value",  DoubleType(),    nullable=False),
    StructField("mean_value",     DoubleType(),    nullable=False),
    StructField("std_value",      DoubleType(),    nullable=False),
    StructField("z_score",        DoubleType(),    nullable=False),
    StructField("price",          DoubleType(),    nullable=True),
    StructField("volume",         DoubleType(),    nullable=True),
    StructField("severity",       StringType(),    nullable=False),
])
