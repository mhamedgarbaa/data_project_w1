"""
Configuration module for Kafka producers.
Reads settings from environment variables with sensible defaults.
"""

import os
import logging

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(name)-18s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Topic names
TOPIC_RAW_STOCK    = "raw_stock_ticks"
TOPIC_RAW_CRYPTO   = "raw_crypto_ticks"
TOPIC_CLEAN_DATA   = "clean_market_data"
TOPIC_ANOMALIES    = "market_anomalies"

# ---------------------------------------------------------------------------
# Alpha Vantage  (Stock data)
# ---------------------------------------------------------------------------
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Symbols to track (comma-separated env var)
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA").split(",")
STOCK_SYMBOLS = [s.strip() for s in STOCK_SYMBOLS if s.strip()]

# Rate-limit: Alpha Vantage allows 5 calls/min on free tier, 500/day
AV_MAX_CALLS_PER_MINUTE = int(os.getenv("AV_MAX_CALLS_PER_MINUTE", "5"))
AV_MAX_CALLS_PER_DAY    = int(os.getenv("AV_MAX_CALLS_PER_DAY", "500"))

# ---------------------------------------------------------------------------
# CoinGecko  (Crypto data)
# ---------------------------------------------------------------------------
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Coin IDs (comma-separated env var)
CRYPTO_IDS = os.getenv("CRYPTO_IDS", "bitcoin,ethereum,solana,cardano,ripple").split(",")
CRYPTO_IDS = [c.strip() for c in CRYPTO_IDS if c.strip()]

# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# ---------------------------------------------------------------------------
# Retry / back-off
# ---------------------------------------------------------------------------
MAX_RETRIES     = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_S = int(os.getenv("RETRY_BACKOFF_S", "5"))
