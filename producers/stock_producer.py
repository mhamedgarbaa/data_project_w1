"""
Stock Producer – Polls Alpha Vantage for intraday stock prices
and publishes raw JSON ticks to Kafka topic `raw_stock_ticks`.

Usage:
    python stock_producer.py          (standalone)
    Launched via docker-compose        (containerised)
"""

import json
import time
import logging
import sys
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_STOCK,
    ALPHA_VANTAGE_API_KEY,
    ALPHA_VANTAGE_BASE_URL,
    STOCK_SYMBOLS,
    AV_MAX_CALLS_PER_MINUTE,
    POLL_INTERVAL_SECONDS,
    MAX_RETRIES,
    RETRY_BACKOFF_S,
)

logger = logging.getLogger("stock_producer")

# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def create_kafka_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a Kafka producer with retry logic for broker availability."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=10000,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d). Retrying in %ds...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    logger.error("Could not connect to Kafka after %d attempts. Exiting.", retries)
    sys.exit(1)


def publish(producer: KafkaProducer, topic: str, key: str, value: dict) -> None:
    """Send a message to Kafka, logging delivery success/failure."""
    future = producer.send(topic, key=key, value=value)
    try:
        metadata = future.get(timeout=10)
        logger.debug(
            "Sent %s → %s [partition=%d offset=%d]",
            key, topic, metadata.partition, metadata.offset,
        )
    except Exception as exc:
        logger.error("Failed to send %s: %s", key, exc)


# ---------------------------------------------------------------------------
# Alpha Vantage fetcher
# ---------------------------------------------------------------------------

def fetch_intraday(symbol: str) -> list[dict]:
    """
    Fetch the latest intraday (5-min) data for a stock symbol.
    Returns a list of tick dicts or an empty list on failure.
    """
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "apikey": ALPHA_VANTAGE_API_KEY,
        "datatype": "json",
        "outputsize": "compact",  # last 100 data points
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # Alpha Vantage returns an error note when rate-limited
            if "Note" in data:
                logger.warning("Rate-limited by Alpha Vantage: %s", data["Note"])
                time.sleep(60)  # wait a full minute before retrying
                continue

            if "Error Message" in data:
                logger.error("API error for %s: %s", symbol, data["Error Message"])
                return []

            time_series = data.get("Time Series (5min)", {})
            if not time_series:
                logger.warning("No time-series data returned for %s", symbol)
                return []

            ticks = []
            for timestamp_str, ohlcv in time_series.items():
                ticks.append({
                    "symbol":     symbol,
                    "asset_type": "stock",
                    "timestamp":  timestamp_str,
                    "open":       float(ohlcv.get("1. open", 0)),
                    "high":       float(ohlcv.get("2. high", 0)),
                    "low":        float(ohlcv.get("3. low", 0)),
                    "close":      float(ohlcv.get("4. close", 0)),
                    "volume":     float(ohlcv.get("5. volume", 0)),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                })

            logger.info("Fetched %d ticks for %s", len(ticks), symbol)
            return ticks

        except requests.exceptions.RequestException as exc:
            logger.error(
                "HTTP error fetching %s (attempt %d/%d): %s",
                symbol, attempt, MAX_RETRIES, exc,
            )
            time.sleep(RETRY_BACKOFF_S * attempt)

    return []


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Main producer loop: poll → parse → publish → sleep."""
    logger.info("=== Stock Producer starting ===")
    logger.info("Symbols : %s", STOCK_SYMBOLS)
    logger.info("Interval: %ds", POLL_INTERVAL_SECONDS)
    logger.info("API Key : %s***", ALPHA_VANTAGE_API_KEY[:4] if len(ALPHA_VANTAGE_API_KEY) > 4 else "****")

    producer = create_kafka_producer()

    call_count = 0
    while True:
        for symbol in STOCK_SYMBOLS:
            # Respect rate limits: 5 calls/min on the free tier
            if call_count > 0 and call_count % AV_MAX_CALLS_PER_MINUTE == 0:
                logger.info("Rate-limit pause: sleeping 62s after %d calls", call_count)
                time.sleep(62)

            ticks = fetch_intraday(symbol)
            call_count += 1

            if not ticks:
                continue

            # Publish only the most recent tick to keep the stream real-time
            latest_tick = max(ticks, key=lambda t: t["timestamp"])
            publish(producer, TOPIC_RAW_STOCK, symbol, latest_tick)

            # Small delay between symbols to be gentle on the API
            time.sleep(1)

        producer.flush()
        logger.info("Cycle complete. Sleeping %ds...", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Stock producer stopped by user.")
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)
