"""
Crypto Producer – Polls CoinGecko for cryptocurrency prices & volume
and publishes raw JSON ticks to Kafka topic `raw_crypto_ticks`.

CoinGecko requires NO API key – completely free.

Usage:
    python crypto_producer.py          (standalone)
    Launched via docker-compose          (containerised)
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
    TOPIC_RAW_CRYPTO,
    COINGECKO_BASE_URL,
    CRYPTO_IDS,
    POLL_INTERVAL_SECONDS,
    MAX_RETRIES,
    RETRY_BACKOFF_S,
)

logger = logging.getLogger("crypto_producer")

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
# CoinGecko fetcher
# ---------------------------------------------------------------------------

def fetch_crypto_prices(coin_ids: list[str]) -> list[dict]:
    """
    Fetch current prices & 24h data for a list of coin IDs from CoinGecko.
    Uses /simple/price endpoint which supports multiple coins in one call.
    Returns a list of tick dicts or an empty list on failure.
    """
    params = {
        "ids":             ",".join(coin_ids),
        "vs_currencies":   "usd",
        "include_market_cap":       "true",
        "include_24hr_vol":         "true",
        "include_24hr_change":      "true",
        "include_last_updated_at":  "true",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                f"{COINGECKO_BASE_URL}/simple/price",
                params=params,
                timeout=15,
            )

            # CoinGecko returns 429 when rate-limited
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                logger.warning("CoinGecko rate-limited. Sleeping %ds...", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            ticks = []
            now_iso = datetime.now(timezone.utc).isoformat()

            for coin_id, info in data.items():
                ticks.append({
                    "symbol":           coin_id,
                    "asset_type":       "crypto",
                    "timestamp":        datetime.fromtimestamp(
                                            info.get("last_updated_at", 0),
                                            tz=timezone.utc,
                                        ).isoformat() if info.get("last_updated_at") else now_iso,
                    "price":            info.get("usd", 0.0),
                    "market_cap":       info.get("usd_market_cap", 0.0),
                    "volume_24h":       info.get("usd_24h_vol", 0.0),
                    "change_24h_pct":   info.get("usd_24h_change", 0.0),
                    "ingested_at":      now_iso,
                })

            logger.info("Fetched prices for %d coins", len(ticks))
            return ticks

        except requests.exceptions.RequestException as exc:
            logger.error(
                "HTTP error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc,
            )
            time.sleep(RETRY_BACKOFF_S * attempt)

    return []


def fetch_crypto_ohlc(coin_id: str, days: int = 1) -> list[dict]:
    """
    Fetch OHLC candlestick data for a single coin (used as supplementary feed).
    Returns list of dicts with open/high/low/close or empty list on failure.
    """
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/ohlc"
    params = {"vs_currency": "usd", "days": str(days)}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                time.sleep(60)
                continue
            resp.raise_for_status()
            raw = resp.json()  # list of [timestamp_ms, open, high, low, close]

            candles = []
            for candle in raw:
                if len(candle) < 5:
                    continue
                candles.append({
                    "symbol":     coin_id,
                    "asset_type": "crypto",
                    "timestamp":  datetime.fromtimestamp(
                                      candle[0] / 1000, tz=timezone.utc
                                  ).isoformat(),
                    "open":       candle[1],
                    "high":       candle[2],
                    "low":        candle[3],
                    "close":      candle[4],
                    "volume":     0.0,  # OHLC endpoint doesn't include volume
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                })

            logger.info("Fetched %d OHLC candles for %s", len(candles), coin_id)
            return candles

        except requests.exceptions.RequestException as exc:
            logger.error("OHLC fetch error for %s (attempt %d): %s", coin_id, attempt, exc)
            time.sleep(RETRY_BACKOFF_S * attempt)

    return []


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Main producer loop: poll → parse → publish → sleep."""
    logger.info("=== Crypto Producer starting ===")
    logger.info("Coins   : %s", CRYPTO_IDS)
    logger.info("Interval: %ds", POLL_INTERVAL_SECONDS)

    producer = create_kafka_producer()

    while True:
        # ---- Simple price ticks (one API call for all coins) ----
        ticks = fetch_crypto_prices(CRYPTO_IDS)
        for tick in ticks:
            publish(producer, TOPIC_RAW_CRYPTO, tick["symbol"], tick)

        # ---- OHLC candles (one call per coin, used for richer data) ----
        for coin_id in CRYPTO_IDS:
            candles = fetch_crypto_ohlc(coin_id, days=1)
            if candles:
                # Send only the latest candle
                latest = candles[-1]
                publish(producer, TOPIC_RAW_CRYPTO, coin_id, latest)
            time.sleep(2)  # gentle on the free API

        producer.flush()
        logger.info("Cycle complete. Sleeping %ds...", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Crypto producer stopped by user.")
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)
