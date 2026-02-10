# 📈 Real-Time Financial Market Anomaly Detection Platform

A production-like Big Data system that ingests free, public financial market data, processes it in real time with Spark Structured Streaming, detects price and volume anomalies using statistical methods, and visualizes results in Grafana.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Apache Airflow (Orchestrator)                  │
│  • Monitors & auto-restarts all pipeline components                 │
│  • Validates data freshness, quality & load status                  │
│  • Verifies PostgreSQL + Parquet writes every 5 min                 │
│  • Hourly data quality checks & maintenance                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────┐    ┌──────────────┐                              │
│   │ Alpha Vantage│    │  CoinGecko   │    Data Sources              │
│   │  (Stocks)    │    │  (Crypto)    │                              │
│   └──────┬───────┘    └──────┬───────┘                              │
│          │  REST API         │  REST API                            │
│          ▼                   ▼                                      │
│   ┌──────────────┐    ┌──────────────┐                              │
│   │ Stock        │    │ Crypto       │    Ingestion                 │
│   │ Producer     │    │ Producer     │                              │
│   │ (Python)     │    │ (Python)     │                              │
│   └──────┬───────┘    └──────┬───────┘                              │
│          │                   │                                      │
│          ▼                   ▼                                      │
│   ┌─────────────────────────────────┐                               │
│   │          Apache Kafka           │    Message Broker             │
│   │  Topics:                        │                               │
│   │  • raw_stock_ticks              │                               │
│   │  • raw_crypto_ticks             │                               │
│   │  • clean_market_data            │                               │
│   │  • market_anomalies             │                               │
│   └──────────────┬──────────────────┘                               │
│                  │                                                  │
│                  ▼                                                  │
│   ┌─────────────────────────────────┐                               │
│   │   Spark Structured Streaming    │    Processing                 │
│   │  • JSON parsing & schema        │                               │
│   │  • Window aggregations (5min)   │                               │
│   │  • Feature engineering          │                               │
│   │  • Z-score anomaly detection    │                               │
│   └──────┬──────────────┬───────────┘                               │
│          │              │                                           │
│          ▼              ▼                                           │
│   ┌────────────┐  ┌─────────────┐                                   │
│   │  Parquet   │  │ PostgreSQL  │         Storage                   │
│   │  (Local)   │  │ (Anomalies) │                                   │
│   └────────────┘  └──────┬──────┘                                   │
│                          │                                          │
└──────────────────────────┼──────────────────────────────────────────┘
                           │
                           ▼
                   ┌───────────────┐
                   │    Grafana    │         Visualization
                   │  Dashboards   │
                   └───────────────┘
```

### Components

| Service | Technology | Purpose |
|---------|-----------|---------|
| **Kafka** | Confluent Kafka 7.5 | Message broker for real-time data streaming |
| **Zookeeper** | Confluent Zookeeper 7.5 | Kafka cluster coordination |
| **Stock Producer** | Python + kafka-python | Polls Alpha Vantage for stock data |
| **Crypto Producer** | Python + kafka-python | Polls CoinGecko for crypto data |
| **Spark** | Bitnami Spark 3.5 (Master + Worker) | Stream processing & anomaly detection |
| **PostgreSQL** | PostgreSQL 15 | Anomaly & clean data storage |
| **Grafana** | Grafana 10.2 | Real-time dashboards |
| **Airflow** | Apache Airflow 2.8.1 | Pipeline orchestration, scheduling & monitoring |

---

## 📊 Data Sources

All data sources are **free and public**. No paid API required.

### 1. Alpha Vantage (Stocks)
- **Endpoint**: `TIME_SERIES_INTRADAY` (5-minute intervals)
- **Symbols**: AAPL, MSFT, GOOGL, AMZN, TSLA (configurable)
- **Rate limit**: 5 calls/minute, 500 calls/day (free tier)
- **API Key**: Free at [alphavantage.co](https://www.alphavantage.co/support/#api-key)

### 2. CoinGecko (Cryptocurrencies)
- **Endpoints**: `/simple/price` and `/coins/{id}/ohlc`
- **Coins**: Bitcoin, Ethereum, Solana, Cardano, Ripple (configurable)
- **No API key required**
- **Rate limit**: ~50 calls/minute

---

## 🚀 How to Run

### Prerequisites

- **Docker** & **Docker Compose** installed
- **Alpha Vantage API key** (free): [Get one here](https://www.alphavantage.co/support/#api-key)

### Quick Start

```bash
# 1. Clone and navigate to the project
cd market-anomaly-platform

# 2. Create your .env file
cp .env.example .env
# Edit .env and add your Alpha Vantage API key

# 3. Start everything
docker-compose up -d

# 4. Check services are running
docker-compose ps

# 5. Open Grafana
# Navigate to http://localhost:3000 (admin/admin)
```

### Submit the Spark Job

Once the Spark cluster is running:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-app/streaming_job.py
```

### Service URLs

| Service | URL |
|---------|-----|
| Grafana | [http://localhost:3000](http://localhost:3000) |
| Airflow UI | [http://localhost:8082](http://localhost:8082) |
| Spark Master UI | [http://localhost:8080](http://localhost:8080) |
| Spark Worker UI | [http://localhost:8081](http://localhost:8081) |
| Kafka (external) | `localhost:29092` |
| PostgreSQL | `localhost:5432` |

### Stop & Clean Up

```bash
# Stop all services
docker-compose down

# Stop and remove all data volumes
docker-compose down -v
```

---

## 🧠 How Anomaly Detection Works

### Method: Z-Score Statistical Detection

The system uses the **Z-score method** to detect anomalies — a simple, interpretable statistical approach that requires no machine learning.

#### What is a Z-Score?

$$z = \frac{x - \mu}{\sigma}$$

Where:
- $x$ = current observed value
- $\mu$ = rolling mean of the feature
- $\sigma$ = rolling standard deviation

A Z-score tells you **how many standard deviations** a value is from the mean.

#### Detection Rules

| Condition | Interpretation |
|-----------|---------------|
| $\|z\| > 3$ | **Anomaly detected** (default threshold) |
| $\|z\| > 4$ | Medium severity |
| $\|z\| > 5$ | High severity |

#### Features Monitored

| Feature | Anomaly Type | Description |
|---------|-------------|-------------|
| **Price Return %** | `price_spike` | `(close - open) / open × 100` — sudden price jumps |
| **Total Volume** | `volume_surge` | Aggregated volume in window — unusual trading activity |
| **Rolling Volatility** | `volatility_burst` | Stddev of price within window — market instability |

#### Processing Pipeline

1. **Raw ticks** arrive from Kafka (stock + crypto)
2. **Parsed & normalized** into a unified format with event-time
3. **5-minute window** aggregations compute OHLCV + features
4. **Rolling statistics** (mean, stddev) computed over a 20-window lookback per symbol
5. **Z-scores** calculated for each feature
6. **Anomalies flagged** when |z| > 3 and written to PostgreSQL

---

## 📊 Grafana Dashboards

Three pre-built dashboards are auto-provisioned when Grafana starts:

### 1. Market Overview
- Total anomalies detected (24h)
- Anomalies by type (pie chart)
- High severity count
- Anomaly timeline (hourly bar chart)
- Latest prices per symbol
- Anomalies by symbol ranking

### 2. Anomaly Alerts
- Live anomaly alert table (auto-refresh 10s)
- Color-coded severity (red/yellow/green)
- Z-score distribution histogram
- Anomalies per hour (stacked bars by severity)
- Top anomalous symbols
- Severity breakdown by asset type

### 3. Volatility Trends
- Rolling volatility over time (line chart per symbol)
- Price return % with threshold bands
- Volume delta bar chart
- Asset volatility comparison (latest snapshot)
- Volatility vs. Price Return scatter plot

### Grafana Credentials
- **Username**: `admin`
- **Password**: `admin` (change on first login)
- **PostgreSQL datasource**: auto-configured, named "MarketDB"

---

## 📁 Project Structure

```
market-anomaly-platform/
├── producers/
│   ├── Dockerfile            # Docker image for Python producers
│   ├── main.py               # Entrypoint – routes to stock or crypto producer
│   ├── config.py             # Centralized configuration (env vars)
│   ├── stock_producer.py     # Alpha Vantage → Kafka producer
│   └── crypto_producer.py    # CoinGecko → Kafka producer
├── spark/
│   ├── schemas.py            # PySpark schema definitions
│   └── streaming_job.py      # Spark Structured Streaming job
├── airflow/
│   ├── dags/
│   │   ├── market_pipeline_dag.py   # Main pipeline orchestration DAG
│   │   └── data_quality_dag.py      # Hourly data quality monitoring DAG
│   ├── logs/                         # Airflow task logs (auto-generated)
│   └── plugins/                      # Custom Airflow plugins
├── storage/
│   └── init.sql              # PostgreSQL schema initialization
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml    # Auto-provision PostgreSQL datasource
│   │   └── dashboards/
│   │       └── dashboards.yml    # Dashboard provider config
│   └── dashboards/
│       ├── market_overview.json  # Market Overview dashboard
│       ├── anomaly_alerts.json   # Anomaly Alerts dashboard
│       └── volatility_trends.json # Volatility Trends dashboard
├── docker-compose.yml        # Full stack orchestration
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variable template
├── .gitignore
└── README.md                 # This file
```

---

## ⚙️ Configuration

All configuration is done via environment variables (`.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `ALPHA_VANTAGE_API_KEY` | `demo` | Your Alpha Vantage API key |
| `STOCK_SYMBOLS` | `AAPL,MSFT,GOOGL,AMZN,TSLA` | Comma-separated stock tickers |
| `CRYPTO_IDS` | `bitcoin,ethereum,solana,cardano,ripple` | CoinGecko coin IDs |
| `STOCK_POLL_INTERVAL` | `60` | Seconds between stock API polls |
| `CRYPTO_POLL_INTERVAL` | `30` | Seconds between crypto API polls |
| `POSTGRES_USER` | `marketuser` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `marketpass` | PostgreSQL password |
| `POSTGRES_DB` | `marketdb` | PostgreSQL database name |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Grafana admin password |

---

## � Airflow Orchestration

Apache Airflow manages and monitors the entire pipeline lifecycle through two DAGs:

### DAG 1: `market_anomaly_pipeline` (every 5 min)

End-to-end pipeline health monitoring and self-healing:

```
check_kafka ──┐
              ├─► verify_topics ──┬─► check_crypto_producer ──┐
check_postgres┘                  ├─► check_stock_producer  ───┼─► check_data_freshness ──┬─► check_row_counts ──┐
                                 └─► check_spark_job ─────────┘                          └─► check_kafka_lag ───┼─► pipeline_summary
```

| Task | What it does |
|------|--------------|
| `check_kafka_health` | Verifies Kafka broker is responsive |
| `check_postgres_health` | Verifies PostgreSQL is accepting connections |
| `verify_kafka_topics` | Confirms all 4 required topics exist |
| `check_crypto_producer` | Validates producer is running; **auto-restarts** if down |
| `check_stock_producer` | Validates producer is running; **auto-restarts** if down |
| `check_or_submit_spark_job` | Detects if Spark job is running; **auto-submits** if not |
| `check_data_freshness` | Fails if no data in the last 15 minutes |
| `check_row_counts` | Logs row counts for clean_market_data and market_anomalies |
| `check_kafka_consumer_lag` | Reports consumer group lag |
| `pipeline_health_summary` | Prints per-symbol data summary |

### DAG 2: `data_quality_monitoring` (hourly)

Data quality checks and maintenance:

| Task | What it does |
|------|--------------|
| `check_null_values` | Scans for NULL values in critical columns |
| `check_duplicate_records` | Detects duplicate (symbol, window_start) groups |
| `check_price_sanity` | Validates price ranges aren't negative or extreme |
| `anomaly_statistics` | Reports anomaly counts by type and severity |
| `data_volume_trend` | Shows rows per window for the last hour |
| `check_parquet_output` | Reports Parquet storage size and file count |
| `cleanup_old_checkpoints` | Removes Spark checkpoints older than 24 hours |
| `quality_report_summary` | Aggregated data quality report |

### Airflow Credentials

- **URL**: [http://localhost:8082](http://localhost:8082)
- **Username**: `admin`
- **Password**: `admin`

---

## �🛠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Kafka not starting | Wait for Zookeeper health check to pass; check `docker logs kafka` |
| Alpha Vantage rate limit | Reduce `STOCK_SYMBOLS` or increase `STOCK_POLL_INTERVAL` |
| Spark job fails to connect to Kafka | Ensure Kafka is healthy: `docker-compose ps` |
| Grafana shows no data | Ensure Spark job is running and producing data to PostgreSQL |
| PostgreSQL connection refused | Check `docker logs postgres` for initialization errors |
| Airflow webserver not starting | Wait for `airflow-init` to complete; check `docker logs airflow-webserver` |
| Airflow DAGs not appearing | Verify files exist in `airflow/dags/`; check `docker logs airflow-scheduler` |
| Airflow DB errors | Airflow shares PostgreSQL; ensure `init.sql` doesn't conflict with Airflow tables |

---

## 📝 License

This project is built for educational / portfolio purposes. Data sourced from free, public APIs.
