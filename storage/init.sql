-- =============================================================================
-- Market Anomaly Detection Platform – Database Schema
-- =============================================================================
-- This script runs automatically when PostgreSQL starts for the first time.
-- It creates the tables required for anomaly storage and Grafana dashboards.
-- =============================================================================

-- Enable the uuid-ossp extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- Table: market_anomalies
-- Stores every detected anomaly (price spike, volume surge, volatility burst)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_anomalies (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,           -- e.g. AAPL, bitcoin
    asset_type      VARCHAR(10)    NOT NULL,           -- 'stock' or 'crypto'
    anomaly_type    VARCHAR(30)    NOT NULL,           -- 'price_spike', 'volume_surge', 'volatility_burst'
    detected_at     TIMESTAMP      NOT NULL,           -- when the anomaly was flagged
    window_start    TIMESTAMP      NOT NULL,           -- aggregation window start
    window_end      TIMESTAMP      NOT NULL,           -- aggregation window end
    current_value   DOUBLE PRECISION NOT NULL,         -- the anomalous metric value
    mean_value      DOUBLE PRECISION NOT NULL,         -- rolling mean at detection time
    std_value       DOUBLE PRECISION NOT NULL,         -- rolling std dev at detection time
    z_score         DOUBLE PRECISION NOT NULL,         -- z-score of the anomalous value
    price           DOUBLE PRECISION,                  -- price at anomaly time
    volume          DOUBLE PRECISION,                  -- volume at anomaly time
    severity        VARCHAR(10)    NOT NULL DEFAULT 'medium', -- 'low', 'medium', 'high'
    metadata        JSONB,                             -- extra context (JSON)
    created_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- Indexes for fast Grafana queries
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol       ON market_anomalies (symbol);
CREATE INDEX IF NOT EXISTS idx_anomalies_asset_type   ON market_anomalies (asset_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at  ON market_anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_type         ON market_anomalies (anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity     ON market_anomalies (severity);

-- Composite index: common Grafana filter (time range + symbol)
CREATE INDEX IF NOT EXISTS idx_anomalies_time_symbol
    ON market_anomalies (detected_at DESC, symbol);

-- ---------------------------------------------------------------------------
-- Table: clean_market_data
-- Stores aggregated clean market data for historical analysis
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean_market_data (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    asset_type      VARCHAR(10)    NOT NULL,
    window_start    TIMESTAMP      NOT NULL,
    window_end      TIMESTAMP      NOT NULL,
    open_price      DOUBLE PRECISION,
    close_price     DOUBLE PRECISION,
    high_price      DOUBLE PRECISION,
    low_price       DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    price_return_pct    DOUBLE PRECISION,              -- % return within window
    rolling_volatility  DOUBLE PRECISION,              -- rolling std of returns
    volume_delta        DOUBLE PRECISION,              -- change in volume vs previous window
    record_count    INTEGER        NOT NULL DEFAULT 0, -- ticks in window
    created_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_symbol     ON clean_market_data (symbol);
CREATE INDEX IF NOT EXISTS idx_clean_window     ON clean_market_data (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_clean_asset_type ON clean_market_data (asset_type);

-- ---------------------------------------------------------------------------
-- View: anomaly_summary  (used by Grafana "Market Overview" dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW anomaly_summary AS
SELECT
    date_trunc('hour', detected_at) AS hour,
    symbol,
    asset_type,
    anomaly_type,
    COUNT(*)                         AS anomaly_count,
    AVG(ABS(z_score))               AS avg_abs_z_score,
    MAX(ABS(z_score))               AS max_abs_z_score
FROM market_anomalies
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC;

-- ---------------------------------------------------------------------------
-- View: volatility_trends (used by Grafana "Volatility Trends" dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW volatility_trends AS
SELECT
    window_start,
    symbol,
    asset_type,
    rolling_volatility,
    price_return_pct,
    volume,
    volume_delta
FROM clean_market_data
ORDER BY window_start DESC;

-- ---------------------------------------------------------------------------
-- Insert a startup marker so we know the DB initialized properly
-- ---------------------------------------------------------------------------
INSERT INTO market_anomalies (
    symbol, asset_type, anomaly_type, detected_at, window_start, window_end,
    current_value, mean_value, std_value, z_score, severity, metadata
) VALUES (
    'SYSTEM', 'system', 'db_init', NOW(), NOW(), NOW(),
    0, 0, 1, 0, 'low',
    '{"message": "Database initialized successfully"}'::jsonb
);

-- Database initialized successfully.
