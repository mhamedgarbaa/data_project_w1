import json
import os

base = r"c:\Users\mhame\OneDrive - Ministere de l'Enseignement Superieur et de la Recherche Scientifique\Bureau\data_project_w1\market-anomaly-platform\grafana\dashboards"

# ─── MARKET OVERVIEW ───
market_overview = {
    "annotations": {"list": []},
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 1,
    "id": None,
    "links": [],
    "liveNow": False,
    "panels": [
        {
            "title": "Symbols Tracked",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 0, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT COUNT(DISTINCT symbol) AS symbols FROM clean_market_data;",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {"steps": [{"color": "blue", "value": None}]},
                    "unit": "short"
                }
            }
        },
        {
            "title": "Total Ticks",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 4, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT SUM(record_count) AS total_ticks FROM clean_market_data;",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {"steps": [{"color": "purple", "value": None}]},
                    "unit": "short"
                }
            }
        },
        {
            "title": "Aggregation Windows",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 8, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT COUNT(*) AS windows FROM clean_market_data;",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {"steps": [{"color": "orange", "value": None}]},
                    "unit": "short"
                }
            }
        },
        {
            "title": "Anomalies Detected",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 12, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT COUNT(*) AS anomalies FROM market_anomalies WHERE symbol != 'SYSTEM';",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 5},
                            {"color": "red", "value": 20}
                        ]
                    },
                    "unit": "short"
                }
            }
        },
        {
            "title": "High Severity",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 16, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT COUNT(*) AS high_severity FROM market_anomalies WHERE severity = 'high' AND symbol != 'SYSTEM';",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "red", "value": 1}
                        ]
                    },
                    "unit": "short"
                }
            }
        },
        {
            "title": "Latest Update",
            "type": "stat",
            "gridPos": {"h": 4, "w": 4, "x": 20, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT MAX(window_start) AS latest FROM clean_market_data;",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "thresholds": {"steps": [{"color": "cyan", "value": None}]},
                    "unit": "dateTimeAsIso"
                }
            }
        },
        {
            "title": "Live Price Tracker",
            "type": "timeseries",
            "gridPos": {"h": 10, "w": 16, "x": 0, "y": 4},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT window_start AS time, symbol, close_price AS price FROM clean_market_data WHERE $__timeFilter(window_start) ORDER BY window_start;",
                    "format": "time_series",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "line",
                        "lineWidth": 2,
                        "fillOpacity": 10,
                        "pointSize": 4,
                        "spanNulls": True
                    },
                    "unit": "currencyUSD"
                }
            }
        },
        {
            "title": "Current Prices",
            "type": "table",
            "gridPos": {"h": 10, "w": 8, "x": 16, "y": 4},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT DISTINCT ON (symbol) symbol, ROUND(close_price::numeric, 2) AS price, ROUND(volume::numeric, 0) AS volume, ROUND(price_return_pct::numeric, 4) AS return_pct, window_start AS updated_at FROM clean_market_data ORDER BY symbol, window_start DESC;",
                    "format": "table",
                    "refId": "A"
                }
            ]
        },
        {
            "title": "Volume Over Time",
            "type": "timeseries",
            "gridPos": {"h": 10, "w": 12, "x": 0, "y": 14},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT window_start AS time, symbol, volume FROM clean_market_data WHERE $__timeFilter(window_start) ORDER BY window_start;",
                    "format": "time_series",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "line",
                        "lineWidth": 2,
                        "fillOpacity": 20,
                        "spanNulls": True
                    },
                    "unit": "short"
                }
            }
        },
        {
            "title": "Price Return %",
            "type": "timeseries",
            "gridPos": {"h": 10, "w": 12, "x": 12, "y": 14},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT window_start AS time, symbol, price_return_pct FROM clean_market_data WHERE $__timeFilter(window_start) ORDER BY window_start;",
                    "format": "time_series",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "line",
                        "lineWidth": 1,
                        "fillOpacity": 15,
                        "spanNulls": True
                    },
                    "unit": "percent"
                }
            }
        },
        {
            "title": "OHLC Summary (Latest per Symbol)",
            "type": "table",
            "gridPos": {"h": 8, "w": 24, "x": 0, "y": 24},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT DISTINCT ON (symbol) symbol, asset_type, ROUND(open_price::numeric, 2) AS open, ROUND(high_price::numeric, 2) AS high, ROUND(low_price::numeric, 2) AS low, ROUND(close_price::numeric, 2) AS close, ROUND(volume::numeric, 0) AS volume, ROUND(rolling_volatility::numeric, 6) AS volatility, ROUND(price_return_pct::numeric, 4) AS return_pct, window_start FROM clean_market_data ORDER BY symbol, window_start DESC;",
                    "format": "table",
                    "refId": "A"
                }
            ]
        }
    ],
    "refresh": "30s",
    "schemaVersion": 38,
    "style": "dark",
    "tags": ["market", "overview"],
    "templating": {"list": []},
    "time": {"from": "now-3h", "to": "now"},
    "timepicker": {},
    "timezone": "utc",
    "title": "Market Overview",
    "uid": "market-overview",
    "version": 1
}

# ─── ANOMALY ALERTS ───
anomaly_alerts = {
    "annotations": {"list": []},
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 1,
    "id": None,
    "links": [],
    "liveNow": True,
    "panels": [
        {
            "title": "\ud83d\udcca Recent Market Data Activity",
            "type": "table",
            "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT symbol, asset_type, ROUND(close_price::numeric, 2) AS close_price, ROUND(volume::numeric, 0) AS volume, ROUND(price_return_pct::numeric, 4) AS return_pct, ROUND(rolling_volatility::numeric, 6) AS volatility, window_start, window_end FROM clean_market_data WHERE $__timeFilter(window_start) ORDER BY window_start DESC LIMIT 50;",
                    "format": "table",
                    "refId": "A"
                }
            ]
        },
        {
            "title": "\ud83d\udd34 Active Anomaly Alerts",
            "type": "table",
            "gridPos": {"h": 10, "w": 24, "x": 0, "y": 8},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT detected_at, symbol, asset_type, anomaly_type, severity, ROUND(z_score::numeric, 2) AS z_score, ROUND(current_value::numeric, 4) AS current_value, ROUND(mean_value::numeric, 4) AS mean_value, ROUND(price::numeric, 2) AS price, ROUND(volume::numeric, 0) AS volume FROM market_anomalies WHERE $__timeFilter(detected_at) AND symbol != 'SYSTEM' ORDER BY detected_at DESC LIMIT 100;",
                    "format": "table",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "overrides": [
                    {
                        "matcher": {"id": "byName", "options": "severity"},
                        "properties": [
                            {
                                "id": "custom.displayMode",
                                "value": "color-background"
                            },
                            {
                                "id": "mappings",
                                "value": [
                                    {"type": "value", "options": {"high": {"color": "red", "text": "HIGH"}}},
                                    {"type": "value", "options": {"medium": {"color": "yellow", "text": "MEDIUM"}}},
                                    {"type": "value", "options": {"low": {"color": "green", "text": "LOW"}}}
                                ]
                            }
                        ]
                    }
                ]
            }
        },
        {
            "title": "Anomaly Z-Score Distribution",
            "type": "histogram",
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 18},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT z_score FROM market_anomalies WHERE $__timeFilter(detected_at) AND symbol != 'SYSTEM';",
                    "format": "table",
                    "refId": "A"
                }
            ]
        },
        {
            "title": "Anomalies per Hour",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 18},
            "datasource": {"type": "postgres", "uid": "MarketDB"},
            "targets": [
                {
                    "rawSql": "SELECT date_trunc('hour', detected_at) AS time, severity, COUNT(*) AS count FROM market_anomalies WHERE $__timeFilter(detected_at) AND symbol != 'SYSTEM' GROUP BY 1, 2 ORDER BY 1;",
                    "format": "time_series",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "bars",
                        "fillOpacity": 70,
                        "stacking": {"mode": "normal"}
                    }
                }
            }
        }
    ],
    "refresh": "10s",
    "schemaVersion": 38,
    "style": "dark",
    "tags": ["market", "anomalies", "alerts"],
    "templating": {"list": []},
    "time": {"from": "now-6h", "to": "now"},
    "timepicker": {},
    "timezone": "utc",
    "title": "Anomaly Alerts",
    "uid": "anomaly-alerts",
    "version": 1
}

# ─── WRITE FILES ───
for name, data in [
    ("market_overview.json", market_overview),
    ("anomaly_alerts.json", anomaly_alerts),
]:
    path = os.path.join(base, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Written: {path}")

print("Done!")
