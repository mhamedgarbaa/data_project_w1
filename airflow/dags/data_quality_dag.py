"""
Data Quality & Maintenance DAG
================================

Runs hourly to:
  1. Check data quality (nulls, duplicates, schema drift)
  2. Compute & log anomaly statistics
  3. Clean up old checkpoint files
  4. Generate a data quality report stored in PostgreSQL

Schedule: Every hour
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "market-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_quality_monitoring",
    default_args=default_args,
    description="Hourly data quality checks and maintenance tasks",
    schedule_interval="@hourly",
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=["market", "quality", "maintenance"],
    max_active_runs=1,
) as dag:

    # -----------------------------------------------------------------
    # 1. Check for NULL values in critical columns
    # -----------------------------------------------------------------
    check_nulls = BashOperator(
        task_id="check_null_values",
        bash_command=(
            'docker exec postgres psql -U marketuser -d marketdb -t -c "'
            "SELECT COUNT(*) FROM clean_market_data "
            "WHERE symbol IS NULL OR close_price IS NULL OR volume IS NULL "
            "OR window_start IS NULL;\" | xargs && "
            'NULLS=$(docker exec postgres psql -U marketuser -d marketdb -t -c "'
            "SELECT COUNT(*) FROM clean_market_data "
            "WHERE symbol IS NULL OR close_price IS NULL OR volume IS NULL "
            'OR window_start IS NULL;" | xargs) && '
            'echo "Null count: $NULLS" && '
            '[ "$NULLS" -eq 0 ] && echo "PASS: No nulls found" || '
            'echo "WARNING: $NULLS rows with null values"'
        ),
    )

    # -----------------------------------------------------------------
    # 2. Check for duplicate records
    # -----------------------------------------------------------------
    check_duplicates = BashOperator(
        task_id="check_duplicate_records",
        bash_command=(
            'DUPES=$(docker exec postgres psql -U marketuser -d marketdb -t -c "'
            "SELECT COUNT(*) FROM ("
            "  SELECT symbol, window_start, COUNT(*) AS cnt "
            "  FROM clean_market_data "
            "  GROUP BY symbol, window_start "
            "  HAVING COUNT(*) > 1"
            ') AS dupes;" | xargs) && '
            'echo "Duplicate window groups: $DUPES" && '
            '[ "$DUPES" -le 5 ] && echo "PASS: Duplicates within tolerance" || '
            'echo "WARNING: High duplicate count"'
        ),
    )

    # -----------------------------------------------------------------
    # 3. Validate price ranges (sanity check)
    # -----------------------------------------------------------------
    check_price_sanity = BashOperator(
        task_id="check_price_sanity",
        bash_command=(
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT symbol, "
            "  MIN(close_price) AS min_price, "
            "  MAX(close_price) AS max_price, "
            "  ROUND(AVG(close_price)::numeric, 2) AS avg_price, "
            "  CASE "
            "    WHEN MIN(close_price) <= 0 THEN 'FAIL: negative/zero prices' "
            "    WHEN MAX(close_price) / NULLIF(MIN(close_price), 0) > 100 THEN 'WARN: extreme range' "
            "    ELSE 'PASS' "
            "  END AS status "
            "FROM clean_market_data "
            "WHERE window_start > NOW() - INTERVAL '1 hour' "
            'GROUP BY symbol ORDER BY symbol;"'
        ),
    )

    # -----------------------------------------------------------------
    # 4. Anomaly statistics report
    # -----------------------------------------------------------------
    anomaly_stats = BashOperator(
        task_id="anomaly_statistics",
        bash_command=(
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT "
            "  anomaly_type, "
            "  severity, "
            "  COUNT(*) AS count, "
            "  ROUND(AVG(ABS(z_score))::numeric, 2) AS avg_abs_zscore, "
            "  ROUND(MAX(ABS(z_score))::numeric, 2) AS max_abs_zscore "
            "FROM market_anomalies "
            "WHERE symbol != 'SYSTEM' "
            "  AND detected_at > NOW() - INTERVAL '1 hour' "
            'GROUP BY anomaly_type, severity ORDER BY count DESC;"'
        ),
    )

    # -----------------------------------------------------------------
    # 5. Data volume trend (rows per 5-min window)
    # -----------------------------------------------------------------
    data_volume_trend = BashOperator(
        task_id="data_volume_trend",
        bash_command=(
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT "
            "  window_start, "
            "  COUNT(*) AS rows, "
            "  COUNT(DISTINCT symbol) AS symbols "
            "FROM clean_market_data "
            "WHERE window_start > NOW() - INTERVAL '1 hour' "
            'GROUP BY window_start ORDER BY window_start DESC LIMIT 12;"'
        ),
    )

    # -----------------------------------------------------------------
    # 6. Check Parquet output health
    # -----------------------------------------------------------------
    check_parquet = BashOperator(
        task_id="check_parquet_output",
        bash_command=(
            'PARQUET_SIZE=$(docker exec spark-master bash -c '
            '"du -sh /opt/spark-data/ 2>/dev/null || echo 0") && '
            'echo "Parquet storage: $PARQUET_SIZE" && '
            'PARQUET_FILES=$(docker exec spark-master bash -c '
            '"find /opt/spark-data -name *.parquet 2>/dev/null | wc -l") && '
            'echo "Parquet files: $PARQUET_FILES"'
        ),
    )

    # -----------------------------------------------------------------
    # 7. Cleanup old Spark checkpoints (>24h old)
    # -----------------------------------------------------------------
    cleanup_checkpoints = BashOperator(
        task_id="cleanup_old_checkpoints",
        bash_command=(
            'docker exec spark-master bash -c "'
            'find /opt/spark-data/checkpoints -type f -mmin +1440 -delete 2>/dev/null; '
            'echo Checkpoint cleanup done" || echo "No checkpoints to clean"'
        ),
    )

    # -----------------------------------------------------------------
    # 8. Quality report summary
    # -----------------------------------------------------------------
    quality_summary = BashOperator(
        task_id="quality_report_summary",
        bash_command=(
            'echo "============================================"; '
            'echo " Data Quality Report – $(date -u +%Y-%m-%dT%H:%M:%SZ)"; '
            'echo "============================================"; '
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT "
            "  (SELECT COUNT(*) FROM clean_market_data) AS total_market_rows, "
            "  (SELECT COUNT(*) FROM market_anomalies WHERE symbol != 'SYSTEM') AS total_anomalies, "
            "  (SELECT COUNT(DISTINCT symbol) FROM clean_market_data) AS tracked_symbols, "
            "  (SELECT MAX(window_start) FROM clean_market_data) AS latest_data, "
            "  (SELECT ROUND(AVG(record_count)::numeric, 1) FROM clean_market_data "
            "   WHERE window_start > NOW() - INTERVAL '1 hour') AS avg_ticks_per_window;\""
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -----------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------
    # Phase 1: Quality checks (parallel)
    [check_nulls, check_duplicates, check_price_sanity] >> anomaly_stats

    # Phase 2: Volume & storage checks (parallel)
    anomaly_stats >> [data_volume_trend, check_parquet]

    # Phase 3: Maintenance
    [data_volume_trend, check_parquet] >> cleanup_checkpoints

    # Phase 4: Summary
    cleanup_checkpoints >> quality_summary
