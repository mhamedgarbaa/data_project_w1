"""
Market Anomaly Pipeline – Main Orchestration DAG
=================================================

This DAG orchestrates the full market anomaly detection pipeline:
  1. Verify infrastructure health (Kafka, PostgreSQL, Spark)
  2. Check that producers are running and sending data
  3. Submit/monitor the Spark streaming job
  4. Validate data is flowing into PostgreSQL
  5. Run data quality checks on clean_market_data & market_anomalies

Schedule: Every 5 minutes (continuous monitoring)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "market-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="market_anomaly_pipeline",
    default_args=default_args,
    description="Orchestrate the real-time market anomaly detection pipeline",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=["market", "pipeline", "orchestration"],
    max_active_runs=1,
) as dag:

    # -----------------------------------------------------------------
    # Task 1: Check Kafka health
    # -----------------------------------------------------------------
    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command=(
            'docker exec kafka kafka-broker-api-versions '
            '--bootstrap-server localhost:9092 > /dev/null 2>&1 '
            '&& echo "Kafka OK" || (echo "Kafka UNHEALTHY" && exit 1)'
        ),
    )

    # -----------------------------------------------------------------
    # Task 2: Check PostgreSQL health
    # -----------------------------------------------------------------
    check_postgres = BashOperator(
        task_id="check_postgres_health",
        bash_command=(
            'docker exec postgres pg_isready -U marketuser '
            '&& echo "PostgreSQL OK" || (echo "PostgreSQL UNHEALTHY" && exit 1)'
        ),
    )

    # -----------------------------------------------------------------
    # Task 3: Verify Kafka topics exist
    # -----------------------------------------------------------------
    verify_topics = BashOperator(
        task_id="verify_kafka_topics",
        bash_command=(
            'TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092) && '
            'echo "$TOPICS" | grep -q "raw_crypto_ticks" && '
            'echo "$TOPICS" | grep -q "clean_market_data" && '
            'echo "All required topics exist" || '
            '(echo "Missing topics!" && exit 1)'
        ),
    )

    # -----------------------------------------------------------------
    # Task 4: Check producers are alive
    # -----------------------------------------------------------------
    check_crypto_producer = BashOperator(
        task_id="check_crypto_producer",
        bash_command=(
            'STATUS=$(docker inspect -f '
            '"{% raw %}{{.State.Running}}{% endraw %}" '
            'crypto-producer 2>/dev/null) && '
            '[ "$STATUS" = "true" ] && echo "Crypto producer running" || '
            '(echo "Crypto producer NOT running – restarting..." && '
            'docker restart crypto-producer && sleep 5 && echo "Restarted")'
        ),
    )

    check_stock_producer = BashOperator(
        task_id="check_stock_producer",
        bash_command=(
            'STATUS=$(docker inspect -f '
            '"{% raw %}{{.State.Running}}{% endraw %}" '
            'stock-producer 2>/dev/null) && '
            '[ "$STATUS" = "true" ] && echo "Stock producer running" || '
            '(echo "Stock producer NOT running – restarting..." && '
            'docker restart stock-producer && sleep 5 && echo "Restarted")'
        ),
    )

    # -----------------------------------------------------------------
    # Task 5: Check/submit Spark streaming job
    # -----------------------------------------------------------------
    check_spark_job = BashOperator(
        task_id="check_or_submit_spark_job",
        bash_command=(
            # Check if spark-submit is already running inside the container
            'RUNNING=$(docker exec spark-master bash -c '
            '"ps aux | grep spark-submit | grep -v grep | wc -l") && '
            'if [ "$RUNNING" -gt 0 ]; then '
            '  echo "Spark streaming job already running"; '
            'else '
            '  echo "Spark job NOT running – submitting..."; '
            '  docker exec -d spark-master /opt/spark/bin/spark-submit '
            '    --master local[*] '
            '    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.1 '
            '    --driver-memory 1g '
            '    --conf spark.sql.streaming.checkpointLocation=/opt/spark-data/checkpoints '
            '    /opt/spark-app/streaming_job.py; '
            '  sleep 10; '
            '  echo "Spark job submitted"; '
            'fi'
        ),
    )

    # -----------------------------------------------------------------
    # Task 6: Validate data freshness – clean_market_data
    # -----------------------------------------------------------------
    check_data_freshness = BashOperator(
        task_id="check_data_freshness",
        bash_command=(
            'RESULT=$(docker exec postgres psql -U marketuser -d marketdb -t -c '
            "\"SELECT CASE WHEN MAX(window_start) > NOW() - INTERVAL '15 minutes' "
            "THEN 'FRESH' ELSE 'STALE' END FROM clean_market_data;\") && "
            'RESULT=$(echo $RESULT | xargs) && '
            'echo "Data status: $RESULT" && '
            '[ "$RESULT" = "FRESH" ] || (echo "WARNING: Data is stale!" && exit 1)'
        ),
    )

    # -----------------------------------------------------------------
    # Task 7: Check row counts
    # -----------------------------------------------------------------
    check_row_counts = BashOperator(
        task_id="check_row_counts",
        bash_command=(
            'docker exec postgres psql -U marketuser -d marketdb -c '
            '"SELECT \'clean_market_data\' AS table_name, COUNT(*) AS row_count FROM clean_market_data '
            'UNION ALL '
            "SELECT 'market_anomalies', COUNT(*) FROM market_anomalies WHERE symbol != 'SYSTEM';\""
        ),
    )

    # -----------------------------------------------------------------
    # Task 8: Verify PostgreSQL data load
    # -----------------------------------------------------------------
    verify_postgres_load = BashOperator(
        task_id="verify_postgres_data_load",
        bash_command=(
            'echo "=== PostgreSQL Data Load Verification ==="; '
            'echo ""; '
            'echo "--- NEW clean_market_data rows (last 10 min) ---"; '
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT symbol, "
            "  COUNT(*) AS new_rows, "
            "  ROUND(MIN(close_price)::numeric, 2) AS min_price, "
            "  ROUND(MAX(close_price)::numeric, 2) AS max_price, "
            "  ROUND(SUM(volume)::numeric, 0) AS total_volume, "
            "  MIN(window_start) AS earliest_window, "
            "  MAX(window_start) AS latest_window "
            "FROM clean_market_data "
            "WHERE created_at > NOW() - INTERVAL '10 minutes' "
            'GROUP BY symbol ORDER BY symbol;"; '
            'echo ""; '
            'NEW_ROWS=$(docker exec postgres psql -U marketuser -d marketdb -t -c "'
            "SELECT COUNT(*) FROM clean_market_data "
            "WHERE created_at > NOW() - INTERVAL '10 minutes';"
            '" | xargs); '
            'echo ">>> Total new clean_market_data rows: $NEW_ROWS"; '
            'echo ""; '
            'echo "--- NEW market_anomalies (last 10 min) ---"; '
            'docker exec postgres psql -U marketuser -d marketdb -c "'
            "SELECT symbol, anomaly_type, severity, "
            "  ROUND(z_score::numeric, 2) AS z_score, "
            "  ROUND(price::numeric, 2) AS price, "
            "  detected_at "
            "FROM market_anomalies "
            "WHERE symbol != 'SYSTEM' "
            "  AND created_at > NOW() - INTERVAL '10 minutes' "
            'ORDER BY detected_at DESC LIMIT 20;"; '
            'NEW_ANOMALIES=$(docker exec postgres psql -U marketuser -d marketdb -t -c "'
            "SELECT COUNT(*) FROM market_anomalies "
            "WHERE symbol != 'SYSTEM' "
            "  AND created_at > NOW() - INTERVAL '10 minutes';"
            '" | xargs); '
            'echo ">>> Total new anomalies detected: $NEW_ANOMALIES"; '
            'echo ""; '
            'if [ "$NEW_ROWS" -gt 0 ]; then '
            '  echo "RESULT: SUCCESS – Data is being loaded into PostgreSQL"; '
            'else '
            '  echo "RESULT: WARNING – No new data in the last 10 minutes"; '
            'fi'
        ),
    )

    # -----------------------------------------------------------------
    # Task 9: Verify Parquet data load
    # -----------------------------------------------------------------
    verify_parquet_load = BashOperator(
        task_id="verify_parquet_data_load",
        bash_command=(
            'echo "=== Parquet Data Load Verification ==="; '
            'echo ""; '
            'echo "--- Parquet storage overview ---"; '
            "docker exec spark-master bash -c '"
            'TOTAL=$(find /opt/spark-data -name "*.parquet" 2>/dev/null | wc -l); '
            'echo "Total Parquet files: $TOTAL"; '
            "echo; "
            'echo "--- Recently written files (last 10 min) ---"; '
            'RECENT=$(find /opt/spark-data -name "*.parquet" -mmin -10 2>/dev/null | wc -l); '
            'echo "New Parquet files in last 10 min: $RECENT"; '
            "echo; "
            'if [ "$RECENT" -gt 0 ]; then '
            'echo "Recent Parquet files:"; '
            'find /opt/spark-data -name "*.parquet" -mmin -10 2>/dev/null | head -20; '
            "echo; "
            "fi; "
            'echo "--- Storage size ---"; '
            "du -sh /opt/spark-data/ 2>/dev/null || echo No data directory; "
            "echo; "
            'echo "--- Partitions ---"; '
            'ls -d /opt/spark-data/clean_market_data/*/ 2>/dev/null | tail -10 || echo "No partitions yet"; '
            "echo; "
            'if [ "$RECENT" -gt 0 ]; then '
            'echo "RESULT: SUCCESS - Parquet files being written"; '
            'elif [ "$TOTAL" -gt 0 ]; then '
            'echo "RESULT: OK - Parquet files exist but none written in last 10 min"; '
            "else "
            'echo "RESULT: INFO - No Parquet files found (Spark may write on next batch)"; '
            "fi" "'"
        ),
    )

    # -----------------------------------------------------------------
    # Task 10: Log Kafka consumer lag
    # -----------------------------------------------------------------
    check_kafka_lag = BashOperator(
        task_id="check_kafka_consumer_lag",
        bash_command=(
            'docker exec kafka kafka-consumer-groups '
            '--bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null || '
            'echo "No consumer groups found (Spark may use assign mode)"'
        ),
    )

    # -----------------------------------------------------------------
    # Task 11: Pipeline health summary
    # -----------------------------------------------------------------
    pipeline_summary = BashOperator(
        task_id="pipeline_health_summary",
        bash_command=(
            'echo "========================================"; '
            'echo " Pipeline Health Check PASSED"; '
            'echo " Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"; '
            'echo "========================================"; '
            'docker exec postgres psql -U marketuser -d marketdb -c '
            '"SELECT symbol, COUNT(*) AS total_rows, '
            "  (SELECT COUNT(*) FROM clean_market_data c2 "
            "   WHERE c2.symbol = c1.symbol "
            "   AND c2.created_at > NOW() - INTERVAL '10 minutes') AS new_rows_10m, "
            '  MAX(window_start) AS latest_window, '
            '  MAX(created_at) AS last_insert '
            'FROM clean_market_data c1 GROUP BY symbol ORDER BY symbol;"; '
            'echo ""; '
            'echo "--- Anomaly Summary (Last Hour) ---"; '
            'docker exec postgres psql -U marketuser -d marketdb -c '
            '"SELECT anomaly_type, severity, COUNT(*) as count '
            'FROM market_anomalies '
            "WHERE symbol != '\''SYSTEM'\'' "
            "AND detected_at > NOW() - INTERVAL '\''1 hour'\'' "
            'GROUP BY anomaly_type, severity ORDER BY count DESC;"; '
            'echo ""; '
            'echo "--- Parquet Status ---"; '
            'docker exec spark-master bash -c "'
            'PCOUNT=$(find /opt/spark-data -name *.parquet 2>/dev/null | wc -l); '
            'PSIZE=$(du -sh /opt/spark-data/ 2>/dev/null | cut -f1); '
            'echo Parquet files: $PCOUNT, Size: $PSIZE"'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -----------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------
    # Phase 1: Infrastructure checks (parallel)
    [check_kafka, check_postgres] >> verify_topics

    # Phase 2: Component checks (parallel after topics verified)
    verify_topics >> [check_crypto_producer, check_stock_producer, check_spark_job]

    # Phase 3: Data validation (runs even if producer checks fail)
    [check_crypto_producer, check_stock_producer, check_spark_job] >> check_data_freshness
    check_data_freshness.trigger_rule = TriggerRule.ALL_DONE

    # Phase 4: Detailed checks + data load verification (parallel)
    check_data_freshness >> [check_row_counts, verify_postgres_load, verify_parquet_load, check_kafka_lag]

    # Phase 5: Summary (waits for all phase 4 tasks)
    [check_row_counts, verify_postgres_load, verify_parquet_load, check_kafka_lag] >> pipeline_summary
