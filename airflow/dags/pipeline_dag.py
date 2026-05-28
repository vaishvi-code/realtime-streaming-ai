"""
Pipeline Orchestration DAG
===========================
Airflow DAG that orchestrates the full batch cycle:
  1. Health-check Kafka broker
  2. Trigger dbt Silver → Gold transformations
  3. Validate row counts (data quality gate)
  4. Notify on failure (logs to console; swap for Slack/email in prod)

Schedule: every 15 minutes
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─── DAG Defaults ────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

DBT_PROJECT_DIR = "/opt/airflow/data/../dbt"   # adjust if needed
DATA_DIR        = "/opt/airflow/data"


# ─── Task Functions ──────────────────────────────────────────────────────────

def check_kafka_health(**ctx):
    """Verify Kafka broker is reachable via kafka-topics CLI."""
    try:
        result = subprocess.run(
            ["kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            logger.info("Kafka healthy. Topics: %s", result.stdout.strip())
            return "run_dbt_silver"
        else:
            logger.error("Kafka unhealthy: %s", result.stderr)
            return "handle_kafka_failure"
    except Exception as e:
        logger.error("Kafka check failed: %s", e)
        return "handle_kafka_failure"


def validate_silver_data(**ctx):
    """Data quality gate: check Silver Parquet has new records."""
    import duckdb
    silver_path = f"{DATA_DIR}/silver/posts"
    try:
        con = duckdb.connect()
        result = con.execute(
            f"SELECT COUNT(*) as cnt FROM read_parquet('{silver_path}/**/*.parquet')"
        ).fetchone()
        count = result[0] if result else 0
        logger.info("Silver record count: %d", count)
        if count == 0:
            raise ValueError("Silver layer is empty — pipeline may not be running")
        ctx["ti"].xcom_push(key="silver_count", value=count)
    except Exception as e:
        logger.error("Silver validation failed: %s", e)
        raise


def validate_gold_data(**ctx):
    """Data quality gate: Gold tables must exist and have rows."""
    import duckdb
    db_path = f"{DATA_DIR}/gold/sentiment_warehouse.duckdb"
    try:
        con = duckdb.connect(db_path, read_only=True)
        for table in ["gold_sentiment_trends", "gold_topic_leaderboard"]:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logger.info("Gold table %s: %d rows", table, count)
                if count == 0:
                    logger.warning("Gold table %s is empty", table)
            except Exception:
                logger.warning("Gold table %s does not exist yet", table)
    except Exception as e:
        logger.error("Gold validation failed: %s", e)
        raise


def log_pipeline_stats(**ctx):
    silver_count = ctx["ti"].xcom_pull(key="silver_count", task_ids="validate_silver")
    logger.info("=" * 50)
    logger.info("Pipeline cycle complete")
    logger.info("Silver records available: %s", silver_count)
    logger.info("Next run in 15 minutes")
    logger.info("=" * 50)


def handle_failure(**ctx):
    task_id = ctx.get("task_instance").task_id
    logger.error("PIPELINE FAILURE in task: %s", task_id)
    logger.error("Check Airflow logs for details. Consider restarting the Spark consumer.")
    # In production: send Slack/PagerDuty alert here


# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id          = "realtime_sentiment_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "Orchestrate Kafka→Spark→dbt→Gold sentiment pipeline",
    schedule_interval = "*/15 * * * *",   # every 15 minutes
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["data-engineering", "streaming", "sentiment", "portfolio"],
) as dag:

    # ── 1. Health check ──────────────────────────────────────────────────────
    kafka_health = BranchPythonOperator(
        task_id         = "check_kafka_health",
        python_callable = check_kafka_health,
    )

    kafka_failure = PythonOperator(
        task_id         = "handle_kafka_failure",
        python_callable = handle_failure,
    )

    # ── 2. dbt transformations ───────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id      = "run_dbt_silver",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select silver --profiles-dir .",
    )

    dbt_gold = BashOperator(
        task_id      = "run_dbt_gold",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select gold --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id      = "run_dbt_tests",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    # ── 3. Data quality validation ───────────────────────────────────────────
    validate_silver = PythonOperator(
        task_id         = "validate_silver",
        python_callable = validate_silver_data,
    )

    validate_gold = PythonOperator(
        task_id         = "validate_gold",
        python_callable = validate_gold_data,
    )

    # ── 4. Completion ────────────────────────────────────────────────────────
    log_stats = PythonOperator(
        task_id         = "log_pipeline_stats",
        python_callable = log_pipeline_stats,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    pipeline_done = EmptyOperator(
        task_id      = "pipeline_done",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── DAG Flow ─────────────────────────────────────────────────────────────
    kafka_health >> [dbt_silver, kafka_failure]
    dbt_silver >> validate_silver >> dbt_gold >> dbt_test >> validate_gold >> log_stats
    log_stats >> pipeline_done
    kafka_failure >> pipeline_done
