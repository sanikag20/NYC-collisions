"""
Airflow DAG: check raw data -> clean (Spark) -> transform (Spark) -> validate outputs.

Set PROJECT_DIR (Airflow Variable or environment variable) to the repo location on the
machine running Airflow, e.g. /home/<user>/nyc-collisions-spark-airflow.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/nyc-collisions-spark-airflow")
OUTPUTS = [
    "borough_summary",
    "monthly_summary",
    "severity_summary",
    "factor_summary",
    "borough_ranking",
]

default_args = {
    "owner": "sanika",
    "retries": 1,
}

with DAG(
    dag_id="nyc_collision_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["big-data", "pyspark", "nyc"],
) as dag:

    env = {"PROJECT_DIR": PROJECT_DIR}

    # 1. Fail fast if the raw CSV is missing
    check_raw_data = BashOperator(
        task_id="check_raw_data",
        env=env,
        append_env=True,
        bash_command="""
        RAW="$PROJECT_DIR/data/raw/collisions.csv"
        if [ -f "$RAW" ]; then
            echo "Raw NYC collision dataset found."
            ls -lh "$RAW"
        else
            echo "ERROR: Raw dataset not found at $RAW"
            exit 1
        fi
        """,
    )

    # 2. Clean -> Parquet
    clean_data = BashOperator(
        task_id="clean_data",
        env=env,
        append_env=True,
        bash_command='spark-submit "$PROJECT_DIR/spark/clean_collisions.py"',
    )

    # 3. Five analytical outputs
    transform_data = BashOperator(
        task_id="transform_data",
        env=env,
        append_env=True,
        bash_command='spark-submit "$PROJECT_DIR/spark/transform_collisions.py"',
    )

    # 4. Check every expected Parquet output exists
    validate_output = BashOperator(
        task_id="validate_output",
        env=env,
        append_env=True,
        bash_command="""
        set -e
        echo "Checking Spark outputs..."
        for d in """ + " ".join(OUTPUTS) + """; do
            test -d "$PROJECT_DIR/output/$d" || { echo "Missing output: $d"; exit 1; }
        done
        echo "All expected outputs found."
        find "$PROJECT_DIR/output" -name "*.parquet" -ls
        """,
    )

    check_raw_data >> clean_data >> transform_data >> validate_output
