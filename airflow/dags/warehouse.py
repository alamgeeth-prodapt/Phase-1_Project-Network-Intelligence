import json
import logging

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from pyspark.sql import functions as F


DAGS_DIR = "/mnt/d/phase_1_project/airflow/dags"

# de6_warehouse.py sets LOG_DIR = PROJECT_ROOT / "logs", where
# PROJECT_ROOT is the parent of the script's own directory
# (warehouse/). The BashOperator below cds into DAGS_DIR and
# runs "warehouse/de6_warehouse.py", so PROJECT_ROOT resolves
# to DAGS_DIR, and the status file lands at
# DAGS_DIR/logs/de6_status.json.
STATUS_PATH = (
    "/mnt/d/phase_1_project/"
    "airflow/dags/logs/de6_status.json"
)


@dag(
    dag_id="de6_network_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DE6", "warehouse", "mysql", "spark"]
)
def de6_network_warehouse():

    @task
    def check_analytics_output():

        import os

        analytics_path = (
            "/mnt/d/phase_1_project/"
            "airflow/dags/data/analytics"
        )

        logging.info(
            f"Checking analytics output: {analytics_path}"
        )

        if not os.path.exists(analytics_path):

            raise FileNotFoundError(
                f"Analytics output does not exist: "
                f"{analytics_path}"
            )

        parquet_files = []

        for root, directories, files in os.walk(
            analytics_path
        ):

            for file in files:

                if file.endswith(".parquet"):

                    parquet_files.append(
                        os.path.join(root, file)
                    )

        if not parquet_files:

            raise FileNotFoundError(
                "No Parquet files found in analytics output."
            )

        logging.info(
            f"Found {len(parquet_files)} Parquet files."
        )

    run_warehouse = BashOperator(
        task_id="run_de6_warehouse",

        bash_command="""
        set -e

        echo "======================================"
        echo "Starting DE6 Warehouse Processing"
        echo "======================================"

        cd /mnt/d/phase_1_project/airflow/dags

        /mnt/d/phase_1_project/venv-ubuntu/bin/python \
            warehouse/de6_warehouse.py

        echo "======================================"
        echo "DE6 Warehouse Processing Completed"
        echo "======================================"
        """
    )

    @task
    def warehouse_complete():

        import os

        # ------------------------------------------------
        # Gate on de6_warehouse.py's own machine-readable
        # status, not just the BashOperator's exit code —
        # this surfaces per-table row counts and, if a
        # future change ever lets the script exit 0 without
        # raising, still catches a reported FAILURE.
        # ------------------------------------------------

        if not os.path.exists(STATUS_PATH):

            raise FileNotFoundError(
                f"DE6 status file was not created: {STATUS_PATH}"
            )

        with open(STATUS_PATH, "r", encoding="utf-8") as status_file:
            status = json.load(status_file)

        logging.info(f"DE6 status payload: {status}")

        if status.get("status") != "SUCCESS":

            raise ValueError(
                "DE6 warehouse pipeline reported failure: "
                f"{status.get('reason')}"
            )

        logging.info(
            f"dim_grid rows: {status.get('dim_grid_rows')}, "
            f"dim_time rows: {status.get('dim_time_rows')}, "
            f"fact_network_activity rows: "
            f"{status.get('fact_network_activity_rows')}"
        )

        logging.info(
            "DE6 warehouse pipeline completed successfully."
        )

    check = check_analytics_output()

    check >> run_warehouse >> warehouse_complete()


de6_network_warehouse()