
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from ingestion.de2_ingestion import (
    detect_files,
    validate_schema,
    validate_minimum_quality,
    route_file,
    write_metadata,
)


@dag(
    dag_id="de2_landing_to_raw",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["DE2", "ingestion", "landing", "raw"],
)
def de2_landing_to_raw():

    # ========================================================
    # 1. DETECT
    # ========================================================

    @task
    def detect():

        files = detect_files()

        if not files:
            print("No files detected in landing zone.")

        return files

    # ========================================================
    # 2. VALIDATE
    # ========================================================

    @task
    def validate(files):

        results = []

        for file_path in files:

            filename = file_path.split("/")[-1]

            # -----------------------------
            # Schema validation
            # -----------------------------

            schema_valid, schema_reason = (
                validate_schema(file_path)
            )

            if not schema_valid:

                results.append({
                    "file_path": file_path,
                    "filename": filename,
                    "status": "REJECTED",
                    "row_count": 0,
                    "reason": schema_reason,
                })

                continue

            # -----------------------------
            # Minimum quality validation
            # -----------------------------

            quality_valid, row_count, quality_reason = (
                validate_minimum_quality(file_path)
            )

            if not quality_valid:

                results.append({
                    "file_path": file_path,
                    "filename": filename,
                    "status": "REJECTED",
                    "row_count": row_count,
                    "reason": quality_reason,
                })

            else:

                results.append({
                    "file_path": file_path,
                    "filename": filename,
                    "status": "VALID",
                    "row_count": row_count,
                    "reason": quality_reason,
                })

        return results

    # ========================================================
    # 3. ROUTE
    # ========================================================

    @task
    def route(results):

        for result in results:

            route_file(
                file_path=result["file_path"],
                status=result["status"],
                reason=result["reason"],
                row_count=result["row_count"],
            )

        return results

    # ========================================================
    # 4. LOG
    # ========================================================

    @task
    def log(results):

        for result in results:

            write_metadata(
            filename=result["filename"],
            status=result["status"],
            row_count=result["row_count"],
            reason=result["reason"]
        )
            
            print(
                f"FILE={result['filename']} | "
                f"STATUS={result['status']} | "
                f"ROWS={result['row_count']} | "
                f"REASON={result['reason']}"
            )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    detected = detect()

    validated = validate(detected)

    routed = route(validated)

    logged = log(routed)

    trigger_de3 = TriggerDagRunOperator(
        task_id="trigger_de3",
        trigger_dag_id="de3_spark_processing",
        wait_for_completion=False,
    )

    logged >> trigger_de3


de2_landing_to_raw()
