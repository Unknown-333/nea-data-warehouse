"""
Airflow DAG: NEA Daily Pipeline

Orchestrates the daily extraction pipeline:
1. Download today's NDOR PDF from NEA website
2. Extract data to CSV using pdfplumber
3. Load CSV into PostgreSQL raw schema
4. Run dbt transformations and tests

Schedule: Daily at 10:00 AM NPT (reports are typically published by morning)
Idempotent: Re-running any task will not create duplicates (upsert logic + date-scoped extraction).
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ─── DAG Configuration ───────────────────────────────────────

PROJECT_ROOT = os.environ.get("AIRFLOW_PROJECT_ROOT", "/opt/airflow/project")

default_args = {
    "owner": "nea_data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "start_date": datetime(2025, 1, 1),
}

dag = DAG(
    dag_id="nea_daily_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for NEA grid operational data",
    schedule_interval="0 10 * * *",  # 10:00 AM daily
    catchup=False,
    tags=["nea", "energy", "etl"],
    doc_md="""
    ## NEA Daily Pipeline
    
    Automatically downloads, extracts, and loads Nepal's daily
    power grid operational report (NDOR) into the data warehouse.
    
    ### Tasks
    1. **download_pdf**: Download today's NDOR PDF from NEA website
    2. **extract_to_csv**: Parse PDF tables into structured CSV
    3. **load_to_postgres**: Upsert CSV data into PostgreSQL
    4. **run_dbt**: Execute dbt models and tests
    
    ### Idempotency
    - Downloads are skipped if file already exists (unless --force)
    - Database uses upsert (ON CONFLICT UPDATE) — re-runs update, never duplicate
    - dbt models are full-refresh views/tables — always consistent
    
    ### Recovery
    - Retries 2 times with 15-minute delay
    - Missing PDF dates are logged but don't fail the pipeline
    """,
)


# ─── Task Functions ───────────────────────────────────────────

def download_today_pdf(**context):
    """Download today's NDOR PDF. Skips if already downloaded."""
    import sys
    import logging

    logger = logging.getLogger(__name__)
    sys.path.insert(0, PROJECT_ROOT)

    from extractor.download import download_recent_days

    # Download today + yesterday as safety net
    result = download_recent_days(days=2)

    if result["success"] == 0 and result["skipped"] == 0:
        raise Exception("No PDFs downloaded — NEA may not have published today's report yet")

    logger.info(
        "Download complete: %d new, %d skipped, %d failed",
        result["success"], result["skipped"], result["failed"],
    )

    # Return the download directory path via XCom for downstream tasks
    return str(Path(PROJECT_ROOT) / "data" / "raw_pdfs")


def extract_to_csv(**context):
    """Extract data from downloaded PDFs to CSV."""
    import sys
    import logging

    logger = logging.getLogger(__name__)
    sys.path.insert(0, PROJECT_ROOT)

    from extractor.extract import extract_batch

    # Pull the PDF directory from the download task via XCom
    ti = context["ti"]
    pdf_dir = ti.xcom_pull(task_ids="download_pdf")
    if not pdf_dir:
        pdf_dir = str(Path(PROJECT_ROOT) / "data" / "raw_pdfs")

    output_dir = str(Path(PROJECT_ROOT) / "data" / "bronze")
    reports = extract_batch(pdf_dir, output_dir)

    if not reports:
        raise Exception("No data extracted from PDFs")

    logger.info("Extracted %d reports to %s", len(reports), output_dir)

    # Return CSV path via XCom for the load task
    return str(Path(output_dir) / "daily_grid_report.csv")


def load_to_postgres(**context):
    """Load CSV data into PostgreSQL using upsert."""
    import sys
    import logging

    logger = logging.getLogger(__name__)
    sys.path.insert(0, PROJECT_ROOT)

    from db.load import load_daily_report_csv

    # Pull the CSV path from the extract task via XCom
    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="extract_to_csv")
    if not csv_path:
        csv_path = str(Path(PROJECT_ROOT) / "data" / "bronze" / "daily_grid_report.csv")

    load_daily_report_csv(Path(csv_path))
    logger.info("Load complete from %s", csv_path)


# ─── Define Tasks ─────────────────────────────────────────────

download_pdf = PythonOperator(
    task_id="download_pdf",
    python_callable=download_today_pdf,
    dag=dag,
)

extract_csv = PythonOperator(
    task_id="extract_to_csv",
    python_callable=extract_to_csv,
    dag=dag,
)

load_db = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag,
)

run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command=f"cd {PROJECT_ROOT}/dbt_nea && dbt run --profiles-dir . && dbt test --profiles-dir .",
    dag=dag,
)

# ─── Dependencies ─────────────────────────────────────────────

download_pdf >> extract_csv >> load_db >> run_dbt
