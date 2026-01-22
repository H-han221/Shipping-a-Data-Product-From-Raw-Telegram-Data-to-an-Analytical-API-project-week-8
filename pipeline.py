import sys
import subprocess
from dagster import job, op


@op
def scrape_telegram_data():
    """
    Step 1: Scrape Telegram messages and images
    """
    subprocess.run(
        [sys.executable, "src/scraper.py"],
        check=True
    )


@op
def load_raw_to_postgres():
    """
    Step 2: Load raw JSON data into PostgreSQL (raw schema)
    """
    subprocess.run(
        [sys.executable, "src/load_raw.py"],
        check=True
    )


@op
def run_yolo_enrichment():
    """
    Step 3: Run YOLO object detection on downloaded images
    """
    subprocess.run(
        [sys.executable, "src/yolo_detect.py"],
        check=True
    )


@op
def run_dbt_transformations():
    """
    Step 4: Run dbt models (staging + marts)
    """
    subprocess.run(
        ["dbt", "run"],
        cwd="medical_warehouse",   # VERY IMPORTANT
        check=True
    )


@job
def medical_pipeline():
    """
    Full end-to-end pipeline
    """
    scrape_telegram_data()
    load_raw_to_postgres()
    run_yolo_enrichment()
    run_dbt_transformations()
