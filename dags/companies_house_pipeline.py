import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Import your existing local classes
from populate_csv import CSVIngestor
from populate_api import APIIngestor

from run_analysis import (
    run_reconciliation_status,
    run_reconciliation_sic,
    run_reconciliation_name
)

# Define default behaviors for failures/retries
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='companies_house_elt_pipeline',
    default_args=default_args,
    description='Pipeline de Companies House: Ingesta CSV, API y Reconciliación SQL',
    schedule_interval='@monthly',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['companies_house', 'elt'],
)


def companies_house_pipeline():
    DATA_DIR = "data"
    FILE_DATE = "2026-05-01"   # <--- Corregido para que coincida con tu zip
    CSV_LIMIT = 1
    BASE_DIR = Path('/opt/airflow/pipeline_code')

    # Ahora sí puedes usar el operador / de forma segura
    SQL_ANALYSIS_STATUS = str(BASE_DIR / 'sql' / 'status_comparison.sql')
    SQL_ANALYSIS_SIC = str(BASE_DIR / 'sql' / 'sic_comparison.sql')
    SQL_ANALYSIS_NAME = str(BASE_DIR / 'sql' / 'name_comparison.sql')


    @task()
    def run_csv_ingestion():
        """Task 1: Download zip and upsert records into companies_csv table"""

        print("--- Ingesting CSV via Airflow ---")
        csv_loader = CSVIngestor(data_dir=DATA_DIR, file_date=FILE_DATE, chunk_size=400)
        csv_loader.download_data()
        csv_loader.setup_table(force_refresh=False)
        csv_loader.run_data_ingestion(limit_chunks=CSV_LIMIT)
        csv_loader.db_size()
        return "CSV Step Complete"

    @task()
    def run_api_enrichment(csv_status):
        """Task 2: Look up new entries via API and merge live metadata"""
        print(f"Triggered after: {csv_status}")
        print("--- Updating from API via Airflow ---")
        api_loader = APIIngestor()
        api_loader.setup_table(force_refresh=False) 
        api_loader.run_data_ingestion()
        return "API Step Complete"
    

    @task()
    def run_reconciliation(api_status):
        """Task 3: Execute SQL discrepancy reports and export to /output"""
        print(f"Triggered after: {api_status}")
        print("--- Running Reconciliation Analysis via Airflow ---")

        run_reconciliation_status(sql_file=SQL_ANALYSIS_STATUS)
        run_reconciliation_sic(sql_file=SQL_ANALYSIS_SIC)    
        run_reconciliation_name(sql_file=SQL_ANALYSIS_NAME)
        
        print("Pipeline Complete. Check the 'output_docker' folder for results.")

    # --- DEFINE THE PIPELINE WORKFLOW ORDER ---
    csv_phase = run_csv_ingestion()
    api_phase = run_api_enrichment(csv_phase)
    reconciliation_phase = run_reconciliation(api_phase)

# Instantiate the DAG
companies_pipeline_dag = companies_house_pipeline()