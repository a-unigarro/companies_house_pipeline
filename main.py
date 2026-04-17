from populate_csv import CSVIngestor
from populate_api import APIIngestor
from run_analysis import run_reconciliation_pipeline
import os

def main():
    # --- Configuration ---
    CSV_FILE_PATH = './data/BasicCompanyDataAsOneFile-2026-03-02.zip'
    SQL_ANALYSIS_FILE = 'sql/status_comparison.sql'
    OUTPUT_REPORT = 'status_report.csv'
    OUTPUT_DIR = 'output'
    
    # Test Limits
    CSV_LIMIT = 1  # Process only the first 400 rows
    API_LIMIT = 20 # Call the API for only the first 20 companies
    
    print("Starting Data Pipeline...")

    # Step 1: Ingest CSV
    print("\n Ingesting CSV ---")
    csv_loader = CSVIngestor(CSV_FILE_PATH, chunk_size=400)
    csv_loader.setup_table() # Clean start
    csv_loader.run_data_ingestion(limit_chunks=CSV_LIMIT)
    csv_loader.db_size()

    # Step 2: Update from API
    print("\n Updating from API ---")
    api_loader = APIIngestor()
    api_loader.run_data_ingestion()#limit=API_LIMIT)

    # Step 3: Run Analysis
    print("\n Running Reconciliation Analysis ---")
    # This uses the function we wrote to compare the two tables
    run_reconciliation_pipeline(
        sql_file=SQL_ANALYSIS_FILE, 
        output_csv=OUTPUT_REPORT,
        output_dir=OUTPUT_DIR
    )

    print("\n Pipeline Complete! Check the 'output' folder for results.")

if __name__ == "__main__":
    main()