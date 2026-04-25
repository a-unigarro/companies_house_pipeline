from populate_csv import CSVIngestor
from populate_api import APIIngestor
from run_analysis import run_reconciliation_status, run_reconciliation_sic, run_reconciliation_name
import os

def main():
    # --- Configuration ---
    CSV_FILE_PATH = './data/BasicCompanyDataAsOneFile-2026-03-02.zip'
    SQL_ANALYSIS_STATUS = 'sql/status_comparison.sql'
    SQL_ANALYSIS_SIC = 'sql/sic_comparison.sql'
    SQL_ANALYSIS_NAME = 'sql/name_comparison.sql'
    
    # Test Limits
    CSV_LIMIT = 1  # Process only the first chunk 
    #API_LIMIT = 20 # Call the API for only the first 20 companies
    
    print("Starting Data Pipeline...")

    #Ingest CSV
    print("\n Ingesting CSV ---")
    csv_loader = CSVIngestor(CSV_FILE_PATH, chunk_size=400)
    csv_loader.setup_table() # Clean start
    csv_loader.run_data_ingestion(limit_chunks=CSV_LIMIT)
    csv_loader.db_size()

    #Update from API
    print("\n Updating from API ---")
    api_loader = APIIngestor()
    api_loader.run_data_ingestion()

    #Run Analysis
    print("\n Running Reconciliation Analysis ---")
    run_reconciliation_status(
        sql_file=SQL_ANALYSIS_STATUS
    )
    run_reconciliation_sic(
        sql_file=SQL_ANALYSIS_SIC
    )    
    run_reconciliation_name(
        sql_file=SQL_ANALYSIS_NAME
    )
    print("\n Pipeline Complete! Check the 'output' folder for results.")

if __name__ == "__main__":
    main()