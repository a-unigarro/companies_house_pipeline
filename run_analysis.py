from sqlalchemy import text
from database import engine
import pandas as pd
import time
import os 


def run_reconciliation_status(sql_file, output_dir=None):

    start_time = time.time()

    if output_dir is None:
        output_dir="output"
    
    # Define the output directory
    OUTPUT_DIR = output_dir

    # Ensure the directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Read and Execute the SQL 
    with open(sql_file, 'r') as f:
        query = f.read()
    
    with engine.connect() as conn:
        # This df contains columns: sic_csv, status_api
        df = pd.read_sql(text(query), conn)

    if df.empty:
        print("No data found to process.")
        return
   
    is_match = df['status_csv'].str.lower() == df['status_api'].str.lower()
    matches_df = df[is_match]
    mismatches_df = df[~is_match]


    # Calculate Stats
    total_processed = len(df)
    total_matches = len(matches_df)
    total_mismatches = len(mismatches_df)
    
    output_file='status_discrepancy_report.csv'
    output_path = os.path.join(OUTPUT_DIR, output_file)
    mismatches_df.to_csv(output_path, sep=';', index=False)

    # Print the Combined Report
    print("\n" + "="*50)
    print("      DATA RECONCILIATION SUMMARY REPORT")
    print("="*50)
    print(f"Total Companies Processed: {total_processed}")
    print(f"Total Matches:             {total_matches} ({ (total_matches/total_processed)*100:.1f}%)")
    print(f"Total Discrepancies:       {total_mismatches} ({ (total_mismatches/total_processed)*100:.1f}%)")
    
    print("\n--- Top Matching Statuses ---")
    # Sort matches by count before printing
    matches_summary = matches_df.groupby(['status_csv', 'status_api']).size().reset_index(name='count')
    matches_summary = matches_summary.sort_values('count', ascending=False)    
    for _, row in matches_summary.head(10).iterrows():
        print(f" CSV: {row['status_csv']} -> API: {row['status_api']} | Count: {row['count']}")

    print("\n--- Top Discrepancies (Saved to CSV) ---")
    # Group by the statuses to see which mappings are most common
    mismatches_summary = mismatches_df.groupby(['status_csv', 'status_api'], dropna=False).size().reset_index(name='count')
    mismatches_summary = mismatches_summary.sort_values('count', ascending=False)    
    for _, row in mismatches_summary.head(10).iterrows():
        print(f" CSV: {row['status_csv']} -> API: {row['status_api']} | Count: {row['count']}")
    end_time = time.time()
    print(f"Query Ingestion completed in {end_time - start_time:.2f} seconds ---")


def run_reconciliation_sic(sql_file, output_dir=None):

    if output_dir is None:
        output_dir="output"
    

    start_time = time.time()
    
    # Define the output directory
    OUTPUT_DIR = output_dir

    # Ensure the directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Read and Execute the SQL 
    with open(sql_file, 'r') as f:
        query = f.read()
    
    with engine.connect() as conn:

        df = pd.read_sql(text(query), conn)

    if df.empty:
        print("No data found to process.")
        return
    df['sic_csv_clean'] = df['sic_csv'].astype(str).str.extract(r'(\d+)', expand=False).fillna('MISSING')
    df['sic_api_clean'] = df['sic_api'].astype(str).str.strip().replace(['nan', '', 'None'], None).fillna('MISSING')
    
    ### SIC with description
    df['sic_label'] = df['sic_csv'].fillna('MISSING')

    is_match = df['sic_csv_clean'] == df['sic_api_clean']


    matches_df = df[is_match].copy()
    mismatches_df = df[~is_match].copy()

    # Calculate Stats
    total_processed = len(df)
    total_matches = len(matches_df)
    total_mismatches = len(mismatches_df)
    

    # Matches Summary
    matches_summary = matches_df.groupby('sic_label').size().reset_index(name='count')
    matches_summary = matches_summary.sort_values('count', ascending=False)

    # Mismatches Summary 
    mismatches_summary = mismatches_df.groupby(['sic_label', 'sic_api_clean'], dropna=False).size().reset_index(name='count')
    mismatches_summary = mismatches_summary.sort_values('count', ascending=False)

    #Export to CSV ---
    output_file_matches= "sic_matches_summary.csv"
    # Define paths
    matches_csv_path = os.path.join(OUTPUT_DIR,output_file_matches )


    # Save to CSV
    
    matches_summary.to_csv(matches_csv_path, sep=';', index=False)


    # Print the Reports

    print("\n" + "="*50)
    print("      DATA RECONCILIATION SUMMARY REPORT")
    print("="*50)
    print(f"Total Companies Processed: {total_processed}")
    print(f"Total Matches:             {total_matches} ({ (total_matches/total_processed)*100:.1f}%)")
    print(f"Total Discrepancies:       {total_mismatches} ({ (total_mismatches/total_processed)*100:.1f}%)")

    print(f"\n--- Top Matching Statuses (Saved to {output_file_matches}) ---")
    for _, row in matches_summary.head(10).iterrows():
        print(f" SIC CODE: {row['sic_label']} | Count: {row['count']}")

    if not mismatches_df.empty:        
        output_file_mismatches= "sic_mismatches_summary.csv"
        mismatches_csv_path = os.path.join(OUTPUT_DIR, output_file_mismatches) # Using your output_csv name for discrepancies
        mismatches_summary.to_csv(mismatches_csv_path, sep=';', index=False)
        mismatches_summary.to_csv(mismatches_csv_path, sep=';', index=False)
        print(f"\n--- Top Discrepancies (Saved to {output_file_mismatches}) ---")
        for _, row in mismatches_summary.head(10).iterrows():
            print(f" CSV: {row['sic_label']} -> API: {row['sic_api_clean']} | Count: {row['count']}")
    end_time = time.time()
    print(f"Query completed in {end_time - start_time:.2f} seconds ---")

def run_reconciliation_name(sql_file, output_dir=None):

    if output_dir is None:
        output_dir="output"
    

    start_time = time.time()
    
    # Define the output directory
    OUTPUT_DIR = output_dir

    # Ensure the directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Read and Execute the SQL 
    with open(sql_file, 'r') as f:
        query = f.read()
    
    with engine.connect() as conn:

        df = pd.read_sql(text(query), conn)

    if df.empty:
        print("No data found to process.")
        return


    df['name_csv'] = df['name_csv'].astype(str).str.strip()
    df['name_api'] = df['name_api'].astype(str).str.strip()


    matches_df = (
        df.loc[df['name_csv'] == df['name_api'], 
            ['company_number', 'name_csv']]
        .copy()
    )

    mismatches_df = (
        df.loc[df['name_csv'] != df['name_api'], 
            ['company_number', 'name_csv', 'name_api', 'last_name_change_date']]
        .copy()
    )

    # Calculate Stats
    total_processed = len(df)
    total_matches = len(matches_df)
    total_mismatches = len(mismatches_df)
    




    # Print the Reports
    print("\n" + "="*50)
    print("      DATA RECONCILIATION SUMMARY REPORT")
    print("="*50)
    print(f"Total Companies Processed: {total_processed}")
    print(f"Total Matches:             {total_matches} ({ (total_matches/total_processed)*100:.1f}%)")
    print(f"Total Discrepancies:       {total_mismatches} ({ (total_mismatches/total_processed)*100:.1f}%)")

    if not mismatches_df.empty:    
        output_file_mismatches= "name_mismatches_summary.csv"
        mismatches_csv_path = os.path.join(OUTPUT_DIR, output_file_mismatches) 
        mismatches_df.to_csv(mismatches_csv_path, sep=';', index=False)    
        
        print(f"\n--- Discrepancies (Saved to {output_file_mismatches}) ---")

        # Seleccionamos solo las columnas que quieres mostrar
        cols = ['company_number', 'name_csv', 'name_api', 'last_name_change_date']
        print(mismatches_df[cols].head(10).to_string(index=False))
        end_time = time.time()
        print(f"Query completed in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    run_reconciliation_status(
        sql_file="sql/status_comparison.sql"
    )

    run_reconciliation_sic(
        sql_file="sql/sic_comparison.sql"
    )

    run_reconciliation_name(
        sql_file="sql/name_comparison.sql"
    )
