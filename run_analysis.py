from sqlalchemy import text
from database import engine
import pandas as pd
import time
import os 


def run_reconciliation_pipeline(sql_file, output_csv, output_dir):

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
        # This df contains columns: status_csv, status_api, total_count
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
    

    output_path = os.path.join(OUTPUT_DIR, output_csv)
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
    


if __name__ == "__main__":
    run_reconciliation_pipeline(
        sql_file="sql/status_comparison.sql", 
        output_csv="status_discrepancy_report.csv",
        output_dir="output"
    )
