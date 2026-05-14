import os
from sqlalchemy import create_engine, Date, Integer, String, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import csv
from models import CompanyCSV
from database import engine
import httpx ### library for communicating with webpages
### for testing purposes ###
from itertools import islice

    
class  CSVIngestor:
    def __init__(self, data_dir, file_date, chunk_size):
        
        self.data_dir=data_dir
        self.file_date=file_date
        self.file_name=f"BasicCompanyDataAsOneFile-{self.file_date}.zip"
        self.file_path = f"./{self.data_dir}/{self.file_name}"
        self.base_url = f"https://download.companieshouse.gov.uk"


        self.chunk_size=chunk_size
        ##### This columns have a 100% filling rate
        self.csv_mapping = {
        "CompanyName": CompanyCSV.company_name.key,
        "CompanyNumber": CompanyCSV.company_number.key,
        "CompanyCategory": CompanyCSV.company_category.key,
        "CompanyStatus": CompanyCSV.company_status.key,
        "CountryOfOrigin": CompanyCSV.country_of_origin.key,
        "IncorporationDate": CompanyCSV.incorporation_date.key,
        "SICCode.SicText_1": CompanyCSV.sic_code.key,
        "Mortgages.NumMortCharges": CompanyCSV.no_mortgages.key,
        "Mortgages.NumMortOutstanding": CompanyCSV.mortgages_outstanding.key,
        "Mortgages.NumMortPartSatisfied": CompanyCSV.mortgages_part_satisfied.key,
        "Mortgages.NumMortSatisfied": CompanyCSV.mortgages_satisfied.key,
        }
        self.filled_cols = list(self.csv_mapping.values())

    def download_data(self):
            
        ###Create directory if it doesn't exist###
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        ####Downloads the zip file if it doesn't already exist###
        if os.path.exists(self.file_path):
            print(f"File already exists at {self.file_path}. Skipping download.")
            return

        print(f"Downloading data from {self.base_url}/{self.file_name}")
        with httpx.stream("GET", f"{self.base_url}/{self.file_name}", follow_redirects=True) as response:
            if response.status_code != 200:
                raise Exception(f"Failed to download file. Status code: {response.status_code}")
            
            with open(self.file_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        print(f"Download complete: {self.file_path}")

    def setup_table(self, force_refresh=False):
        ####Creates the table if it doesn't exist. Drop it if force_refresh is True ###
        
        with engine.begin() as conn:
            if force_refresh:
                print("Dropping and recreating CSV table...")
                CompanyCSV.__table__.drop(conn, checkfirst=True)
            
            CompanyCSV.__table__.create(conn, checkfirst=True)
            print("Table verified/created.")

    
    def transform_dtype_chunk(self, df):
        df.columns = df.columns.str.strip()
        df=df.rename(columns=self.csv_mapping)

        model_columns = CompanyCSV.__table__.columns
        for col in model_columns:
            if col.name in df.columns:
                # If the model says it's a Date, convert in Pandas
                if isinstance(col.type, Date):
                    df[col.name] = pd.to_datetime(
                                                df[col.name], 
                                                dayfirst=True, 
                                                errors='coerce'
                                                ).dt.date
                
                # If the model says it's an Integer, fill nulls and convert
                elif isinstance(col.type, Integer):
                    df[col.name] = df[col.name].fillna(0).astype(int)

                if isinstance(col.type, String):
                    # Fill NaNs so they aren't 'None' objects
                    df[col.name] = df[col.name].fillna('')
                    # Convert to string and strip
                    df[col.name] = df[col.name].astype(str).str.strip()
                    # Truncate to the max length defined in models.py
                    if col.type.length:
                        df[col.name] = df[col.name].str.slice(0, col.type.length)

        return df

    def run_data_ingestion(self,limit_chunks=None):


        df_iter = pd.read_csv(
            self.file_path,
            compression='zip', 
            chunksize=self.chunk_size,
            low_memory=False )
        
        if limit_chunks:
            df_iter = islice(df_iter, limit_chunks)

        for i, df_chunk in enumerate(df_iter):
            clean_chunk=self.transform_dtype_chunk(df_chunk)
            clean_chunk = clean_chunk[self.filled_cols]

        # Convert chunk to list of dictionaries for SQLAlchemy
            data_dicts = clean_chunk.to_dict(orient='records')

            if not data_dicts:
                continue

            with engine.begin() as conn:
                # Create the base insert statement
                stmt = insert(CompanyCSV).values(data_dicts)
                
                # Define what happens on conflict (Update these columns)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['company_number'], 
                    set_={
                    "company_name": stmt.excluded.company_name,
                    "company_status": stmt.excluded.company_status,
                    "company_category": stmt.excluded.company_category,
                    "country_of_origin": stmt.excluded.country_of_origin, # Added
                    "incorporation_date": stmt.excluded.incorporation_date, # Added
                    "sic_code": stmt.excluded.sic_code,
                    "no_mortgages": stmt.excluded.no_mortgages,
                    "mortgages_outstanding": stmt.excluded.mortgages_outstanding,
                    "mortgages_part_satisfied": stmt.excluded.mortgages_part_satisfied,
                    "mortgages_satisfied": stmt.excluded.mortgages_satisfied,
                }
                )
                conn.execute(upsert_stmt)
            print(f"Inserted {len(clean_chunk)} rows.")


    def db_size(self):
        with engine.begin() as conn:
            
            query = text("SELECT pg_size_pretty(pg_database_size(current_database()));")
            res = conn.execute(query).scalar()
            print(f"Total size of the current DB: {res}")


if __name__ == "__main__":
    # Now running the script is clean and descriptive
    ingestor = CSVIngestor(data_dir="data", file_date="2026-05-01",chunk_size=400)
    ingestor.download_data()
    ingestor.setup_table()
    ingestor.run_data_ingestion(limit_chunks=1) # Remove limit_chunks for full run
    ingestor.db_size()

