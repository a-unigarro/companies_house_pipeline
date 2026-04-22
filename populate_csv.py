import os
from sqlalchemy import create_engine, Date, Integer, String, text
import pandas as pd
import csv
from models import CompanyCSV
from database import engine
import httpx ### library for communicating with webpages
### for testing purposes ###
from itertools import islice
from monty.functools import lazy_property

    
class  CSVIngestor:
    def __init__(self, file_path, chunk_size):
        
        self.file_path=file_path
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

    def setup_table(self):
        """Drops and recreates the table schema."""
        CompanyCSV.__table__.drop(bind=engine, checkfirst=True)
        CompanyCSV.__table__.create(bind=engine, checkfirst=True)
        print("Table schema recreated.")

    
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

            clean_chunk.to_sql(
                name=CompanyCSV.__tablename__,
                con=engine,
                if_exists="append",
                index=False
            )

            print(f"Inserted {len(clean_chunk)} rows.")


    def db_size(self):
        with engine.begin() as conn:
            
            query = text("SELECT pg_size_pretty(pg_database_size(current_database()));")
            res = conn.execute(query).scalar()
            print(f"Total size of the current DB: {res}")


if __name__ == "__main__":
    # Now running the script is clean and descriptive
    ingestor = CSVIngestor('./data/BasicCompanyDataAsOneFile-2026-03-02.zip',400)
    ingestor.setup_table()
    ingestor.run_data_ingestion(limit_chunks=1) # Remove limit_chunks for full run
    ingestor.db_size()

