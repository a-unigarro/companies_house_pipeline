import os
from sqlalchemy import create_engine, Date, Integer, String, text
import pandas as pd
import csv
from models import CompanyCSV
from database import engine
import httpx ### library for communicating with webpages
### for testing purposes ###
from itertools import islice


    

csv_mapping = {
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

filled_cols = list(csv_mapping.values())

#transform the data type according to the Base class
def transform_dtype_chunk(df, model_columns):
    df.columns = df.columns.str.strip()
    df=df.rename(columns=csv_mapping)


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

# Esto borra los datos pero mantiene la estructura
#with engine.begin() as conn:
#    conn.execute(text("DROP TABLE companies;"))
#print("Table rows deleted") 


df_iter = pd.read_csv(
    './data/BasicCompanyDataAsOneFile-2026-03-02.zip',
    compression='zip', 
    chunksize=400,
    low_memory=False )


CompanyCSV.__table__.drop(bind=engine, checkfirst=True)
print("Table deleted") 
CompanyCSV.__table__.create(bind=engine, checkfirst=True) 
print("Table schema verified/created via SQLAlchemy Model")

#for i, df_chunk in enumerate(df_iter):
for i, df_chunk in enumerate(islice(df_iter,1)):
    df_chunk_clean=transform_dtype_chunk(df_chunk, CompanyCSV.__table__.columns)
    df_chunk_clean = df_chunk_clean[filled_cols]

    df_chunk_clean.to_sql(
        name=CompanyCSV.__tablename__,
        con=engine,
        if_exists="append",
        index=False
    )

    print("Inserted:", len(df_chunk))


with engine.begin() as conn:
    # Sustituye 'nombre_de_tu_db' por el nombre real
    query = text("SELECT pg_size_pretty(pg_database_size(current_database()));")
    res = conn.execute(query).scalar()
    print(f"🏠 Tamaño total de la DB en disco: {res}")