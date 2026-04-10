import os
from sqlalchemy import create_engine, Date, Integer, String, text
import pandas as pd
import csv
from models import Company
from database import engine

### for testing purposes ###
from itertools import islice


    

csv_mapping = {
    "CompanyName": Company.company_name.key,
    "CompanyNumber": Company.company_number.key,
    "CompanyCategory": Company.company_category.key,
    "CompanyStatus": Company.company_status.key,
    "CountryOfOrigin": Company.country_of_origin.key,
    "IncorporationDate": Company.incorporation_date.key,
    "SICCode.SicText_1": Company.sic_code.key,
    "Mortgages.NumMortCharges": Company.no_mortgages.key,
    "Mortgages.NumMortOutstanding": Company.mortgages_outstanding.key,
    "Mortgages.NumMortPartSatisfied": Company.mortgages_part_satisfied.key,
    "Mortgages.NumMortSatisfied": Company.mortgages_satisfied.key,
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
    chunksize=100000,
    low_memory=False )


Company.__table__.drop(bind=engine, checkfirst=True)
print("Table deleted") 
Company.__table__.create(bind=engine, checkfirst=True) 
print("Table schema verified/created via SQLAlchemy Model")

for i, df_chunk in enumerate(df_iter):
    df_chunk_clean=transform_dtype_chunk(df_chunk, Company.__table__.columns)
    df_chunk_clean = df_chunk_clean[filled_cols]

    df_chunk_clean.to_sql(
        name="companies",
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