import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import time
import httpx ### library for communicating with webpages
import csv
from bs4 import BeautifulSoup

start_time = time.time()


## 1. Load the variables from your .env file
#load_dotenv()
## 2. Grab the variables using the keys you defined
#DB_USER = os.getenv("DB_USER")
#DB_PASS = os.getenv("DB_PASSWORD")
#DB_HOST = os.getenv("DB_HOST")
#DB_PORT = os.getenv("DB_PORT")
#DB_NAME = os.getenv("DB_NAME")
#

#
#DATABASE_URL=f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
#engine = create_engine(DATABASE_URL)
#
##with engine.connect() as conn:
##    print('Connection Succesfull')


common_columns = [
   "company_name",
   "company_number",
   "company_category",
   "company_status",
   "country_of_origin",
   "incorporation_date",
   "SIC_code",
   "no_mortages",
   "mortages_outstanding",
   "mortages_part_satisfied",
    "mortages_satisfied"
]

print(common_columns[0])
csv_mapping={
    "CompanyName": common_columns[0],
    "CompanyNumber": common_columns[1],
    "CompanyCategory": common_columns[2],
    "CompanyStatus": common_columns[3],
    "CountryOfOrigin": common_columns[4],
    "IncorporationDate": common_columns[5],
    "SICCode.SicText_1": common_columns[6],
    "Mortgages.NumMortCharges": common_columns[7],
    "Mortgages.NumMortOutstanding": common_columns[8],
    "Mortgages.NumMortPartSatisfied": common_columns[9],
    "Mortgages.NumMortSatisfied": common_columns[10],
}


api_mapping={
    "company_name": common_columns[0],
    "company_number": common_columns[1],
#    "CompanyCategory": common_columns[2],
    "company_status": common_columns[3],
    "registered_office_address.country": common_columns[4],
#    "IncorporationDate": common_columns[5],
    "sic_codes[]": common_columns[6],
    "Mortgages.NumMortCharges": common_columns[7],
    "Mortgages.NumMortOutstanding": common_columns[8],
    "Mortgages.NumMortPartSatisfied": common_columns[9],
    "Mortgages.NumMortSatisfied": common_columns[10],
}



df = pd.read_csv('./data/BasicCompanyDataAsOneFile-2026-03-02.zip',compression='zip', nrows=100000,low_memory=False )
df.columns = df.columns.str.strip()
print(df.columns)
df=df.rename(columns=csv_mapping)
print(df.columns)

# Check data types
#df.dtypes

# Check data shape
#df.shape




print("--- %s seconds ---" % (time.time() - start_time))