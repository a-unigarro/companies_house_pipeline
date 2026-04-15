import os
from sqlalchemy import create_engine, Date, Integer, String, text
from dotenv import load_dotenv
import pandas as pd
import csv
import httpx
import json
from models import CompanyAPI, CompanyCSV
from database import engine, SessionLocal

import time
start_time = time.time()

# Load the variables from your .env file
load_dotenv()
# Grab the variables using the keys you defined
DB_API_KEY = os.getenv("DB_API_KEY")

# Create a new session 
session = SessionLocal()
#Use this session to talk to your tables
results = session.query(CompanyCSV.company_number).all()
session.close()

cmp= [row.company_number for row in results]


CompanyAPI.__table__.drop(bind=engine, checkfirst=True)
print("Table deleted") 
CompanyAPI.__table__.create(bind=engine, checkfirst=True) 
print("Table schema verified/created via SQLAlchemy Model")


for company_number in cmp:
  url = f"https://api.company-information.service.gov.uk/company/{company_number}"
  response = httpx.get(url, auth=(DB_API_KEY, ""))
  
  if response.status_code == 200:  
    data_full = response.json()
    # Create the model instance
    new_entry = CompanyAPI(
        company_number=company_number,
        api_company_status=data_full.get("company_status"),
        profile_data=data_full
    )
    
    
    session.merge(new_entry)# update if exists (consindering the primary key), insert if not
    session.commit()        # save
    print(f"Ingested: {company_number}")


  elif response.status_code != 200:
    print(f"The status code is {response.status_code}")
   
 
print("--- %s seconds ---" % (time.time() - start_time))
