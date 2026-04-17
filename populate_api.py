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

load_dotenv()

class  APIIngestor:
  def __init__(self):
          self.api_key = os.getenv("DB_API_KEY")
          if not self.api_key:
              raise ValueError("DB_API_KEY not found in .env file")
          self.base_url = "https://api.company-information.service.gov.uk/company/"
    
  def setup_table(self):
          """Wipes and recreates the API table."""
          print("Refreshing API table schema...")
          CompanyAPI.__table__.drop(bind=engine, checkfirst=True)
          CompanyAPI.__table__.create(bind=engine, checkfirst=True)

  def get_company_numbers_from_db(self, limit=None):
          """Fetches company numbers already stored in the CSV table."""
          with SessionLocal() as session:
              query = session.query(CompanyCSV.company_number)
              if limit:
                  query = query.limit(limit)
              results = query.all()
              return [row.company_number for row in results]

  def fetch_and_insert(self, company_numbers):
          """Iterates through company numbers, fetches from API, and saves to DB."""
          with SessionLocal() as session:
              for company_number in company_numbers:
                  url = f"{self.base_url}{company_number}"
                  
                  try:
                      # Authentication in Companies House API is the key as the username, empty password
                      response = httpx.get(url, auth=(self.api_key, ""))
                      
                      if response.status_code == 200:
                          data_full = response.json()
                          
                          new_entry = CompanyAPI(
                              company_number=company_number,
                              # Using .get() with a fallback 'unknown' as we discussed for Charities
                              api_company_status=data_full.get("company_status"),
                              profile_data=data_full
                          )
                          
                          session.merge(new_entry)
                          session.commit()
                          #print(f"Ingested: {company_number}")
                      
                      elif response.status_code != 200:
                        print(f"The status code is {response.status_code}")
                                  # --- FALTA ESTA PARTE ---
                  except Exception as e:
                      print(f"Error fetching company {company_number}: {e}")
                      session.rollback() 

  def run_data_ingestion(self, limit=None):
          """Orchestrates the API ingestion process."""
          start_time = time.time()
          
          # For your test dataset approach, we setup the table first
#          self.setup_table()
          
          # Get the numbers we need to look up
          company_list = self.get_company_numbers_from_db(limit=limit)
          print(f"Starting ingestion for {len(company_list)} companies...")
          
          self.fetch_and_insert(company_list)
          
          end_time = time.time()
          print(f"--- API Ingestion completed in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    ingestor = APIIngestor()
    # Testing with a small limit
    ingestor.run_data_ingestion()