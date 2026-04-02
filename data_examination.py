import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import time
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





## Read a sample of the data

df = pd.read_csv('./data/BasicCompanyDataAsOneFile-2026-03-02.zip',compression='zip', nrows=100000,low_memory=False )

## Display first rows
fill_rate= df.notna().mean()
fill_rate=fill_rate.reset_index()
#print(fill_rate)
fill_rate.columns=["column_name","fill_rate"]
fill_rate["fill_rate"]=(pd.to_numeric(fill_rate["fill_rate"])*100).round(2)

filename_dimensions = './fill_rate_csv.csv'
df_fill = pd.DataFrame(fill_rate)
df_fill.to_csv(filename_dimensions,  sep=';', index=False)


# Check data types
#df.dtypes

# Check data shape
#df.shape

print("--- %s seconds ---" % (time.time() - start_time))