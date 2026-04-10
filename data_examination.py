import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import time
import httpx ### library for communicating with webpages
import csv
from bs4 import BeautifulSoup
import json

start_time = time.time()






### Read a sample of the data
#
df = pd.read_csv('./data/BasicCompanyDataAsOneFile-2026-03-02.zip',compression='zip', nrows=100000,low_memory=False )
#
### Display first rows
fill_rate= df.notna().mean()
fill_rate=fill_rate.reset_index()
##print(fill_rate)
fill_rate.columns=["column_name","fill_rate"]
fill_rate["fill_rate"]=(pd.to_numeric(fill_rate["fill_rate"])*100).round(2)

filename_dimensions = './fill_rate_csv.csv'
#df_fill = fill_rate[fill_rate["fill_rate"]>90] # condition to export the columns with higher filled rate.
df_fill.to_csv(filename_dimensions,  sep=';', index=False)




############# flattern dictionaries ##################
def get_table_data(data, parent_key='', rows=None):
    """
    Recursively explores the dictionary to build a list of (field, type) rows.
    """
    if rows is None:
        rows = []
        
    for key, value in data.items():
        # 1. Build the dotted path (e.g., accounts.next_accounts.due_on)
        full_key = f"{parent_key}.{key}" if parent_key else key
        
        # 2. Get the clean data type name (dict, str, int, etc.)
        data_type = type(value).__name__
        
        # 3. Add to our collection
        rows.append({'unique_field': full_key, 'data_type': data_type})
        
        # 4. If it's a dictionary, dive deeper
        if isinstance(value, dict):
            get_table_data(value, full_key, rows)
            
    return rows



######################### field for individual companies #########################
# Tu API Key (sin contraseña)
api_key = "40acc72b-e914-4571-8282-756e05dc3fab"
url = "https://api.company-information.service.gov.uk/advanced-search/companies"

parametros= {
    "size": 50
}


response = httpx.get(url, auth=(api_key, ''), params=parametros) ### envia la peticion de "traer" los datos (get). usa la llave, sin contraseña (''->vacio)
data = response.json() ## convierte de json a diccionario de python

# Imprimimos las llaves del primer resultado para ver los atributos
#print(data.items())
companies = data.get('items', [])
first_company = companies[0]

# look for companies with more than 1 SIC code
#multi_sic_companies_no = [
#    company.get('company_number', 'N/A') 
#    for company in companies 
#    if len(company.get("sic_codes", [])) > 1
#]
#
#target_number = "NI016095"
#found_company = None
#
#for company in companies:
#    if company.get('company_number') == target_number:
#        found_company = company
#        break  # Detenemos la búsqueda porque ya la encontramos
#
## Ahora aplicamos tu inspección de atributos
#if found_company:
#    print(f"✅ Empresa encontrada: {target_number}")
#    for key, value in found_company.items():
#        print(f"Atributo: {key} -> Ejemplo: {value}")
#

#print(f"Empresas mas de un SIC: {len(multi_sic_companies_no)}")
#print(multi_sic_companies_no)


#for key, value in company.items():
#    print(f"Atributo: {key} -> Ejemplo: {value}")

url = f"https://api.company-information.service.gov.uk/company/{first_company['company_number']}"
response = httpx.get(url, auth=(api_key, '')) ### envia la peticion de "traer" los datos (get). usa la llave, sin contraseña (''->vacio)
data_full = response.json() ## convierte de json a diccionario de python


#### Extract all rows from your data_full object
all_rows = get_table_data(data_full)

# Convert to a DataFrame
df = pd.DataFrame(all_rows)
# 3. Export to CSV
# Using sep=';' as per your example, and index=False to avoid row numbers
filename = './companies_house_master_schema.csv'
df.to_csv(filename, sep=';', index=False, encoding='utf-8')


################# Possible values with webcrawling ################################

# 1. Target the official documentation URL
url = "https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/companyprofile?v=latest"

response = httpx.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    master_fields = []
    
    # We look for all table rows (defined by <tr>) in the main documentation table
    for row in soup.find_all('tr'):
        # Columns (or table data defined within rows as <td>)
        cells = row.find_all('td')
        
        # We need rows that have at least the 'Name' and 'Type' columns, i.e. >= 2
        if len(cells) >= 2:
            #extract plain text and clean it using strip=True. e.g " company_name \n" --> "company_name"
            field_name = cells[0].get_text(strip=True) 
            data_type = cells[1].get_text(strip=True)
            
            # Filter out header noise and focus on actual property names
            if field_name and data_type and cells[3].get_text(strip=True)!="Optional":
                master_fields.append({
                    'unique_field': field_name,
                    'data_type': data_type
                })

    # 3. Use Pandas to export to CSV
    df_master = pd.DataFrame(master_fields)
    df_master.to_csv('company_house_uk_crawler_mandatory.csv', sep=';', index=False)



print("--- %s seconds ---" % (time.time() - start_time))