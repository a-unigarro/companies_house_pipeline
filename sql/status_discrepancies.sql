CREATE INDEX IF NOT EXISTS idx_companies_csv_status_lower ON companies_csv (LOWER(company_status));
CREATE INDEX IF NOT EXISTS idx_companies_api_status_lower ON companies_api (LOWER(api_company_status));

ANALYZE companies_csv;
ANALYZE companies_api;

SELECT 
    csv.company_number, 
    csv.company_status AS status_csv, 
    api.api_company_status AS status_api
FROM companies_csv AS csv
JOIN companies_api AS api ON csv.company_number = api.company_number
WHERE LOWER(csv.company_status) IS DISTINCT FROM LOWER(api.api_company_status);