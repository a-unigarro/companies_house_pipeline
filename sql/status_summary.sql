SELECT 
    csv.company_status AS status_csv, 
    api.api_company_status AS status_api,
    COUNT(*) as total_count
FROM companies_csv AS csv
JOIN companies_api AS api ON csv.company_number = api.company_number
GROUP BY csv.company_status, api.api_company_status
ORDER BY total_count DESC;