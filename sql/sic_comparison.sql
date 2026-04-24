SELECT 
    csv.company_number as company_number,
    csv.sic_code AS sic_csv, 
    api.profile_data -> 'sic_codes'->> 0 AS sic_api
FROM companies_csv AS csv
JOIN companies_api AS api ON csv.company_number = api.company_number;

