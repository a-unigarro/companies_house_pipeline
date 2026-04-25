SELECT DISTINCT ON (csv.company_number)
    csv.company_number AS company_number,
    csv.company_name AS name_csv, 
    api.profile_data ->> 'company_name' AS name_api,
    item ->> 'date' AS last_name_change_date,
    item -> 'description_values' ->> 'description' AS new_name_from_api,
    item AS full_change_event
FROM companies_csv AS csv
JOIN companies_api AS api ON csv.company_number = api.company_number
-- Cambiamos a LEFT JOIN para no perder las empresas sin cambios de nombre
LEFT JOIN LATERAL jsonb_array_elements(api.filing_history -> 'items') AS item 
    ON item ->> 'category' = 'change-of-name'
-- Quitamos el WHERE porque el filtro ahora está dentro del ON del LEFT JOIN
ORDER BY csv.company_number, (item ->> 'date') DESC NULLS LAST;