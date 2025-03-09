SELECT 
    file_name,
    JSON_CONTENT:cik::VARCHAR AS cik,  
    JSON_CONTENT:company_name::VARCHAR AS company_name,  
    JSON_CONTENT:num::VARIANT AS num_data, 
    JSON_CONTENT:pre::VARIANT AS pre_data, 
    JSON_CONTENT:tag::VARIANT AS tag_data
FROM {{ source('json_financials', 'json_table') }}
