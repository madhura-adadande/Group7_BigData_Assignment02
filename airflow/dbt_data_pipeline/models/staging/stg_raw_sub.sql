SELECT 
    adsh::VARCHAR AS adsh,  -- Unique Filing ID
    cik::VARCHAR AS cik,  -- Central Index Key for company
    name::VARCHAR AS name,  -- Company Name
    sic::VARCHAR AS sic,  -- Standard Industrial Classification Code
    fye::VARCHAR AS fye,  -- Fiscal Year End
    form::VARCHAR AS form,  -- Filing Form Type
    period::DATE AS period,  -- Reporting Period
    fy::INTEGER AS fy,  -- Fiscal Year
    fp::VARCHAR AS fp,  -- Fiscal Period
    filed::DATE AS filed  -- Filing Date
FROM {{ source('financial_db', 'sub') }}
WHERE fp IN ('Q1', 'Q2', 'Q3', 'Q4')  -- ✅ Ensures valid fiscal periods
AND fy IS NOT NULL  -- ✅ Removes invalid fiscal years
AND fp IS NOT NULL
