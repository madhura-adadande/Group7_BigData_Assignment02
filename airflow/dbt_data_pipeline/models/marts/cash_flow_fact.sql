SELECT 
    s.cik, 
    s.name AS company_name, 
    s.fy AS fiscal_year, 
    s.fp AS fiscal_period, 
    n.tag AS financial_metric, 
    n.value AS amount, 
    n.uom AS unit_of_measurement 
FROM {{ ref('stg_raw_sub') }} s
JOIN {{ ref('stg_raw_num') }} n
ON s.adsh = n.adsh
WHERE n.tag IN (
    'NetCashProvidedByUsedInOperatingActivities', 
    'NetCashProvidedByUsedInInvestingActivities',
    'NetCashProvidedByUsedInFinancingActivities'
)