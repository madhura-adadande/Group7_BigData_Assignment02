SELECT 
    n.adsh::VARCHAR AS adsh, 
    n.tag::VARCHAR AS tag, 
    n.ddate::DATE AS ddate, 
    n.qtrs::INTEGER AS qtrs, 
    n.uom::VARCHAR AS uom, 
    n.value::FLOAT AS value 
FROM {{ source('financial_db', 'num') }} n
JOIN {{ ref('stg_raw_sub') }} s  -- ✅ Ensures only valid adsh keys exist
ON n.adsh = s.adsh
WHERE n.value IS NOT NULL
