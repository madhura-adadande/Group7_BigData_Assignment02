SELECT 
    p.adsh::VARCHAR AS adsh, 
    p.stmt::VARCHAR AS stmt, 
    p.tag::VARCHAR AS tag, 
    p.plabel::VARCHAR AS plabel
FROM {{ source('financial_db', 'pre') }} p
JOIN {{ ref('stg_raw_sub') }} s  -- ✅ Ensures only valid adsh keys exist
ON p.adsh = s.adsh
WHERE p.stmt IS NOT NULL AND p.plabel IS NOT NULL
