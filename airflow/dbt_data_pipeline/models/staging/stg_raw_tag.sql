SELECT 
    tag::VARCHAR AS tag,  -- Financial Tag Name
    datatype::VARCHAR AS datatype,  -- Data Type (monetary, text, etc.)
    tlabel::VARCHAR AS tlabel  -- Tag Label
FROM {{ source('financial_db', 'tag') }}
WHERE tag IS NOT NULL  -- ✅ Ensure tag is not NULL
AND datatype IS NOT NULL -- ✅ Ensure datatype is valid
