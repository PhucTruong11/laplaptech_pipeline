-- Silver: brand — cleaned dimension
SELECT
    id::int                        AS brand_id,
    TRIM(name)                     AS brand_name
FROM {{ ref('bronze_brand') }}
WHERE id IS NOT NULL
