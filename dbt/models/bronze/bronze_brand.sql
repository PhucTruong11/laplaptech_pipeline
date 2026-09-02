-- Bronze: brand — raw pass-through
SELECT * FROM {{ source('raw', 'brand') }}
