-- Bronze: laptop_model — raw pass-through
SELECT * FROM {{ source('raw', 'laptop_model') }}
