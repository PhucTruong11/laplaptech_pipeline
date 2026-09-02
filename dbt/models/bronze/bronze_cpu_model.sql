-- Bronze: cpu_model — raw pass-through
SELECT * FROM {{ source('raw', 'cpu_model') }}
