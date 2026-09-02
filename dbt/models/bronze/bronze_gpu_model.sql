-- Bronze: gpu_model — raw pass-through
SELECT * FROM {{ source('raw', 'gpu_model') }}
