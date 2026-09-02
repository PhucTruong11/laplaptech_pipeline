-- Silver: gpu_model — cleaned dimension
SELECT
    id::int                        AS gpu_id,
    TRIM(name)                     AS gpu_name
FROM {{ ref('bronze_gpu_model') }}
WHERE id IS NOT NULL
