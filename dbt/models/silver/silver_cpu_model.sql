-- Silver: cpu_model — cleaned dimension
SELECT
    id::int                        AS cpu_id,
    {{ clean_string('name') }}                     AS cpu_name
FROM {{ ref('bronze_cpu_model') }}
WHERE id IS NOT NULL
