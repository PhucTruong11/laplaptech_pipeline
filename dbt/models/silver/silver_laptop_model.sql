-- Silver: laptop_model — enriched with brand, CPU, GPU names
-- Joins dimension tables to create a human-readable product catalog

SELECT
    lm.id::int                                  AS laptop_model_id,
    TRIM(lm.name)                               AS laptop_name,

    -- Brand info
    lm.brand_id::int                            AS brand_id,
    b.brand_name,

    -- CPU info
    lm.cpu_model_id::int                        AS cpu_id,
    c.cpu_name,

    -- GPU info
    lm.gpu_model_id::int                        AS gpu_id,
    g.gpu_name,

    -- Specs
    lm.screen_size::numeric                     AS screen_size,
    lm.laptop_weight::numeric                   AS laptop_weight_kg,
    lm.battery_capacity_whr::numeric            AS battery_capacity_whr,

    -- Status flags
    lm.is_visible::boolean                      AS is_visible,
    lm.is_active::boolean                       AS is_active

FROM {{ ref('bronze_laptop_model') }} lm
LEFT JOIN {{ ref('silver_brand') }}     b ON lm.brand_id::int = b.brand_id
LEFT JOIN {{ ref('silver_cpu_model') }} c ON lm.cpu_model_id::int = c.cpu_id
LEFT JOIN {{ ref('silver_gpu_model') }} g ON lm.gpu_model_id::int = g.gpu_id
WHERE lm.id IS NOT NULL
