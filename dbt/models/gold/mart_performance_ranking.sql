-- Gold: mart_performance_ranking
-- Laptops ranked by benchmark scores within brand segments
-- Hướng 3: Performance vs Price Segment

SELECT
    lm.laptop_model_id,
    lm.laptop_name,
    lm.brand_name,
    lm.cpu_name,
    lm.gpu_name,
    lm.screen_size,
    lm.battery_capacity_whr,
    lm.laptop_weight_kg,

    -- Benchmark scores
    bm.office_battery_minutes,
    bm.office_battery_hours,
    bm.gaming_battery_minutes,
    bm.gaming_battery_hours,
    bm.geekbench6_single,
    bm.geekbench6_multi,

    -- Rankings within brand
    RANK() OVER (
        PARTITION BY lm.brand_name
        ORDER BY bm.geekbench6_multi DESC NULLS LAST
    )                                           AS geekbench_rank_in_brand,

    RANK() OVER (
        PARTITION BY lm.brand_name
        ORDER BY bm.office_battery_minutes DESC NULLS LAST
    )                                           AS battery_rank_in_brand,

    -- Overall rankings
    RANK() OVER (ORDER BY bm.geekbench6_multi DESC NULLS LAST)
                                                AS geekbench_rank_overall,

    RANK() OVER (ORDER BY bm.office_battery_minutes DESC NULLS LAST)
                                                AS battery_rank_overall,

    -- Performance tier (based on Geekbench multi-core)
    CASE
        WHEN bm.geekbench6_multi >= 12000 THEN 'High Performance'
        WHEN bm.geekbench6_multi >= 8000  THEN 'Mid-Range'
        WHEN bm.geekbench6_multi >= 4000  THEN 'Entry Level'
        WHEN bm.geekbench6_multi IS NOT NULL THEN 'Budget'
        ELSE 'No Benchmark'
    END                                         AS performance_tier

FROM {{ ref('silver_laptop_model') }} lm
LEFT JOIN {{ ref('silver_laptop_benchmark_result') }} bm
    ON lm.laptop_model_id = bm.laptop_model_id
WHERE lm.is_active = true
ORDER BY bm.geekbench6_multi DESC NULLS LAST
