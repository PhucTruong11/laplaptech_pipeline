-- Gold: gold_battery_vs_interest
-- Correlation: Do laptops with better battery life get more clicks?
-- Hướng 3: Performance vs Interest (Level 3 challenge)

WITH details AS (
    SELECT device_id::int AS laptop_model_id, COUNT(*) AS total_views,
        COUNT(DISTINCT session_id) AS view_sessions
    FROM {{ ref('silver_user_event_tracking') }}
    WHERE event_name = 'pageview' AND page_name = 'DeviceDetail' AND device_id IS NOT NULL
    GROUP BY device_id::int
), comparisons AS (
    SELECT laptop_model_id, COUNT(DISTINCT event_id) AS comparison_events
    FROM {{ ref('silver_comparison_event_device') }} GROUP BY laptop_model_id
), product_interest AS (
    SELECT COALESCE(d.laptop_model_id,c.laptop_model_id) AS laptop_model_id,
        d.total_views,d.view_sessions,c.comparison_events
    FROM details d FULL OUTER JOIN comparisons c USING (laptop_model_id)
)

SELECT
    lm.laptop_model_id,
    lm.laptop_name,
    lm.brand_name,
    lm.cpu_name,
    lm.gpu_name,

    -- Benchmark data
    bm.office_battery_hours,
    bm.gaming_battery_hours,
    bm.geekbench6_single,
    bm.geekbench6_multi,

    -- Interest data
    COALESCE(pi.total_views, 0)                 AS total_views,
    COALESCE(pi.view_sessions, 0)               AS view_sessions,
    COALESCE(pi.comparison_events, 0)           AS comparison_events,

    -- Battery tier
    CASE
        WHEN bm.office_battery_hours >= 8 THEN '8h+ (Excellent)'
        WHEN bm.office_battery_hours >= 6 THEN '6-8h (Good)'
        WHEN bm.office_battery_hours >= 4 THEN '4-6h (Average)'
        WHEN bm.office_battery_hours IS NOT NULL THEN '<4h (Poor)'
        ELSE 'No Data'
    END                                         AS battery_tier,

    -- Performance tier
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
LEFT JOIN product_interest pi
    ON lm.laptop_model_id = pi.laptop_model_id
WHERE lm.is_active = true
ORDER BY pi.total_views DESC NULLS LAST
