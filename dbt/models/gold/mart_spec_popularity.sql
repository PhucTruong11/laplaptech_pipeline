-- Gold: mart_spec_popularity
-- Which specs (CPU, GPU, screen size, RAM) appear most in viewed products
-- Hướng 2: CPU/GPU Trend Analysis

WITH spec_views AS (
    SELECT
        lm.laptop_model_id,
        lm.cpu_name,
        lm.gpu_name,
        lm.screen_size,
        lm.battery_capacity_whr,
        COUNT(*) AS view_count
    FROM {{ ref('silver_user_event_tracking') }} evt
    INNER JOIN {{ ref('silver_laptop_model') }} lm
        ON evt.device_id::int = lm.laptop_model_id
    WHERE evt.event_name = 'pageview'
      AND evt.page_name = 'DeviceDetail'
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    cpu_name,
    gpu_name,
    screen_size,
    battery_capacity_whr,
    COUNT(DISTINCT laptop_model_id)         AS product_count,
    SUM(view_count)                         AS total_views,
    ROUND(AVG(view_count), 0)               AS avg_views_per_product
FROM spec_views
WHERE cpu_name IS NOT NULL AND cpu_name != ''
GROUP BY cpu_name, gpu_name, screen_size, battery_capacity_whr
ORDER BY total_views DESC
