-- Gold: mart_cpu_trend
-- Monthly interest trend per CPU model
-- Hướng 2: CPU/GPU Trend Analysis

SELECT
    DATE_TRUNC('month', evt.server_timestamp)::date   AS month,
    lm.cpu_name,
    lm.brand_name,
    COUNT(*)                                          AS total_views,
    COUNT(DISTINCT evt.session_id)                    AS unique_sessions,
    COUNT(DISTINCT lm.laptop_model_id)                AS products_viewed

FROM {{ ref('silver_user_event_tracking') }} evt
INNER JOIN {{ ref('silver_laptop_model') }} lm
    ON evt.device_id::int = lm.laptop_model_id
WHERE evt.event_name = 'pageview'
  AND evt.page_name = 'DeviceDetail'
  AND lm.cpu_name IS NOT NULL
  AND lm.cpu_name != ''
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 5
ORDER BY month DESC, total_views DESC
