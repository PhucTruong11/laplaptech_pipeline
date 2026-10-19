-- Grain: brand. Session counts là distinct session toàn thời gian.
-- comparison_rate_pct là co-reach detail + comparison, không phải sequential conversion.
WITH contributions AS (
    SELECT lm.brand_id, evt.session_id, COUNT(*) AS detail_views, 0::bigint AS comparison_views
    FROM {{ ref('silver_user_event_tracking') }} evt
    JOIN {{ ref('silver_laptop_model') }} lm ON evt.device_id::int = lm.laptop_model_id
    WHERE evt.event_name = 'pageview' AND evt.page_name = 'DeviceDetail'
    GROUP BY lm.brand_id, evt.session_id
    UNION ALL
    SELECT lm.brand_id, evt.session_id, 0::bigint, COUNT(DISTINCT evt.event_id)
    FROM {{ ref('silver_comparison_event_device') }} evt
    JOIN {{ ref('silver_laptop_model') }} lm USING (laptop_model_id)
    GROUP BY lm.brand_id, evt.session_id
), event_flags AS (
    SELECT brand_id, session_id, SUM(detail_views) AS detail_views, SUM(comparison_views) AS comparison_views
    FROM contributions GROUP BY brand_id, session_id
), interest AS (
    SELECT
        brand_id,
        COUNT(*) FILTER (WHERE detail_views > 0) AS view_sessions,
        SUM(detail_views) AS total_views,
        COUNT(*) FILTER (WHERE comparison_views > 0) AS comparison_sessions,
        SUM(comparison_views) AS total_comparisons,
        COUNT(*) FILTER (WHERE detail_views > 0 AND comparison_views > 0) AS detail_and_comparison_sessions
    FROM event_flags GROUP BY brand_id
), products AS (
    SELECT brand_id, COUNT(*) AS total_products,
        COUNT(*) FILTER (WHERE is_active) AS active_products
    FROM {{ ref('silver_laptop_model') }} GROUP BY brand_id
), combined AS (
    SELECT b.brand_id, b.brand_name, p.total_products, p.active_products,
        COALESCE(i.view_sessions, 0) AS view_sessions,
        COALESCE(i.total_views, 0) AS total_views,
        COALESCE(i.comparison_sessions, 0) AS comparison_sessions,
        COALESCE(i.total_comparisons, 0) AS total_comparisons,
        COALESCE(i.detail_and_comparison_sessions, 0) AS detail_and_comparison_sessions
    FROM products p
    JOIN {{ ref('silver_brand') }} b USING (brand_id)
    LEFT JOIN interest i USING (brand_id)
)
SELECT *,
    ROUND(total_views::numeric / NULLIF(MAX(total_views) OVER (), 0), 3) AS view_interest_score,
    ROUND(100.0 * detail_and_comparison_sessions / NULLIF(view_sessions, 0), 2) AS comparison_rate_pct,
    ROUND(total_views::numeric / NULLIF(active_products, 0), 0) AS views_per_product
FROM combined
