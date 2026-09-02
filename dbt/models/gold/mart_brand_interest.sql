-- Gold: mart_brand_interest
-- Brand-level interest analysis: views, comparisons, interest score
-- Hướng 1: Brand Analysis

WITH brand_views AS (
    SELECT
        lm.brand_id,
        lm.brand_name,
        COUNT(DISTINCT evt.session_id)          AS view_sessions,
        COUNT(*)                                AS total_views
    FROM {{ ref('silver_user_event_tracking') }} evt
    INNER JOIN {{ ref('silver_laptop_model') }} lm
        ON evt.device_id::int = lm.laptop_model_id
    WHERE evt.event_name = 'pageview'
      AND evt.page_name = 'DeviceDetail'
    GROUP BY lm.brand_id, lm.brand_name
),

brand_comparisons AS (
    SELECT
        lm.brand_id,
        COUNT(DISTINCT csd.session_id)          AS comparison_sessions,
        COUNT(*)                                AS total_comparisons
    FROM {{ ref('silver_comparison_session_device') }} csd
    INNER JOIN {{ ref('silver_laptop_model') }} lm
        ON csd.laptop_model_id = lm.laptop_model_id
    GROUP BY lm.brand_id
),

brand_products AS (
    SELECT
        brand_id,
        COUNT(*)                                AS total_products,
        COUNT(*) FILTER (WHERE is_active)       AS active_products
    FROM {{ ref('silver_laptop_model') }}
    GROUP BY brand_id
)

SELECT
    bv.brand_id,
    bv.brand_name,
    bp.total_products,
    bp.active_products,
    bv.view_sessions,
    bv.total_views,
    COALESCE(bc.comparison_sessions, 0)         AS comparison_sessions,
    COALESCE(bc.total_comparisons, 0)           AS total_comparisons,

    -- Interest score: normalized views + comparisons
    ROUND(
        bv.total_views::numeric /
        NULLIF(MAX(bv.total_views) OVER (), 0),
        3
    )                                           AS view_interest_score,

    -- Comparison rate: what % of view sessions lead to comparison
    ROUND(
        COALESCE(bc.comparison_sessions, 0)::numeric /
        NULLIF(bv.view_sessions, 0) * 100,
        1
    )                                           AS comparison_rate_pct,

    -- Views per product (efficiency)
    ROUND(
        bv.total_views::numeric /
        NULLIF(bp.active_products, 0),
        0
    )                                           AS views_per_product

FROM brand_views bv
LEFT JOIN brand_comparisons bc ON bv.brand_id = bc.brand_id
LEFT JOIN brand_products bp ON bv.brand_id = bp.brand_id
ORDER BY bv.total_views DESC
