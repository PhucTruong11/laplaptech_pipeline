SELECT 'brand_rate' AS failure FROM {{ ref('gold_brand_interest') }}
WHERE comparison_rate_pct NOT BETWEEN 0 AND 100
   OR detail_and_comparison_sessions > view_sessions
UNION ALL
SELECT 'brand_pair_direction' FROM {{ ref('gold_brand_comparison') }} WHERE brand_a_id > brand_b_id
UNION ALL
SELECT 'funnel_daily_grain' FROM {{ ref('gold_behavior_funnel_daily') }} GROUP BY event_date HAVING COUNT(*) > 1
UNION ALL
SELECT 'daily_timezone' FROM {{ ref('silver_session_day') }} d
WHERE NOT EXISTS (
    SELECT 1 FROM {{ ref('silver_user_event_tracking') }} e
    WHERE e.session_id = d.session_id AND {{ business_date('e.server_timestamp') }} = d.event_date
)
