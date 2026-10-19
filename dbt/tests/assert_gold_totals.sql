WITH checks AS (
    SELECT 'daily_events' AS metric,
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }}) AS expected,
        (SELECT COALESCE(SUM(total_events), 0) FROM {{ ref('gold_daily_site_kpis') }}) AS actual
    UNION ALL
    SELECT 'session_days',
        (SELECT COUNT(*) FROM {{ ref('silver_session_day') }}),
        (SELECT COALESCE(SUM(total_sessions), 0) FROM {{ ref('gold_daily_site_kpis') }})
    UNION ALL
    SELECT 'funnel_session_days',
        (SELECT COUNT(*) FROM {{ ref('silver_session_day') }}),
        (SELECT COALESCE(SUM(total_sessions), 0) FROM {{ ref('gold_behavior_funnel_daily') }})
    UNION ALL
    SELECT 'os_sessions',
        (SELECT COUNT(DISTINCT session_id) FROM {{ ref('silver_user_event_tracking') }}),
        (SELECT COALESCE(SUM(sessions), 0) FROM {{ ref('gold_user_os') }})
    UNION ALL
    SELECT 'search_events',
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }}
         WHERE event_name = 'search_for_device' AND NULLIF(TRIM(search_keyword), '') IS NOT NULL),
        (SELECT COALESCE(SUM(search_volume), 0) FROM {{ ref('gold_search_analytics') }})
    UNION ALL
    SELECT 'cpu_detail_views',
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }} e
         JOIN {{ ref('silver_laptop_model') }} l ON e.device_id::int = l.laptop_model_id
         WHERE e.event_name = 'pageview' AND e.page_name = 'DeviceDetail'
           AND NULLIF(l.cpu_name, '') IS NOT NULL),
        (SELECT COALESCE(SUM(total_views), 0) FROM {{ ref('gold_cpu_trend') }})
    UNION ALL
    SELECT 'gpu_detail_views',
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }} e
         JOIN {{ ref('silver_laptop_model') }} l ON e.device_id::int = l.laptop_model_id
         WHERE e.event_name = 'pageview' AND e.page_name = 'DeviceDetail'
           AND NULLIF(l.gpu_name, '') IS NOT NULL),
        (SELECT COALESCE(SUM(total_views), 0) FROM {{ ref('gold_gpu_trend') }})
    UNION ALL
    SELECT 'brand_detail_views',
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }} e
         JOIN {{ ref('silver_laptop_model') }} l ON e.device_id::int = l.laptop_model_id
         WHERE e.event_name = 'pageview' AND e.page_name = 'DeviceDetail'),
        (SELECT COALESCE(SUM(total_views), 0) FROM {{ ref('gold_brand_interest') }})
    UNION ALL
    SELECT 'spec_detail_views',
        (SELECT COUNT(*) FROM {{ ref('silver_user_event_tracking') }} e
         JOIN {{ ref('silver_laptop_model') }} l ON e.device_id::int = l.laptop_model_id
         WHERE e.event_name = 'pageview' AND e.page_name = 'DeviceDetail'
           AND NULLIF(l.cpu_name, '') IS NOT NULL),
        (SELECT COALESCE(SUM(total_views), 0) FROM {{ ref('gold_spec_popularity') }})
)
SELECT * FROM checks WHERE expected IS DISTINCT FROM actual
