-- Grain: session_id + ngày server nhận event theo timezone nghiệp vụ.
SELECT
    {{ business_date('server_timestamp') }} AS event_date,
    session_id,
    COUNT(*) AS event_count,
    MAX(server_timestamp) AS latest_event_at,
    COUNT(*) FILTER (WHERE event_name = 'pageview') AS pageview_count,
    COUNT(*) FILTER (WHERE event_name = 'pageview' AND page_name = 'DeviceDetail') AS detail_pageview_count,
    COUNT(*) FILTER (WHERE event_name = 'pageview' AND page_name = 'CompareDevice') AS comparison_pageview_count,
    BOOL_OR(event_name = 'pageview') AS has_pageview,
    BOOL_OR(event_name = 'pageview' AND page_name = 'DeviceDetail') AS has_detail,
    BOOL_OR(event_name IN ('select_device_for_comparison', 'add_to_comparison') OR (event_name = 'click' AND page_name = 'DeviceDetail')) AS has_select_compare,
    BOOL_OR(event_name = 'pageview' AND page_name = 'CompareDevice') AS has_comparison,
    BOOL_OR((event_name = 'comparison_chart_sort_selection' OR (event_name = 'click' AND page_name = 'CompareDevice'))
        AND NULLIF(TRIM(sort_by), '') IS NOT NULL) AS has_sort
FROM {{ ref('silver_user_event_tracking') }}
GROUP BY 1, 2
