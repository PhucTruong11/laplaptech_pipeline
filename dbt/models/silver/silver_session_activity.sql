-- Silver: session_activity — one row per session with activity summary

SELECT
    session_id,
    MIN(server_timestamp) AS session_start,
    {{ business_date('MIN(server_timestamp)') }} AS session_start_date,
    MAX(server_timestamp)                       AS session_end,
    COUNT(*)                                    AS event_count,
    COUNT(DISTINCT event_name)                  AS distinct_event_types,

    -- Session-level flags
    MAX(CASE WHEN event_name = 'pageview' THEN 1 ELSE 0 END)::boolean
                                                AS has_pageview,
    MAX(CASE WHEN event_name = 'pageview' AND page_name = 'DeviceDetail' THEN 1 ELSE 0 END)::boolean
                                                AS has_product_detail_view,
    MAX(CASE WHEN (event_name = 'pageview' AND page_name = 'CompareDevice') OR event_name IN ('select_device_for_comparison', 'add_to_comparison', 'comparison_chart_sort_selection') THEN 1 ELSE 0 END)::boolean
                                                AS has_comparison,

    -- Thuộc tính đầu tiên không rỗng theo timestamp và event ID, deterministic.
    COALESCE((ARRAY_AGG(NULLIF(TRIM(user_os), '') ORDER BY server_timestamp, id) FILTER (WHERE NULLIF(TRIM(user_os), '') IS NOT NULL))[1], 'Unknown')                                AS user_os,
    COALESCE((ARRAY_AGG(NULLIF(TRIM(user_device_type), '') ORDER BY server_timestamp, id) FILTER (WHERE NULLIF(TRIM(user_device_type), '') IS NOT NULL))[1], 'Unknown')                       AS user_device_type

FROM {{ ref('silver_user_event_tracking') }}
GROUP BY session_id
