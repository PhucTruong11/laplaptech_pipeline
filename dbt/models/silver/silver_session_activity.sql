-- Silver: session_activity — one row per session with activity summary

SELECT
    session_id,
    MIN(server_timestamp)                       AS session_start,
    MAX(server_timestamp)                       AS session_end,
    COUNT(*)                                    AS event_count,
    COUNT(DISTINCT event_name)                  AS distinct_event_types,

    -- Session-level flags
    MAX(CASE WHEN event_name = 'pageview' THEN 1 ELSE 0 END)::boolean
                                                AS has_pageview,
    MAX(CASE WHEN page_name = 'DeviceDetail' THEN 1 ELSE 0 END)::boolean
                                                AS has_product_detail_view,
    MAX(CASE WHEN event_name = 'click' AND page_name = 'CompareDevice' THEN 1 ELSE 0 END)::boolean
                                                AS has_comparison,

    -- OS info (take first non-null)
    MAX(user_os)                                AS user_os,
    MAX(user_device_type)                       AS user_device_type

FROM {{ ref('silver_user_event_tracking') }}
GROUP BY session_id
