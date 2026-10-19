-- Grain: một dòng/ngày server nhận event theo giờ Việt Nam.
-- total_sessions là daily active sessions; SUM qua nhiều ngày là session-days.
-- Rates đo cùng đạt các bước trong ngày, không yêu cầu trình tự hành vi.
WITH session_day AS (
    SELECT
        (server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS event_date,
        session_id,
        COUNT(*) AS event_count,
        BOOL_OR(event_name = 'pageview') AS has_pageview,
        BOOL_OR(event_name = 'pageview' AND page_name = 'DeviceDetail') AS has_detail,
        BOOL_OR(event_name = 'pageview' AND page_name = 'CompareDevice') AS has_comparison,
        BOOL_OR(
            event_name = 'click' AND page_name = 'CompareDevice'
            AND NULLIF(TRIM(sort_by), '') IS NOT NULL
        ) AS has_sort
    FROM {{ ref('silver_user_event_tracking') }}
    GROUP BY 1, 2
), daily AS (
    SELECT
        event_date,
        COUNT(*) AS total_sessions,
        SUM(event_count)::bigint AS total_events,
        COUNT(*) FILTER (WHERE has_pageview) AS sessions_with_pageview,
        COUNT(*) FILTER (WHERE has_detail) AS sessions_with_detail_view,
        COUNT(*) FILTER (WHERE has_comparison) AS sessions_with_comparison,
        COUNT(*) FILTER (WHERE has_sort) AS sessions_with_sort,
        COUNT(*) FILTER (WHERE has_pageview AND has_detail) AS pageview_and_detail,
        COUNT(*) FILTER (WHERE has_detail AND has_comparison) AS detail_and_comparison
    FROM session_day
    GROUP BY event_date
)
SELECT
    event_date, total_sessions, total_events,
    sessions_with_pageview, sessions_with_detail_view,
    sessions_with_comparison, sessions_with_sort,
    ROUND(100.0 * pageview_and_detail / NULLIF(sessions_with_pageview, 0), 2) AS detail_rate_pct,
    ROUND(100.0 * detail_and_comparison / NULLIF(sessions_with_detail_view, 0), 2) AS comparison_rate_pct
FROM daily
