-- Silver: session_funnel — funnel step reach per session
-- Each session gets flags for which funnel steps it reached (at least once)

SELECT
    session_id,
    MIN(server_timestamp)::date                 AS event_date,

    -- Funnel steps (reach-based, not sequential)
    MAX(CASE WHEN event_name = 'pageview' THEN 1 ELSE 0 END)::boolean
        AS reached_pageview,

    MAX(CASE WHEN event_name = 'pageview' AND page_name = 'DeviceDetail' THEN 1 ELSE 0 END)::boolean
        AS reached_product_detail,

    MAX(CASE WHEN event_name = 'click' AND page_name = 'DeviceDetail' THEN 1 ELSE 0 END)::boolean
        AS reached_select_for_comparison,

    MAX(CASE WHEN event_name = 'pageview' AND page_name = 'CompareDevice' THEN 1 ELSE 0 END)::boolean
        AS reached_comparison_page,

    MAX(CASE WHEN event_name = 'click' AND page_name = 'CompareDevice' AND sort_by IS NOT NULL THEN 1 ELSE 0 END)::boolean
        AS reached_sort_in_comparison

FROM {{ ref('silver_user_event_tracking') }}
GROUP BY session_id
