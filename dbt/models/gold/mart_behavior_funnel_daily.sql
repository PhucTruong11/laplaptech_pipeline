-- Gold: mart_behavior_funnel_daily
-- Daily funnel conversion rates

SELECT
    event_date,

    COUNT(*)                                    AS total_sessions,

    SUM(reached_pageview::int)                  AS step_1_pageview,
    SUM(reached_product_detail::int)            AS step_2_product_detail,
    SUM(reached_select_for_comparison::int)     AS step_3_select_compare,
    SUM(reached_comparison_page::int)           AS step_4_comparison_page,
    SUM(reached_sort_in_comparison::int)        AS step_5_sort_action

FROM {{ ref('silver_session_funnel') }}
GROUP BY event_date
ORDER BY event_date
