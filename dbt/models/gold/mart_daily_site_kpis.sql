-- Gold: mart_daily_site_kpis
-- Daily overview: sessions, pageviews, unique visitors

SELECT
    event_date,
    COUNT(DISTINCT session_id)                  AS total_sessions,
    COUNT(*)                                    AS total_events,

    -- Funnel metrics
    SUM(reached_pageview::int)                  AS sessions_with_pageview,
    SUM(reached_product_detail::int)            AS sessions_with_detail_view,
    SUM(reached_comparison_page::int)           AS sessions_with_comparison,
    SUM(reached_sort_in_comparison::int)        AS sessions_with_sort,

    -- Conversion rates
    {{ safe_divide('SUM(reached_product_detail::int)', 'SUM(reached_pageview::int)', decimal_places=3) }} * 100 AS detail_rate_pct,

    {{ safe_divide('SUM(reached_comparison_page::int)', 'SUM(reached_product_detail::int)', decimal_places=3) }} * 100 AS comparison_rate_pct

FROM {{ ref('silver_session_funnel') }}
GROUP BY event_date
ORDER BY event_date
