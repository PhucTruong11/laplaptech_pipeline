SELECT *
FROM {{ ref('gold_daily_site_kpis') }}
WHERE total_events < total_sessions
   OR detail_rate_pct NOT BETWEEN 0 AND 100
   OR comparison_rate_pct NOT BETWEEN 0 AND 100
   OR (sessions_with_pageview > 0 AND detail_rate_pct IS NULL)
   OR (sessions_with_detail_view > 0 AND comparison_rate_pct IS NULL)
