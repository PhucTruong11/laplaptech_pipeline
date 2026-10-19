WITH expected AS (
    SELECT
        (server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS event_date,
        COUNT(*) AS total_events
    FROM {{ ref('silver_user_event_tracking') }}
    GROUP BY 1
)
SELECT COALESCE(e.event_date, a.event_date) AS event_date
FROM expected e
FULL OUTER JOIN {{ ref('gold_daily_site_kpis') }} a USING (event_date)
WHERE e.total_events IS DISTINCT FROM a.total_events
