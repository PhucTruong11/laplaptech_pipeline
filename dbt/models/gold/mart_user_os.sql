-- Gold: mart_user_os
-- User OS and device distribution

SELECT
    user_os,
    user_device_type AS device_type,
    COUNT(DISTINCT session_id)                  AS sessions,
    ROUND(
        COUNT(DISTINCT session_id)::numeric /
        NULLIF(SUM(COUNT(DISTINCT session_id)) OVER (), 0) * 100,
        1
    )                                           AS session_pct

FROM {{ ref('silver_session_activity') }}
WHERE user_os IS NOT NULL
GROUP BY user_os, user_device_type
ORDER BY sessions DESC
