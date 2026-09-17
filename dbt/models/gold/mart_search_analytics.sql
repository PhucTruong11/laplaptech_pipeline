-- Gold: mart_search_analytics
-- Aggregates search keywords to identify Top Searches and Potential Product Gaps

SELECT
    LOWER(TRIM(search_keyword)) as search_keyword,
    COUNT(*) as search_volume,
    COUNT(DISTINCT session_id) as unique_sessions
    
FROM {{ ref('silver_user_event_tracking') }}
WHERE event_name = 'search_for_device'
  AND search_keyword IS NOT NULL
  AND TRIM(search_keyword) != ''
GROUP BY 1
ORDER BY search_volume DESC
