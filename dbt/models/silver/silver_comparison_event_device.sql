-- Grain: event ID + laptop ID. Tracking taxonomy includes select/add/sort actions
-- and comparison pageviews; device_ids arrays are expanded without duplicate IDs.
WITH eligible AS (
    SELECT * FROM {{ ref('silver_user_event_tracking') }}
    WHERE event_name IN ('select_device_for_comparison', 'add_to_comparison',
        'comparison_chart_sort_selection', 'comparison_chart_geekbench_version_selection')
       OR (event_name = 'pageview' AND page_name = 'CompareDevice')
), candidates AS (
    SELECT id AS event_id, session_id, server_timestamp, device_id AS candidate FROM eligible
    UNION ALL
    SELECT e.id, e.session_id, e.server_timestamp, item.value
    FROM eligible e
    CROSS JOIN LATERAL json_array_elements_text(
        CASE WHEN json_typeof(e.device_ids_raw::json) = 'array' THEN e.device_ids_raw::json ELSE '[]'::json END
    ) item(value)
), typed AS (
    SELECT event_id, session_id, {{ business_date('server_timestamp') }} AS event_date,
        server_timestamp,
        CASE WHEN candidate ~ '^[0-9]{1,10}$'
             THEN CASE WHEN candidate::numeric BETWEEN 1 AND 2147483647 THEN candidate::int END END AS laptop_model_id
    FROM candidates
)
SELECT DISTINCT event_id, session_id, event_date, server_timestamp, laptop_model_id
FROM typed WHERE laptop_model_id IS NOT NULL
