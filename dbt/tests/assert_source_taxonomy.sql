SELECT 'source_os_mapping' AS failure
FROM {{ source('raw', 'user_event_tracking') }} r
JOIN {{ ref('silver_user_event_tracking') }} s USING (id)
WHERE COALESCE(r.device::json->>'os', r.device::json->>'os_name') IS DISTINCT FROM s.user_os
UNION ALL
SELECT 'selection_reach' WHERE
    (SELECT COUNT(DISTINCT (session_id, {{ business_date('server_timestamp') }}))
     FROM {{ ref('silver_user_event_tracking') }}
     WHERE event_name IN ('select_device_for_comparison','add_to_comparison')
        OR (event_name = 'click' AND page_name = 'DeviceDetail'))
    IS DISTINCT FROM
    (SELECT COALESCE(SUM(step_3_select_compare),0) FROM {{ ref('gold_behavior_funnel_daily') }})
UNION ALL
SELECT 'comparison_bridge_duplicate'
FROM {{ ref('silver_comparison_event_device') }}
GROUP BY event_id, laptop_model_id HAVING COUNT(*) > 1
