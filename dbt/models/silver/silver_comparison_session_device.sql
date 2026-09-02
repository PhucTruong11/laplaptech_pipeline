-- Silver: comparison_session_device — devices seen in comparison sessions

SELECT
    session_id,
    device_id::int                              AS laptop_model_id,
    MIN(server_timestamp)                       AS first_seen_at,
    MIN(server_timestamp)::date                 AS event_date,
    COUNT(*)                                    AS interaction_count

FROM {{ ref('silver_user_event_tracking') }}
WHERE page_name = 'CompareDevice'
  AND device_id IS NOT NULL
  AND device_id != ''
GROUP BY session_id, device_id
