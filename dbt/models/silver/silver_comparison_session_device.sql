-- Grain: session-day + laptop, based on attributed comparison interactions.
SELECT session_id, laptop_model_id, MIN(server_timestamp) AS first_seen_at,
    event_date, COUNT(*) AS interaction_count
FROM {{ ref('silver_comparison_event_device') }}
GROUP BY session_id, laptop_model_id, event_date
