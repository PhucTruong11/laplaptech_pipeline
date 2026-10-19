WITH expected AS (
    SELECT id, event_name, session_id, event_received_on_server_timestamp,
        event_data, device
    FROM {{ source('raw', 'user_event_tracking') }}
    WHERE event_name IS NOT NULL AND session_id IS NOT NULL
), actual AS (
    SELECT * FROM {{ ref('silver_user_event_tracking') }}
)
SELECT COALESCE(e.id, a.id) AS id
FROM expected e FULL OUTER JOIN actual a USING (id)
WHERE e.id IS NULL OR a.id IS NULL
   OR to_timestamp(e.event_received_on_server_timestamp) IS DISTINCT FROM a.server_timestamp
   OR e.event_name IS DISTINCT FROM a.event_name
   OR e.session_id IS DISTINCT FROM a.session_id
   OR e.event_data::text IS DISTINCT FROM a.event_data::text
   OR e.device::text IS DISTINCT FROM a.device::text
