-- Silver: user_event_tracking — parsed JSON fields
-- Extracts key fields from event_data and device JSON columns
-- Uses PostgreSQL JSON operators (->>, #>>)

SELECT
    id,
    event_name,
    session_id,

    -- Timestamps
    to_timestamp(event_local_timestamp)         AS event_timestamp,
    to_timestamp(event_received_on_server_timestamp) AS server_timestamp,

    -- Parse event_data JSON
    CASE 
        WHEN (event_data::json ->> 'device_id') ~ '^[0-9]+$' 
        THEN event_data::json ->> 'device_id'
        ELSE NULL 
    END                                         AS device_id,
    event_data::json ->> 'page_name'            AS page_name,
    event_data::json ->> 'keyword'              AS search_keyword,
    event_data::json ->> 'sort_by'              AS sort_by,
    event_data::json ->> 'sort_direction'        AS sort_direction,
    event_data::json ->> 'device_ids'           AS device_ids_raw,

    -- Parse device JSON (client metadata)
    device::json ->> 'os'                       AS user_os,
    device::json ->> 'os_version'               AS user_os_version,
    device::json ->> 'browser'                  AS user_browser,
    device::json ->> 'device_type'              AS user_device_type,

    -- Keep raw JSON for ad-hoc queries
    event_data,
    device

FROM {{ ref('bronze_user_event_tracking') }}
WHERE event_name IS NOT NULL
  AND session_id IS NOT NULL
