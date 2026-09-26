{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

-- Silver: user_event_tracking — parsed JSON fields + deduplication
-- Extracts key fields from event_data and device JSON columns
-- Uses PostgreSQL JSON operators (->>, #>>)

WITH parsed AS (
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
    device,

    -- Đánh số thứ tự: nếu trùng id, giữ bản ghi mới nhất
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY event_received_on_server_timestamp DESC
    ) AS row_num

    FROM {{ ref('bronze_user_event_tracking') }}
    WHERE event_name IS NOT NULL
    AND session_id IS NOT NULL

    {% if is_incremental() %}
    -- Chỉ lấy các event mới hơn event mới nhất đã được lưu trong model này
    AND to_timestamp(event_received_on_server_timestamp) > (
        SELECT MAX(server_timestamp) FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id, event_name, session_id,
    event_timestamp, server_timestamp,
    device_id, page_name, search_keyword,
    sort_by, sort_direction,
    device_ids_raw, user_os, user_os_version, user_browser,
    user_device_type, event_data, device
FROM parsed
WHERE row_num = 1