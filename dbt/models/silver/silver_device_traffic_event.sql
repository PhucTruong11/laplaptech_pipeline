-- Silver: device_traffic_event — pageview events with device/OS info

SELECT
    session_id,
    server_timestamp::date                      AS event_date,
    COALESCE(user_os, 'Unknown')                AS user_os,
    COALESCE(user_device_type, 'Unknown')       AS device_type,
    COALESCE(user_browser, 'Unknown')           AS browser,
    device_id::int                              AS laptop_model_id

FROM {{ ref('silver_user_event_tracking') }}
WHERE event_name = 'pageview'
