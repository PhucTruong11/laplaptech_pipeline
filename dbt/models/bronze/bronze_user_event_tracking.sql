-- Bronze: user_event_tracking — raw pass-through
SELECT * FROM {{ source('raw', 'user_event_tracking') }}
