-- Silver: comparison_sort_event — sort actions during comparisons

SELECT
    session_id,
    sort_by,
    sort_direction,
    server_timestamp,
    server_timestamp::date                      AS event_date

FROM {{ ref('silver_user_event_tracking') }}
WHERE page_name = 'CompareDevice'
  AND sort_by IS NOT NULL
  AND sort_by != ''
