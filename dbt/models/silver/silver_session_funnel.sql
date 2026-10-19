-- Daily step reach, cùng grain session-day với daily KPI; không yêu cầu thứ tự.
SELECT
    session_id, event_date,
    has_pageview AS reached_pageview,
    has_detail AS reached_product_detail,
    has_select_compare AS reached_select_for_comparison,
    has_comparison AS reached_comparison_page,
    has_sort AS reached_sort_in_comparison
FROM {{ ref('silver_session_day') }}
