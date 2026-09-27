/*
  Gold Model: mart_daily_site_kpis
  =================================
  Mục đích:
    - Bảng tổng hợp Data Mart phục vụ theo dõi chỉ số hiệu suất hằng ngày (Daily Site KPIs).
    - Thống kê tổng số phiên truy cập (sessions), tổng số sự kiện (events).
    - Đo lường phễu chuyển đổi (Funnel metrics): từ Pageview -> Xem chi tiết máy -> Vào trang so sánh -> Sắp xếp so sánh.
    - Áp dụng macro `safe_divide` để tính toán tỷ lệ chuyển đổi phần trăm (%) an toàn,
      tuyệt đối không bị lỗi chia cho 0 khi mẫu số bằng 0 hoặc NULL.
*/

SELECT
    event_date,
    COUNT(DISTINCT session_id)                  AS total_sessions,
    COUNT(*)                                    AS total_events,

    -- Số lượng phiên đạt tới từng bước của phễu người dùng
    SUM(reached_pageview::int)                  AS sessions_with_pageview,
    SUM(reached_product_detail::int)            AS sessions_with_detail_view,
    SUM(reached_comparison_page::int)           AS sessions_with_comparison,
    SUM(reached_sort_in_comparison::int)        AS sessions_with_sort,

    -- Tỷ lệ chuyển đổi (%): Áp dụng macro safe_divide chống lỗi ZeroDivisionError
    {{ safe_divide('SUM(reached_product_detail::int)', 'SUM(reached_pageview::int)', decimal_places=3) }} * 100 AS detail_rate_pct,

    {{ safe_divide('SUM(reached_comparison_page::int)', 'SUM(reached_product_detail::int)', decimal_places=3) }} * 100 AS comparison_rate_pct

FROM {{ ref('silver_session_funnel') }}
GROUP BY event_date
ORDER BY event_date
