-- Cặp brand không có hướng; co-occurrence trong cùng session-day,
-- không khẳng định cùng một comparison action.
WITH pairs AS (
    SELECT
        a.session_id, a.event_date,
        LEAST(la.brand_id, lb.brand_id) AS brand_a_id,
        GREATEST(la.brand_id, lb.brand_id) AS brand_b_id
    FROM {{ ref('silver_comparison_session_device') }} a
    JOIN {{ ref('silver_comparison_session_device') }} b
      ON a.session_id = b.session_id AND a.event_date = b.event_date
     AND a.laptop_model_id < b.laptop_model_id
    JOIN {{ ref('silver_laptop_model') }} la ON a.laptop_model_id = la.laptop_model_id
    JOIN {{ ref('silver_laptop_model') }} lb ON b.laptop_model_id = lb.laptop_model_id
)
SELECT p.brand_a_id, p.brand_b_id, a.brand_name AS brand_a, b.brand_name AS brand_b,
    COUNT(DISTINCT p.session_id) AS comparison_sessions,
    COUNT(DISTINCT (p.session_id, p.event_date)) AS comparison_session_days,
    COUNT(*) AS comparison_count,
    CASE WHEN p.brand_a_id = p.brand_b_id THEN 'Same Brand' ELSE 'Cross Brand' END AS comparison_type
FROM pairs p
JOIN {{ ref('silver_brand') }} a ON p.brand_a_id = a.brand_id
JOIN {{ ref('silver_brand') }} b ON p.brand_b_id = b.brand_id
GROUP BY p.brand_a_id, p.brand_b_id, a.brand_name, b.brand_name
