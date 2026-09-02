-- Gold: mart_brand_comparison
-- Which brand pairs are most compared against each other
-- Hướng 1: Brand Analysis

WITH comparison_pairs AS (
    SELECT
        a.session_id,
        a.laptop_model_id    AS laptop_a,
        b.laptop_model_id    AS laptop_b
    FROM {{ ref('silver_comparison_session_device') }} a
    INNER JOIN {{ ref('silver_comparison_session_device') }} b
        ON a.session_id = b.session_id
       AND a.laptop_model_id < b.laptop_model_id
)

SELECT
    lm_a.brand_name                             AS brand_a,
    lm_b.brand_name                             AS brand_b,
    COUNT(DISTINCT cp.session_id)               AS comparison_sessions,
    COUNT(*)                                    AS comparison_count,

    -- Is it same-brand or cross-brand comparison?
    CASE
        WHEN lm_a.brand_id = lm_b.brand_id THEN 'Same Brand'
        ELSE 'Cross Brand'
    END                                         AS comparison_type

FROM comparison_pairs cp
INNER JOIN {{ ref('silver_laptop_model') }} lm_a ON cp.laptop_a = lm_a.laptop_model_id
INNER JOIN {{ ref('silver_laptop_model') }} lm_b ON cp.laptop_b = lm_b.laptop_model_id
GROUP BY lm_a.brand_name, lm_b.brand_name, lm_a.brand_id, lm_b.brand_id
ORDER BY comparison_count DESC
