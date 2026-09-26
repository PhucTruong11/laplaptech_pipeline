-- Gold: mart_performance_efficiency
-- Calculates derived efficiency metrics: Performance per Wh, Performance per kg, Battery efficiency

SELECT
    lm.laptop_name,
    lm.brand_name,
    lm.laptop_weight_kg,
    lm.battery_capacity_whr,
    
    lb.office_battery_hours,
    lb.office_battery_minutes,
    lb.geekbench6_multi,
    lb.geekbench6_multi_battery,
    
    -- Derived Metrics
    {{ safe_divide('lb.geekbench6_multi', 'lm.battery_capacity_whr') }} AS performance_per_wh,
    
    {{ safe_divide('lb.geekbench6_multi', 'lm.laptop_weight_kg') }} AS performance_per_kg,
    
    {{ safe_divide('lb.office_battery_minutes', 'lm.battery_capacity_whr') }} AS battery_efficiency_mins_per_wh,
    
    CASE 
        WHEN lb.geekbench6_multi > 0 AND lb.geekbench6_multi_battery IS NOT NULL
        THEN ROUND((1.0 - ({{ safe_divide('lb.geekbench6_multi_battery', 'lb.geekbench6_multi', decimal_places=4) }})) * 100, 2)
        ELSE NULL 
    END AS performance_drop_pct
    
FROM {{ ref('silver_laptop_model') }} lm
JOIN {{ ref('silver_laptop_benchmark_result') }} lb 
  ON lm.laptop_model_id = lb.laptop_model_id
WHERE lm.is_active = true 
  AND lm.is_visible = true
  AND lb.geekbench6_multi IS NOT NULL
