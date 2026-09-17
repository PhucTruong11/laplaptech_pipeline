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
    CASE 
        WHEN lm.battery_capacity_whr > 0 
        THEN ROUND((lb.geekbench6_multi / lm.battery_capacity_whr)::numeric, 2) 
        ELSE NULL 
    END AS performance_per_wh,
    
    CASE 
        WHEN lm.laptop_weight_kg > 0 
        THEN ROUND((lb.geekbench6_multi / lm.laptop_weight_kg)::numeric, 2) 
        ELSE NULL 
    END AS performance_per_kg,
    
    CASE 
        WHEN lm.battery_capacity_whr > 0 
        THEN ROUND((lb.office_battery_minutes / lm.battery_capacity_whr)::numeric, 2) 
        ELSE NULL 
    END AS battery_efficiency_mins_per_wh,
    
    CASE 
        WHEN lb.geekbench6_multi > 0 AND lb.geekbench6_multi_battery IS NOT NULL
        THEN ROUND((1.0 - (lb.geekbench6_multi_battery::numeric / lb.geekbench6_multi::numeric)) * 100, 2)
        ELSE NULL 
    END AS performance_drop_pct
    
FROM {{ ref('silver_laptop_model') }} lm
JOIN {{ ref('silver_laptop_benchmark_result') }} lb 
  ON lm.laptop_model_id = lb.laptop_model_id
WHERE lm.is_active = true 
  AND lm.is_visible = true
  AND lb.geekbench6_multi IS NOT NULL
