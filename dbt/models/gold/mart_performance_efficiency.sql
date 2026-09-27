/*
  Gold Model: mart_performance_efficiency
  ========================================
  Mục đích:
    - Bảng tổng hợp Data Mart phục vụ phân tích hiệu suất phần cứng và tính hiệu quả (Efficiency Matrix).
    - Tính toán các chỉ số phái sinh cốt lõi:
        * performance_per_wh: Điểm hiệu năng đa nhân Geekbench 6 trên mỗi Watt-giờ pin.
        * performance_per_kg: Điểm hiệu năng trên mỗi kg trọng lượng máy (đo lường độ cơ động / sức mạnh di động).
        * battery_efficiency_mins_per_wh: Số phút dùng pin văn phòng trên mỗi Wh dung lượng pin.
        * performance_drop_pct: Tỷ lệ (%) suy giảm hiệu năng CPU khi rút sạc chuyển sang dùng pin.
    - Toàn bộ các phép chia đều dùng macro `safe_divide` để bảo vệ pipeline khỏi crash khi thông số kỹ thuật bị thiếu hoặc bằng 0.
*/

SELECT
    lm.laptop_name,
    lm.brand_name,
    lm.laptop_weight_kg,
    lm.battery_capacity_whr,
    
    lb.office_battery_hours,
    lb.office_battery_minutes,
    lb.geekbench6_multi,
    lb.geekbench6_multi_battery,
    
    -- Chỉ số hiệu năng trên dung lượng pin (Performance per Wh)
    {{ safe_divide('lb.geekbench6_multi', 'lm.battery_capacity_whr') }} AS performance_per_wh,
    
    -- Chỉ số hiệu năng trên trọng lượng máy (Performance per kg)
    {{ safe_divide('lb.geekbench6_multi', 'lm.laptop_weight_kg') }} AS performance_per_kg,
    
    -- Hiệu suất sử dụng pin văn phòng (Số phút hoạt động / Wh)
    {{ safe_divide('lb.office_battery_minutes', 'lm.battery_capacity_whr') }} AS battery_efficiency_mins_per_wh,
    
    -- Mức độ sụt giảm hiệu năng đa nhân khi dùng pin so với cắm sạc (%)
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
