/*
  Silver Model: silver_laptop_model
  ==================================
  Mục đích:
    - Bảng thông tin laptop đầy đủ (Enriched Product Catalog).
    - JOIN kết hợp bảng laptop_model với các bảng danh mục thương hiệu (Brand), CPU, GPU
      để tạo view tường minh, dễ đọc, phục vụ tầng Gold.
    - Áp dụng macro `clean_string` cho tên laptop.
    - Ép kiểu số liệu thông số kỹ thuật (kích thước màn hình, trọng lượng kg, dung lượng pin Whr).
*/

SELECT
    lm.id::int                                  AS laptop_model_id,
    {{ clean_string('lm.name') }}               AS laptop_name,

    -- Thông tin thương hiệu (Brand)
    lm.brand_id::int                            AS brand_id,
    b.brand_name,

    -- Thông tin vi xử lý (CPU)
    lm.cpu_model_id::int                        AS cpu_id,
    c.cpu_name,

    -- Thông tin card đồ họa (GPU)
    lm.gpu_model_id::int                        AS gpu_id,
    g.gpu_name,

    -- Thông số kỹ thuật phần cứng (Specs)
    lm.screen_size::numeric                     AS screen_size,
    lm.laptop_weight::numeric                   AS laptop_weight_kg,
    lm.battery_capacity_whr::numeric            AS battery_capacity_whr,

    -- Cờ trạng thái hiển thị & kích hoạt
    lm.is_visible::boolean                      AS is_visible,
    lm.is_active::boolean                       AS is_active

FROM {{ ref('bronze_laptop_model') }} lm
LEFT JOIN {{ ref('silver_brand') }}     b ON lm.brand_id::int = b.brand_id
LEFT JOIN {{ ref('silver_cpu_model') }} c ON lm.cpu_model_id::int = c.cpu_id
LEFT JOIN {{ ref('silver_gpu_model') }} g ON lm.gpu_model_id::int = g.gpu_id
WHERE lm.id IS NOT NULL
