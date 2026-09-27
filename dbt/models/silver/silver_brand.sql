/*
  Silver Model: silver_brand
  ===========================
  Mục đích:
    - Làm sạch và ép kiểu dữ liệu cho danh mục thương hiệu (Brand dimension).
    - Áp dụng macro `clean_string` để chuẩn hóa khoảng trắng thừa và ký tự rác trong tên hãng.
*/

SELECT
    id::int                        AS brand_id,
    {{ clean_string('name') }}     AS brand_name
FROM {{ ref('bronze_brand') }}
WHERE id IS NOT NULL
