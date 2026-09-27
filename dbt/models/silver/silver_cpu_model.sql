/*
  Silver Model: silver_cpu_model
  ===============================
  Mục đích:
    - Làm sạch và ép kiểu dữ liệu cho danh mục CPU (CPU model dimension).
    - Áp dụng macro `clean_string` để loại bỏ khoảng trắng thừa trong tên dòng CPU.
*/

SELECT
    id::int                        AS cpu_id,
    {{ clean_string('name') }}     AS cpu_name
FROM {{ ref('bronze_cpu_model') }}
WHERE id IS NOT NULL
