{#
  Macro: clean_string
  --------------------
  Mục đích:
    Chuẩn hóa các trường văn bản dạng chuỗi:
    1. REGEXP_REPLACE(..., '\s+', ' ', 'g'): Gộp nhiều khoảng trắng liền kề, dấu tab, xuống dòng thành đúng 1 dấu cách đơn.
    2. TRIM(...): Cắt bỏ khoảng trắng thừa ở 2 đầu chuỗi.
  Tham số:
    - column_name: Tên cột hoặc biểu thức chuỗi cần chuẩn hóa.
  Ví dụ sử dụng:
    {{ clean_string('name') }} AS brand_name
#}
{% macro clean_string(column_name) %}
    TRIM(REGEXP_REPLACE({{ column_name }}, '\s+', ' ', 'g'))
{% endmacro %}
