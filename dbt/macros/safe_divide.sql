{#
  Macro: safe_divide
  -------------------
  Mục đích:
    Thực hiện phép chia an toàn trong SQL, chống lỗi nghiêm trọng chia cho 0 (Division by zero)
    hoặc mẫu số bị NULL.
  Tham số:
    - numerator: Tử số (chuỗi biểu thức hoặc tên cột)
    - denominator: Mẫu số (chuỗi biểu thức hoặc tên cột)
    - decimal_places: Số chữ số phần thập phân muốn làm tròn (mặc định = 2)
  Logic:
    Nếu mẫu số khác NULL và khác 0 thì ép kiểu ::numeric rồi chia và ROUND.
    Ngược lại trả về NULL để không làm sập câu truy vấn SQL hay pipeline.
  Ví dụ sử dụng:
    {{ safe_divide('SUM(converted_sessions)', 'SUM(total_sessions)', decimal_places=4) }}
#}
{% macro safe_divide(numerator, denominator, decimal_places=2) %}
    CASE
        WHEN {{ denominator }} IS NOT NULL AND {{ denominator }} != 0
        THEN ROUND(({{ numerator }}::numeric / {{ denominator }}::numeric), {{ decimal_places }})
        ELSE NULL
    END
{% endmacro %}
