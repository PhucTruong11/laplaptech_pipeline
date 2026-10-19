{% macro business_date(column) -%}
    ({{ column }} AT TIME ZONE '{{ var("business_timezone", "Asia/Ho_Chi_Minh") }}')::date
{%- endmacro %}

{% macro business_month(column) -%}
    DATE_TRUNC('month', {{ column }} AT TIME ZONE '{{ var("business_timezone", "Asia/Ho_Chi_Minh") }}')::date
{%- endmacro %}
