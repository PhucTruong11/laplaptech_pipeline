{% macro clean_string(column_name) %}
    TRIM(REGEXP_REPLACE({{ column_name }}, '\s+', ' ', 'g'))
{% endmacro %}
