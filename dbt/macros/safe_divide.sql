{% macro safe_divide(numerator, denominator, decimal_places=2) %}
    CASE
        WHEN {{ denominator }} IS NOT NULL AND {{ denominator }} != 0
        THEN ROUND(({{ numerator }}::numeric / {{ denominator }}::numeric), {{ decimal_places }})
        ELSE NULL
    END
{% endmacro %}
