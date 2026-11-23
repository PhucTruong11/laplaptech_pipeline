{% macro event_lookback_seconds() %}
    {% set raw = env_var('INGESTION_LOOKBACK_SECONDS', '7200') | trim %}
    {% if not modules.re.fullmatch('[0-9]{1,10}', raw) or (raw | int) > 2147483647 %}
        {{ exceptions.raise_compiler_error(
            'INGESTION_LOOKBACK_SECONDS must be an integer from 0 to 2147483647'
        ) }}
    {% endif %}
    {{ return(raw | int) }}
{% endmacro %}
