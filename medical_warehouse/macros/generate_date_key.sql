{% macro generate_date_key(date_column) %}
    -- Generate date key in YYYYMMDD format
    TO_CHAR({{ date_column }}, 'YYYYMMDD')::integer
{% endmacro %}