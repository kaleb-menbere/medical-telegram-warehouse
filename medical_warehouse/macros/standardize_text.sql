{% macro standardize_text(column_name) %}
    -- Standardize text: trim, remove extra spaces, handle nulls
    COALESCE(
        REGEXP_REPLACE(
            TRIM({{ column_name }}),
            '\s+', 
            ' ', 
            'g'
        ),
        ''
    )
{% endmacro %}