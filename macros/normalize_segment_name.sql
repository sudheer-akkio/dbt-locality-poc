{% macro normalize_segment_name(column_name) %}
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower({{ column_name }}),
                    ' ', '-'
                ),
                '&', 'and'
            ),
            '\\s*\\/\\s*', '/'
        ),
        '\\s*-\\s*', '-'
    )
{% endmacro %}
