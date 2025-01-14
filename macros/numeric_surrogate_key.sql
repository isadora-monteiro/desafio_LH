{% macro numeric_surrogate_key(columns) %}
    cast(
        abs(
            hash(
                {% for column in columns %}
                    coalesce(cast({{ column }} as string), ''){% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        ) as int
    )
{% endmacro %}
