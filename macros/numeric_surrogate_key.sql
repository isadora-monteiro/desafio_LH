{% macro numeric_surrogate_key(columns) %}
    cast(
        abs(
            hash(
                {% if columns | length == 1 %}
                    coalesce(cast({{ columns[0] }} as string), '')
                {% else %}
                    concat_ws(
                        '|',
                        {% for column in columns %}
                            coalesce(cast({{ column }} as string), '')
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                {% endif %}
            )
        ) as int
    )
{% endmacro %}