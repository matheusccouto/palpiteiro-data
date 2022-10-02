{% macro create_udfs() %}

{% do run_query(predict_points()) %}
{% do run_query(points_cartola()) %}
{% do run_query(all_time_round()) %}

{% endmacro %}