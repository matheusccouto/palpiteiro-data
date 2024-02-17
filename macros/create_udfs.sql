{% macro create_udfs() %}

{% do run_query(predict_points()) %}

{% endmacro %}