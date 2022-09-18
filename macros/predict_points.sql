{% macro predict_points() %}

CREATE OR REPLACE FUNCTION {{ target.dataset }}.PREDICT_POINTS(player_json JSON) RETURNS FLOAT64
AS (3.33);

{% endmacro %}
