{% macro predict_points() %}

CREATE OR REPLACE FUNCTION {{ target.dataset }}.PREDICT_POINTS(player_json JSON)
RETURNS FLOAT64
REMOTE WITH CONNECTION `us-east4.remote-function` OPTIONS (
    endpoint = 'https://us-east4-{{ target.project }}.cloudfunctions.net/points'
)

{% endmacro %}
