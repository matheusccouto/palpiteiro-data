{% macro all_time_round() %}

CREATE OR REPLACE FUNCTION {{ target.dataset }}.ALL_TIME_ROUND(
    season INT,
    round INT
) RETURNS INT AS (
    38 * (season - 2017) + round
);

{% endmacro %}