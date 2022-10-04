SELECT
    fct.player_id AS id,
    fct.timestamp,
    dim.short_nickname AS name, -- noqa: L029
    dim.photo,
    clb.slug AS club,
    clb.badge60 AS club_badge,
    fct.position,
    fct.price_cartola,
    fct.price_cartola_express,
    EXP({{ target.dataset }}.PREDICT_POINTS(TO_JSON(fct))) AS points, -- noqa: L027, L016
    CURRENT_TIMESTAMP AS materialized_at
FROM
    {{ ref("fct_player") }} AS fct
LEFT JOIN {{ ref("dim_player") }} AS dim ON fct.player_id = dim.player_id
LEFT JOIN {{ ref("dim_club") }} AS clb ON fct.club = clb.slug
WHERE
    {{ target.dataset }}.ALL_TIME_ROUND(
        fct.season, fct.round
    ) = (
        SELECT
            MAX({{ target.dataset }}.ALL_TIME_ROUND(season, round))
        FROM {{ ref("fct_match") }}
    )
    AND fct.valid IS TRUE
    AND fct.status = 'expected'
