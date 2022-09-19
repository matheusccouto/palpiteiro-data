SELECT
    fct.player_id,
    fct.timestamp,
    dim.short_nickname AS name, -- noqa: L029
    dim.photo,
    clb.slug AS club,
    clb.badge60 AS club_badge,
    fct.position,
    dim.price_cartola,
    dim.price_cartola_express,
    {{ target.dataset }}.PREDICT_POINTS(TO_JSON(fct)) AS points, -- noqa: L027
    CURRENT_TIMESTAMP AS materialized_at
FROM
    {{ ref("fct_player") }} AS fct
LEFT JOIN {{ ref("dim_player") }} AS dim ON fct.player_id = dim.player_id
LEFT JOIN {{ ref("dim_club") }} AS clb ON fct.club = clb.slug
WHERE
    fct.season = (SELECT MAX(season) FROM {{ ref("fct_match") }})
    AND fct.round = (SELECT MAX(round) FROM {{ ref("fct_match") }})
    AND fct.valid IS TRUE
    AND fct.status = 'expected'
