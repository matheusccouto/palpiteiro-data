SELECT
    CONCAT(CAST(season AS STRING), " ", team1, " x ", team2) AS id,
    DATE(date) AS date,
    season AS season,
    league_id AS league_id,
    league AS league,
    c_home.slug AS home,
    c_away.slug AS away,
    spi1 AS spi_home,
    spi2 AS spi_away,
    prob1 AS prob_home,
    prob2 AS prob_away,
    probtie AS prob_tie,
    importance1 AS importance_home,
    importance2 AS importance_away,
    proj_score1 AS proj_score_home,
    proj_score2 AS proj_score_away,
    score1 AS score_home,
    score2 AS score_away,
    adj_score1 AS adj_score_home,
    adj_score2 AS adj_score_away,
    xg1 AS xg_home,
    xg2 AS xg_away,
    nsxg1 AS nsxg_home,
    nsxg2 AS nsxg_away
FROM
    {{ source ('fivethirtyeight', 'spi') }}
LEFT JOIN {{ ref ("dim_slug") }} AS c_home ON team1 = c_home.name
LEFT JOIN {{ ref ("dim_slug") }} AS c_away ON team2 = c_away.name
