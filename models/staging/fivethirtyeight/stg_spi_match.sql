SELECT
    spi.season AS season,
    spi.league_id AS league_id,
    c_home.slug AS home,
    c_away.slug AS away,
    spi.spi1 AS spi_home,
    spi.spi2 AS spi_away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_tie,
    spi.importance1 AS importance_home,
    spi.importance2 AS importance_away,
    spi.proj_score1 AS proj_score_home,
    spi.proj_score2 AS proj_score_away,
    DATE(spi.date) AS date, -- noqa: L029
    CONCAT(spi.season, '-', c_home.slug, '-', c_away.slug) AS match_slug

FROM
    {{ source ('fivethirtyeight', 'spi') }} AS spi
LEFT JOIN {{ ref ("dim_slug") }} AS c_home ON spi.team1 = c_home.name
LEFT JOIN {{ ref ("dim_slug") }} AS c_away ON spi.team2 = c_away.name
