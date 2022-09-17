WITH spi AS (
    SELECT
        season,
        date,
        league_id,
        league,
        home AS club,
        away AS opponent,
        spi_home AS spi_club,
        spi_away AS spi_opponent,
        prob_home AS prob_club,
        prob_away AS prob_opponent,
        prob_tie,
        proj_score_home AS proj_score_club,
        proj_score_away AS proj_score_opponent,
        COALESCE(
            importance_home,
            AVG(importance_home) OVER (PARTITION BY EXTRACT(WEEK FROM date))
        ) AS importance_club,
        COALESCE(
            importance_away,
            AVG(importance_away) OVER (PARTITION BY EXTRACT(WEEK FROM date))
        ) AS importance_opponent
    FROM
        {{ ref ("stg_spi_match") }}
),

inv AS (
    SELECT
        season,
        date,
        club AS opponent,
        opponent AS club,
        spi_club AS spi_opponent,
        spi_opponent AS spi_club,
        prob_club AS prob_opponent,
        prob_opponent AS prob_club,
        prob_tie,
        importance_club AS importance_opponent,
        importance_opponent AS importance_club,
        proj_score_club AS proj_score_opponent,
        proj_score_opponent AS proj_score_club
    FROM
        spi
)

SELECT
    season,
    date,
    club,
    opponent,
    spi_club,
    spi_opponent,
    prob_club,
    prob_opponent,
    prob_tie,
    importance_club,
    importance_opponent,
    proj_score_club,
    proj_score_opponent
FROM
    spi
UNION ALL
SELECT
    season,
    date,
    club,
    opponent,
    spi_club,
    spi_opponent,
    prob_club,
    prob_opponent,
    prob_tie,
    importance_club,
    importance_opponent,
    proj_score_club,
    proj_score_opponent
FROM
    inv
