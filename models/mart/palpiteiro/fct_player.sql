SELECT
    s.player_id,
    s.season,
    s.round,
    c.timestamp,
    c.club,
    c.valid,
    s.position,
    s.position_id,
    s.status,
    s.price AS price_cartola,
    s.played,
    s.total_points,
    s.offensive_points,
    s.defensive_points,
    c.spi_club,
    c.spi_opponent,
    c.prob_club,
    c.prob_opponent,
    c.prob_tie,
    c.importance_club,
    c.importance_opponent,
    c.proj_score_club,
    c.proj_score_opponent,
    c.valid_club_last_19,
    c.valid_opponent_last_19,
    c.total_points_club_last_19,
    c.offensive_points_club_last_19,
    c.defensive_points_club_last_19,
    c.total_allowed_points_opponent_last_19,
    c.offensive_allowed_points_opponent_last_19,
    c.defensive_allowed_points_opponent_last_19,
    c.avg_odds_club,
    c.avg_odds_opponent,
    c.avg_odds_draw,
    s.price - s.variation AS price_cartola_express,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player_id
            ORDER BY s.season, s.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_19,
    AVG(
        s.total_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS total_points_last_19,
    AVG(
        s.offensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_last_19,
    AVG(
        s.defensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_last_19
FROM
    {{ ref ("fct_scoring") }} AS s
INNER JOIN
    {{ ref ("fct_club") }} AS c ON
        s.club = c.club AND s.season = c.season AND s.round = c.round
