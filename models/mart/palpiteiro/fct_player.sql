SELECT
    s.player_id,
    s.all_time_round,
    c.timestamp,
    c.club,
    c.valid,
    s.position,
    s.status,
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
    c.total_points_club,
    c.offensive_points_club,
    c.defensive_points_club,
    c.total_points_opponent,
    c.offensive_points_opponent,
    c.defensive_points_opponent,
    c.total_points_club_last_5_at,
    c.offensive_points_club_last_5_at,
    c.defensive_points_club_last_5_at,
    c.total_allowed_points_opponent_last_5_at,
    c.offensive_allowed_points_opponent_last_5_at,
    c.defensive_allowed_points_opponent_last_5_at,
    c.valid_club_last_5,
    c.valid_club_last_5_at,
    c.valid_opponent_last_5,
    c.valid_opponent_last_5_at,
    c.avg_odds_club,
    c.avg_odds_opponent,
    c.avg_odds_draw,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player_id
            ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_5,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player_id, c.home
            ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_5_at,
    AVG(
        s.total_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_points_last_5_at,
    AVG(
        s.offensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_last_5_at,
    AVG(
        s.defensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_last_5_at
FROM
    {{ ref ("fct_scoring") }} AS s
INNER JOIN
    {{ ref ("fct_club") }} AS c ON
        s.club = c.club AND s.all_time_round = c.all_time_round
