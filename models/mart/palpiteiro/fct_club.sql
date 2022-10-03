WITH club AS (
    SELECT
        s.club,
        s.season,
        s.round,
        ANY_VALUE(m.opponent) AS opponent,
        ANY_VALUE(m.home) AS home,
        ANY_VALUE(m.timestamp) AS timestamp, -- noqa: L029
        COALESCE(ANY_VALUE(m.valid), SUM(s.total_points) IS NOT NULL) AS valid,
        SUM(s.total_points) AS total_points,
        SUM(s.offensive_points) AS offensive_points,
        SUM(s.defensive_points) AS defensive_points
    FROM
        {{ ref("fct_scoring") }} AS s
    INNER JOIN
        {{ ref ("fct_match") }} AS m ON
            s.club = m.club AND s.season = m.season AND s.round = m.round
    GROUP BY
        s.club, s.season, s.round
)

SELECT
    c.club,
    c.season,
    c.round,
    c.timestamp,
    c.home,
    c.opponent,
    c.valid,
    s.spi_club,
    s.spi_opponent,
    s.prob_club,
    s.prob_opponent,
    s.prob_tie,
    s.importance_club,
    s.importance_opponent,
    s.proj_score_club,
    s.proj_score_opponent,
    c.total_points AS total_points_club,
    c.offensive_points AS offensive_points_club,
    c.defensive_points AS defensive_points_club,
    o.total_points AS total_points_opponent,
    o.offensive_points AS offensive_points_opponent,
    o.defensive_points AS defensive_points_opponent,
    h2h.avg_club AS avg_odds_club,
    h2h.avg_opponent AS avg_odds_opponent,
    h2h.avg_draw AS avg_odds_draw,
    COALESCE(
        SUM(
            CAST(c.valid AS INT64)
        ) OVER (
            PARTITION BY
                c.club, c.home
            ORDER BY c.season, c.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS valid_club_last_19,
    COALESCE(
        SUM(
            CAST(o.valid AS INT64)
        ) OVER (
            PARTITION BY
                o.club, o.home
            ORDER BY o.season, o.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS valid_opponent_last_19,
    AVG(
        c.total_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS total_points_club_last_19,
    AVG(
        c.offensive_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_club_last_19,
    AVG(
        c.defensive_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_club_last_19,
    AVG(
        o.total_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS total_allowed_points_opponent_last_19,
    AVG(
        o.offensive_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS offensive_allowed_points_opponent_last_19,
    AVG(
        o.defensive_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS defensive_allowed_points_opponent_last_19,
    COALESCE(
        SUM(
            CAST(c.valid AS INT64)
        ) OVER (
            PARTITION BY
                c.club, c.home
            ORDER BY c.season, c.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS valid_club_last_5,
    COALESCE(
        SUM(
            CAST(o.valid AS INT64)
        ) OVER (
            PARTITION BY
                o.club, o.home
            ORDER BY o.season, o.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS valid_opponent_last_5,
    AVG(
        c.total_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_points_club_last_5,
    AVG(
        c.offensive_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_club_last_5,
    AVG(
        c.defensive_points
    ) OVER (
        PARTITION BY
            c.club, c.home
        ORDER BY c.season, c.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_club_last_5,
    AVG(
        o.total_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_allowed_points_opponent_last_5,
    AVG(
        o.offensive_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_allowed_points_opponent_last_5,
    AVG(
        o.defensive_points
    ) OVER (
        PARTITION BY
            o.club, o.home
        ORDER BY o.season, o.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_allowed_points_opponent_last_5
FROM
    club AS c
INNER JOIN
    club AS o ON
        c.opponent = o.club AND c.season = o.season AND c.round = o.round
INNER JOIN
    {{ ref ("fct_spi") }} AS s ON
        EXTRACT(
            DATE FROM c.timestamp AT TIME ZONE 'America/Sao_Paulo'
        ) = s.date AND c.club = s.club
INNER JOIN
    {{ ref ("fct_h2h") }} AS h2h ON
        EXTRACT(
            DATE FROM h2h.timestamp AT TIME ZONE 'America/Sao_Paulo'
        ) = s.date AND c.club = h2h.club
