WITH bins AS (
    SELECT
        season,
        round,
        total_points,
        NTILE(
            2
        ) OVER (
            PARTITION BY season, round, played ORDER BY total_points ASC
        ) - 1 AS tier
    FROM
        {{ ref ("fct_scoring") }}
    WHERE
        status = 'expected'
        AND played IS TRUE
),

tiers AS (
    SELECT
        season,
        round,
        tier,
        MIN(total_points) AS lower_bound,
        MAX(total_points) AS upper_bound
    FROM
        bins
    GROUP BY
        season, round, tier
),

tiers_fixed AS (
  SELECT
    this.season,
    this.round,
    this.tier,
    IF (this.lower_bound = prev.upper_bound, this.lower_bound + 0.1, this.lower_bound) AS lower_bound,
    this.upper_bound,
  FROM
    tiers this
  LEFT JOIN
    tiers prev ON this.season = prev.season AND this.round = prev.round AND this.tier = prev.tier + 1
)

SELECT
    s.play_id,
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
    c.valid_club_last_5,
    c.valid_opponent_last_5,
    c.total_points_club_last_5,
    c.offensive_points_club_last_5,
    c.defensive_points_club_last_5,
    c.total_allowed_points_opponent_last_5,
    c.offensive_allowed_points_opponent_last_5,
    c.defensive_allowed_points_opponent_last_5,
    c.avg_odds_club,
    c.avg_odds_opponent,
    c.avg_odds_draw,
    p.drafts,
    p.drafts_norm,
    t.tier,
    ROUND(s.price - s.variation, 2) AS price_cartola_express,
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
    ) AS defensive_points_last_19,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player_id
            ORDER BY s.season, s.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_5,
    AVG(
        s.total_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_points_last_5,
    AVG(
        s.offensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_last_5,
    AVG(
        s.defensive_points
    ) OVER (
        PARTITION BY
            s.player_id, c.home
        ORDER BY s.season, s.round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_last_5
FROM
    {{ ref ("fct_scoring") }} AS s
INNER JOIN
    {{ ref ("fct_club") }} AS c ON
        s.club = c.club AND s.season = c.season AND s.round = c.round
LEFT JOIN
    {{ ref ("fct_popular") }} AS p ON
        s.play_id = p.play_id
LEFT JOIN
    tiers_fixed AS t ON
        s.season = t.season
        AND s.round = t.round
        AND s.total_points >= t.lower_bound
        AND s.total_points <= t.upper_bound
