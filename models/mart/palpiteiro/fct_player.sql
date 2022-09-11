SELECT
    s.id,
    s.player,
    s.all_time_round,
    c.timestamp,
    c.club,
    c.valid,
    s.position,
    s.status,
    s.price AS price_cartola,
    s.played,
    s.total_points,
    s.offensive_points,
    s.defensive_points,
    s.goal,
    s.assist,
    s.yellow_card,
    s.red_card,
    s.missed_shoot,
    s.on_post_shoot,
    s.saved_shoot,
    s.received_foul,
    s.received_penalty,
    s.missed_penalty,
    s.outside,
    s.missed_pass,
    s.tackle,
    s.foul,
    s.penalty,
    s.own_goal,
    s.allowed_goal,
    s.no_goal,
    s.save,
    s.penalty_save,
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
    c.total_points_club_last_19_at,
    c.offensive_points_club_last_19_at,
    c.defensive_points_club_last_19_at,
    c.total_allowed_points_opponent_last_19_at,
    c.offensive_allowed_points_opponent_last_19_at,
    c.defensive_allowed_points_opponent_last_19_at,
    c.valid_club_last_5,
    c.valid_club_last_5_at,
    c.valid_opponent_last_5,
    c.valid_opponent_last_5_at,
    c.pinnacle_odds_club,
    c.pinnacle_odds_opponent,
    c.pinnacle_odds_draw,
    c.max_odds_club,
    c.max_odds_opponent,
    c.max_odds_draw,
    c.avg_odds_club,
    c.avg_odds_opponent,
    c.avg_odds_draw,
    c.goals_club_last_5_at,
    c.assists_club_last_5_at,
    c.yellow_cards_club_last_5_at,
    c.red_cards_club_last_5_at,
    c.missed_shoots_club_last_5_at,
    c.on_post_shoots_club_last_5_at,
    c.saved_shoots_club_last_5_at,
    c.received_fouls_club_last_5_at,
    c.received_penalties_club_last_5_at,
    c.missed_penalties_club_last_5_at,
    c.outsides_club_last_5_at,
    c.missed_passes_club_last_5_at,
    c.tackles_club_last_5_at,
    c.fouls_club_last_5_at,
    c.penalties_club_last_5_at,
    c.own_goals_club_last_5_at,
    c.allowed_goals_club_last_5_at,
    c.no_goals_club_last_5_at,
    c.saves_club_last_5_at,
    c.penalty_saves_club_last_5_at,
    c.goals_opponent_last_5_at,
    c.assists_opponent_last_5_at,
    c.yellow_cards_opponent_last_5_at,
    c.red_cards_opponent_last_5_at,
    c.missed_shoots_opponent_last_5_at,
    c.on_post_shoots_opponent_last_5_at,
    c.saved_shoots_opponent_last_5_at,
    c.received_fouls_opponent_last_5_at,
    c.received_penalties_opponent_last_5_at,
    c.missed_penalties_opponent_last_5_at,
    c.outsides_opponent_last_5_at,
    c.missed_passes_opponent_last_5_at,
    c.tackles_opponent_last_5_at,
    c.fouls_opponent_last_5_at,
    c.penalties_opponent_last_5_at,
    c.own_goals_opponent_last_5_at,
    c.allowed_goals_opponent_last_5_at,
    c.no_goals_opponent_last_5_at,
    c.saves_opponent_last_5_at,
    c.penalty_saves_opponent_last_5_at,
    c.goals_club_last_19_at,
    c.assists_club_last_19_at,
    c.yellow_cards_club_last_19_at,
    c.red_cards_club_last_19_at,
    c.missed_shoots_club_last_19_at,
    c.on_post_shoots_club_last_19_at,
    c.saved_shoots_club_last_19_at,
    c.received_fouls_club_last_19_at,
    c.received_penalties_club_last_19_at,
    c.missed_penalties_club_last_19_at,
    c.outsides_club_last_19_at,
    c.missed_passes_club_last_19_at,
    c.tackles_club_last_19_at,
    c.fouls_club_last_19_at,
    c.penalties_club_last_19_at,
    c.own_goals_club_last_19_at,
    c.allowed_goals_club_last_19_at,
    c.no_goals_club_last_19_at,
    c.saves_club_last_19_at,
    c.penalty_saves_club_last_19_at,
    c.goals_opponent_last_19_at,
    c.assists_opponent_last_19_at,
    c.yellow_cards_opponent_last_19_at,
    c.red_cards_opponent_last_19_at,
    c.missed_shoots_opponent_last_19_at,
    c.on_post_shoots_opponent_last_19_at,
    c.saved_shoots_opponent_last_19_at,
    c.received_fouls_opponent_last_19_at,
    c.received_penalties_opponent_last_19_at,
    c.missed_penalties_opponent_last_19_at,
    c.outsides_opponent_last_19_at,
    c.missed_passes_opponent_last_19_at,
    c.tackles_opponent_last_19_at,
    c.fouls_opponent_last_19_at,
    c.penalties_opponent_last_19_at,
    c.own_goals_opponent_last_19_at,
    c.allowed_goals_opponent_last_19_at,
    c.no_goals_opponent_last_19_at,
    c.saves_opponent_last_19_at,
    c.penalty_saves_opponent_last_19_at,
    s.price - s.variation AS price_cartola_express,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player
            ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_5,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player, c.home
            ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_5_at,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player
            ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_19,
    COALESCE(
        SUM(
            CAST(s.played AS INT64)
        ) OVER (
            PARTITION BY
                s.player, c.home
            ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ),
        0
    ) AS played_last_19_at,
    AVG(
        s.total_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_points_last_5_at,
    AVG(
        s.offensive_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_last_5_at,
    AVG(
        s.defensive_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_last_5_at,
    s.total_points / NULLIF(c.total_points_club, 0) AS total_points_repr,
    s.offensive_points / NULLIF(
        c.offensive_points_club, 0
    ) AS offensive_points_repr,
    s.defensive_points / NULLIF(
        c.defensive_points_club, 0
    ) AS defensive_points_repr,
    AVG(
        s.total_points / NULLIF(c.total_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS total_points_repr_last_5_at,
    AVG(
        s.offensive_points / NULLIF(c.offensive_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_repr_last_5_at,
    AVG(
        s.defensive_points / NULLIF(c.defensive_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_repr_last_5_at,
    AVG(
        s.total_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS total_points_last_19_at,
    AVG(
        s.offensive_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_last_19_at,
    AVG(
        s.defensive_points
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_last_19_at,
    AVG(
        s.total_points / NULLIF(c.total_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS total_points_repr_last_19_at,
    AVG(
        s.offensive_points / NULLIF(c.offensive_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS offensive_points_repr_last_19_at,
    AVG(
        s.defensive_points / NULLIF(c.defensive_points_club, 0)
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS defensive_points_repr_last_19_at,
    AVG(
        s.goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS goals_last_5_at,
    AVG(
        s.assist
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS assists_last_5_at,
    AVG(
        s.yellow_card
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS yellow_cards_last_5_at,
    AVG(
        s.red_card
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS red_cards_last_5_at,
    AVG(
        s.missed_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS missed_shoots_last_5_at,
    AVG(
        s.on_post_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS on_post_shoots_last_5_at,
    AVG(
        s.saved_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS saved_shoots_last_5_at,
    AVG(
        s.received_foul
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS received_fouls_last_5_at,
    AVG(
        s.received_penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS received_penalties_last_5_at,
    AVG(
        s.missed_penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS missed_penalties_last_5_at,
    AVG(
        s.outside
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS outsides_last_5_at,
    AVG(
        s.missed_pass
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS missed_passes_last_5_at,
    AVG(
        s.tackle
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS tackles_last_5_at,
    AVG(
        s.foul
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS fouls_last_5_at,
    AVG(
        s.penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS penalties_last_5_at,
    AVG(
        s.own_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS own_goals_last_5_at,
    AVG(
        s.allowed_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS allowed_goals_last_5_at,
    AVG(
        s.no_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS no_goals_last_5_at,
    AVG(
        s.save
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS saves_last_5_at,
    AVG(
        s.penalty_save
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS penalty_saves_last_5_at,
    AVG(
        s.goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS goals_last_19_at,
    AVG(
        s.assist
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS assists_last_19_at,
    AVG(
        s.yellow_card
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS yellow_cards_last_19_at,
    AVG(
        s.red_card
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS red_cards_last_19_at,
    AVG(
        s.missed_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS missed_shoots_last_19_at,
    AVG(
        s.on_post_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS on_post_shoots_last_19_at,
    AVG(
        s.saved_shoot
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS saved_shoots_last_19_at,
    AVG(
        s.received_foul
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS received_fouls_last_19_at,
    AVG(
        s.received_penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS received_penalties_last_19_at,
    AVG(
        s.missed_penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS missed_penalties_last_19_at,
    AVG(
        s.outside
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS outsides_last_19_at,
    AVG(
        s.missed_pass
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS missed_passes_last_19_at,
    AVG(
        s.tackle
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS tackles_last_19_at,
    AVG(
        s.foul
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS fouls_last_19_at,
    AVG(
        s.penalty
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS penalties_last_19_at,
    AVG(
        s.own_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS own_goals_last_19_at,
    AVG(
        s.allowed_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS allowed_goals_last_19_at,
    AVG(
        s.no_goal
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS no_goals_last_19_at,
    AVG(
        s.save
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS saves_last_19_at,
    AVG(
        s.penalty_save
    ) OVER (
        PARTITION BY
            s.player, c.home
        ORDER BY s.all_time_round ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) AS penalty_saves_last_19_at
FROM
    {{ ref ("fct_scoring") }} AS s
INNER JOIN
    {{ ref ("fct_club") }} AS c ON
        s.club = c.club AND s.all_time_round = c.all_time_round
