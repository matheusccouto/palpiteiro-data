WITH scoring AS (
    SELECT
        *,
        {{ target.dataset }}.OFFENSIVE_POINTS_CARTOLA(
            position,
            goal,
            assist,
            yellow_card,
            red_card,
            missed_shoot,
            on_post_shoot,
            saved_shoot,
            received_foul,
            received_penalty,
            missed_penalty,
            outside,
            missed_pass,
            tackle,
            foul,
            penalty,
            own_goal,
            allowed_goal,
            no_goal,
            save,
            penalty_save
        ) AS offensive,
        {{ target.dataset }}.DEFENSIVE_POINTS_CARTOLA(
            position,
            goal,
            assist,
            yellow_card,
            red_card,
            missed_shoot,
            on_post_shoot,
            saved_shoot,
            received_foul,
            received_penalty,
            missed_penalty,
            outside,
            missed_pass,
            tackle,
            foul,
            penalty,
            own_goal,
            allowed_goal,
            no_goal,
            save,
            penalty_save
        ) AS defensive
    FROM
        {{ ref("stg_pontuados_scoring") }}
),

point AS (
    SELECT
        play_id,
        player_id,
        round,
        season,
        played,
        IF(played IS TRUE, ROUND(offensive, 1), NULL) AS offensive,
        IF(played IS TRUE, ROUND(defensive, 1), NULL) AS defensive,
        IF(played IS TRUE, ROUND(offensive + defensive, 1), NULL) AS total
    FROM
        scoring
)

SELECT
    atl.play_id,
    atl.player_id,
    atl.season,
    atl.round,
    atl.club,
    atl.position,
    atl.position_id,
    atl.status,
    pnt.total AS total_points,
    pnt.offensive AS offensive_points,
    pnt.defensive AS defensive_points,
    atl.price,
    atl.variation,
    CASE
        WHEN atl.season = 2022 THEN COALESCE(pnt.played, FALSE)
        ELSE COALESCE(pnt.total IS NOT NULL, FALSE)
    END AS played
FROM
    {{ ref ("stg_atletas_scoring") }} AS atl
LEFT JOIN
    point AS pnt ON atl.play_id = pnt.play_id
