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
        player_id,
        round,
        season,
        played,
        if(played IS TRUE, round(offensive, 1), NULL) AS offensive,
        if(played IS TRUE, round(defensive, 1), NULL) AS defensive,
        if(played IS TRUE, round(offensive + defensive, 1), NULL) AS total
    FROM
        scoring
)

SELECT
    atl.player_id,
    atl.club,
    atl.position,
    atl.status,
    pnt.total AS total_points,
    pnt.offensive AS offensive_points,
    pnt.defensive AS defensive_points,
    atl.price,
    atl.variation,
    pnt.played,
    38 * (atl.season - 2017) + atl.round AS all_time_round
FROM
    {{ ref ("stg_atletas_scoring") }} AS atl
LEFT JOIN point AS pnt ON atl.player_id = pnt.player_id
