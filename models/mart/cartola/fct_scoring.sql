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
        penalty_save,
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
    atl.matches,
    pnt.played,
    pnt.goal,
    pnt.assist,
    pnt.yellow_card,
    pnt.red_card,
    pnt.missed_shoot,
    pnt.on_post_shoot,
    pnt.saved_shoot,
    pnt.received_foul,
    pnt.received_penalty,
    pnt.missed_penalty,
    pnt.outside,
    pnt.missed_pass,
    pnt.tackle,
    pnt.foul,
    pnt.penalty,
    pnt.own_goal,
    pnt.allowed_goal,
    pnt.no_goal,
    pnt.save,
    pnt.penalty_save,
    38 * (atl.season - 2017) + atl.round AS all_time_round
FROM
    {{ ref ("stg_atletas_scoring") }} AS atl
LEFT JOIN point AS pnt ON atl.player_id = pnt.player_id
