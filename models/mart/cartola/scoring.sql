with stg_pontuados__scoring as (

    select * from {{ ref("stg_pontuados__scoring") }}

),

points as (

    select
        *,
        -- 2023 offensive pointing system
        8.0 * goal
        + 5.0 * assist
        + 3.0 * on_post_shoot
        + 1.2 * saved_shoot
        + 0.8 * missed_shoot
        + 0.5 * received_foul
        + 1.0 * received_penalty
        - 4.0 * missed_penalty
        - 0.1 * outside
        - if(position != 1, 0.1 * missed_pass, 0.0)
            as offensive,
        -- 2023 defensive pointing system
        if(position in (1, 2, 3), 5.0 * no_goal, 0.0)
        + if(position = 1, 7.0 * penalty_save, 0.0)
        + if(position = 1, 1.0 * save, 0.0)
        - if(position = 1, 1.0 * allowed_goal, 0.0)
        + 1.2 * tackle
        - 3.0 * own_goal
        - 3.0 * red_card
        - 1.0 * yellow_card
        - 0.3 * foul
        - 1.0 * penalty
            as defensive


    from
        stg_pontuados__scoring
),

fix_played as (

    select
        player_id,
        season,
        round,
        club_id,
        coalesce(
            played,
            goal
            + assist
            + yellow_card
            + red_card
            + missed_shoot
            + on_post_shoot
            + saved_shoot
            + received_foul
            + received_penalty
            + missed_penalty
            + outside
            + missed_pass
            + tackle
            + foul
            + penalty
            + own_goal
            + allowed_goal
            + no_goal
            + save
            + penalty_save
            > 0
        ) as played,
        round(offensive, 1) as offensive,
        round(defensive, 1) as defensive,
        round(offensive + defensive, 1) as total

    from
        points

),

renamed as (

    select
        player_id,
        season,
        round,
        club_id,
        played,
        if(played is TRUE, offensive, NULL) as offensive_points,
        if(played is TRUE, defensive, NULL) as defensive_points,
        if(played is TRUE, total, NULL) as total_points

    from
        fix_played

)

select * from renamed
