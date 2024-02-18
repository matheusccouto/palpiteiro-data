with stg_destaques__picks as (

    select * from {{ ref("stg_destaques__picks") }}

),

picks_sum as (

    select
        season,
        round,
        sum(picks) as picks_sum

    from stg_destaques__picks

    group by season, round

),

picks_ratio as (

    select
        p.season,
        p.round,
        p.player_id,
        p.picks,
        p.picks / s.picks_sum as picks_ratio

    from stg_destaques__picks as p

    left join picks_sum as s on p.season = s.season and p.round = s.round

)

select * from picks_ratio
