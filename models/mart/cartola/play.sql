with play as (

    select * from {{ ref("stg_atletas__play") }}

),

scoring as (

    select * from {{ ref("stg_pontuados__scoring") }}

)

select
    p.player_id,
    p.club_id,
    p.season,
    p.round,
    p.position,
    p.status,
    p.price,
    p.variation

from play as p
