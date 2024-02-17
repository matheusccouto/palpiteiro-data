with source as (

    select * from {{ source('cartola', 'pontuados') }}

),

renamed as (

    select
        atleta_id as player_id,
        _temporada as season,
        _rodada as round,
        posicao_id as position,
        entrou_em_campo as played,
        pontuacao as points,
        coalesce(scout.g, 0) as goal,
        coalesce(scout.a, 0) as assist,
        coalesce(scout.ca, 0) as yellow_card,
        coalesce(scout.cv, 0) as red_card,
        coalesce(scout.ff, 0) as missed_shoot,
        coalesce(scout.ft, 0) as on_post_shoot,
        coalesce(scout.fd, 0) as saved_shoot,
        coalesce(scout.fs, 0) as received_foul,
        coalesce(scout.ps, 0) as received_penalty,
        coalesce(scout.pp, 0) as missed_penalty,
        coalesce(scout.i, 0) as outside,
        coalesce(scout.pe, scout.pi, 0) as missed_pass,
        coalesce(scout.rb, scout.ds, 0) as tackle,
        coalesce(scout.fc, 0) as foul,
        coalesce(scout.pc, 0) as penalty,
        coalesce(scout.gc, 0) as own_goal,
        coalesce(scout.gs, 0) as allowed_goal,
        coalesce(scout.sg, 0) as no_goal,
        -- Difficult Save was replaced by Save. I calculated that: 
        -- On average for each difficult save, there are 2.218 save.
        coalesce(scout.de, cast(round(scout.dd * 2.218, 0) as int), 0) as save,
        coalesce(scout.dp, 0) as penalty_save

    from source

)

select * from renamed
