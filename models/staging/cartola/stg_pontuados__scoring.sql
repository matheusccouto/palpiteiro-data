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
        coalesce(scout.g, 0) AS goal,
        coalesce(scout.a, 0) AS assist,
        coalesce(scout.ca, 0) AS yellow_card,
        coalesce(scout.cv, 0) AS red_card,
        coalesce(scout.ff, 0) AS missed_shoot,
        coalesce(scout.ft, 0) AS on_post_shoot,
        coalesce(scout.fd, 0) AS saved_shoot,
        coalesce(scout.fs, 0) AS received_foul,
        coalesce(scout.ps, 0) AS received_penalty,
        coalesce(scout.pp, 0) AS missed_penalty,
        coalesce(scout.i, 0) AS outside,
        coalesce(scout.pe, scout.pi, 0) AS missed_pass,
        coalesce(scout.rb, scout.ds, 0) AS tackle,
        coalesce(scout.fc, 0) AS foul,
        coalesce(scout.pc, 0) AS penalty,
        coalesce(scout.gc, 0) AS own_goal,
        coalesce(scout.gs, 0) AS allowed_goal,
        coalesce(scout.sg, 0) AS no_goal,
        -- Difficult Save was replaced by Save. I calculated that: 
        -- On average for each difficult save, there are 2.218 save.
        coalesce(scout.de, cast(round(scout.dd * 2.218, 0) AS int), 0) AS save,
        coalesce(scout.dp, 0) AS penalty_save

    from source

)

select * from renamed