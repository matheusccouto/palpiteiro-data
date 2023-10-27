with source as (

    select * from {{ source('cartola', 'atletas') }}

),

renamed as (

    select
        atleta_id as player_id,
        _temporada as season,
        _rodada as round,
        clube_id as club_id,
        apelido as nickname,
        apelido_abreviado as short_nickname,
        foto as photo,
        posicao_id as position,
        status_id as status,
        preco_num as price,
        variacao_num as variation,
        entrou_em_campo as played,
        pontos_num as points

    from source

)

select * from renamed