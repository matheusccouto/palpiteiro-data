with source as (

    select * from {{ source("cartola", "destaques") }}

),

renamed as (

    select
        _temporada as season,
        _rodada as round,
        atleta.atleta_id as player_id,
        escalacoes as picks

    from source

)

select * from renamed
