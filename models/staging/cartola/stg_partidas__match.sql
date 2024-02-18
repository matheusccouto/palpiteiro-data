with source as (

    select * from {{ source("cartola", "partidas") }}

),

renamed as (

    select
        _temporada as season,
        _rodada as round,
        partida_id as match_id,
        partida_data as date,
        valida as valid,
        clube_casa_id as home_id,
        clube_visitante_id as away_id

    from source

)

select * from renamed
