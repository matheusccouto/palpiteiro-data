with source as (

    select * from {{ source('cartola', 'clubes') }}

),

renamed as (

    select
        id as club_id,
        _temporada as season,
        _rodada as round,
        nome as name,  --noqa
        abreviacao as abbreviation,
        "escudos.30x30" as badge_30,
        "escudos.45x45" as badge_45,
        "escudos.60x60" as badge_60

    from source

)

select * from renamed
