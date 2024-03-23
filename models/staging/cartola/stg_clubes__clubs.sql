with source as (

    select * from {{ source('cartola', 'clubes') }}

),

renamed as (

    select
        id as club_id,
        _temporada as season,
        _rodada as round,
        nome as name,
        abreviacao as abbreviation,
        escudos.30x30 as badge_30,
        escudos.45x45 as badge_45,
        escudos.60x60 as badge_60

    from source

),

grouped as (

    select
        club_id,
        name,
        abbreviation,
        badge_30,
        badge_45,
        badge_60

    from renamed
    
    qualify row_number() over (partition by club_id order by season desc, round desc) = 1

)

select * from grouped
