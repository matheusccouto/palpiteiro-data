with source as (

    select * from {{ source('cartola', 'posicoes') }}

),

renamed as (

    select
        id as position_id,
        _temporada as season,
        _rodada as round,
        nome as name,
        abreviacao as abbreviation

    from source

),

drop_duplicates as (

    select
        position_id,
        name,
        abbreviation

    from renamed

    qualify
        row_number()
            over (
                partition by position_id order by season desc, round desc
            )
        = 1

),

translated as (

    select
        position_id,
        case
            when name = 'Atacante' then 'Forward'
            when name = 'Goleiro' then 'Goalkeeper'
            when name = 'Lateral' then 'Fullback'
            when name = 'Meia' then 'Midfielder'
            when name = 'Técnico' then 'Coach'
            when name = 'Zagueiro' then 'Defender'
        end as name,
        case
            when name = 'ata' then 'FWD'
            when name = 'gol' then 'GK'
            when name = 'lat' then 'FB'
            when name = 'mei' then 'MID'
            when name = 'tec' then 'COA'
            when name = 'zag' then 'DEF'
        end as abbreviation

    from drop_duplicates

)

select * from translated
