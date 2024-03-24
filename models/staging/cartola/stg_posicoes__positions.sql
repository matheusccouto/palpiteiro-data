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
            when abbreviation = 'ata' then 'FWD'
            when abbreviation = 'gol' then 'GK'
            when abbreviation = 'lat' then 'FB'
            when abbreviation = 'mei' then 'MID'
            when abbreviation = 'tec' then 'COA'
            when abbreviation = 'zag' then 'DEF'
        end as abbreviation

    from drop_duplicates

)

select * from translated
