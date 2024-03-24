with source as (

    select * from {{ source('cartola', 'status') }}

),

renamed as (

    select
        id as status_id,
        _temporada as season,
        _rodada as round,
        nome as name

    from source

),

drop_duplicates as (

    select
        status_id,
        name

    from renamed

    qualify
        row_number()
            over (
                partition by status_id order by season desc, round desc
            )
        = 1

),

translated as (

    select
        status_id,
        case
            when name = 'Dúvida' then 'Doubt'
            when name = 'Suspenso' then 'Suspended'
            when name = 'Contundido' then 'Injured'
            when name = 'Nulo' then 'Null'
            when name = 'Provável' then 'Probable'
        end as name

    from drop_duplicates

)

select * from translated
