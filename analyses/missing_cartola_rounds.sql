with pontuados as (

    select
        _temporada as temporada,
        lpad(cast(_rodada as string), 2, '0') as rodada

    from {{ source("cartola", "atletas" ) }}


),

temporada as (

    select temporada from unnest(generate_array(2017, 2023)) as temporada

),

rodada as (

    select lpad(cast(rodada as string), 2, '0') as rodada
    from unnest(generate_array(1, 38)) as rodada

),

combos as (

    select concat(t.temporada, '-', r.rodada) as id

    from
        temporada as t

    cross join
        rodada as r

),

pontuados_rounds as (

    select distinct concat(temporada, '-', rodada) as id
    from pontuados

)

select c.id

from
    combos as c
left join pontuados_rounds as p on c.id = p.id

where
    p.id is NULL
