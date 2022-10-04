SELECT
    ptd.temporada AS season,
    ptd.rodada AS round,
    ptd.valida AS valid,
    c_home.slug AS home,
    c_away.slug AS away,
    TIMESTAMP(ptd.partida_data, "America/Sao_Paulo") AS timestamp -- noqa: L029
FROM
    {{ source ('cartola', 'partidas') }} AS ptd
LEFT JOIN {{ ref ("dim_club") }} AS c_home ON ptd.clube_casa_id = c_home.id
LEFT JOIN {{ ref ("dim_club") }} AS c_away ON ptd.clube_visitante_id = c_away.id
