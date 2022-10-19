SELECT
    ptd.temporada AS season,
    ptd.rodada AS round,
    ptd.valida AS valid,
    c_home.slug AS home,
    c_away.slug AS away,
    (ptd.temporada - 2000)
    * 10000000000
    + ptd.rodada
    * 100000000
    + ptd.clube_casa_id
    * 10000
    + ptd.clube_visitante_id AS match_id,
    CONCAT(ptd.temporada, '-', c_home.slug, '-', c_away.slug) AS match_slug,
    TIMESTAMP(ptd.partida_data, 'America/Sao_Paulo') AS timestamp -- noqa: L029
FROM
    {{ source ('cartola', 'partidas') }} AS ptd
LEFT JOIN {{ ref ("dim_club") }} AS c_home ON ptd.clube_casa_id = c_home.id
LEFT JOIN {{ ref ("dim_club") }} AS c_away ON ptd.clube_visitante_id = c_away.id
