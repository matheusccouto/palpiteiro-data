SELECT
    ptd.temporada AS season,
    ptd.rodada AS round,
    ptd.local AS venue,
    ptd.valida AS valid,
    c_home.slug AS home,
    c_away.slug AS away,
    ptd.clube_casa_posicao AS position_home,
    ptd.clube_visitante_posicao AS position_away,
    ptd.placar_oficial_mandante AS score_home,
    ptd.placar_oficial_visitante AS score_away,
    ptd.status_transmissao_tr AS broadcasting_status,
    ptd.status_cronometro_tr AS broadcasting_time_status,
    ptd.periodo_tr AS broadcasting_phase,
    ptd.transmissao_label AS broadcasting_label,
    ptd.transmissao_url AS broadcasting_url,
    ptd.aproveitamento_mandante_4 AS previous_result_1_home,
    ptd.aproveitamento_mandante_3 AS previous_result_2_home,
    ptd.aproveitamento_mandante_2 AS previous_result_3_home,
    ptd.aproveitamento_mandante_1 AS previous_result_4_home,
    ptd.aproveitamento_mandante_0 AS previous_result_5_home,
    ptd.aproveitamento_visitante_4 AS previous_result_1_away,
    ptd.aproveitamento_visitante_3 AS previous_result_2_away,
    ptd.aproveitamento_visitante_2 AS previous_result_3_away,
    ptd.aproveitamento_visitante_1 AS previous_result_4_away,
    ptd.aproveitamento_visitante_0 AS previous_result_5_away,
    TIMESTAMP(ptd.partida_data, "America/Sao_Paulo") AS timestamp, -- noqa: L029
    TIMESTAMP(ptd.inicio_cronometro_tr) AS broadcasting_time_start
FROM
    {{ source ('cartola', 'partidas') }} AS ptd
LEFT JOIN {{ ref ("dim_club") }} AS c_home ON ptd.clube_casa_id = c_home.id
LEFT JOIN {{ ref ("dim_club") }} AS c_away ON ptd.clube_visitante_id = c_away.id
