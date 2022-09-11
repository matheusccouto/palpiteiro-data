SELECT
    temporada AS season,
    rodada AS round,
    local AS venue,
    valida AS valid,
    clube_casa_id AS home,
    clube_visitante_id AS away,
    clube_casa_posicao AS position_home,
    clube_visitante_posicao AS position_away,
    placar_oficial_mandante AS score_home,
    placar_oficial_visitante AS score_away,
    status_transmissao_tr AS broadcasting_status,
    status_cronometro_tr AS broadcasting_time_status,
    periodo_tr AS broadcasting_phase,
    transmissao_label AS broadcasting_label,
    transmissao_url AS broadcasting_url,
    aproveitamento_mandante_4 AS previous_result_1_home,
    aproveitamento_mandante_3 AS previous_result_2_home,
    aproveitamento_mandante_2 AS previous_result_3_home,
    aproveitamento_mandante_1 AS previous_result_4_home,
    aproveitamento_mandante_0 AS previous_result_5_home,
    aproveitamento_visitante_4 AS previous_result_1_away,
    aproveitamento_visitante_3 AS previous_result_2_away,
    aproveitamento_visitante_2 AS previous_result_3_away,
    aproveitamento_visitante_1 AS previous_result_4_away,
    aproveitamento_visitante_0 AS previous_result_5_away,
    (
        temporada - 2000
    ) * 100000000 + clube_casa_id * 10000 + clube_visitante_id AS id,
    TIMESTAMP(partida_data, "America/Sao_Paulo") AS timestamp,
    TIMESTAMP(inicio_cronometro_tr) AS broadcasting_time_start
FROM
    {{ source ('cartola', 'partidas') }}
