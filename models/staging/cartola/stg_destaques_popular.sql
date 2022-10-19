SELECT
    temporada AS season,
    rodada AS round,
    atleta_id AS player_id,
    escalacoes AS drafts,
    (temporada - 2000) * 100000000 + rodada * 1000000 + atleta_id AS play_id
FROM
    {{ source("cartola", "destaques") }}
