SELECT
    temporada AS season,
    rodada AS round,
    atleta_id AS player_id,
    escalacoes AS drafts
FROM
    {{ source("cartola", "destaques") }}