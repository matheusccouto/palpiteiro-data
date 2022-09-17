WITH scoring AS (
    SELECT
        atleta_id AS player,
        rodada_id AS round,
        temporada_id AS season,
        clb.slug AS club,
        p.slug AS position,
        s.slug AS status,
        pontos_num AS points,
        preco_num AS price,
        variacao_num AS variation,
        media_num AS mean,
        CAST(
            (
                temporada_id - 2000
            ) * 100000000 + rodada_id * 1000000 + atleta_id AS int
        ) AS id,
        COALESCE(jogos_num, 0) AS matches
    FROM
        {{ source("cartola", "atletas") }}
    LEFT JOIN {{ ref ("dim_position") }} AS p ON posicao_id = p.id
    LEFT JOIN {{ ref ("dim_status") }} AS s ON status_id = s.id
    LEFT JOIN {{ ref ("dim_club") }} AS clb ON clube_id = clb.id

)

SELECT
    curr.id,
    curr.player,
    curr.round,
    curr.season,
    curr.club,
    curr.position,
    curr.points,
    curr.mean,
    curr.matches,
    IF(curr.season >= 2022, curr.status, prev.status) AS status,
    IF(curr.season >= 2022, curr.price, curr.price - curr.variation) AS price,
    IF(
        curr.season >= 2022, curr.variation, COALESCE(prev.variation, 0)
    ) AS variation
FROM
    scoring AS curr
LEFT JOIN
    scoring AS prev ON
        curr.season = prev.season AND curr.round = prev.round + 1 AND curr.player = prev.player
