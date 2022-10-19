WITH scoring AS (
    SELECT
        atl.atleta_id AS player_id,
        atl.rodada_id AS round,
        atl.temporada_id AS season,
        clb.slug AS club,
        pos.slug AS position,
        pos.id AS position_id,
        sts.slug AS status,
        atl.pontos_num AS points,
        atl.preco_num AS price,
        atl.variacao_num AS variation,
        (
            atl.temporada_id - 2000
        ) * 100000000 + atl.rodada_id * 1000000 + atl.atleta_id AS play_id
    FROM
        {{ source("cartola", "atletas") }} AS atl
    LEFT JOIN {{ ref ("dim_position") }} AS pos ON atl.posicao_id = pos.id
    LEFT JOIN {{ ref ("dim_status") }} AS sts ON atl.status_id = sts.id
    LEFT JOIN {{ ref ("dim_club") }} AS clb ON atl.clube_id = clb.id

)

SELECT
    curr.play_id,
    curr.player_id,
    curr.round,
    curr.season,
    curr.club,
    curr.position,
    curr.position_id,
    curr.points,
    curr.price,
    curr.variation,
    IF(curr.season >= 2022, curr.status, prev.status) AS status
FROM
    scoring AS curr
LEFT JOIN
    scoring AS prev ON
        curr.season = prev.season
        AND curr.round = prev.round + 1
        AND curr.player_id = prev.player_id
