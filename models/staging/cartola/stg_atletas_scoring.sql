WITH scoring AS (
    SELECT
        atl.atleta_id AS player_id,
        atl.rodada_id AS round,
        atl.temporada_id AS season,
        clb.slug AS club,
        pos.slug AS position,
        sts.slug AS status,
        atl.pontos_num AS points,
        atl.preco_num AS price,
        atl.variacao_num AS variation
    FROM
        {{ source("cartola", "atletas") }} AS atl
    LEFT JOIN {{ ref ("dim_position") }} AS pos ON atl.posicao_id = pos.id
    LEFT JOIN {{ ref ("dim_status") }} AS sts ON atl.status_id = sts.id
    LEFT JOIN {{ ref ("dim_club") }} AS clb ON atl.clube_id = clb.id

)

SELECT
    curr.player_id,
    curr.round,
    curr.season,
    curr.club,
    curr.position,
    curr.points,
    IF(curr.season >= 2022, curr.status, prev.status) AS status,
    IF(curr.season >= 2022, curr.price, curr.price - curr.variation) AS price,
    IF(
        curr.season >= 2022, curr.variation, COALESCE(prev.variation, 0)
    ) AS variation
FROM
    scoring AS curr
LEFT JOIN
    scoring AS prev ON
        curr.season = prev.season
        AND curr.round = prev.round + 1
        AND curr.player_id = prev.player_id
