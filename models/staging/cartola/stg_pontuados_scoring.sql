SELECT
    pnt.id AS player_id,
    pnt.rodada AS round,
    pnt.temporada AS season,
    clb.slug AS club,
    pos.slug AS position,
    pnt.pontuacao AS points,
    (
        pnt.temporada - 2000
    ) * 100000000 + pnt.rodada * 1000000 + pnt.id AS play_id,
    CAST(
        IF(
            pnt.posicao_id = 6,
            FALSE,
            COALESCE(pnt.entrou_em_campo, pnt.pontuacao != 0)
        ) AS boolean
    ) AS played,
    COALESCE(pnt.g, 0) AS goal,
    COALESCE(pnt.a, 0) AS assist,
    COALESCE(pnt.ca, 0) AS yellow_card,
    COALESCE(pnt.cv, 0) AS red_card,
    COALESCE(pnt.ff, 0) AS missed_shoot,
    COALESCE(pnt.ft, 0) AS on_post_shoot,
    COALESCE(pnt.fd, 0) AS saved_shoot,
    COALESCE(pnt.fs, 0) AS received_foul,
    COALESCE(pnt.ps, 0) AS received_penalty,
    COALESCE(pnt.pp, 0) AS missed_penalty,
    COALESCE(pnt.i, 0) AS outside,
    COALESCE(pnt.pe, pnt.pi, 0) AS missed_pass,
    COALESCE(pnt.rb, pnt.ds, 0) AS tackle,
    COALESCE(pnt.fc, 0) AS foul,
    COALESCE(pnt.pc, 0) AS penalty,
    COALESCE(pnt.gc, 0) AS own_goal,
    COALESCE(pnt.gs, 0) AS allowed_goal,
    COALESCE(pnt.sg, 0) AS no_goal,
    -- Difficult Save was replaced by Save. I calculated that: 
    -- On average for each difficult save, there are 2.218 save.
    COALESCE(pnt.de, CAST(ROUND(pnt.dd * 2.218, 0) AS int), 0) AS save,
    COALESCE(pnt.dp, 0) AS penalty_save
FROM
    {{ source ('cartola', 'pontuados') }} AS pnt
LEFT JOIN {{ ref ("dim_position") }} AS pos ON pnt.posicao_id = pos.id
LEFT JOIN {{ ref ("dim_club") }} AS clb ON pnt.clube_id = clb.id
