WITH agg AS (
    SELECT
        season,
        round,
        MAX(drafts) AS drafts_max
    FROM
        {{ ref("stg_destaques_popular") }}
    GROUP BY
        season, round
)

SELECT
    p.play_id,
    p.season,
    p.round,
    p.player_id,
    p.drafts,
    p.drafts / agg.drafts_max AS drafts_norm
FROM
    {{ ref("stg_destaques_popular") }} AS p
LEFT JOIN agg ON p.season = agg.season AND p.round = agg.round
