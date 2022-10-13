WITH agg AS (
    SELECT
        season,
        round,
        SUM(drafts) AS drafts_sum
    FROM
        {{ ref("stg_destaques_popular") }}
    GROUP BY
        season, round
)

SELECT
    p.season,
    p.round,
    p.player_id,
    p.drafts,
    p.drafts / agg.drafts_sum AS drafts_norm
FROM
    {{ ref("stg_destaques_popular") }} AS p
LEFT JOIN agg ON p.season = agg.season AND p.round = agg.round
