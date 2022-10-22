SELECT
    prz.season,
    prz.round,
    prz.contest,
    prz.prizes,
    prz.contest * 100000 + prz.rank AS rank_id,
    (prz.points - pop.points) / (bst.points - pop.points) AS points_norm
FROM
    {{ ref("fct_prize") }} AS prz
LEFT JOIN
    {{ ref("fct_popular_points") }} AS pop ON
        prz.season = pop.season AND prz.round = pop.round
LEFT JOIN
    {{ ref("fct_best_expected_points") }} AS bst ON
        prz.season = bst.season AND prz.round = bst.round
