SELECT
    cnt.season,
    cnt.round,
    prz.contest,
    prz.prizes,
    prz.contest * 100000 + prz.rank AS rank_id,
    (prz.points - pop.points) / (bst.points - pop.points) AS points_norm
FROM
    {{ source("express", "prize") }} AS prz
LEFT JOIN
    {{ ref("contests_metadata") }} AS cnt ON prz.contest = cnt.contest
LEFT JOIN
    {{ ref("fct_popular_points") }} AS pop ON
        cnt.season = pop.season AND cnt.round = pop.round
LEFT JOIN
    {{ ref("fct_best_expected_points") }} AS bst ON
        cnt.season = bst.season AND cnt.round = bst.round