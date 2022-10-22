SELECT
    cnt.season,
    cnt.round,
    prz.contest,
    prz.prizes,
    prz.points,
    prz.rank,
    prz.contest * 100000 + prz.rank AS rank_id
FROM
    {{ source("express", "prize") }} AS prz
LEFT JOIN
    {{ ref("contests_metadata") }} AS cnt ON prz.contest = cnt.contest
