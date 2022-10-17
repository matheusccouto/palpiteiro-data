WITH popular AS (
    SELECT
        season,
        round,
        position,
        total_points,
        ROW_NUMBER() OVER (PARTITION BY season, round, position ORDER BY total_points DESC) AS ranking
    FROM
        {{ ref("fct_scoring") }}
    WHERE
        played IS TRUE
        AND status = 'expected'
)

SELECT
    season,
    round,
    ROUND(SUM(total_points), 1) AS points
FROM
    popular
WHERE
    (position = 'goalkeeper' AND ranking <= 1)
    OR (position = 'defender' AND ranking <= 2)
    OR (position = 'fullback' AND ranking <= 2)
    OR (position = 'midfielder' AND ranking <= 3)
    OR (position = 'forward' AND ranking <= 3)
GROUP BY
    season, round
