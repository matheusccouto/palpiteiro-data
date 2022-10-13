WITH popular AS (
  SELECT
    p.season,
    p.round,
    s.position,
    s.total_points,
    ROW_NUMBER() OVER (PARTITION BY p.season, p.round, s.position ORDER BY p.drafts DESC) AS ranking
  FROM
    {{ ref("stg_destaques_popular") }} p
  LEFT JOIN
    {{ ref("fct_scoring") }} s ON
      p.player_id = s.player_id
      AND p.season = s.season
      AND p.round = s.round
  WHERE
    s.played IS TRUE
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
