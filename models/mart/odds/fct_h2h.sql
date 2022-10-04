WITH h2h AS (
    SELECT
        h2h.season,
        h2h.timestamp,
        h2h.home AS club,
        h2h.away AS opponent,
        h2h.avg_home AS avg_club,
        h2h.avg_away AS avg_opponent,
        h2h.avg_draw AS avg_draw
    FROM
        {{ ref("stg_brasileirao_h2h") }} AS h2h
),

inv AS (
    SELECT
        season,
        timestamp,
        opponent AS club,
        club AS opponent,
        avg_opponent AS avg_club,
        avg_club AS avg_opponent,
        avg_draw
    FROM
        h2h
)

SELECT
    season,
    timestamp,
    club,
    opponent,
    avg_club,
    avg_opponent,
    avg_draw
FROM
    h2h
UNION ALL
SELECT
    season,
    timestamp,
    club,
    opponent,
    avg_club,
    avg_opponent,
    avg_draw
FROM
    inv
