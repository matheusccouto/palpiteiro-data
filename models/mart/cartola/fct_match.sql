WITH mat AS (
    SELECT
        season,
        round,
        timestamp,
        valid,
        home AS club,
        away AS opponent,
        TRUE AS home,
        38 * (season - 2017) + round AS all_time_round
    FROM
        {{ ref ('stg_partidas_match') }}
),

inv AS (
    SELECT
        season,
        round,
        all_time_round,
        timestamp,
        valid,
        club AS opponent,
        opponent AS club,
        FALSE AS home
    FROM
        mat
)

SELECT
    season,
    round,
    all_time_round,
    timestamp,
    valid,
    club,
    opponent,
    home
FROM
    mat
UNION ALL
SELECT
    season,
    round,
    all_time_round,
    timestamp,
    valid,
    club,
    opponent,
    home
FROM
    inv
