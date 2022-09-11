SELECT
    season,
    timestamp,
    home,
    away,
    pinnacle_home,
    pinnacle_away,
    pinnacle_draw,
    max_home,
    max_away,
    max_draw,
    avg_home,
    avg_away,
    avg_draw,
    loaded_at,
    CONCAT(CAST(season AS STRING), " ", home, " x ", away) AS id
FROM
    {{ source ('odds', 'brasileirao') }}
