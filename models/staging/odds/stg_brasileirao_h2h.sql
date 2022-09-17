SELECT
    CONCAT(CAST(season AS STRING), " ", home, " x ", away) AS id,
    season,
    timestamp,
    c_home.slug AS home,
    c_away.slug AS away,
    pinnacle_home,
    pinnacle_away,
    pinnacle_draw,
    max_home,
    max_away,
    max_draw,
    avg_home,
    avg_away,
    avg_draw
FROM
    {{ source ('odds', 'brasileirao') }}
LEFT JOIN {{ ref ("dim_slug") }} AS c_home ON home = c_home.name
LEFT JOIN {{ ref ("dim_slug") }} AS c_away ON away = c_away.name
