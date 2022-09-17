SELECT
    odds.season,
    odds.timestamp,
    c_home.slug AS home,
    c_away.slug AS away,
    odds.pinnacle_home,
    odds.pinnacle_away,
    odds.pinnacle_draw,
    odds.max_home,
    odds.max_away,
    odds.max_draw,
    odds.avg_home,
    odds.avg_away,
    odds.avg_draw,
    CONCAT(
        CAST(odds.season AS STRING), " ", c_home.slug, " x ", c_away.slug
    ) AS odds_id
FROM
    {{ source ('odds', 'brasileirao') }} AS odds
LEFT JOIN {{ ref ("dim_slug") }} AS c_home ON home = c_home.name
LEFT JOIN {{ ref ("dim_slug") }} AS c_away ON away = c_away.name
