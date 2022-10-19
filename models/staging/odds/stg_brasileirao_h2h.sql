SELECT
    odds.season,
    odds.timestamp,
    c_home.slug AS home,
    c_away.slug AS away,
    odds.avg_home,
    odds.avg_away,
    odds.avg_draw,
    CONCAT(odds.season, '-', c_home.slug, '-', c_away.slug) AS match_slug
FROM
    {{ source ('odds', 'brasileirao') }} AS odds
LEFT JOIN {{ ref ("dim_slug") }} AS c_home ON home = c_home.name
LEFT JOIN {{ ref ("dim_slug") }} AS c_away ON away = c_away.name
