SELECT
    player_id,
    name,
    nickname,
    photo,
    slug,
    short_nickname
FROM
    {{ ref ("stg_atletas_player") }}
