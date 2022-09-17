SELECT
    player_id,
    nickname,
    photo,
    slug,
    short_nickname
FROM
    {{ ref ("stg_atletas_player") }}
