SELECT
    player_id,
    nickname,
    photo,
    slug,
    short_nickname,
    price AS price_cartola,
    price - variation AS price_cartola_express
FROM
    {{ ref ("stg_atletas_player") }}
