SELECT
    atleta_id AS id,
    slug AS slug,
    nome AS name,
    apelido AS nickname,
    apelido_abreviado AS short_nickname,
    REPLACE(foto, "FORMATO", "140x140") AS photo
FROM
    {{ source ('cartola', 'atletas') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY atleta_id ORDER BY temporada_id DESC, rodada_id DESC
    ) = 1
