SELECT
    atleta_id AS id,
    nome AS name,
    apelido AS nickname,
    COALESCE(
        slug,
        TRANSLATE(
            REPLACE(LOWER(apelido), ' ', '-'), 'áâãçéêíñóôõú', 'aaaceeinooou'
        )
    ) AS slug,
    COALESCE(
        apelido_abreviado, REGEXP_REPLACE(TRIM(apelido), r'[a-z]+\s', '. ')
    ) AS short_nickname,
    REPLACE(foto, 'FORMATO', '140x140') AS photo
FROM
    {{ source ('cartola', 'atletas') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY atleta_id ORDER BY temporada_id DESC, rodada_id DESC
    ) = 1
