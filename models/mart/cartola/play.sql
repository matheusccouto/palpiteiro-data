WITH source AS (

    SELECT * FROM {{ ref("stg_atletas__play") }}
    
)