{% snapshot partidas_snapshot %}

{{
    config(
      target_schema="cartola",
      unique_key="temporada||'-'||rodada||'-'||clube_casa_id||'-'||clube_visitante_id",
      strategy="timestamp",
      updated_at="loaded_at",
    )
}}

SELECT * FROM {{ source("cartola", "partidas") }}

{% endsnapshot %}