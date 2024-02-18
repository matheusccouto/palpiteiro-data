with play as (

    select * from {{ ref("stg_atletas__play") }}

),

scoring as (

    select * from {{ ref("scoring") }}

),

matches as (

    select * from {{ ref("stg_partidas__match") }}

),

picks as (

    select * from {{ ref("picks") }}

),

join_scoring as (

    select
        play.player_id,
        play.club_id,
        play.season,
        play.round,
        play.position,
        play.status,
        play.price,
        play.variation,
        score.offensive_points,
        score.defensive_points,
        score.total_points,
        coalesce(score.played, FALSE) as played

    from play as play
    left join
        scoring as score
        on
            play.player_id = score.player_id
            and play.season = score.season
            and play.round = score.round

),

join_matches as (

    select
        play.*,
        coalesce(home.match_id, away.match_id) as match_id,
        coalesce(home.date, away.date) as date,
        if(away.match_id is null, 'home', 'away') as venue,
        coalesce(home.away_id, away.home_id) as opponent_id,
        coalesce(home.valid, away.valid) as valid

    from join_scoring as play

    left join
        matches as home
        on
            play.club_id = home.home_id
            and play.season = home.season
            and play.round = home.round

    left join
        matches as away on
        play.club_id = away.away_id
        and play.season = away.season
        and play.round = away.round

),

join_picks as (

    select 
        play.*,
        picks.picks,
        picks.picks_ratio

    from
        join_matches as play

    left join
        picks as picks on
        play.player_id = picks.player_id
        and play.season = picks.season
        and play.round = picks.round

),

rolling as (

    select
        play.*,
        AVG(CAST(played AS INT)) OVER (PARTITION BY player_id, venue ORDER BY season, round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS avg_played_last_5_at,
        AVG(total_points) OVER (PARTITION BY player_id, venue ORDER BY season, round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS avg_total_points_last_5_at,
        AVG(offensive_points) OVER (PARTITION BY player_id, venue ORDER BY season, round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS avg_offensive_points_last_5_at,
        AVG(defensive_points) OVER (PARTITION BY player_id, venue ORDER BY season, round ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS avg_defensive_points_last_5_at,

    from join_picks as play
      
)

select * from rolling
