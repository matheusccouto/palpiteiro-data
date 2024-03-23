with scores as (

    select * from {{ ref("scores") }}

),

matches as (

    select * from {{ ref("stg_partidas__matches") }}

),

clubs as (

    select * from {{ ref("stg_clubes__clubs") }}

),

club_scores as (

    select
        club_id,
        season,
        round,
        round(sum(offensive_points), 1) as offensive_points,
        round(sum(defensive_points), 1) as defensive_points,
        round(sum(total_points), 1) as total_points

    from scores

    group by club_id, season, round

),


join_matches as (

    select
        scores.*,
        coalesce(home.date, away.date) as date,
        if(away.match_id is NULL, 'home', 'away') as venue,
        coalesce(home.away_id, away.home_id) as opponent_id,
        coalesce(home.valid, away.valid) as valid

    from club_scores as scores

    left join
        matches as home
        on
            scores.club_id = home.home_id
            and scores.season = home.season
            and scores.round = home.round

    left join
        matches as away on
        scores.club_id = away.away_id
        and scores.season = away.season
        and scores.round = away.round

),

join_opponent_scores as (

    select
        matches.club_id,
        matches.season,
        matches.round,
        matches.opponent_id,
        matches.date,
        matches.venue,
        matches.valid,
        matches.offensive_points as club_offensive_points,
        matches.defensive_points as club_defensive_points,
        matches.total_points as club_total_points,
        scores.offensive_points as opponent_offensive_points,
        scores.defensive_points as opponent_defensive_points,
        scores.total_points as opponent_total_points

    from join_matches as matches

    left join
        club_scores as scores
        on
            matches.opponent_id = scores.club_id
            and matches.season = scores.season
            and matches.round = scores.round

),

join_metadata as (

    select
        scores.*,
        clubs.name as club_name,
        clubs.abbreviation as club_abbreviation,
        clubs.badge_30 as club_badge_30,
        clubs.badge_45 as club_badge_45,
        clubs.badge_60 as club_badge_60,
        opponents.name as opponent_name,
        opponents.abbreviation as opponent_abbreviation,
        opponents.badge_30 as opponent_badge_30,
        opponents.badge_45 as opponent_badge_45,
        opponents.badge_60 as opponent_badge_60

    from join_opponent_scores as scores

    left join
        clubs
        on scores.club_id = clubs.club_id

    left join
        clubs as opponents
        on scores.opponent_id = opponents.club_id

),

rolling as (

    select
        *,
        avg(club_total_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_total_points_last_5_at,
        avg(club_offensive_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_offensive_points_last_5_at,
        avg(club_defensive_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_defensive_points_last_5_at,
        avg(opponent_total_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_total_points_last_5_at,
        avg(opponent_offensive_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_offensive_points_last_5_at,
        avg(opponent_defensive_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_defensive_points_last_5_at,
        avg(opponent_total_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_allowed_total_points_last_5_at,
        avg(opponent_offensive_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_allowed_offensive_points_last_5_at,
        avg(opponent_defensive_points)
            over (
                partition by club_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as club_allowed_defensive_points_last_5_at,
        avg(club_total_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_allowed_total_points_last_5_at,
        avg(club_offensive_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_allowed_offensive_points_last_5_at,
        avg(club_defensive_points)
            over (
                partition by opponent_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as opponent_allowed_defensive_points_last_5_at

    from
        join_metadata

)

select * from rolling
