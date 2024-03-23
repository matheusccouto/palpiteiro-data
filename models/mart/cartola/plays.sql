with play as (

    select * from {{ ref("stg_atletas__plays") }}

),

player_scoring as (

    select * from {{ ref("scores") }}

),

matches as (

    select * from {{ ref("stg_partidas__matches") }}

),

picks as (

    select * from {{ ref("picks") }}

),

join_player_scoring as (

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
        player_scoring as score
        on
            play.player_id = score.player_id
            and play.season = score.season
            and play.round = score.round

),

join_matches as (

    select
        play.*,
        coalesce(home.date, away.date) as date,
        if(away.match_id is NULL, 'home', 'away') as venue,
        coalesce(home.away_id, away.home_id) as opponent_id,
        coalesce(home.valid, away.valid) as valid

    from join_player_scoring as play

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

club_matches as (

    select
        club_id,
        season,
        round,
        any_value(venue) as venue,
        any_value(valid) as valid,
        any_value(opponent_id) as opponent_id

    from
        join_matches

    group by club_id, season, round

),

club_scoring as (

    select
        season,
        round,
        club_id,
        round(sum(total_points), 1) as total_points,
        round(sum(offensive_points), 1) as offensive_points,
        round(sum(defensive_points), 1) as defensive_points

    from
        player_scoring

    group by season, round, club_id

),

join_club_scoring as (

    select
        match.*,
        club.total_points as club_total_points,
        club.offensive_points as club_offensive_points,
        club.defensive_points as club_defensive_points,
        opponent.total_points as opponent_total_points,
        opponent.offensive_points as opponent_offensive_points,
        opponent.defensive_points as opponent_defensive_points

    from
        club_matches as match

    left join
        club_scoring as club
        on
            match.club_id = club.club_id
            and match.season = club.season
            and match.round = club.round

    left join
        club_scoring as opponent
        on
            match.opponent_id = opponent.club_id
            and match.season = opponent.season
            and match.round = opponent.round
),

club_rolling as (

    select
        club_id,
        season,
        round,
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
        join_club_scoring

),

player_rolling as (

    select
        *,
        avg(cast(played as INT))
            over (
                partition by player_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as played_last_5_at,
        avg(total_points)
            over (
                partition by player_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as total_points_last_5_at,
        avg(offensive_points)
            over (
                partition by player_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as offensive_points_last_5_at,
        avg(defensive_points)
            over (
                partition by player_id, venue
                order by season, round rows between 5 preceding and 1 preceding
            )
            as defensive_points_last_5_at

    from join_picks

),

join_club_rolling as (

    select
        player.*,
        club.club_total_points_last_5_at,
        club.club_offensive_points_last_5_at,
        club.club_defensive_points_last_5_at,
        club.opponent_total_points_last_5_at,
        club.opponent_offensive_points_last_5_at,
        club.opponent_defensive_points_last_5_at,
        club.club_allowed_total_points_last_5_at,
        club.club_allowed_offensive_points_last_5_at,
        club.club_allowed_defensive_points_last_5_at,
        club.opponent_allowed_total_points_last_5_at,
        club.opponent_allowed_offensive_points_last_5_at,
        club.opponent_allowed_defensive_points_last_5_at

    from
        player_rolling as player

    left join
        club_rolling as club
        on
            player.club_id = club.club_id
            and player.season = club.season
            and player.round = club.round

)

select * from join_club_rolling
