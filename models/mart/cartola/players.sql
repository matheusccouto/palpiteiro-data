with players as (

    select * from {{ ref("stg_atletas__players") }}

),

status as (

    select * from {{ ref("stg_status__status") }}

),

positions as (

    select * from {{ ref("stg_posicoes__positions") }}

),

scores as (

    select * from {{ ref("scores") }}

),


picks as (

    select * from {{ ref("picks") }}

),

clubs as (

    select * from {{ ref("clubs") }}

),

join_status as (

    select
        players.* except (status),
        status.status_id,
        status.name as status

    from players

    left join status on players.status = status.status_id

),

join_position as (

    select
        players.* except (position),
        positions.position_id,
        positions.name as position,
        positions.abbreviation as position_abbreviation

    from join_status as players

    left join positions on players.position = positions.position_id

),

join_player_scoring as (

    select
        players.player_id,
        players.club_id,
        players.season,
        players.round,
        players.position,
        players.position_abbreviation,
        players.status,
        players.price,
        players.variation,
        scores.offensive_points,
        scores.defensive_points,
        scores.total_points,
        players.nickname,
        players.short_nickname,
        players.photo,
        coalesce(scores.played, FALSE) as played

    from join_position as players

    left join scores
        on
            players.player_id = scores.player_id
            and players.season = scores.season
            and players.round = scores.round

),


join_picks as (

    select
        player.*,
        picks.picks,
        picks.picks_ratio

    from
        join_player_scoring as player

    left join picks
        on
            player.player_id = picks.player_id
            and player.season = picks.season
            and player.round = picks.round

),

join_clubs as (

    select
        players.*,
        clubs.* except (club_id, season, round)

    from
        join_picks as players

    left join
        clubs
        on
            players.club_id = clubs.club_id
            and players.season = clubs.season
            and players.round = clubs.round

),

rolling as (

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

    from join_clubs

)

select * from rolling
