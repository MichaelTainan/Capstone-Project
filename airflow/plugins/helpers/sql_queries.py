class SqlQueries:
    player_statistics_table_insert = ("""
        SELECT  player_id, statistics_team_id, statistics_league_id,
        statistics_league_season, c.venue_id,  
        statistics_games_appearences, statistics_games_lineups,
        statistics_games_minutes, statistics_games_number,
        statistics_games_position, statistics_games_rating,
        statistics_games_captain, statistics_substitutes_in,
        statistics_substitutes_out,
        statistics_substitutes_bench, statistics_shots_total, statistics_shots_on,
        statistics_goals_total, statistics_goals_conceded, statistics_goals_assists, 
        statistics_goals_saves, statistics_passes_total, statistics_passes_key,
        statistics_passes_accuracy, statistics_tackles_total, 
        statistics_tackles_blocks, statistics_tackles_interceptions, 
        statistics_duels_total, statistics_duels_won, statistics_dribbles_attempts,
        statistics_dribbles_success, statistics_dribbles_past,
        statistics_fouls_drawn,
        statistics_fouls_committed, statistics_cards_yellow,
        statistics_cards_yellowred, statistics_cards_red, statistics_penalty_won, 
        statistics_penalty_commited, statistics_penalty_scored, 
        statistics_penalty_missed, statistics_penalty_saved
        FROM stage_players a
        LEFT JOIN TEAMS c
        on a.statistics_team_id = c.id
        and a.statistics_league_id = c.league_id
        and a.statistics_league_season = c.season        
        WHERE a.player_id is not null
        AND a.statistics_team_id is not null
        AND a.statistics_league_id is not null
        AND a.statistics_league_season is not null
    """)
    
    match_statistics_table_insert = ("""
        SELECT a.team_id, a.fixture_id, c.league_id, c.league_season, c.venue_id,
        Shots_on_Goal, Shots_off_Goal, Total_Shots, Blocked_Shots,
        Shots_insidebox, Shots_outsidebox, Fouls, Corner_Kicks, Offsides,
        Ball_Possession, Yellow_Cards, Red_Cards, Goalkeeper_Saves, Total_passes,
        Passes_accurate,Passes_percentage  
        FROM stage_statistics a 
        LEFT JOIN fixtures c
        on a.fixture_id = c.id
        LEFT JOIN LEAGUES d
        on c.league_id = d.id
        AND c.league_season = d.seasons_year
        WHERE a.fixture_id is not null
        AND a.team_id is not null
    """)
        
    league_standings_table_insert = ("""
        SELECT  a.league_id, a.league_season,  
        league_standings_team_id, c.venue_id, league_standings_index, 
        league_standings_rank,
        league_standings_points, league_standings_goalsDiff, league_standings_group,
        league_standings_form, league_standings_status,
        league_standings_description,
        CAST(substring(league_standings_update, 1, 10) AS DATE), 
        CAST(substring(league_standings_update, 12, 8) AS TIME), 
        league_standings_all_played, league_standings_all_win,
        league_standings_all_draw, league_standings_all_lose,
        league_standings_all_goals_for,
        league_standings_all_goals_against, league_standings_home_played,
        league_standings_home_win, league_standings_home_draw,
        league_standings_home_lose,
        league_standings_home_goals_for, league_standings_home_goals_against,
        league_standings_away_played, league_standings_away_win,
        league_standings_away_draw,
        league_standings_away_lose, league_standings_away_goals_for,
        league_standings_away_goals_against   
        FROM stage_standings a
        LEFT JOIN leagues b 
        ON a.league_id = b.id
        AND a.league_season = b.seasons_year
        LEFT JOIN teams c
        ON a. league_standings_team_id = c.id 
        AND a.league_season = c.season
        AND a.league_id = c.league_id
        WHERE a.league_id is not null 
        and a.league_season is not null 
        and a.league_standings_index is not null 
        and a.league_standings_team_id is not null
    """)
    
    league_fixtures_table_insert = ("""
        SELECT a.id as fixture_id, a.fixture_date, a.fixture_time, a.referee,
        a.status_elapsed,
        a. status_long, a.status_short, a.timezone, 
        a.teams_home_id, b.name as team_home_name, a.goals_home,
        a.score_extratime_home, a.score_fulltime_home, a.score_halftime_home,
        a.score_penalty_home,
        a.teams_away_id, c.name as team_away_name, a.goals_away,
        a.score_extratime_away, a.score_fulltime_away, a.score_halftime_away,
        a.score_penalty_away,
        case when teams_home_winner is true then b.name
             when teams_away_winner is true then c.name
             else '' end as winner, 
        c.country as team_country, f.name as venue_name, f.city, 
        d.name as league_name, d.seasons_year, d.country_name, 
        e.code as league_code, a.league_round
        FROM fixtures a 
        LEFT JOIN teams b
        on a.teams_home_id = b.id
        AND a.league_season = b.season
        AND a.league_id = b.league_id
        LEFT JOIN teams c
        on a.teams_away_id = c.id
        AND a.league_season = c.season
        AND a.league_id = c.league_id
        LEFT JOIN leagues d
        on a.league_id = d.id
        AND a.league_season = d.seasons_year
        LEFT JOIN countries e
        on d.country_name = e.name
        LEFT JOIN venues f
        on a.venue_id = f.id
        and d.seasons_year = f.season
        where a.id = 868033
    """)
    
    fixtures_lineups_table_insert = ("""
        SELECT a.formation, a.team_id, a.season, b.name as team_name, 
        a.fixture_id, a.player_id, c.name as player_name, c.nationality,  
        a.player_number, a.player_pos, a.player_grid, a.lineup_type, 
        a.coach_id, a.coach_name, d.name as league_name, d.seasons_year,
        d.country_name, e.code as country_code
        FROM lineups a 
        LEFT JOIN fixtures g
        on a.fixture_id = g.id
        LEFT JOIN teams b
        on a.team_id = b.id
        AND a.season = b.season
        AND g.league_id = b.league_id
        LEFT JOIN players c
        on a.player_id = c.id
        LEFT JOIN leagues d
        on g.league_id = d.id
        AND g.league_season = d.seasons_year
        LEFT JOIN countries e
        on d.country_name = e.name  
        where fixture_id= 868033
        AND lineup_type='starter'
        order by a.team_id, a.player_id;
    """)
        
    countries_table_insert = ("""
        SELECT distinct code, name, flag
        FROM stage_countries 
        WHERE code is not null
    """)
    
    leagues_table_insert = ("""
        SELECT distinct league_id, league_name, league_type, league_logo,
        country_name, seasons_year, seasons_start, seasons_end, seasons_current 
        FROM stage_leagues 
        WHERE league_id is not null and seasons_year is not null
    """)
    
    teams_table_insert = ("""
        SELECT distinct team_id, season, team_name, team_code, team_country,
        league_id, team_founded, team_national, team_logo, venue_id 
        FROM stage_teams
        WHERE team_id is not null
        AND season is not null
        AND league_id is not null
    """)
    
    venues_table_insert = ("""
        select venue_id, season, venue_name, venue_address, venue_city,
        venue_capacity, venue_surface, venue_image  
        from(
        SELECT venue_id, season, venue_name, venue_address, venue_city,
        venue_capacity, venue_surface, venue_image, 
        ROW_NUMBER() OVER (PARTITION BY venue_id, season ORDER BY venue_id) as rn
        FROM stage_teams
        WHERE venue_id is not null
        AND season is not null
        ) a where a.rn = 1 
    """)

    players_table_insert = ("""
        SELECT player_id, player_name, player_firstname, player_lastname,
        player_birth_date, player_birth_place, player_birth_country,
        player_nationality, player_height, player_weight, player_injured,
        player_photo 
        FROM (
        SELECT player_id, player_name, player_firstname, player_lastname,
        player_birth_date, player_birth_place, player_birth_country,
        player_nationality, player_height, player_weight, player_injured,
        player_photo,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY statistics_league_season         DESC) as rn
        FROM stage_players
        WHERE player_id IS NOT NULL
        ) AS player_rank
        WHERE rn = 1
    """)
    
    fixtures_table_insert = ("""
        SELECT distinct fixture_id, CAST(substring(fixture_date, 1 , 10) AS DATE), CAST(substring(fixture_date, 12, 8) AS TIME),
        fixture_periods_first, fixture_periods_second, fixture_referee, fixture_status_elapsed,
        fixture_status_long, fixture_status_short, fixture_timestamp, fixture_timezone,
        fixture_venue_id, goals_away, goals_home,
        league_id, league_round, league_season, score_extratime_away, score_extratime_home, score_fulltime_away,
        score_fulltime_home, score_halftime_away, score_halftime_home, score_penalty_away, score_penalty_home,
        teams_away_id, teams_away_winner, teams_home_id, teams_home_winner 
        FROM stage_fixtures 
        WHERE fixture_id is not null
    """)
        
    lineups_table_insert = ("""
        SELECT distinct formation, team_id, season, coach_id, coach_name,
        coach_photo, fixture_id, lineup_player_id,
        lineup_player_number, lineup_player_pos,
        lineup_player_grid, lineup_type 
        FROM stage_lineups 
        WHERE fixture_id is not null 
        and team_id is not null 
        and lineup_player_id is not null 
    """)
