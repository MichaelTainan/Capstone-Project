from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, CreateTableOperator)
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator

from subdag import load_dimension_table_dag
from helpers import SqlQueries
import logging

#def log_print():
#    logging.info(f"current = {os.path.join('/home/workspace/airflow', 'create_tables.sql')}")
        
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False    
}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval = '@monthly',
          #schedule_interval='0 0 1 * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables = CreateTableOperator(
    task_id='create_tables',
    redshift_conn_id = "redshift",
    sql_path = os.path.join("/home/workspace/airflow", "create_tables.sql"), 
    provide_context=True,
    dag=dag
)


"""
log_print_task = PythonOperator(
    task_id = "log_print",
    python_callable = log_print,
    dag=dag
)
"""

stage_countries_to_redshift = StageToRedshiftOperator(
    task_id='Stage_countries',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_countries",
    s3_bucket="capstonefootball",
    s3_prefix="countries",
    region="us-west-2",
    file_format="csv",
    delimiter = ",",
    ignore_headers = 1,
    provide_context=True,
    dag=dag
)

stage_leagues_to_redshift = StageToRedshiftOperator(
    task_id='Stage_leagues',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_leagues",
    s3_bucket="capstonefootball",
    s3_prefix="leagues",
    region="us-west-2",
    file_format="json",
    #json_path="s3://capstonefootball/json_path/jsonpath_leagues.json", 
    json_path='auto',
    provide_context=True,
    dag=dag
)

stage_teams_to_redshift = StageToRedshiftOperator(
    task_id='Stage_teams',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_teams",
    s3_bucket="capstonefootball",
    s3_prefix="league_teams",
    region="us-west-2",
    file_format="json",
    #json_path="s3://capstonefootball/json_path/jsonpath_teams.json", 
    json_path='auto',
    provide_context=True,
    dag=dag
)   

stage_players_to_redshift = StageToRedshiftOperator(
    task_id='Stage_players',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_players",
    s3_bucket="capstonefootball",
    s3_prefix="league_players",
    region="us-west-2",
    file_format="json",
    #json_path="s3://capstonefootball/json_path/jsonpath_players.json", 
    json_path='auto',
    provide_context=True,
    dag=dag
)

stage_fixtures_to_redshift = StageToRedshiftOperator(
    task_id='Stage_fixtures',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="Stage_fixtures",
    s3_bucket="capstonefootball",
    s3_prefix="league_fixtures",
    region="us-west-2",
    file_format="json",
    json_path='auto',
    provide_context=True,
    dag=dag
)

stage_standings_to_redshift = StageToRedshiftOperator(
    task_id='stage_standings',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_standings",
    s3_bucket="capstonefootball",
    s3_prefix="league_standings",
    region="us-west-2",
    file_format="json",
    json_path='auto',
    provide_context=True,
    dag=dag
)

stage_lineups_to_redshift = StageToRedshiftOperator(
    task_id='stage_lineups',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_lineups",
    s3_bucket="capstonefootball",
    s3_prefix="fixture_lineups",
    region="us-west-2",
    file_format="json",
    json_path='auto',
    provide_context=True,
    dag=dag
)

stage_statistics_to_redshift = StageToRedshiftOperator(
    task_id='stage_statistics',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="stage_statistics",
    s3_bucket="capstonefootball",
    s3_prefix="fixture_statistics",
    region="us-west-2",
    file_format="json",
    json_path='s3://capstonefootball/json_path/jsonpath_statistics.json',
    provide_context=True,
    dag=dag
)

country_task_id = "Load_countries_dim_table"
load_country_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        country_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.countries_table_insert,
        table="countries",
        is_append_mode=0,
        start_date=default_args['start_date'] #must have
    ),
    task_id=country_task_id,
    dag=dag
)

league_task_id = "Load_leagues_dim_table"
load_league_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        league_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.leagues_table_insert,
        table="leagues",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=league_task_id,
    dag=dag
)

team_task_id = "Load_teams_dim_table"
load_team_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        team_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.teams_table_insert,
        table="teams",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=team_task_id,
    dag=dag
)

venue_task_id = "Load_venues_dim_table"
load_venue_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        venue_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.venues_table_insert,
        table="venues",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=venue_task_id,
    dag=dag
)

player_task_id = "Load_players_dim_table"
load_player_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        player_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.players_table_insert,
        table="players",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=player_task_id,
    dag=dag
)

fixture_task_id = "Load_fixture_dim_table"
Load_fixture_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        fixture_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.fixtures_table_insert,
        table="fixtures",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=fixture_task_id,
    dag=dag
)

lineup_task_id = "Load_lineup_dim_table"
Load_lineup_dimension_table = SubDagOperator(
    subdag=load_dimension_table_dag(
        "capstone_dag",
        lineup_task_id,
        redshift_conn_id="redshift",
        sql_query=SqlQueries.lineups_table_insert,
        table="lineups",
        is_append_mode=0,
        start_date=default_args['start_date']
    ),
    task_id=lineup_task_id,
    dag=dag
)

load_player_statistic_table = LoadFactOperator(
    task_id='Load_player_statistics_fact_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.player_statistics_table_insert,
    table="player_statistics",
    provide_context=True,
    dag=dag
)

load_match_statistics_table = LoadFactOperator(
    task_id='Load_match_statistics_fact_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.match_statistics_table_insert,
    table="match_statistics",
    provide_context=True,
    dag=dag
)

load_league_standings_table = LoadFactOperator(
    task_id='Load_league_standings_fact_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.league_standings_table_insert,
    table="league_standings",
    provide_context=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables = ['countries','leagues','teams','venues','players', 'fixtures', 
               'lineups', 'player_statistics', 'match_statistics',
               'league_standings'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Create a dictionary where the keys are the stage tasks 
#and the values are the corresponding load tasks
task_pairs = {
    stage_countries_to_redshift: load_country_dimension_table,
    stage_leagues_to_redshift: load_league_dimension_table,
    #stage_teams_to_redshift: load_team_dimension_table,
    stage_players_to_redshift: load_player_dimension_table,
    stage_fixtures_to_redshift: Load_fixture_dimension_table,
    stage_lineups_to_redshift: Load_lineup_dimension_table
}

start_operator >> create_tables
create_tables >> list(task_pairs.keys())+[stage_teams_to_redshift, \
stage_statistics_to_redshift, stage_standings_to_redshift]
for stage_task, load_task in task_pairs.items():
    stage_task >> load_task >> load_player_statistic_table
stage_teams_to_redshift >> [load_team_dimension_table, \
load_venue_dimension_table ] >> load_player_statistic_table
stage_statistics_to_redshift >> load_player_statistic_table
stage_standings_to_redshift >> load_player_statistic_table
load_player_statistic_table >> load_match_statistics_table
load_match_statistics_table >> load_league_standings_table 
load_league_standings_table >> run_quality_checks
run_quality_checks >> end_operator
