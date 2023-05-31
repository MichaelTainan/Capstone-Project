from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def checkCount(self, redshift):
        """
          Check the insert result if have record in the table
        """
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) 
        
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if records is None:
                self.log.error(f"{table} records is None type")
            elif len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data qurlity check failed. {table} return no results")
                raise ValueError(f"Data qurlity check failed. {table} return no results")
            else:
                self.log.info(f'{table} data Quality check pass. It have {records[0][0]} records')
    
    def switchQuerySQL(self, value):
        return {
            'countries': "SELECT * FROM(SELECT name, ROW_NUMBER OVER(PARTITION BY \
                          name) AS rn FROM countries) t WHERE t.rn >1;",
            'leagues': "SELECT * FROM(SELECT id, ROW_NUMBER OVER(PARTITION BY \
                        id, seasons_year) AS rn FROM leagues) t WHERE t.rn >1;",
            'teams': "SELECT * FROM(SELECT id, ROW_NUMBER OVER(PARTITION BY \
                      id, season, league_id) AS rn FROM teams) t WHERE \
                      t.rn >1;",
            'venues': "SELECT * FROM(SELECT id, ROW_NUMBER OVER(PARTITION BY \
                       id, season) AS rn FROM venues) t WHERE \
                       t.rn >1;",
            'players': "SELECT * FROM(SELECT id, ROW_NUMBER OVER(PARTITION BY \
                        id) AS rn FROM players) t WHERE \
                        t.rn >1;",
            'fixtures': "SELECT * FROM(SELECT id, ROW_NUMBER OVER(PARTITION BY \
                        id) AS rn FROM fixtures) t WHERE \
                        t.rn >1;",
            'lineups': "SELECT * FROM(SELECT fixture_id, ROW_NUMBER OVER \
                        (PARTITION BY fixture_id, team_id, player_id) AS rn \
                        FROM lineups) t WHERE t.rn >1;",
            'player_statistics': "SELECT * FROM(SELECT player_id, ROW_NUMBER \
                        OVER(PARTITION BY \
                        player_id, team_id, league_id, league_season) AS rn \
                        FROM player_statistics) t WHERE t.rn >1;",
            'match_statistics': "SELECT * FROM(SELECT fixture_id, ROW_NUMBER \
                        OVER(PARTITION BY \
                        fixture_id, team_id) AS rn \
                        FROM match_statistics) t WHERE t.rn >1;",
            'league_standings': "SELECT * FROM(SELECT league_id, ROW_NUMBER \
                        OVER(PARTITION BY \
                        league_id, league_season, standings_index, team_id) AS rn \
                        FROM league_standings) t WHERE t.rn >1;",
        }.get(value, '')
    
    def checkUniqueKey(self, redshift):
        """
          Check the table if have PK duplicate row in the table
        """
        for table in self.tables:
            query = self.switchQuerySQL(table)
            records = redshift.get_records(query)
            if records is None:
                self.log.error(f"{table} records is None type")
            elif len(records) >= 1:
                self.log.error(f"Data qurlity check failed. {table} UniqueKey duplicate")
                raise ValueError(f"Data qurlity check failed. {table} return no results")
            else:
                self.log.info(f'{table} Unique Key check pass.')
        
    def execute(self, context):
        """
          Check the insert result if have record in the table
        """
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) 
        self.checkCount(redshift)
        self.checkUniqueKey(redshift)