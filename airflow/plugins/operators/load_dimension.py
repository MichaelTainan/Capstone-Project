from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {} {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 is_append_mode=0,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.is_append_mode = is_append_mode

    def execute(self, context):
        """
          insert stage data into dimension table, and if is_append_mode is not a 
          append mode value, using truncate table first.
        """
        self.log.info("debugging message")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) 
        if self.is_append_mode == 0:
            self.log.info ('Truncate Dimension table')
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        self.log.info('Insert Stage Data into Dimension table')
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table, 
            self.sql_query
        )
        redshift.run(formatted_sql)
        self.log.info(f'Insert Dimension Table Success:{self.task_id}')