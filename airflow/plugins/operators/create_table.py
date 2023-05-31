from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#358140'
    #sql_statment_file = '/home/workspace/airflow/create_tables.sql'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_path="", 
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_path=sql_path
        #self.sql_path=self.sql_statment_file
        
    def execute(self, context):
        """
        Create All Tables in redshift DB, if it has not the same tables in it.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Create Tables')
        self.log.info("{}".format(self.sql_path))
        fd = open(self.sql_path, 'r')
        sql_file = fd.read()
        fd.close()
        
        sql_commands = sql_file.split(';')
        
        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
        