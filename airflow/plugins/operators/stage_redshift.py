from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    staging_copy = ("""
        copy {} from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF
        REGION '{}'
        {};    
    """)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_prefix="",
                 region="",
                 file_format="json",
                 json_path="auto",
                 delimiter=",",
                 ignore_headers=1,
                 s3_folder_depth="year",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.region = region
        self.file_format = file_format.lower()
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.s3_folder_depth = s3_folder_depth
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
          insert S3 data into stage table, And check if it is json format.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)        
        
        self.log.info(f"Check file format '{self.file_format}'")
        if self.file_format == "json":
            format_string = f"FORMAT AS JSON '{self.json_path}'"
        elif self.file_format == "csv": 
            format_string = f"FORMAT AS CSV IGNOREHEADER {self.ignore_headers} DELIMITER '{self.delimiter}'" 
        else:
            raise ValueError(f"Invalid file_format: {self.file_format}. Supported formats are 'json' and 'csv'.")
        
        s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}"
        if self.execution_date:
            if self.s3_folder_depth =="year":
                s3_path += f"/{self.execution_date.strftime('%Y')}"
            elif self.s3_folder_depth =="month":
                s3_path += f"/{self.execution_date.strftime('%Y')}/{self.execution_date.strftime('%m')}"
                
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f"s3_path = {s3_path}")
        self.log.info('Copying data from S3 to Redshift')
            
        formatted_sql = StageToRedshiftOperator.staging_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            format_string
        )
        redshift.run(formatted_sql)



