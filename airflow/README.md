# Data Engineering Capstone Project with Apache Airflow

## 1. Develop airflow pipeline dags
### 1.1 Create Redshift Postgres DB and Table
I had to create the redshift DB first, then use create_tables.sql statement to create tables, so I create a create_table.py operator plugin to handle the job.
### 1.2 Copy S3 data into Redshift 
I use stage_redshift.py to handle the S3 copy job, copy all data from S3 to redshift, and have to provide a parameter to choice if used json format or csv to execute. 
### 1.3 Use Subdag to develop task to select stage data into fact table and dimension table
Because the dimension tables are the same behavior, I try building a subdag to achieve them.
### 1.4 Add data quality check task
Finally, I had to check the execution result to ensure the data quality.

## 2. About the Airflow Job
In the DAG, add default parameters according to these guidelines
1.The DAG does not have dependencies on past runs
2.On failure, the task are retried 3 times
3.Retries happen every 5 minutes
4.Catchup is turned off
5.Do not email on retry

So I practice to use default_args in the DAG to match the request, with the following keys:
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False    
}

The dag follows the data flow provided in the instructions, all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.
