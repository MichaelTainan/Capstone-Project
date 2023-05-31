from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.udacity_plugin import LoadDimensionOperator

def load_dimension_table_dag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    table,
    sql_query,
    is_append_mode,    
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    load_dimension_table = LoadDimensionOperator(
        task_id=f"load_{table}_dimension_table",
        redshift_conn_id=redshift_conn_id,
        sql_query=sql_query,
        table=table,
        is_append_mode=is_append_mode,
        dag=dag
    )
    
    load_dimension_table
    
    return dag