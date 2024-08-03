from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.table_create_delete import TableCreateDelete
from udacity.common import delete_create_sql as dcsql

st_date="01/11/2018"
run_date = datetime.datetime.strptime(st_date, "%d/%m/%Y")
sql = dcsql()

default_args = {
    'owner': 'slawterc',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Delete and Create Tables',
    schedule_interval='0 * * * *'
)
def table_delete_create():

    start_operator = DummyOperator(task_id='Begin_execution')

    drop_tables = TableCreateDelete(
        task_id='Dropping Tables',
        redshift_conn_id='redshift',
        drop=True,
        sql_list=sql.drop_table_queries)

    create_tables = TableCreateDelete(
        task_id='Creating Tables',
        redshift_conn_id='redshift',
        drop=False,
        sql_list=sql.create_table_queries
    )

    end_operator = DummyOperator(task_id='End_execution')

    chain(start_operator,drop_tables,create_tables,end_operator)