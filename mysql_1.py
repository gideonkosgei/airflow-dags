from airflow.models import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime


default_args = {
    'start_date': datetime(2022, 1, 1)
}

with DAG('mysql_modify',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    start = DummyOperator(
        task_id='start'
    )

    # Create Mysql Table
    creating_table = MySqlOperator(
        task_id='creating_table',
        mysql_conn_id='local_mysql',
        sql='''
            CREATE TABLE IF NOT EXISTS gd_power (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL
            );
            '''
    )

    finish = DummyOperator(
        task_id='finish'
    )

    start >> creating_table >> finish
