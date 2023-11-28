from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import create_table_snowflake 
import model  

def test_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowid')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT CURRENT_VERSION()")
        one_row = cur.fetchone()
        print("Snowflake version:", one_row)
    finally:
        cur.close()
        conn.close()

def test_mongo_conn():
    hook = MongoHook(conn_id='mongoid')
    client = hook.get_conn()
    db = client.get_default_database()
    print("MongoDB Database:", db.name)

def create_snowflake_tables():
    create_table_snowflake.main()  

def execute_model_dimension_fact():
    model.main()  

def read_sql_file():
    dir_path = os.path.dirname(os.path.realpath(__file__))  
    file_path = os.path.join(dir_path, 'uber_eat_views.sql')
    with open(file_path, 'r') as file:
        return file.read()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('main_conn_task',
          default_args=default_args,
          schedule_interval=None,  # Ce DAG ne s'exécutera pas automatiquement
          )

snowflake_task = PythonOperator(task_id='test_snowflake_connection',
                                python_callable=test_snowflake_conn,
                                dag=dag)

mongo_task = PythonOperator(task_id='test_mongo_connection',
                            python_callable=test_mongo_conn,
                            dag=dag)

create_tables_task = PythonOperator(task_id='create_snowflake_tables',
                                    python_callable=create_snowflake_tables,
                                    dag=dag)

# tâche pour exécuter le script model.py
upload_dim_fact = PythonOperator(task_id='execute_model_dimension_fact',
                            python_callable=execute_model_dimension_fact,
                            dag=dag)

# Nouvelle tâche pour exécuter le script SQL
create_views_task = SnowflakeOperator(
    task_id='execute_sql_views',
    snowflake_conn_id='snowid',
    sql=read_sql_file(),
    dag=dag
)
#  l'ordre d'exécution des tâches
snowflake_task >> mongo_task >> create_tables_task >> upload_dim_fact >> create_views_task
