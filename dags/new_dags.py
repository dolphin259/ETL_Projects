from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_world():
    print("Hello World! This is a test DAG for Git-Sync.")


with DAG(
    dag_id='example_dag',
    default_args=default_args,
    description='Một DAG ví dụ để test git-sync',
    schedule_interval='@daily', 
    catchup=False
) as dag:

    task_hello_world = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world
    )
    task_hello_world
