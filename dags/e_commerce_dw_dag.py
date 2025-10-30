from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from transform_dim_customers import transform_dim_customers
from transform_dim_products import transform_dim_products
from transform_dim_sellers import transform_dim_sellers
from transform_dim_geolocation import transform_dim_geolocation
from transform_dim_dates import transform_dim_dates
from transform_dim_payments import transform_dim_payments
from transform_fact_orders import transform_fact_orders
from extract_data import extract_and_load_to_staging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# ... các import khác giữ nguyên ...

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'e_commerce_dw_etl',
    default_args=default_args,
    description='ETL process for E-commerce Data Warehouse',
    schedule_interval=timedelta(days=1),
) as dag:

    # Task Extract
    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_to_staging',
            python_callable=extract_and_load_to_staging,
            provide_context=True,
        )

    # Task Group Transform
    with TaskGroup("transform") as transform_group:
        task_dim_customers = PythonOperator(
            task_id='transform_dim_customers',
            python_callable=transform_dim_customers,
        )
        
        task_dim_products = PythonOperator(
            task_id='transform_dim_products',
            python_callable=transform_dim_products,
        )
        
        task_dim_sellers = PythonOperator(
            task_id='transform_dim_sellers',
            python_callable=transform_dim_sellers,
        )
        
        task_dim_geolocation = PythonOperator(
            task_id='transform_dim_geolocation',
            python_callable=transform_dim_geolocation,
        )
        
        task_dim_dates = PythonOperator(
            task_id='transform_dim_dates',
            python_callable=transform_dim_dates,
        )
        
        task_dim_payments = PythonOperator(
            task_id='transform_dim_payments',
            python_callable=transform_dim_payments,
        )

    # Task Group Load
    with TaskGroup("load") as load_group:
        task_fact_orders = PythonOperator(
            task_id='transform_fact_orders',
            python_callable=transform_fact_orders,
        )

    # Thiết lập dependencies
    extract_group >> transform_group >> load_group


