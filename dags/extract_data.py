from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

from mysql_operator import MySQLOperators
from postgresql_operator import PostgresOperators


def extract_and_load_to_staging(**kwargs):
    source_operator = MySQLOperators('mysql')
    staging_operator = PostgresOperators('postgres')
    
    tables = [
        "product_category_name_translation",
        "geolocation",
        "sellers",
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "order_reviews"
    ]
    
    for table in tables:
        df = source_operator.get_data_to_pd(f"SELECT * FROM {table}")
        staging_operator.save_data_to_postgres(
            df,
            f"stg_{table}",
            schema='staging',
            if_exists='replace'
        )
        
        print(f"Đã trích xuất và lưu bảng {table} từ MySQL vào PostgreSQL public")

