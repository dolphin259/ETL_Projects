# from airflow.operators.postgres_operator import PostgresOperators
from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_geolocation():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
    # Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_geolocation")
    
    # Transform và làm sạch dữ liệu
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()
    
    # Loại bỏ các bản ghi trùng lặp
    df = df.drop_duplicates(subset=['geolocation_zip_code_prefix'])
    
    # Tạo surrogate key
    df['geolocation_key'] = df.index + 1
    
    # Lưu dữ liệu vào bảng dim_geolocation
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_geolocation',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_geolocation")
