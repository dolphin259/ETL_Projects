import pandas as pd
from datetime import datetime, timedelta
from postgresql_operator import PostgresOperators

def transform_dim_customers():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_customers")
    df['customer_unique_id'] = df['customer_unique_id'].astype(str)
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df['customer_city'] = df['customer_city'].str.title()
    df['customer_state'] = df['customer_state'].str.upper()
    df['customer_key'] = df.index + 1
    current_date = datetime.now().date()
    future_date = current_date + timedelta(days=365*10)
    df['effective_date'] = current_date
    df['end_date'] = future_date
    df['is_current'] = True
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_customers',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_customers")