from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_geolocation():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_geolocation")
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()
    df = df.drop_duplicates(subset=['geolocation_zip_code_prefix'])
    df['geolocation_key'] = df.index + 1
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_geolocation',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_geolocation")
