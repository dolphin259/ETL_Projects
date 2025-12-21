from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_geolocation():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df = db_operator.get_data_to_pd("SELECT * FROM staging.geolocation")
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()
    df = df.drop_duplicates(subset=['geolocation_zip_code_prefix'])
    df['geolocation_key'] = df.index + 1
    dim_columns = [
        'geolocation_key', 'geolocation_zip_code_prefix', 
        'geolocation_lat', 'geolocation_lng',
        'geolocation_city', 'geolocation_state'
    ]
    df_final = df[dim_columns]

    temp_table = 'dim_geolocation_temp'

    db_operator.save_data_to_postgres(
        df_final,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_geolocation;
    ALTER TABLE warehouse.{} RENAME TO dim_geolocation;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_geolocation")