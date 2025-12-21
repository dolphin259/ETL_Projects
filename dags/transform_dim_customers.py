import pandas as pd
from datetime import datetime, timedelta
from postgresql_operator import PostgresOperators

def transform_dim_customers():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df = db_operator.get_data_to_pd("SELECT * FROM staging.customers")
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

    dim_columns = [
        'customer_key',
        'customer_id',
        'customer_unique_id',
        'customer_zip_code_prefix',
        'customer_city',
        'customer_state',
        'effective_date',
        'end_date',
        'is_current'
    ]
    df = df[dim_columns]

    temp_table = 'dim_customers_temp'

    db_operator.save_data_to_postgres(
        df,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_customers;
    ALTER TABLE warehouse.{} RENAME TO dim_customers;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_customers")