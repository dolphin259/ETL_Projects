from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_sellers():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df = db_operator.get_data_to_pd("SELECT * FROM staging.sellers")
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title()
    df['seller_state'] = df['seller_state'].str.upper()
    df['seller_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()
    dim_columns = [
        'seller_key', 'seller_id', 'seller_zip_code_prefix', 
        'seller_city', 'seller_state', 'last_updated'
    ]
    df_final = df[dim_columns]

    temp_table = 'dim_sellers_temp'

    db_operator.save_data_to_postgres(
        df_final,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_sellers;
    ALTER TABLE warehouse.{} RENAME TO dim_sellers;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_sellers")