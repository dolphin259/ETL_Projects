from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_payments():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df = db_operator.get_data_to_pd("SELECT * FROM staging.payments")
    df['payment_type'] = df['payment_type'].str.lower()
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    df['payment_key'] = df.index + 1
    df = df.drop_duplicates(subset=['payment_type', 'payment_installments'])
    dim_columns = ['payment_key', 'payment_type', 'payment_installments']
    df_final = df[dim_columns]

    temp_table = 'dim_payments_temp'

    db_operator.save_data_to_postgres(
        df_final,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_payments;
    ALTER TABLE warehouse.{} RENAME TO dim_payments;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_payments")