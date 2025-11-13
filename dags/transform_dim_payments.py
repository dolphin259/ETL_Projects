from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_payments():
    # SỬA: Dùng 1 Connection ID
    db_operator = PostgresOperators('data_warehouse_db')

    # Tạo schema WAREHOUSE nếu chưa có
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    
    # SỬA: Đọc từ staging.payments
    df = db_operator.get_data_to_pd("SELECT * FROM staging.payments")
    
    # Transform (Giữ nguyên)
    df['payment_type'] = df['payment_type'].str.lower()
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    
    # Tạo surrogate key
    df['payment_key'] = df.index + 1
    
    # Loại bỏ các bản ghi trùng lặp (Giữ nguyên)
    df = df.drop_duplicates(subset=['payment_type', 'payment_installments'])
    
    # Sắp xếp cột (Tùy chọn)
    dim_columns = ['payment_key', 'payment_type', 'payment_installments']
    df_final = df[dim_columns]

    # SỬA: Dùng kỹ thuật "load-and-swap" an toàn
    temp_table = 'dim_payments_temp'
    
    # 1. Nạp vào bảng tạm
    db_operator.save_data_to_postgres(
        df_final,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )
    
    # 2. Hoán đổi (Swap)
    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_payments;
    ALTER TABLE warehouse.{} RENAME TO dim_payments;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_payments")