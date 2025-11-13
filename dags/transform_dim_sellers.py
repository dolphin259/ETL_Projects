from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_sellers():
    # SỬA: Dùng 1 Connection ID
    db_operator = PostgresOperators('data_warehouse_db')
    
    # Tạo schema WAREHOUSE nếu chưa có
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    
    # SỬA: Đọc từ staging.sellers
    df = db_operator.get_data_to_pd("SELECT * FROM staging.sellers")
    
    # Transform (Giữ nguyên)
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title()
    df['seller_state'] = df['seller_state'].str.upper()
    
    # Tạo surrogate key
    df['seller_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()
    
    # Sắp xếp cột (Tùy chọn)
    dim_columns = [
        'seller_key', 'seller_id', 'seller_zip_code_prefix', 
        'seller_city', 'seller_state', 'last_updated'
    ]
    df_final = df[dim_columns]

    # SỬA: Dùng kỹ thuật "load-and-swap" an toàn
    temp_table = 'dim_sellers_temp'
    
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
    DROP TABLE IF EXISTS warehouse.dim_sellers;
    ALTER TABLE warehouse.{} RENAME TO dim_sellers;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_sellers")