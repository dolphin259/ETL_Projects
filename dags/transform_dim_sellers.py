# from airflow.operators.postgres_operator import PostgresOperators
from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_sellers():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
    # Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_sellers")
    
    # Transform và làm sạch dữ liệu
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title()
    df['seller_state'] = df['seller_state'].str.upper()
    
    # Tạo surrogate key
    df['seller_key'] = df.index + 1
    
    # Thêm cột để theo dõi thay đổi (SCD Type 1)
    df['last_updated'] = pd.Timestamp.now().date()
    
    # Lưu dữ liệu vào bảng dim_sellers
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_sellers',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_sellers")
