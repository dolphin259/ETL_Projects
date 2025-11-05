import pandas as pd
from datetime import datetime, timedelta
from postgresql_operator import PostgresOperators

def transform_dim_customers():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
   # Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_customers")
    
    # Transform và làm sạch dữ liệu
    df['customer_unique_id'] = df['customer_unique_id'].astype(str)
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df['customer_city'] = df['customer_city'].str.title()
    df['customer_state'] = df['customer_state'].str.upper()
    
    # Tạo surrogate key
    df['customer_key'] = df.index + 1
    
    # Thêm cột để theo dõi thay đổi (SCD Type 2)
    current_date = datetime.now().date()
    future_date = current_date + timedelta(days=365*10)  # Sử dụng ngày khoảng 10 năm trong tương lai
    
    df['effective_date'] = current_date
    df['end_date'] = future_date
    df['is_current'] = True
    
    # Lưu dữ liệu vào bảng dim_customers
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_customers',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_customers")