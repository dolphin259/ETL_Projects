# from airflow.operators.postgres_operator import PostgresOperators
from postgresql_operator import PostgresOperators
def transform_dim_payments():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
    # Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_payments")
    
    # Transform và làm sạch dữ liệu
    df['payment_type'] = df['payment_type'].str.lower()
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    
    # Tạo surrogate key
    df['payment_key'] = df.index + 1
    
    # Loại bỏ các bản ghi trùng lặp
    df = df.drop_duplicates(subset=['payment_type', 'payment_installments'])
    
    # Lưu dữ liệu vào bảng dim_payments
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_payments',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_payments")
