import pandas as pd
from postgresql_operator import PostgresOperators

def transform_fact_orders():
    # SỬA: Dùng 1 Connection ID
    db_operator = PostgresOperators('data_warehouse_db')
    
    # Tạo schema WAREHOUSE nếu chưa có
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")

    # SỬA: Đọc từ các bảng staging (bỏ 'stg_')
    df_orders = db_operator.get_data_to_pd("SELECT * FROM staging.orders")
    df_order_items = db_operator.get_data_to_pd("SELECT * FROM staging.order_items")
    df_order_payments = db_operator.get_data_to_pd("SELECT * FROM staging.payments")
    
    # SỬA: Đọc từ các bảng dim (để lấy surrogate key)
    df_dim_customers = db_operator.get_data_to_pd("SELECT customer_key, customer_id FROM warehouse.dim_customers")
    df_dim_products = db_operator.get_data_to_pd("SELECT product_key, product_id FROM warehouse.dim_products")
    df_dim_sellers = db_operator.get_data_to_pd("SELECT seller_key, seller_id FROM warehouse.dim_sellers")
    df_dim_geolocation = db_operator.get_data_to_pd("SELECT geolocation_key, geolocation_zip_code_prefix FROM warehouse.dim_geolocation")
    df_dim_payments = db_operator.get_data_to_pd("SELECT payment_key, payment_type, payment_installments FROM warehouse.dim_payments")
    df_dim_dates = db_operator.get_data_to_pd("SELECT date_key, date(date_key) as date_only FROM warehouse.dim_dates") # Lấy date_key

    # --- BẮT ĐẦU TRANSFORM ---
    
    # 1. Xử lý Bảng Orders (Bảng chính)
    df = pd.merge(df_orders, df_order_items, on='order_id', how='left')
    df = pd.merge(df, df_order_payments, on='order_id', how='left')
    
    # 2. Convert kiểu dữ liệu (Giữ nguyên)
    df['order_status'] = df['order_status'].str.lower()
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    
    # 3. Tính toán metrics (Giữ nguyên)
    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_days'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    
    # --- THAY THẾ BẰNG SURROGATE KEY ---
    
    # 4. Lấy Customer Key
    df = pd.merge(df, df_dim_customers, on='customer_id', how='left')
    
    # 5. Lấy Product Key
    df = pd.merge(df, df_dim_products, on='product_id', how='left')
    
    # 6. Lấy Seller Key
    df = pd.merge(df, df_dim_sellers, on='seller_id', how='left')
    
    # 7. Lấy Payment Key
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    df = pd.merge(df, df_dim_payments, on=['payment_type', 'payment_installments'], how='left')

    # 8. Lấy Date Key
    df['date_only'] = df['order_purchase_timestamp'].dt.date
    df = pd.merge(df, df_dim_dates, on='date_only', how='left', suffixes=('_order', '_dim'))
    
    # 9. Lấy Geolocation Key (Hơi phức tạp, phải lấy zip_code từ customers trước)
    df_cust_zip = db_operator.get_data_to_pd("SELECT customer_id, customer_zip_code_prefix FROM staging.customers")
    df = pd.merge(df, df_cust_zip, on='customer_id', how='left')
    df = pd.merge(df, df_dim_geolocation, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

    # 10. Chọn các cột cuối cùng cho bảng Fact
    fact_columns = [
        'order_id', 
        'customer_key',  # Đã thay thế
        'product_key',   # Đã thay thế
        'seller_key',    # Đã thay thế
        'payment_key',   # Đã thay thế
        'date_key',      # Đã thay thế
        'geolocation_key', # Đã thay thế
        'order_status', 
        'price', 
        'freight_value', 
        'total_amount', 
        'payment_value',
        'delivery_time_days', 
        'estimated_delivery_days'
    ]
    
    # Lấy các cột có tồn tại trong df (tránh lỗi)
    final_fact_columns = [col for col in fact_columns if col in df.columns]
    df_fact = df[final_fact_columns].drop_duplicates() # Bỏ trùng
    
    # SỬA: Dùng kỹ thuật "load-and-swap" an toàn
    temp_table = 'fact_orders_temp'
    
    # 1. Nạp vào bảng tạm
    db_operator.save_data_to_postgres(
        df_fact,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )
    
    # 2. Hoán đổi (Swap)
    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.fact_orders;
    ALTER TABLE warehouse.{} RENAME TO fact_orders;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.fact_orders")