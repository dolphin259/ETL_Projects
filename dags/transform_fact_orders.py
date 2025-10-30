import pandas as pd
from postgresql_operator import PostgresOperators

def transform_fact_orders():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    df_orders = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_orders")
    df_order_items = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_order_items")
    df_order_payments = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_payments")
    df_customers = staging_operator.get_data_to_pd("SELECT customer_id, customer_zip_code_prefix FROM staging.stg_customers")
    df = pd.merge(df_orders, df_order_items, on='order_id', how='left')
    df = pd.merge(df, df_order_payments, on='order_id', how='left')
    df = pd.merge(df, df_customers, on='customer_id', how='left')
    print("Các cột trong DataFrame sau khi merge:")
    print(df.columns)
    df['order_status'] = df['order_status'].str.lower()
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_time'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['customer_key'] = df['customer_id']
    df['product_key'] = df['product_id']
    df['seller_key'] = df['seller_id']
    if 'customer_zip_code_prefix' in df.columns:
        df['geolocation_key'] = df['customer_zip_code_prefix']
    else:
        print("Cột customer_zip_code_prefix không tồn tại. Sử dụng giá trị mặc định.")
        df['geolocation_key'] = 'unknown'
    
    df['payment_key'] = df['payment_type'].astype('category').cat.codes + 1
    df['order_date_key'] = df['order_purchase_timestamp'].dt.date
    fact_columns = ['order_id', 'customer_key', 'product_key', 'seller_key', 'geolocation_key', 'payment_key', 'order_date_key',
                    'order_status', 'price', 'freight_value', 'total_amount', 'payment_value',
                    'delivery_time', 'estimated_delivery_time']
    
    df_fact = df[fact_columns]
    warehouse_operator.save_data_to_postgres(
        df_fact,
        'fact_orders',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào fact_orders")