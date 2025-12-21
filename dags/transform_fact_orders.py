import pandas as pd
from postgresql_operator import PostgresOperators

def transform_fact_orders():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df_orders = db_operator.get_data_to_pd("SELECT * FROM staging.orders")
    df_order_items = db_operator.get_data_to_pd("SELECT * FROM staging.order_items")
    df_order_payments = db_operator.get_data_to_pd("SELECT * FROM staging.payments")
    df_dim_customers = db_operator.get_data_to_pd("SELECT customer_key, customer_id FROM warehouse.dim_customers")
    df_dim_products = db_operator.get_data_to_pd("SELECT product_key, product_id FROM warehouse.dim_products")
    df_dim_sellers = db_operator.get_data_to_pd("SELECT seller_key, seller_id FROM warehouse.dim_sellers")
    df_dim_geolocation = db_operator.get_data_to_pd("SELECT geolocation_key, geolocation_zip_code_prefix FROM warehouse.dim_geolocation")
    df_dim_payments = db_operator.get_data_to_pd("SELECT payment_key, payment_type, payment_installments FROM warehouse.dim_payments")
    df_dim_dates = db_operator.get_data_to_pd("SELECT date_key, date(date_key) as date_only FROM warehouse.dim_dates")
    df = pd.merge(df_orders, df_order_items, on='order_id', how='left')
    df = pd.merge(df, df_order_payments, on='order_id', how='left')
    df['order_status'] = df['order_status'].str.lower()
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_days'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df = pd.merge(df, df_dim_customers, on='customer_id', how='left')
    df = pd.merge(df, df_dim_products, on='product_id', how='left')
    df = pd.merge(df, df_dim_sellers, on='seller_id', how='left')
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    df = pd.merge(df, df_dim_payments, on=['payment_type', 'payment_installments'], how='left')
    df['date_only'] = df['order_purchase_timestamp'].dt.date
    df = pd.merge(df, df_dim_dates, on='date_only', how='left', suffixes=('_order', '_dim'))
    df_cust_zip = db_operator.get_data_to_pd("SELECT customer_id, customer_zip_code_prefix FROM staging.customers")
    df = pd.merge(df, df_cust_zip, on='customer_id', how='left')
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str)
    df_dim_geolocation['geolocation_zip_code_prefix'] = df_dim_geolocation['geolocation_zip_code_prefix'].astype(str)
    df = pd.merge(df, df_dim_geolocation, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

    fact_columns = [
        'order_id', 
        'customer_key',
        'product_key',
        'seller_key',
        'payment_key',
        'date_key',
        'geolocation_key',
        'order_status',
        'price',
        'freight_value',
        'total_amount',
        'payment_valuee',
        'delivery_time_days',
        'estimated_delivery_days'
    ]

    final_fact_columns = [col for col in fact_columns if col in df.columns]
    df_fact = df[final_fact_columns].drop_duplicates()

    temp_table = 'fact_orders_temp'

    db_operator.save_data_to_postgres(
        df_fact,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.fact_orders;
    ALTER TABLE warehouse.{} RENAME TO fact_orders;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.fact_orders")