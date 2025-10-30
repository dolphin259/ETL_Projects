from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_products():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    df_products = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_products")
    df_categories = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_product_category_name_translation")
    df = pd.merge(df_products, df_categories, on='product_category_name', how='left')
    df['product_category_name_english'] = df['product_category_name_english'].fillna('Unknown')
    df['product_weight_g'] = df['product_weight_g'].fillna(0)
    df['product_length_cm'] = df['product_length_cm'].fillna(0)
    df['product_height_cm'] = df['product_height_cm'].fillna(0)
    df['product_width_cm'] = df['product_width_cm'].fillna(0)
    df['product_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_products',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào dim_products")
