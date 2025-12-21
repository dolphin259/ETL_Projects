from postgresql_operator import PostgresOperators
import pandas as pd

def transform_dim_products():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    df_products = db_operator.get_data_to_pd("SELECT * FROM staging.products")
    df_categories = db_operator.get_data_to_pd("SELECT * FROM staging.product_category_name_translation")
    df = pd.merge(df_products, df_categories, on='product_category_name', how='left')
    df['product_category_name_english'] = df['product_category_name_english'].fillna('Unknown')
    df['product_weight_g'] = df['product_weight_g'].fillna(0)
    df['product_length_cm'] = df['product_length_cm'].fillna(0)
    df['product_height_cm'] = df['product_height_cm'].fillna(0)
    df['product_width_cm'] = df['product_width_cm'].fillna(0)
    df['product_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()
    dim_columns = [
        'product_key', 'product_id', 'product_category_name', 
        'product_category_name_english', 'product_weight_g', 
        'product_length_cm', 'product_height_cm', 'product_width_cm', 'last_updated'
    ]
    final_columns = [col for col in dim_columns if col in df.columns]
    df_final = df[final_columns]

    temp_table = 'dim_products_temp'

    db_operator.save_data_to_postgres(
        df_final,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_products;
    ALTER TABLE warehouse.{} RENAME TO dim_products;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã transform và lưu dữ liệu vào warehouse.dim_products")