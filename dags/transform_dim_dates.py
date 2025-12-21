import pandas as pd
from postgresql_operator import PostgresOperators

def transform_dim_dates():
    db_operator = PostgresOperators('data_warehouse_db')
    db_operator.execute_query("CREATE SCHEMA IF NOT EXISTS warehouse;")
    start_date = pd.Timestamp('2016-01-01')
    end_date = pd.Timestamp('2025-12-31')
    date_range = pd.date_range(start=start_date, end=end_date)
    
    df = pd.DataFrame({
        'date_key': date_range.date,
        'day': date_range.day,
        'month': date_range.month,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.strftime('%A'),
        'month_name': date_range.strftime('%B'),
        'is_weekend': date_range.dayofweek.isin([5, 6])
    })

    temp_table = 'dim_dates_temp'

    db_operator.save_data_to_postgres(
        df,
        temp_table,
        schema='warehouse',
        if_exists='replace'
    )

    swap_sql = """
    BEGIN;
    DROP TABLE IF EXISTS warehouse.dim_dates;
    ALTER TABLE warehouse.{} RENAME TO dim_dates;
    COMMIT;
    """.format(temp_table)
    
    db_operator.execute_query(swap_sql)
    
    print("Đã tạo và lưu dữ liệu vào warehouse.dim_dates")