import pandas as pd
# from airflow.operators.postgres_operator import PostgresOperator
from postgresql_operator import PostgresOperators
from postgresql_operator import PostgresOperators
def transform_dim_dates():
    warehouse_operator = PostgresOperators('postgres')
    
    # Tạo bảng dim_dates
    start_date = pd.Timestamp('2016-01-01')
    end_date = pd.Timestamp('2025-12-31')
    date_range = pd.date_range(start=start_date, end=end_date)
    
    df = pd.DataFrame({
        'date_key': date_range,
        'day': date_range.day,
        'month': date_range.month,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.strftime('%A'),
        'month_name': date_range.strftime('%B'),
        'is_weekend': date_range.dayofweek.isin([5, 6])
    })
    
    # Lưu dữ liệu vào bảng dim_dates
    warehouse_operator.save_data_to_postgres(
        df,
        'dim_dates',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã tạo và lưu dữ liệu vào dim_dates")
