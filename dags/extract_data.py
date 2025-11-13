# dags/extract_data.py (PHIÊN BẢN MỚI CHO 1 RDS POSTGRES)
import boto3, zipfile, os, pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook # SỬA: CHỈ DÙNG POSTGRES
from sqlalchemy.exc import IntegrityError

AWS_CONN_ID = "aws_default"
DB_CONN_ID = "data_warehouse_db" # SỬA: CHỈ DÙNG 1 CONNECTION TỚI RDS

TMP_DIR = "/opt/airflow/temp_data"
TMP_ZIP_FILE_PATH = os.path.join(TMP_DIR, "data.zip")
EXTRACTED_DIR_PATH = os.path.join(TMP_DIR, "extracted_files")

def extract_and_load_to_staging(**kwargs):
    """
    Task này được Lambda kích hoạt.
    Nó tải file .zip, giải nén, và nạp 9 file CSV vào 
    SCHEMA 'staging' của database Postgres (RDS).
    """
    
    # 1. Lấy thông tin file từ 'conf' mà Lambda gửi qua
    dag_run_conf = kwargs.get('dag_run').conf
    if not dag_run_conf:
        raise ValueError("DAG phải được trigger từ S3 (Lambda).")
        
    s3_bucket = dag_run_conf.get('s3_bucket')
    s3_key = dag_run_conf.get('s3_key')

    if not s3_key or not s3_bucket:
        raise ValueError("Không tìm thấy 's3_key' hoặc 's3_bucket' trong 'conf'.")

    print(f"Bắt đầu xử lý file: {s3_key} từ bucket: {s3_bucket}")

    # 2. Tạo thư mục tạm (nếu chưa có)
    os.makedirs(EXTRACTED_DIR_PATH, exist_ok=True)

    # 3. Dùng S3Hook để tải file .zip từ S3 về
    print(f"Đang tải file {s3_key} từ S3...")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn() 
    s3_client.download_file(s3_bucket, s3_key, TMP_ZIP_FILE_PATH)
    
    print(f"Đã tải file về {TMP_ZIP_FILE_PATH}")

    # 4. Giải nén file .zip
    print(f"Đang giải nén file vào {EXTRACTED_DIR_PATH}...")
    with zipfile.ZipFile(TMP_ZIP_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR_PATH)
        
    print("Đã giải nén thành công.")

    # 5. NẠP VÀ HOÁN ĐỔI (LOAD-AND-SWAP) VÀO POSTGRES
    print("Bắt đầu nạp dữ liệu (an toàn) vào Schema 'staging' (Postgres)...")
    
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID) # SỬA: Dùng PostgresHook
    engine = pg_hook.get_sqlalchemy_engine() # SỬA: Dùng engine Postgres
    
    # Tạo schema STAGING nếu chưa có
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS staging;")

    csv_to_table_map = {
        "olist_customers_dataset.csv": "customers",
        "olist_geolocation_dataset.csv": "geolocation",
        "olist_order_items_dataset.csv": "order_items",
        "olist_order_payments_dataset.csv": "payments",
        "olist_order_reviews_dataset.csv": "order_reviews",
        "olist_orders_dataset.csv": "orders",
        "olist_products_dataset.csv": "products",
        "olist_sellers_dataset.csv": "sellers",
        "product_category_name_translation.csv": "product_category_name_translation"
    }

    for file_name, table_name in csv_to_table_map.items():
        file_path = os.path.join(EXTRACTED_DIR_PATH, file_name)
        temp_table_name = f"{table_name}_temp" # Tên bảng tạm
        
        try:
            if not os.path.exists(file_path):
                print(f"Cảnh báo: Không tìm thấy file {file_name} trong file .zip. Bỏ qua.")
                continue

            print(f"Đang nạp file {file_name} vào bảng TẠM: staging.{temp_table_name}...")
            
            # --- BƯỚC 1: NẠP VÀO BẢNG TẠM ---
            df = pd.read_csv(file_path)
            df = df.where(pd.notnull(df), None)
            
            # Nạp vào bảng TẠM, dùng 'replace' ở đây là an toàn
            # SỬA: Thêm schema='staging'
            df.to_sql(temp_table_name, con=engine, schema='staging', if_exists='replace', index=False)
            
            print(f"Nạp xong bảng tạm. Bắt đầu hoán đổi (swap)...")

            # --- BƯỚC 3: HOÁN ĐỔI (SWAP) BẰNG GIAO DỊCH SQL ---
            swap_sql = f"""
            BEGIN;
            DROP TABLE IF EXISTS staging.{table_name};
            ALTER TABLE staging.{temp_table_name} RENAME TO {table_name};
            COMMIT;
            """
            pg_hook.run(swap_sql) # Chạy lệnh SQL
            
            print(f"Hoán đổi thành công. Bảng staging.{table_name} đã được cập nhật.")
            
        except Exception as e:
            # Nếu có lỗi, dọn dẹp bảng tạm và báo lỗi
            print(f"Lỗi khi xử lý {file_name}: {e}. Đang dọn dẹp bảng tạm...")
            pg_hook.run(f"DROP TABLE IF EXISTS staging.{temp_table_name};")
            raise

    # 6. Xóa file và thư mục tạm
    print("Đang dọn dẹp file tạm...")
    os.remove(TMP_ZIP_FILE_PATH)
    # (Bạn có thể thêm code để xóa thư mục 'EXTRACTED_DIR_PATH' nếu muốn)

    print("Hoàn tất task extract_and_load_to_staging.")