import boto3
import zipfile
import os
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook 
from sqlalchemy.exc import IntegrityError # Import thêm

# --- ĐỊNH NGHĨA CÁC BIẾN ---

# Tên các connection bạn đã tạo trong Airflow
AWS_CONN_ID = "aws_default"
STAGING_DB_CONN_ID = "staging_db_mysql" # Conn Id trỏ đến MySQL (olist-source-db-v2)

# Thư mục tạm trên máy chủ EC2 (bên trong Docker)
TMP_DIR = "/opt/airflow/temp_data"
TMP_ZIP_FILE_PATH = os.path.join(TMP_DIR, "data.zip")
EXTRACTED_DIR_PATH = os.path.join(TMP_DIR, "extracted_files")


# --- ĐỊNH NGHĨA LẠI TASK EXTRACT (PHIÊN BẢN AN TOÀN) ---

def extract_and_load_to_staging(**kwargs):
    """
    Task này được Lambda kích hoạt.
    Nó tải file .zip, giải nén, và nạp 9 file CSV vào Staging (MySQL)
    một cách an toàn bằng kỹ thuật load-and-swap.
    """
    
    # 1. Lấy thông tin file từ 'conf' mà Lambda gửi qua
    dag_run_conf = kwargs.get('dag_run').conf
    if not dag_run_conf:
        raise ValueError("DAG này phải được kích hoạt thủ công với JSON hoặc tự động từ S3 (Lambda).")
        
    s3_bucket = dag_run_conf.get('s3_bucket')
    s3_key = dag_run_conf.get('s3_key') # Tên file, ví dụ: "data.zip"

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

    # 5. NẠP VÀ HOÁN ĐỔI (LOAD-AND-SWAP) AN TOÀN
    print("Bắt đầu nạp dữ liệu (an toàn) vào Staging (MySQL)...")
    
    mysql_hook = MySqlHook(mysql_conn_id=STAGING_DB_CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine() # Để dùng Pandas
    
    csv_files = [
        "olist_customers_dataset.csv", "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv", "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv",
        "olist_products_dataset.csv", "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]

    for file_name in csv_files:
        table_name = file_name.replace(".csv", "").replace("_dataset", "")
        temp_table_name = f"{table_name}_temp" # Tên bảng tạm
        
        try:
            file_path = os.path.join(EXTRACTED_DIR_PATH, file_name)
            
            if not os.path.exists(file_path):
                print(f"Cảnh báo: Không tìm thấy file {file_name} trong file .zip. Bỏ qua.")
                continue

            print(f"Đang nạp file {file_name} vào bảng TẠM: {temp_table_name}...")
            
            # --- BƯỚC 1: NẠP VÀO BẢNG TẠM ---
            df = pd.read_csv(file_path)
            df = df.where(pd.notnull(df), None)
            
            # Nạp vào bảng TẠM, dùng 'replace' ở đây là an toàn
            df.to_sql(temp_table_name, con=engine, if_exists='replace', index=False)
            
            print(f"Nạp xong bảng tạm. Bắt đầu hoán đổi (swap)...")

            # --- BƯỚC 2: KIỂM TRA (Tùy chọn, có thể thêm) ---
            # Ví dụ: Kiểm tra xem bảng tạm có dữ liệu không
            # ...

            # --- BƯỚC 3: HOÁN ĐỔI (SWAP) BẰNG GIAO DỊCH SQL ---
            # Dữ liệu cũ (table_name) chỉ bị xóa SAU KHI 
            # dữ liệu mới (temp_table_name) đã nạp thành công.
            swap_sql = f"""
            BEGIN;
            DROP TABLE IF EXISTS {table_name};
            ALTER TABLE {temp_table_name} RENAME TO {table_name};
            COMMIT;
            """
            mysql_hook.run(swap_sql) # Chạy lệnh SQL
            
            print(f"Hoán đổi thành công. Bảng {table_name} đã được cập nhật.")
            
        except Exception as e:
            # Nếu có lỗi, dọn dẹp bảng tạm và báo lỗi
            print(f"Lỗi khi xử lý {file_name}: {e}. Đang dọn dẹp bảng tạm...")
            mysql_hook.run(f"DROP TABLE IF EXISTS {temp_table_name};")
            # Ném lỗi để Airflow biết task này thất bại
            raise

    # 6. Xóa file và thư mục tạm
    print("Đang dọn dẹp file tạm...")
    os.remove(TMP_ZIP_FILE_PATH)
    # (Bạn có thể thêm code để xóa thư mục 'EXTRACTED_DIR_PATH' nếu muốn)

    print("Hoàn tất task extract_and_load_to_staging.")