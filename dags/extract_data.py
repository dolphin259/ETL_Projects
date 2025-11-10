# Thêm các thư viện này ở đầu file
import boto3
import zipfile
import os
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# Bạn cần hook cho MySQL để nạp vào Staging
from airflow.providers.mysql.hooks.mysql import MySqlHook 


# --- ĐỊNH NGHĨA CÁC BIẾN ---

# Tên các connection bạn đã tạo trong Airflow
AWS_CONN_ID = "aws_default"
STAGING_DB_CONN_ID = "staging_db_mysql" # Conn Id trỏ đến MySQL (olist-source-db-v2)

# Thư mục tạm trên máy chủ EC2 (bên trong Docker)
# Airflow sẽ tải file .zip về đây và giải nén
TMP_DIR = "/opt/airflow/temp_data"
TMP_ZIP_FILE_PATH = os.path.join(TMP_DIR, "data.zip")
EXTRACTED_DIR_PATH = os.path.join(TMP_DIR, "extracted_files")


# --- ĐỊNH NGHĨA LẠI TASK EXTRACT ---

def extract_and_load_to_staging(**kwargs):
    """
    Task này được Lambda kích hoạt và nhận 'conf'.
    Nó sẽ tải file .zip, giải nén, và nạp 9 file CSV vào Staging (MySQL).
    """
    
    # 1. Lấy thông tin file từ 'conf' mà Lambda gửi qua
    dag_run_conf = kwargs.get('dag_run').conf
    s3_bucket = dag_run_conf.get('s3_bucket')
    s3_key = dag_run_conf.get('s3_key') # Tên file, ví dụ: "data.zip"

    # Kiểm tra xem DAG có được kích hoạt tự động không
    if not s3_key:
        print("Không tìm thấy s3_key. DAG này có thể đã chạy thủ công.")
        # Bạn có thể 'return' hoặc ném lỗi ở đây
        raise ValueError("DAG này phải được kích hoạt từ S3 (Lambda) với s3_key.")

    print(f"Bắt đầu xử lý file: {s3_key} từ bucket: {s3_bucket}")

    # 2. Tạo thư mục tạm (nếu chưa có)
    os.makedirs(EXTRACTED_DIR_PATH, exist_ok=True)

    # 3. Dùng S3Hook để tải file .zip từ S3 về
    print(f"Đang tải file {s3_key} từ S3...")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn() # Lấy client boto3
    s3_client.download_file(s3_bucket, s3_key, TMP_ZIP_FILE_PATH)
    
    print(f"Đã tải file về {TMP_ZIP_FILE_PATH}")

    # 4. Giải nén file .zip
    print(f"Đang giải nén file vào {EXTRACTED_DIR_PATH}...")
    with zipfile.ZipFile(TMP_ZIP_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR_PATH)
        
    print("Đã giải nén thành công.")

    # 5. Nạp 9 file .csv vào Staging (MySQL)
    print("Bắt đầu nạp dữ liệu vào Staging (MySQL)...")
    
    # Lấy hook (đường hầm) kết nối tới Staging DB
    mysql_hook = MySqlHook(mysql_conn_id=STAGING_DB_CONN_ID)
    
    # Danh sách các file CSV ta cần nạp
    # Tên file phải khớp với tên file trong .zip của bạn
    csv_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]

    for file_name in csv_files:
        try:
            file_path = os.path.join(EXTRACTED_DIR_PATH, file_name)
            
            # Tên bảng trong MySQL (giả sử tên bảng = tên file bỏ .csv)
            table_name = file_name.replace(".csv", "").replace("_dataset", "")
            
            print(f"Đang nạp file {file_name} vào bảng {table_name}...")
            
            # Dùng Pandas để đọc CSV và nạp vào DB
            # Đây là cách đơn giản nhất
            df = pd.read_csv(file_path)
            
            # Thay thế giá trị NaN (rỗng) bằng None để MySQL hiểu
            df = df.where(pd.notnull(df), None)
            
            # Dùng hook để lấy 'engine' (công cụ) của SQLAlchemy
            engine = mysql_hook.get_sqlalchemy_engine()
            
            # Nạp dataframe vào SQL, thay thế nếu bảng đã tồn tại
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            
            print(f"Nạp thành công {table_name}.")
            
        except FileNotFoundError:
            print(f"Cảnh báo: Không tìm thấy file {file_name} trong file .zip.")
        except Exception as e:
            print(f"Lỗi khi nạp file {file_name}: {e}")
            raise

    # 6. Xóa file và thư mục tạm
    print("Đang dọn dẹp file tạm...")
    os.remove(TMP_ZIP_FILE_PATH)
    # (Bạn có thể thêm code để xóa thư mục 'EXTRACTED_DIR_PATH' nếu muốn)

    print("Hoàn tất task extract_and_load_to_staging.")


# --- SỬA LẠI DAG CỦA BẠN ---
# Trong file DAG chính, hãy đảm bảo task của bạn gọi hàm Python này:

# extract_task = PythonOperator(
#     task_id="extract_and_load_to_staging",
#     python_callable=extract_and_load_to_staging,
#     provide_context=True, # Rất quan trọng! Để hàm nhận được **kwargs
# )

# Các task transform sau đó sẽ phụ thuộc vào task này
# transform_dim_customers_task.set_upstream(extract_task)
# ...