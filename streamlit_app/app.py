import streamlit as st
import streamlit_authenticator as stauth
import boto3
import zipfile
import requests
import os
import time
import json
import hashlib
import io
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2 import OperationalError

S3_BUCKET_NAME = 'ecommerce-etl-raw-data-1'
S3_FOLDER_PATH = ''
POWER_BI_URL = "https://app.powerbi.com/reportEmbed?reportId=0c29477b-a59f-44f4-9c4a-7b26bf0def02&autoAuth=true&ctid=7c87b783-14cd-409e-9737-b83e250c039d"

AIRFLOW_URL = "http://10.0.2.124:8080"
AIRFLOW_DAG_ID = "e_commerce_dw_etl"
AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"

RDS_HOST = 'data-warehouse.ci7sumkiiee6.us-east-1.rds.amazonaws.com'
RDS_DBNAME = 'dw'
RDS_USER = 'de_user'
RDS_PASSWORD = 'de_password'

WAREHOUSE_QUERIES = {
    "fact_orders": 'SELECT * FROM warehouse.fact_orders LIMIT 500000',
    "dim_customers": 'SELECT * FROM warehouse.dim_customers',
    "dim_products": 'SELECT * FROM warehouse.dim_products',
    "dim_sellers": 'SELECT * FROM warehouse.dim_sellers',
    "dim_dates": 'SELECT * FROM warehouse.dim_dates',
    "dim_geolocation": 'SELECT * FROM warehouse.dim_geolocation',
    "dim_payments": 'SELECT * FROM warehouse.dim_payments'
}

CONFIG = {
    'credentials': {
        'usernames': {
            'admin': {'email': 'administrator@etl.com', 'name': 'Admin', 'password': 'admin123'},
            'user': {'email': 'user@etl.com', 'name': 'Người dùng', 'password': 'user123'}
        }
    },
    'cookie': {'expiry_days': 1, 'key': 'random_key', 'name': 'internal_cookie'},
    'preauthorized': {'emails': []}
}

REQUIRED_FILES = {
    "olist_customers_dataset.csv", "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv", "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv",
    "olist_products_dataset.csv", "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
}

def get_file_hash(file_bytes):
    return hashlib.md5(file_bytes).hexdigest()

def upload_to_s3(file_bytes, bucket, object_name):
    s3_client = boto3.client('s3', region_name='us-east-1')
    try:
        s3_client.put_object(Body=file_bytes, Bucket=bucket, Key=object_name)
        return True
    except Exception as e:
        return f"Lỗi S3: {str(e)}"

def fetch_dataframe_from_rds(query):
    conn = None
    try:
        conn = psycopg2.connect(
            host=RDS_HOST,
            database=RDS_DBNAME,
            user=RDS_USER,
            password=RDS_PASSWORD,
            connect_timeout=10
        )
        df = pd.read_sql(query, conn)
        return df
    except OperationalError as e:
        st.error(f"Lỗi kết nối CSDL: {e}")
        return None
    except Exception as e:
        st.error(f"Lỗi truy vấn: {e}")
        return None
    finally:
        if conn:
            conn.close()

def wait_for_external_trigger(timeout=60):
    api_url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
    params = {"limit": 1, "order_by": "-execution_date"}
    start_time = time.time()
    st_status = st.empty()

    while (time.time() - start_time) < timeout:
        try:
            response = requests.get(api_url, params=params, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=5)
            if response.status_code == 200:
                data = response.json()
                dag_runs = data.get('dag_runs', [])
                if dag_runs:
                    latest_run = dag_runs[0]
                    state = latest_run['state']
                    run_id = latest_run['dag_run_id']
                    if state in ['queued', 'running']:
                        st_status.empty()
                        return True, {"dag_run_id": run_id, "message": "DETECTED_AUTO_TRIGGER"}
            st_status.caption(f"Đang đợi tín hiệu từ hệ thống (S3 -> Lambda)... {int(time.time() - start_time)}s")
            time.sleep(3)
        except Exception:
            time.sleep(2)
    return False, "Không tìm thấy tiến trình tự động kích hoạt sau 60s."

def wait_for_dag_completion(dag_run_id):
    if not dag_run_id: return False, "Mất dấu Run ID"
    status_url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns/{dag_run_id}"
    status_container = st.status("Đang xử lý dữ liệu trên Airflow...", expanded=True)
    with status_container:
        st.write(f"Tracking ID: `{dag_run_id}`")
        log_box = st.empty()
        fail_count = 0
        while True:
            try:
                response = requests.get(status_url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=5)
                if response.status_code == 200:
                    fail_count = 0
                    state = response.json().get('state')
                    if state == 'success':
                        status_container.update(label="ETL Hoàn tất!", state="complete", expanded=False)
                        log_box.success("Dữ liệu đã sẵn sàng!")
                        return True, "Thành công"
                    elif state == 'failed':
                        status_container.update(label="ETL Thất bại!", state="error", expanded=True)
                        return False, "Airflow báo lỗi (Failed)"
                    else:
                        log_box.info(f"Trạng thái hiện tại: **{state}**... (Vui lòng đợi)")
                        time.sleep(3)
                else:
                    fail_count += 1
                    time.sleep(3)
            except Exception:
                fail_count += 1
                time.sleep(3)
            if fail_count > 10:
                status_container.update(label="Mất kết nối!", state="error")
                return False, "Không thể kết nối tới Airflow."

st.set_page_config(page_title="Internal Data App", layout="wide")

if 'is_processing' not in st.session_state: st.session_state.is_processing = False
if 'dag_run_id' not in st.session_state: st.session_state.dag_run_id = None
if 'app_notification' not in st.session_state: st.session_state.app_notification = None

authenticator = stauth.Authenticate(
    CONFIG['credentials'], CONFIG['cookie']['name'], CONFIG['cookie']['key'],
    CONFIG['cookie']['expiry_days'], preauthorized=CONFIG['preauthorized']
)
authenticator.login()

if st.session_state["authentication_status"] is True:
    user_name = st.session_state["name"]

    with st.sidebar:
        st.write(f"Xin chào, **{user_name}**")
        authenticator.logout()
        st.divider()

        if st.session_state.app_notification:
            notif = st.session_state.app_notification
            if notif['type'] == 'success': st.success(notif['msg'])
            elif notif['type'] == 'error': st.error(notif['msg'])
            if not st.session_state.is_processing:
                st.session_state.app_notification = None

        st.header("Upload Dữ liệu")

        if not st.session_state.is_processing:
            uploaded_file = st.file_uploader("Chọn file ZIP", type="zip")
            if uploaded_file and st.button("Upload & Chạy ETL"):
                st.session_state.is_processing = True
                st.session_state.app_notification = None
                try:
                    file_bytes = uploaded_file.getvalue()
                    with zipfile.ZipFile(uploaded_file) as z:
                        files_in_zip = {os.path.basename(f) for f in z.namelist()}
                        if not REQUIRED_FILES.issubset(files_in_zip):
                            st.session_state.app_notification = {'type': 'error', 'msg': f"Thiếu file: {REQUIRED_FILES - files_in_zip}"}
                            st.session_state.is_processing = False
                            st.rerun()

                    upload_res = upload_to_s3(file_bytes, S3_BUCKET_NAME, uploaded_file.name)
                    if upload_res is not True:
                        st.session_state.app_notification = {'type': 'error', 'msg': upload_res}
                        st.session_state.is_processing = False
                        st.rerun()

                    st.toast("Upload S3 thành công! Đợi Airflow...")
                    ok, res = wait_for_external_trigger(timeout=60)

                    if ok:
                        st.session_state.dag_run_id = res.get('dag_run_id')
                        st.toast("Đã bắt được tiến trình Airflow!")
                        st.rerun()
                    else:
                        st.session_state.app_notification = {'type': 'error', 'msg': f"Lỗi: {res}"}
                        st.session_state.is_processing = False
                        st.rerun()

                except Exception as e:
                    st.session_state.app_notification = {'type': 'error', 'msg': f"Lỗi hệ thống: {str(e)}"}
                    st.session_state.is_processing = False
                    st.rerun()

        else:
            ok, msg = wait_for_dag_completion(st.session_state.dag_run_id)

            if ok:
                st.balloons()
                st.session_state.app_notification = {
                    'type': 'success',
                    'msg': "Cập nhật thành công! Tải bộ dữ liệu sạch bên dưới."
                }

                st.subheader("Tải xuống Bộ dữ liệu Sạch (Star Schema)")
                st.info("Hệ thống sẽ tải từng bảng và nén vào một file ZIP để bạn dễ dàng Import vào Power BI.")

                zip_buffer = io.BytesIO()
                has_data = False

                with st.spinner("Đang trích xuất dữ liệu từ Warehouse..."):
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                        for table_name, query in WAREHOUSE_QUERIES.items():
                            df = fetch_dataframe_from_rds(query)
                            if df is not None and not df.empty:
                                has_data = True
                                csv_data = df.to_csv(index=False)
                                zf.writestr(f"{table_name}.csv", csv_data)
                                st.text(f"Đã tải bảng: {table_name} ({len(df)} dòng)")
                            else:
                                st.warning(f"ảng {table_name} không có dữ liệu.")

                if has_data:
                    zip_buffer.seek(0)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    st.download_button(
                        label="Tải xuống File ZIP (Chứa 7 file CSV)",
                        data=zip_buffer,
                        file_name=f"clean_warehouse_data_{timestamp}.zip",
                        mime="application/zip",
                        help="Giải nén file này, bạn sẽ có các file Fact và Dim riêng biệt."
                    )
                else:
                    st.error("Không tải được dữ liệu nào từ Warehouse.")

                if st.button("Hoàn tất & Quay lại"):
                    st.session_state.is_processing = False
                    st.session_state.dag_run_id = None
                    st.rerun()

            elif ok is False and msg != "Mất dấu Run ID":
                st.session_state.app_notification = {'type': 'error', 'msg': f"Quy trình thất bại: {msg}"}
                if st.button("Thử lại"):
                    st.session_state.is_processing = False
                    st.session_state.dag_run_id = None
                    st.rerun()

    st.title("🛍️áo cáo Kinh doanh")

    if st.session_state.app_notification and st.session_state.app_notification['type'] == 'success':
        st.markdown(f"""
        <div style="padding: 10px; background-color: #d4edda; color: #155724; border-radius: 5px; margin-bottom: 10px;">
            <strong>THÀNH CÔNG:</strong> {st.session_state.app_notification['msg']}
        </div>
        """, unsafe_allow_html=True)

    current_ts = int(time.time())
    separator = "&" if "?" in POWER_BI_URL else "?"
    fresh_pbi_url = f"{POWER_BI_URL}{separator}refresh_trigger={current_ts}"

    st.markdown(
        f"""
        <div style="border: 1px solid #ddd; padding: 5px; background: white;">
            <iframe title="Report" width="100%" height="800" src="{fresh_pbi_url}" frameborder="0" allowFullScreen="true"></iframe>
        </div>
        """, unsafe_allow_html=True
    )

elif st.session_state["authentication_status"] is False:
    st.error('Sai thông tin đăng nhập')
elif st.session_state["authentication_status"] is None:
    st.warning('Vui lòng đăng nhập')