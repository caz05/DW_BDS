import json
import pandas as pd
import mysql.connector
from datetime import datetime
from zoneinfo import ZoneInfo 
import os, sys
from dotenv import load_dotenv


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
from template.notification import send_error_email

# Chạy gửi mail báo lỗi tại local
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ===== Load cấu hình từ config.json =====
with open("config/config.json", "r", encoding="utf-8") as f:
    config_all = json.load(f)

staging_cfg = config_all["staging"]
control_cfg = config_all["control"]

# DB Staging
conn = mysql.connector.connect(**staging_cfg)
cursor = conn.cursor()

# DB Control
ctl_conn = mysql.connector.connect(**control_cfg)
ctl_cursor = ctl_conn.cursor()

# ============================
#  HÀM GHI LOG
# ============================
def normalize_path(path: str) -> str:
    return path.replace("\\", "/")

def now_vn_str():
    return datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M:%S')

def start_process_log(process_name, file_id=None):
    now_str = now_vn_str()
    sql = """
        INSERT INTO process_log (file_id, process_name, status, started_at, updated_at)
        VALUES (%s, %s, 'PS', %s, %s)
    """
    ctl_cursor.execute(sql, (file_id, process_name, now_str, now_str))
    ctl_conn.commit()
    return ctl_cursor.lastrowid

def update_process_success(process_id, file_id): 
    now_str = now_vn_str()
    sql = """
        UPDATE process_log 
        SET status='SC', file_id=%s, updated_at=%s 
        WHERE process_id=%s
    """
    ctl_cursor.execute(sql, (file_id, now_str, process_id))
    ctl_conn.commit()

def update_process_fail(process_id, error_msg):
    now_str = now_vn_str()
    sql = """
        UPDATE process_log 
        SET status='FL', updated_at=%s, error_msg=%s
        WHERE process_id=%s
    """
    ctl_cursor.execute(sql, (now_str, error_msg, process_id))
    ctl_conn.commit()

def create_file_log(file_path, row_count, status):
    now_str = now_vn_str()
    sql = """
        INSERT INTO file_log (file_path, data_date, row_count, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    ctl_cursor.execute(sql, (normalize_path(file_path), now_str[:10], row_count, status, now_str, now_str))
    ctl_conn.commit()
    return ctl_cursor.lastrowid

def update_file_log(file_id, status):
    now_str = now_vn_str()
    sql = "UPDATE file_log SET status=%s, updated_at=%s WHERE file_id=%s"
    ctl_cursor.execute(sql, (status, now_str, file_id))
    ctl_conn.commit()

# ============================
# MAIN LOAD DATA
# ============================
process_id = start_process_log("Load to Staging")

try:
    today_str = datetime.now(VN_TZ).strftime('%d_%m_%Y')
    file_name = f"bds_{today_str}.xlsx"
    file_path = os.path.join("data", file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} không tồn tại!")

    print(f"Đang đọc file: {file_path}")
    df = pd.read_excel(file_path, engine='openpyxl')
    df.columns = df.columns.str.strip()

    bedroom_col = next((c for c in df.columns if "PN" in c or "Phòng ngủ" in c), "PN")
    area_col = next((c for c in df.columns if "DT" in c or "Diện tích" in c), "DT")

    def parse_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, datetime):
            return val.astimezone(VN_TZ).strftime('%Y-%m-%d')
        try:
            return pd.to_datetime(val).tz_localize('UTC').astimezone(VN_TZ).strftime('%Y-%m-%d')
        except:
            return None

    if 'Ngày đăng' in df.columns:
        df['Ngày đăng'] = df['Ngày đăng'].apply(parse_date)
    if 'Ngày cào' in df.columns:
        df['Ngày cào'] = df['Ngày cào'].apply(parse_date)

    cursor.execute("TRUNCATE TABLE Property_Temp")
    print("Đã làm sạch bảng Property_Temp.")

    insert_query = """
    INSERT INTO Property_Temp 
    (`key`, url, create_date, name, price, area, old_address, street, ward, district, city, bedrooms, floors, street_width, description, posting_date, property_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def clean_text(val, default="N/A"):
        if pd.isna(val):
            return default
        val = str(val).strip()
        if val == "" or val.lower() == "nan":
            return default
        return val

    for idx, row in df.iterrows():
        cursor.execute(insert_query, (
            clean_text(row.get('Key')),
            clean_text(row.get('URL')),
            parse_date(row.get('Ngày cào')),
            clean_text(row.get('Tên')),
            clean_text(row.get('Giá')),
            clean_text(row.get(area_col)),
            clean_text(row.get('Địa chỉ')),
            clean_text(row.get('Đường')) if 'Đường' in df.columns else 'N/A',
            clean_text(row.get('Phường')),
            clean_text(row.get('Quận')),
            clean_text(row.get('Thành phố', 'Hồ Chí Minh')),
            clean_text(row.get(bedroom_col)),
            clean_text(row.get('Tầng')),
            clean_text(row.get('Lộ giới')),
            clean_text(row.get('Mô tả')),
            parse_date(row.get('Ngày đăng')),
            clean_text(row.get('Loại nhà', 'Khác'))
        ))
        print(f"Đã load row {idx + 1}/{len(df)}: {row.get('Tên', 'N/A')}")
    conn.commit()

    file_id = create_file_log(file_path, len(df), "ST")
    update_process_success(process_id, file_id)
    print(f"Đã load {len(df)} dòng vào bảng 'Property_Temp'.")

except Exception as e:
    error_msg = str(e)
    update_process_fail(process_id, error_msg)
    file_id = create_file_log(file_path, 0, "EF")
    send_error_email("Load Staging Failed", error_msg)
    print("Lỗi:", error_msg)

finally:
    cursor.close()
    conn.close()
    ctl_cursor.close()
    ctl_conn.close()

