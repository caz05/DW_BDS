import mysql.connector
import json
import re
import os,sys
from dotenv import load_dotenv

#Load biến môi trường trước
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

# Chạy gửi mail báo lỗi tại local
#env_path = os.path.join(os.path.dirname(__file__), '.env')
env_path = os.path.join(ROOT_DIR, 'template', '.env')
print(env_path)
load_dotenv(env_path)

from config.config import cfg
from template.notification import send_error_email

# 1. Load config [input: config.py,.env, date (default= today)]

staging_config = cfg["staging"]
control_config = cfg["control"]

# 2. Kết nối db.estate_control
try:
    control_conn = mysql.connector.connect(**control_config)
    control_cursor = control_conn.cursor(dictionary=True)

    # Set Timezone Việt Nam cho MySQL
    control_cursor.execute("SET time_zone = '+07:00';")
except Exception as e:
    send_error_email("CONNECT CONTROL DB FAILED", str(e))
    raise    

#Hàm ghi log vào bảng process_log và file_log
def start_process_log(process_name, file_id):
    """Insert log bắt đầu (PS)"""
    sql = """
        INSERT INTO process_log (file_id, process_name, status, started_at)
        VALUES (%s, %s, 'PS', NOW())
    """
    control_cursor.execute(sql, (file_id, process_name))
    control_conn.commit()
    process_id = control_cursor.lastrowid

    return process_id


def success_process_log(process_id):
    """Cập nhật SC"""
    control_cursor.execute("""
        UPDATE process_log 
        SET status='SC', updated_at=NOW()
        WHERE process_id=%s
    """, (process_id,))
    control_conn.commit()


def failed_process_log(process_id, error_msg):
    """Cập nhật FL"""
    control_cursor.execute("""
        UPDATE process_log 
        SET status='FL', updated_at=NOW()
        WHERE process_id=%s
    """, (process_id,))
    control_conn.commit()


def update_file_log_status(file_id, status):
    control_cursor.execute("""
        UPDATE file_log
        SET status=%s, updated_at=NOW()
        WHERE file_id=%s
    """, (status, file_id))
    control_conn.commit()
   
# ------------------ Hàm chuẩn hóa ------------------
def parse_price(price_str):
    if not price_str:
        return 0.0
    price_str = price_str.lower().replace(',', '.').strip()
    try:
        if "triệu" in price_str:
            number = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return number * 1_000_000
        elif "tỷ" in price_str:
            number = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return number * 1_000_000_000
        else:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            return float(cleaned)
    except:
        return 0.0

def parse_area(area_str):
    if not area_str:
        return 0.0
    area_str = area_str.lower().replace(",", ".").strip()
    try:
        return float(re.findall(r"\d+\.?\d*", area_str)[0])
    except:
        return 0.0

def parse_int_from_str(value_str):
    if not value_str:
        return 0
    match = re.search(r"\d+", value_str)
    return int(match.group()) if match else 0


# ------------------ RUN TRANSFORM ------------------

# 3. Kiểm tra record trong control.file_log có giá trị status= ST hoặc TF không
def get_transform_file():
  
    control_cursor.execute("""
        SELECT * FROM file_log 
        WHERE status ='ST'
        ORDER BY file_id DESC
        LIMIT 1;
    """)

    row = control_cursor.fetchone()
    return row


file_info = get_transform_file()

if not file_info:
    print("Không có file nào cần transform (ST/TF).")
    control_cursor.close()
    control_conn.close()
    exit()

file_id = file_info["file_id"]
print(f"Transforming file_id = {file_id}")

# Ghi log bắt đầu
# 4. Thêm status=PS vào process_log
process_id = start_process_log("Transform Data", file_id)

# Thực hiện transform
try:
    
    # 5. Kết nối vối db.estate_stagging
    conn = mysql.connector.connect(**staging_config)
    cursor = conn.cursor(dictionary=True)

    # 6. Transfrom dữ liệu gốc từ bảng Property_temp
    cursor.execute("SELECT * FROM Property_Temp;")
    temp_rows = cursor.fetchall()

    print(f"Fetched {len(temp_rows)} rows from Property_Temp")

    # Xóa bảng Property trước khi ghi dữ liệu mới
    cursor.execute("DELETE FROM Property;")

    # 7. Thêm dữ liệu sạch vào bảng Property
    insert_sql = """
    INSERT IGNORE INTO Property (
        `key`, url, create_date, name, price, area, bedrooms, floors,
        description, street_width, property_type, street, ward, district,
        city, old_address, posting_date
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    count = 0

    for row in temp_rows:
        cursor.execute(insert_sql, (
            row["key"],
            row["url"],
            row["create_date"],
            row["name"],
            parse_price(row["price"]),
            parse_area(row["area"]),
            parse_int_from_str(row["bedrooms"]),
            parse_int_from_str(row["floors"]),
            row["description"],
            row["street_width"],
            row["property_type"],
            row["street"],
            row["ward"],
            row["district"],
            row["city"],
            row["old_address"],
            row["posting_date"]
        ))
        count += 1

    # 9. Đóng kết nối và thông báo "Transform completed inserted into Property" 
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Transform completed → {count} rows inserted into Property")


# 8. Cập nhật status="TR" cho file_log và status="SC" cho process_log
    update_file_log_status(file_id, "TR")
    success_process_log(process_id)

   
except Exception as e:
    print("Transform Failed:", e)

    # ====== LOG FAILED ======
    update_file_log_status(file_id, "TF")
    failed_process_log(process_id, str(e)) 

    # ===== SEND EMAIL ERROR HERE =====
    send_error_email(
        subject=f"TRANSFORM FAILED",
        message=str(e)
    )


control_cursor.close()
control_conn.close()

