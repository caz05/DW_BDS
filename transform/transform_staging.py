import mysql.connector
import json
import re
import os,sys
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
from template.notification import send_error_email

#Chạy gửi mail báo lỗi tại local
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

# ------------------ Load config.json ------------------
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

staging_config = cfg["staging"]
control_config = cfg["control"]

# ------------------ CONNECT CONTROL DB ------------------
control_conn = mysql.connector.connect(**control_config)
control_cursor = control_conn.cursor(dictionary=True)


#Hàm ghi log vào bảng process_log và file_log
def start_process_log(cursor, process_name, file_id):
    sql = """
        INSERT INTO process_log (file_id, process_name, status, started_at)
        VALUES (%s, %s, 'PS', NOW())
    """
    cursor.execute(sql, (file_id, process_name))
    return cursor.lastrowid


def success_process_log(cursor, process_id):
    cursor.execute("""
        UPDATE process_log 
        SET status='SC', updated_at=NOW()
        WHERE process_id=%s
    """, (process_id,))

def failed_process_log(cursor, process_id, error_msg):
    cursor.execute("""
        UPDATE process_log 
        SET status='FL', error_msg=%s, updated_at=NOW()
        WHERE process_id=%s
    """,  (error_msg, process_id))


def update_file_log_status(cursor, file_id, status):
    cursor.execute("""
        UPDATE file_log
        SET status=%s, updated_at=NOW()
        WHERE file_id=%s
    """, (status, file_id))

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
# 
def get_transform_file(cursor):
    cursor.execute("""
        SELECT * FROM file_log 
        WHERE status IN ('ST', 'TF')
        ORDER BY file_id ASC
        LIMIT 1;
    """)
    return cursor.fetchone()


file_info = get_transform_file(control_cursor)

if not file_info:
    print("Không có file nào cần transform (ST/TF).")
    control_cursor.close()
    control_conn.close()
    exit()

file_id = file_info["file_id"]
print(f"Transforming file_id = {file_id}")

# Ghi log bắt đầu
process_id = start_process_log(control_cursor, "Transform Data", file_id)
control_conn.commit()

try:
    # Thực hiện transform
    conn = mysql.connector.connect(**staging_config)
    cursor = conn.cursor(dictionary=True)

    # Lấy dữ liệu gốc từ Property_Temp
    cursor.execute("SELECT * FROM Property_Temp;")
    temp_rows = cursor.fetchall()

    print(f"Fetched {len(temp_rows)} rows from Property_Temp")

    # Xóa bảng Property trước khi ghi dữ liệu mới
    cursor.execute("DELETE FROM Property;")

    # Chuẩn bị insert
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

# ====== LOG SUCCESS ======
    update_file_log_status(control_cursor,file_id, "TR")
    success_process_log(control_cursor,process_id)
    control_conn.commit()

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Transform completed → {count} rows inserted into Property")

except Exception as e:
    print("Transform Failed:", e)

    # ====== LOG FAILED ======
    update_file_log_status(control_cursor, file_id, "TF")
    failed_process_log(control_cursor, process_id, str(e))
    control_conn.commit()   
