# load_data_datawarehouse.py

import json
import mysql.connector
from datetime import datetime
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
dw_config = cfg["datawarehouse"]
ctl_config = cfg["control"]

# CONNECT CONTROL DB
# ===========================
ctl_conn = mysql.connector.connect(**ctl_config)
ctl_cursor = ctl_conn.cursor(dictionary=True)

#Hàm viết log 
def start_process(process_name, file_id):
    sql = """
        INSERT INTO process_log (file_id, process_name, status)
        VALUES (%s, %s, 'PS')
    """
    ctl_cursor.execute(sql, (file_id, process_name))
    ctl_conn.commit()
    return ctl_cursor.lastrowid

def success_process(process_id):
    ctl_cursor.execute("""
        UPDATE process_log SET status='SC', updated_at=NOW()
        WHERE process_id=%s
    """, (process_id,))
    ctl_conn.commit()

def fail_process(process_id, msg):
    ctl_cursor.execute("""
        UPDATE process_log SET status='FL', updated_at=NOW()
        WHERE process_id=%s
    """, (process_id,))
    ctl_conn.commit()

def update_file_status(file_id, status):
    ctl_cursor.execute("""
        UPDATE file_log SET status=%s, updated_at=NOW()
        WHERE file_id=%s
    """, (status, file_id))
    ctl_conn.commit()

#Lấy file có status 'TR' / 'LF'    
ctl_cursor.execute("""
    SELECT * FROM file_log 
    WHERE status IN ('TR','LF') 
    ORDER BY file_id ASC
    LIMIT 1
""")
file_item = ctl_cursor.fetchone()

if not file_item:
    print("No file to load DW.")
    exit()

file_id = file_item["file_id"]
print(f"Loading DW for file_id = {file_id}")
# Bắt đầu ghi
process_id = start_process("Load to DW", file_id)

try:
    # ------------------ LOAD FROM STAGING ------------------
    staging_conn = mysql.connector.connect(**staging_config)
    cursor = staging_conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Property;")
    staging_data = cursor.fetchall()
    cursor.close()
    staging_conn.close()

    print(f"Fetched {len(staging_data)} rows from staging DB.")

    # ------------------ CONNECT TO DW ------------------
    dw_conn = mysql.connector.connect(**dw_config)
    dw_cursor = dw_conn.cursor(dictionary=True)

    # ------------------ Compare old vs new for SCD2 ------------------
    def has_changes(old, new):
        fields = [
            "url", "name", "price", "area", "bedrooms", "floors",
            "description", "street_width",
            "property_type_id", "location_id", "date_id"
        ]
        for f in fields:
            if old[f] != new[f]:
                return True
        return False

    # ------------------ DIM PropertyType ------------------
    def get_property_type_id(dw_cursor, property_type_name):
        dw_cursor.execute(
            "SELECT property_type_id FROM PropertyType WHERE type_name=%s",
            (property_type_name,)
        )
        row = dw_cursor.fetchone()

        if row:
            return row['property_type_id']

        dw_cursor.execute(
            "INSERT INTO PropertyType (type_name) VALUES (%s)",
            (property_type_name,)
        )
        return dw_cursor.lastrowid

    # ------------------ DIM Location ------------------
    def get_location_id(dw_cursor, street, ward, district, city, old_address):
        dw_cursor.execute("""
            SELECT location_id FROM Location
            WHERE street=%s AND ward=%s AND district=%s AND city=%s AND old_address=%s
        """, (street, ward, district, city, old_address))

        row = dw_cursor.fetchone()
        if row:
            return row['location_id']

        dw_cursor.execute("""
            INSERT INTO Location (street, ward, district, city, old_address)
            VALUES (%s, %s, %s, %s, %s)
        """, (street, ward, district, city, old_address))
        
        return dw_cursor.lastrowid
    
    # ------------------ DIM PostingDate ------------------
    def get_date_id(dw_cursor, posting_date):
        dw_cursor.execute(
            "SELECT date_id FROM PostingDate WHERE posting_date=%s",
            (posting_date,)
        )
        row = dw_cursor.fetchone()

        if row:
            return row['date_id']

        dw_cursor.execute(
            "INSERT INTO PostingDate (posting_date) VALUES (%s)",
            (posting_date,)
        )
        return dw_cursor.lastrowid
    
 # ------------------ FACT PropertyListing (SCD2) ------------------
    def load_fact_listing(dw_cursor, row, property_type_id, location_id, date_id):
        key = row['key']
        url = row['url']
        create_date = row['create_date'] or datetime.today().date()
        name = row['name']
        price = row['price']
        area = row['area']
        bedrooms = row['bedrooms']
        floors = row['floors']
        description = row['description']
        street_width = row['street_width']
    
        dw_cursor.execute("""
                    SELECT * FROM PropertyListing
                    WHERE `key`=%s AND isCurrent=1
                """, (key,))
        old_record = dw_cursor.fetchone()

        new_record = {
                        "url": url, "name": name, "price": price, "area": area,
                        "bedrooms": bedrooms, "floors": floors, "description": description,
                        "street_width": street_width, "property_type_id": property_type_id,
                        "location_id": location_id, "date_id": date_id
                    }

         # TH1: chưa từng xuất hiện
        if not old_record:
                        dw_cursor.execute("""
                            INSERT INTO PropertyListing (
                                `key`, url, create_date, name, price, area, bedrooms, floors,
                                description, street_width, property_type_id, location_id,
                                date_id, startDay, isCurrent
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURDATE(),1)
                        """, (key, url, create_date, name, price, area, bedrooms, floors,
                            description, street_width, property_type_id, location_id, date_id))
                        return
        #TH2: Nếu có record cũ nhưng dữ liệu KHÔNG đổi -> bỏ qua ----------
        if not has_changes(old_record, new_record):
                        print(f"SKIP: No change for key = {key}")
                        return

        #TH3: Nếu dữ liệu thay đổi -> đóng bản cũ + tạo bản mới ----------
        print(f"UPDATE: Changes detected -> key = {key}")

        dw_cursor.execute("""
                        UPDATE PropertyListing
                        SET endDay = CURDATE(), isCurrent = 0
                        WHERE sk = %s
                    """, (old_record['sk'],))

        dw_cursor.execute("""
                        INSERT INTO PropertyListing (
                            `key`, url, create_date, name, price, area, bedrooms, floors,
                            description, street_width, property_type_id, location_id,
                            date_id, startDay, isCurrent
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURDATE(),1)
                    """, (key, url, create_date, name, price, area, bedrooms, floors,
                        description, street_width, property_type_id, location_id, date_id))


    # ------------------ ETL LOOP ------------------
    for row in staging_data:

        property_type_id = get_property_type_id(dw_cursor, row['property_type'] or "Unknown")
        location_id = get_location_id(
            dw_cursor, row['street'], row['ward'], row['district'], row['city'], row['old_address']
        )
        posting_date = row['posting_date'] or datetime.today().date()
        date_id = get_date_id(dw_cursor, posting_date)

        load_fact_listing(dw_cursor, row, property_type_id, location_id, date_id) 
        

    dw_conn.commit()
    dw_cursor.close()
    dw_conn.close()

    print("DW Load thành công — SCD2 cho FACT đã hoạt động đúng!")
# LOG SUCCESS
    # ==========================
    update_file_status(file_id, "OK")
    success_process(process_id)

except Exception as e:
    print("DW Load FAILED:", str(e))

    try:
        ctl_conn.ping(reconnect=True, attempts=3, delay=2)
    except:
        pass
    update_file_status(file_id, "LF")
    fail_process(process_id, str(e))

    send_error_email(f"DW Load failed for file_id {file_id}: {e}")

if ctl_conn.is_connected():
        ctl_cursor.close()
        ctl_conn.close()
