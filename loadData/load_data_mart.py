import json
import mysql.connector
from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo 
import os, sys
from dotenv import load_dotenv

#Load biến môi trường trước
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

# Chạy gửi mail báo lỗi tại local
#env_path = os.path.join(os.path.dirname(__file__), '.env')
env_path = os.path.join(ROOT_DIR, 'template', '.env')
print(env_path)
load_dotenv(env_path)

# Import config và hàm gửi mail
try:
    from config.config import cfg
    from template.notification import send_error_email
except ImportError as e:
    print(f"Lỗi load file config: {e}")
    sys.exit(1)

# Giờ Việt Nam
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")   
now_vn = datetime.now(VN_TZ)
today_vn = datetime.now(VN_TZ).date()

# -------------------------
# Load DB config
# -------------------------

ctrl_conn = None
dw_conn = None
dm_conn = None
process_id = None
file_id = None

def success_process(process_id):
    conn = mysql.connector.connect(**cfg["control"])
    cur = conn.cursor()
    cur.execute("""
        UPDATE process_log
        SET status='SC', updated_at=NOW()
        WHERE process_id=%s
    """, (now_vn, process_id))
    conn.commit()
    cur.close()
    conn.close()


def fail_process(process_id, msg):
    conn = mysql.connector.connect(**cfg["control"])
    cur = conn.cursor()
    cur.execute("""
        UPDATE process_log
        SET status='FL', updated_at=%s, error_message=%s
        WHERE process_id=%s
    """, (now_vn, msg, process_id))
    conn.commit()
    cur.close()
    conn.close()

# -------------------------
# Connect DW & DM
# -------------------------
try:
    # --- KẾT NỐI DATABASE ---
    print("Đang kết nối Databases...")
    
    # 1. Control DB (Để ghi log)
    ctrl_cfg = cfg["control"]
    ctrl_conn = mysql.connector.connect(**ctrl_cfg,
                                        connection_timeout=600,
                                        autocommit=False)
    ctrl_cur = ctrl_conn.cursor(dictionary=True)

    # 2. Data Warehouse (Nguồn)
    dw_cfg = cfg["datawarehouse"]
    dw_conn = mysql.connector.connect(**dw_cfg,
                                        connection_timeout=600,
                                        autocommit=False)
    dw_cur = dw_conn.cursor(dictionary=True, buffered=True)

    # 3. Data Mart (Đích)
    dm_cfg = cfg["datamart"]
    dm_conn = mysql.connector.connect(**dm_cfg,
                                        connection_timeout=600,
                                        autocommit=False)
    dm_cur = dm_conn.cursor()

    print("Kết nối thành công!!!")
    # -------------------------
    # Check file 'OK' hôm nay
    print("Đang tìm file status'OK' của ngày hôm nay...")
    
    check_file_sql = """
        SELECT file_id 
        FROM file_log 
        WHERE status = 'OK' 
        AND DATE(created_at) = %s 
        ORDER BY file_id DESC -- Lấy file mới nhất nếu có nhiều file
        LIMIT 1
    """
    ctrl_cur.execute(check_file_sql, (today_vn,))
    file_row = ctrl_cur.fetchone()

    if not file_row:
        print("Không tìm thấy file nào có trạng thái 'OK' của ngày hôm nay để load Mart.")
        print("Vui lòng kiểm tra lại bước Load Data Warehouse.")
        sys.exit(0) # Dừng script bình thường

    file_id = file_row['file_id']
    print(f"Tìm thấy File ID: {file_id}. Bắt đầu Load Data Mart...")

    # --- GHI LOG BẮT ĐẦU (PS - Processing) ---
    ctrl_cur.execute("""
        INSERT INTO process_log (process_name, status, file_id, started_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        """, ('Load Data Mart', 'PS', file_id, now_vn, now_vn))
    process_id = ctrl_cur.lastrowid
    ctrl_conn.commit()

# -------------------------
# Helper functions for dimensions
# -------------------------

    def get_or_create_property_type(type_name):
        if not type_name:
            type_name = "Unknown"
        dm_cur.execute("SELECT property_type_id FROM DimPropertyType_DM WHERE type_name=%s", (type_name,))
        r = dm_cur.fetchone()
        if r:
            return r[0]
        dm_cur.execute("INSERT INTO DimPropertyType_DM (type_name) VALUES (%s)", (type_name,))
        #dm_conn.commit()
        return dm_cur.lastrowid

    def get_or_create_location(street, ward, district, city, old_address=None):
        street = street or ""
        ward = ward or ""
        district = district or ""
        city = city or ""
        dm_cur.execute("""
            SELECT location_id FROM DimLocation_DM
            WHERE street=%s AND ward=%s AND district=%s AND city=%s LIMIT 1
        """, (street, ward, district, city))
        r = dm_cur.fetchone()
        if r:
            return r[0]
        dm_cur.execute("""
            INSERT INTO DimLocation_DM (street, ward, district, city, old_address)
            VALUES (%s,%s,%s,%s,%s)
        """, (street, ward, district, city, old_address))
        #dm_conn.commit()
        return dm_cur.lastrowid

    def get_or_create_date(posting_date):
        if not posting_date:
            return None
        dm_cur.execute("SELECT date_id FROM DimPostingDate_DM WHERE posting_date=%s LIMIT 1", (posting_date,))
        r = dm_cur.fetchone()
        if r:
            return r[0]
        dm_cur.execute(
            "INSERT INTO DimPostingDate_DM (posting_date, year, month, day) VALUES (%s,%s,%s,%s)",
            (posting_date, posting_date.year, posting_date.month, posting_date.day)
        )
        #dm_conn.commit()
        return dm_cur.lastrowid

    # -------------------------
    # Fetch current listings from DW
    # -------------------------
    print("Đang lấy dữ liệu từ DW...")
    dw_cur.execute("""
        SELECT p.sk, p.`key` AS listing_key, p.url, p.name, pt.type_name AS property_type,
            p.price, p.area, l.old_address, l.street, l.ward, l.district, l.city,
            p.bedrooms, p.floors, p.street_width, pd.posting_date, p.create_date,
            p.startDay, p.endDay, p.isCurrent
        FROM PropertyListing p
        LEFT JOIN PropertyType pt ON p.property_type_id = pt.property_type_id
        LEFT JOIN Location l ON p.location_id = l.location_id
        LEFT JOIN PostingDate pd ON p.date_id = pd.date_id
        WHERE p.isCurrent=1
    """)
    #rows = dw_cur.fetchall()
    #print(f"Fetched {len(rows)} rows from DW")

    # -------------------------
    # Insert into FactProperty_DM (including startDay, endDay, isCurrent)
    # -------------------------

    #count = 0
    #vals_to_insert = []
    insert_fact_sql = """
            INSERT INTO FactProperty_DM (
                listing_key, name, property_type_id, location_id, date_id,
                price, area, price_per_m2, bedrooms, floors, street_width,
                posting_date, create_date, startDay, endDay, isCurrent
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                name=VALUES(name),
                property_type_id=VALUES(property_type_id),
                location_id=VALUES(location_id),
                date_id=VALUES(date_id),
                price=VALUES(price),
                area=VALUES(area),
                price_per_m2=VALUES(price_per_m2),
                bedrooms=VALUES(bedrooms),
                floors=VALUES(floors),
                street_width=VALUES(street_width),
                posting_date=VALUES(posting_date),
                create_date=VALUES(create_date),
                startDay=VALUES(startDay),
                endDay=VALUES(endDay),
                isCurrent=VALUES(isCurrent)
        """
    BATCH_SIZE = 200
    total_processed = 0

    while True:
        # 1. Chỉ lấy 200 dòng từ DW
        rows = dw_cur.fetchmany(BATCH_SIZE)
        
        if not rows:
            break # Hết dữ liệu thì dừng vòng lặp

        batch_values = []
        for r in rows:
            price = float(r['price']) if r['price'] is not None else None
            area = float(r['area']) if r['area'] is not None else None
            price_per_m2 = price / area if price and area and area > 0 else None

            property_type_id = get_or_create_property_type(r['property_type'])
            location_id = get_or_create_location(r['street'], r['ward'], r['district'], r['city'], r['old_address'])
            date_id = get_or_create_date(r['posting_date'])

            batch_values.append((
                r['listing_key'], r['name'], property_type_id, location_id, date_id,
                price, area, price_per_m2, r['bedrooms'], r['floors'], r['street_width'],
                r['posting_date'], r['create_date'], r['startDay'], r['endDay'], r['isCurrent'],
                
            ))
        
        #dm_cur.execute(insert_fact_sql, (
         #   r['listing_key'], r['name'], property_type_id, location_id, date_id,
         #   price, area, price_per_m2, r['bedrooms'], r['floors'], r['street_width'],
        #    r['posting_date'], r['create_date'], r['startDay'], r['endDay'], r['isCurrent']
        #))
        #count += 1
    # Thực hiện Insert theo lô (mỗi lần 1000 dòng)
    #batch_size = 1000
    #for i in range(0, len(vals_to_insert), batch_size):
    #    batch = vals_to_insert[i:i + batch_size]

    #    print(f"Executing batch {i} → {i+len(batch)}")
    #    dm_cur.executemany(insert_fact_sql, batch) 
    #    dm_conn.commit() # Commit mỗi 1000 dòng
    #    print(f"Inserted batch {i} to {i+len(batch)}")
        # 3. Insert 1000 dòng này vào Data Mart ngay lập tức
        if batch_values:
            dm_cur.executemany(insert_fact_sql, batch_values)
            dm_conn.commit() # Lưu ngay, giải phóng Transaction Log
            
            # 4. Giải phóng bộ nhớ list tạm
            total_processed += len(batch_values)
            print(f" Đã load batch: {len(batch_values)} dòng. Tổng cộng: {total_processed}")
            
            # Xóa list để giải phóng RAM cho vòng lặp sau
            del batch_values

    print(f"Loaded all rows into FactProperty_DM")
    # --- GHI LOG THÀNH CÔNG (SC - Success) ---
    success_process(process_id)
    print("Cập nhật Process Log: SC (Success)")

except Exception as e:
    # --- XỬ LÝ LỖI (GHI LOG FL + GỬI MAIL) ---
    print(f"LỖI FATAL: {e}")
    
    fail_process(process_id, str(e))
    # Gửi email báo lỗi
    try:
        send_error_email("LOAD DATA MART ERROR", f"Chi tiết lỗi:\n{str(e)}")
        print("Đã gửi email báo lỗi.")
    except Exception as mail_err:
        print(f"Không thể gửi email: {mail_err}")

    raise e             
# -------------------------
# Close connections
# -------------------------
finally:
    # --- ĐÓNG KẾT NỐI AN TOÀN ---
    if dw_cur: dw_cur.close()
    if dw_conn and dw_conn.is_connected(): dw_conn.close()
    
    if dm_cur: dm_cur.close()
    if dm_conn and dm_conn.is_connected(): dm_conn.close()

    if 'ctrl_cur' in locals() and ctrl_cur: ctrl_cur.close()
    if ctrl_conn and ctrl_conn.is_connected(): ctrl_conn.close()
    
    print("Đã đóng tất cả kết nối.")
