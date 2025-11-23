import pandas as pd
import mysql.connector
from datetime import datetime
import os
import json

# ===== Kết nối MySQL =====
# ===== Load cấu hình từ config.json vào load_data_script=====

with open("config/config.json", "r", encoding="utf-8") as f:
    config_all = json.load(f)

mysql_cfg = config_all["staging"]
conn = mysql.connector.connect(**mysql_cfg)
cursor = conn.cursor()

# ===== File Excel theo ngày =====
today_str = datetime.now().strftime('%d_%m_%Y')
file_name = f"bds_{today_str}.xlsx"
file_path = os.path.join("data", file_name)

if not os.path.exists(file_path):
    raise FileNotFoundError(f"File {file_path} không tồn tại!")

# ===== Load Excel =====
df = pd.read_excel(file_path, engine='openpyxl')
df.columns = df.columns.str.strip()

# ===== Detect cột Phòng ngủ và Diện tích =====
bedroom_col = [c for c in df.columns if 'PN' in c or 'Phòng ngủ' in c]
area_col = [c for c in df.columns if 'DT' in c or 'Diện tích' in c]

bedroom_col = bedroom_col[0] if bedroom_col else 'PN'
area_col = area_col[0] if area_col else 'DT'

# ===== Chuyển định dạng ngày =====
def parse_date(val):
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    try:
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except:
        return None

if 'Ngày đăng' in df.columns:
    df['Ngày đăng'] = df['Ngày đăng'].apply(parse_date)

# ===== Parse Phòng ngủ =====
def parse_bedrooms(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 1
        return int(float(val))
    except:
        return 1

# ===== Parse Diện tích =====
def parse_area(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 'N/A'
        return str(val)
    except:
        return 'N/A'

# ===== Xóa dữ liệu cũ =====
cursor.execute("DELETE FROM Property")
conn.commit()
print("Đã xóa dữ liệu cũ trong bảng Property")

# ===== INSERT dữ liệu mới =====
insert_query = """
INSERT INTO Property 
(`key`, url, create_date, name, price, area, old_address, street, ward, district, city, bedrooms, floors, street_width, description, posting_date, property_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
# Xử lý lỗi "nan": khi dữ liệu trống pandas trả về NaN (float), MySQL nhận được float NaN → chuyển thành chuỗi "nan" trong câu SQL gây lỗi
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
        clean_text(parse_area(row[area_col])),
        clean_text(row.get('Địa chỉ')),
        clean_text(row.get('Đường')) if 'Đường' in df.columns else 'N/A',
        clean_text(row.get('Phường')),
        clean_text(row.get('Quận')),
        clean_text(row.get('Thành phố', 'Hồ Chí Minh')),
        parse_bedrooms(row[bedroom_col]),
        clean_text(row.get('Tầng')),
        clean_text(row.get('Lộ giới')),
        clean_text(row.get('Mô tả')),
        parse_date(row.get('Ngày đăng')),
        clean_text(row.get('Loại nhà', 'Khác'))
    ))

    
    print(f"Đã load row {idx + 1}/{len(df)}: {row.get('Tên', 'N/A')}")
update_query = """
UPDATE Property
SET price = '7,9 tỷ/m²'
WHERE `key` = '17672496'
"""
cursor.execute(update_query)
conn.commit()
print("Đã cập nhật giá bản ghi có key = '17672496' thành 7,9 tỷ/m²")
conn.commit()
cursor.close()
conn.close()
print(f"Đã load toàn bộ file {file_name} vào MySQL (overwrite toàn bộ)")