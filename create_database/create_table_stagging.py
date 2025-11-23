import mysql.connector

# --- Cấu hình Railway MySQL (theo DSN bạn cung cấp) ---

config = {
    'host': 'gondola.proxy.rlwy.net',
    'port': 39144,
    'user': 'root',
    'password': 'maqUtxJkDuZlpXXSXyIvXaPoMOcAjddv',
    'database': 'railway'
}

# --- Kết nối ---
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# --- XÓA BẢNG CŨ ---
cursor.execute("DROP TABLE IF EXISTS Property;")
conn.commit()

# --- Tạo lại bảng Property ---
create_table_query = """
CREATE TABLE IF NOT EXISTS Property (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    `key` VARCHAR(100),          -- mã tin
    url TEXT,                    -- đường dẫn bài gốc
    name VARCHAR(255),
    price VARCHAR(100),
    area VARCHAR(100),
    old_address TEXT,
    street VARCHAR(255),
    ward VARCHAR(255),
    district VARCHAR(255),
    city VARCHAR(255),
    bedrooms VARCHAR(50),
    floors VARCHAR(50),
    street_width VARCHAR(100),
    description TEXT,
    posting_date DATE,
    property_type VARCHAR(100),
    create_date DATE             -- ngày cào
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

cursor.execute(create_table_query)
conn.commit()

print("✅ Đã xoá bảng cũ và tạo lại bảng Property thành công!")

cursor.close()
conn.close()
