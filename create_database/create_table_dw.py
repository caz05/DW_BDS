import mysql.connector

# --- Cấu hình Railway mới ---
config = {
    'host': 'shinkansen.proxy.rlwy.net',
    'port': 29701,
    'user': 'root',
    'password': 'IMRYCEqiQiiVCARSApGyHvNnYYKupjfX',
    'database': 'railway'
}

# --- Kết nối ---
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# --- Danh sách bảng ---
tables = ["PropertyListing", "PostingDate", "Location", "PropertyType"]  # Lưu ý: xóa bảng theo thứ tự FK

# --- Xóa bảng cũ ---
for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table};")
    print(f"✅ Dropped table {table} if existed.")

# --- Danh sách lệnh tạo bảng ---
create_queries = [
    """
    CREATE TABLE PropertyType (
        property_type_id INT AUTO_INCREMENT PRIMARY KEY,
        type_name VARCHAR(255) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    """
    CREATE TABLE Location (
        location_id INT AUTO_INCREMENT PRIMARY KEY,
        street VARCHAR(255),
        ward VARCHAR(255),
        district VARCHAR(255),
        city VARCHAR(255),
        old_address VARCHAR(500)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    """
    CREATE TABLE PostingDate (
        date_id INT AUTO_INCREMENT PRIMARY KEY,
        posting_date DATE NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    """
    CREATE TABLE PropertyListing (
        sk BIGINT AUTO_INCREMENT PRIMARY KEY,

        `key` VARCHAR(100),
        url TEXT,
        create_date DATE,

        name VARCHAR(255),
        price DOUBLE,
        area DOUBLE,
        bedrooms INT,
        floors INT,
        description TEXT,
        street_width VARCHAR(255),

        property_type_id INT,
        location_id INT,
        date_id INT,

        startDay DATE NOT NULL,
        endDay DATE DEFAULT NULL,
        isCurrent TINYINT(1) DEFAULT 1,

        FOREIGN KEY (property_type_id) REFERENCES PropertyType(property_type_id),
        FOREIGN KEY (location_id) REFERENCES Location(location_id),
        FOREIGN KEY (date_id) REFERENCES PostingDate(date_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
]

# --- Tạo bảng mới ---
for query in create_queries:
    cursor.execute(query)

conn.commit()
print("✅ All tables created successfully with startDay/endDay/isCurrent in PropertyListing!")

cursor.close()
conn.close()
