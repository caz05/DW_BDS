import mysql.connector
import json

# Load config
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

dm_cfg = cfg["datamart"]

conn = mysql.connector.connect(**dm_cfg)
cur = conn.cursor()

print("Creating upgraded Data Mart schema...")

# Drop old tables
cur.execute("DROP TABLE IF EXISTS MartAgg_District;")
cur.execute("DROP TABLE IF EXISTS MartAgg_TypeMonth;")
cur.execute("DROP TABLE IF EXISTS FactProperty_DM;")
cur.execute("DROP TABLE IF EXISTS DimPostingDate_DM;")
cur.execute("DROP TABLE IF EXISTS DimPropertyType_DM;")
cur.execute("DROP TABLE IF EXISTS DimLocation_DM;")
cur.execute("DROP TABLE IF EXISTS MartProperty;")
conn.commit()

# ------------------------------
# DIMENSION TABLES
# ------------------------------

# DimLocation
cur.execute("""
CREATE TABLE IF NOT EXISTS DimLocation_DM (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    street VARCHAR(150),
    ward VARCHAR(100),
    district VARCHAR(100),
    city VARCHAR(100),
    old_address VARCHAR(200),
    INDEX(street),
    INDEX(district)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# DimPropertyType
cur.execute("""
CREATE TABLE IF NOT EXISTS DimPropertyType_DM (
    property_type_id INT AUTO_INCREMENT PRIMARY KEY,
    type_name VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# DimPostingDate
cur.execute("""
CREATE TABLE IF NOT EXISTS DimPostingDate_DM (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    posting_date DATE UNIQUE,
    year INT,
    month INT,
    day INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# ------------------------------
# FACT TABLE
# ------------------------------
cur.execute("""
CREATE TABLE IF NOT EXISTS FactProperty_DM (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    listing_key VARCHAR(100),
    name VARCHAR(255),

    property_type_id INT,
    location_id INT,
    date_id INT,

    price DOUBLE,
    area DOUBLE,
    price_per_m2 DOUBLE,

    bedrooms INT,
    floors INT,
    street_width VARCHAR(50),

    posting_date DATE,
    create_date DATE,

    startDay DATE, 
    endDay DATE,
    isCurrent TINYINT(1) DEFAULT 1,

    INDEX(listing_key),

    FOREIGN KEY(property_type_id)
        REFERENCES DimPropertyType_DM(property_type_id),

    FOREIGN KEY(location_id)
        REFERENCES DimLocation_DM(location_id),

    FOREIGN KEY(date_id)
        REFERENCES DimPostingDate_DM(date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")
# ------------------------------
# startDay DATE: lưu ngày bắt đầu listing.

#endDay DATE: lưu ngày kết thúc listing (có thể NULL).

#isCurrent TINYINT(1) DEFAULT 1: đánh dấu listing đang hiện hành.
# --

# ------------------------------
# AGGREGATE TABLES
# ------------------------------

cur.execute("""
CREATE TABLE IF NOT EXISTS MartAgg_District (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(255),
    district VARCHAR(255),
    listing_count INT,
    avg_price DOUBLE,
    avg_area DOUBLE,
    avg_price_per_m2 DOUBLE,
    snapshot_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS MartAgg_TypeMonth (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_type VARCHAR(255),
    year INT,
    month INT,
    listing_count INT,
    avg_price DOUBLE,
    avg_area DOUBLE,
    avg_price_per_m2 DOUBLE,
    snapshot_date DATE,
    UNIQUE KEY ux_type_year_month (property_type, year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

conn.commit()
print("✅ Data Mart schema created successfully!")

cur.close()
conn.close()
