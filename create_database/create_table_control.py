import json
import mysql.connector

# Load config
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

control_config = cfg["control"]

conn = mysql.connector.connect(**control_config)
cursor = conn.cursor()

# DROP TABLE nếu đã tồn tại

cursor.execute("DROP TABLE IF EXISTS process_log;")
cursor.execute("DROP TABLE IF EXISTS file_log;")
conn.commit()

# CREATE TABLE file_log
cursor.execute("""
CREATE TABLE IF NOT EXISTS file_log (
    file_id INT PRIMARY KEY AUTO_INCREMENT,
    file_path VARCHAR(255),
    data_date DATE,                 
    row_count INT DEFAULT 0,       
    status VARCHAR(50),            
    created_at DATETIME DEFAULT NOW(), 
    updated_at DATETIME DEFAULT NOW(),
    author VARCHAR(50) DEFAULT 'System'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")
# CREATE TABLE process_log
cursor.execute("""
CREATE TABLE IF NOT EXISTS process_log (
    process_id INT PRIMARY KEY AUTO_INCREMENT,
    file_id INT,                        
    process_name VARCHAR(100),          -- Tên quy trình (VD: "Load to Staging")
    status VARCHAR(20),                 -- PS, FL, SC           
    started_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW(),
    FOREIGN KEY (file_id) REFERENCES file_log(file_id)
);
""")

conn.commit()
cursor.close()
conn.close()

print("Đã tạo bảng file_log và process_log thành công!")
