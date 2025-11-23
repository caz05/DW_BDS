import streamlit as st
import pandas as pd
import mysql.connector
import os
from datetime import datetime
import altair as alt
import subprocess
import re

st.set_page_config(page_title="Data Warehouse BĐS", layout="wide")
st.title("🏗️ DATA WAREHOUSE – FULL UI")

# ==============================
# DB CONFIG
# ==============================
staging_config = {
    'host': 'gondola.proxy.rlwy.net',
    'port': 39144,
    'user': 'root',
    'password': 'maqUtxJkDuZlpXXSXyIvXaPoMOcAjddv',
    'database': 'railway'
}

dw_config = {
    'host': 'shinkansen.proxy.rlwy.net',
    'port': 29701,
    'user': 'root',
    'password': 'IMRYCEqiQiiVCARSApGyHvNnYYKupjfX',
    'database': 'railway'
}

# ==============================
# PAGE SELECTION
# ==============================
page = st.sidebar.radio(
    "Chọn chức năng",
    [
        "1️⃣ Load Excel → STAGING",
        "2️⃣ STAGING → DATA WAREHOUSE (SCD2)",
        "3️⃣ Dashboard phân tích"
    ]
)

# ==============================
# HELPER FUNCTIONS
# ==============================
def safe_str(v, default="N/A"):
    return str(v) if pd.notna(v) else default

def safe_num(v, default=0):
    try:
        return float(v)
    except:
        return default

def parse_date(v):
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%d")
    except:
        return None

def parse_int(v, default=0):
    """
    Chuyển giá trị floors, bedrooms, ... thành số nguyên.
    Nếu string có chữ, chỉ lấy số đầu tiên, nếu không có số thì trả về default
    """
    try:
        nums = re.findall(r'\d+', str(v))
        return int(nums[0]) if nums else default
    except:
        return default

# ==============================
# MODULE 1 – LOAD EXCEL → STAGING
# ==============================
if page == "1️⃣ Load Excel → STAGING":
    st.header("📥 LOAD EXCEL → STAGING DATABASE")

    today_str = datetime.now().strftime('%d_%m_%Y')
    file_name = f"bds_{today_str}.xlsx"
    file_path = os.path.join("data", file_name)

    st.subheader("🕷️ Crawl dữ liệu mới")
    if st.button("🕷️ Crawl dữ liệu từ website"):
        st.info("⏳ Đang crawl dữ liệu...")
        try:
            subprocess.run(
                ["python", "D:/project_python/project_python/craw_data/stagging.py"],
                check=True
            )
            st.success("✅ Crawl thành công! File đã được tạo trong thư mục data/")
        except Exception as e:
            st.error(f"❌ Crawl thất bại: {e}")

    st.write(f"📄 File cần load: **{file_path}**")
    if not os.path.exists(file_path):
        st.error("❌ File không tồn tại! Hãy crawl dữ liệu trước.")
        st.stop()

    if st.button("🚀 Load dữ liệu vào STAGING"):
        df = pd.read_excel(file_path, engine="openpyxl")
        df.columns = df.columns.str.strip()

        conn = mysql.connector.connect(**staging_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Property")  # Xóa staging cũ
        conn.commit()

        # detect bedroom + area columns
        bedroom_col = [c for c in df.columns if "PN" in c or "Phòng ngủ" in c]
        area_col = [c for c in df.columns if "DT" in c or "Diện tích" in c]
        bedroom_col = bedroom_col[0] if bedroom_col else "PN"
        area_col = area_col[0] if area_col else "DT"

        insert_query = """
        INSERT INTO Property
        (`key`, url, create_date, name, price, area, old_address, street, ward, district, city,
         bedrooms, floors, street_width, description, posting_date, property_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        for idx, row in df.iterrows():
            cursor.execute(insert_query, (
                safe_str(row.get("Key", idx)),
                safe_str(row.get("URL")),
                parse_date(row.get("Ngày cào")),
                safe_str(row.get("Tên")),
                safe_num(row.get("Giá")),
                safe_num(row.get(area_col)),
                safe_str(row.get("Địa chỉ")),
                safe_str(row.get("Đường")),
                safe_str(row.get("Phường")),
                safe_str(row.get("Quận")),
                "Hồ Chí Minh",
                int(row.get(bedroom_col) or 1),
                safe_str(row.get("Tầng")),
                safe_str(row.get("Lộ giới")),
                safe_str(row.get("Mô tả")),
                parse_date(row.get("Ngày đăng")),
                safe_str(row.get("Loại nhà"), "Khác")
            ))

        conn.commit()
        cursor.close()
        conn.close()
        st.success("✅ LOAD EXCEL → STAGING THÀNH CÔNG")
        st.dataframe(df.head())

# ==============================
# MODULE 2 – STAGING → DATA WAREHOUSE (SCD2)
# ==============================
if page == "2️⃣ STAGING → DATA WAREHOUSE (SCD2)":
    st.header("🔄 LOAD STAGING → DW (SCD2)")

    if st.button("🚀 Chạy ETL SCD2"):
        st.info("⏳ Đang chạy ETL...")

        staging_conn = mysql.connector.connect(**staging_config)
        staging_cur = staging_conn.cursor(dictionary=True)
        staging_cur.execute("SELECT * FROM Property")
        staging_data = staging_cur.fetchall()
        staging_conn.close()

        dw_conn = mysql.connector.connect(**dw_config)
        dw_cur = dw_conn.cursor(dictionary=True)

        # --- Lấy danh sách FK hiện có ---
        dw_cur.execute("SELECT * FROM PropertyType")
        types = {row["type_name"]: row["property_type_id"] for row in dw_cur.fetchall()}

        dw_cur.execute("SELECT * FROM Location")
        locations = {}
        for row in dw_cur.fetchall():
            key = (row["street"], row["ward"], row["district"], row["city"], row["old_address"])
            locations[key] = row["location_id"]

        dw_cur.execute("SELECT * FROM PostingDate")
        dates = {row["posting_date"]: row["date_id"] for row in dw_cur.fetchall()}

        now = datetime.now().strftime("%Y-%m-%d")

        for row in staging_data:
            # --- PropertyType ---
            pt_name = safe_str(row["property_type"], "Khác")
            if pt_name not in types:
                dw_cur.execute("INSERT INTO PropertyType (type_name) VALUES (%s)", (pt_name,))
                dw_conn.commit()
                dw_cur.execute("SELECT LAST_INSERT_ID() AS id")
                types[pt_name] = dw_cur.fetchone()["id"]

            pt_id = types[pt_name]

            # --- Location ---
            loc_key = (
                safe_str(row["street"]),
                safe_str(row["ward"]),
                safe_str(row["district"]),
                "Hồ Chí Minh",
                safe_str(row["old_address"])
            )
            if loc_key not in locations:
                dw_cur.execute(
                    "INSERT INTO Location (street, ward, district, city, old_address) VALUES (%s,%s,%s,%s,%s)",
                    loc_key
                )
                dw_conn.commit()
                dw_cur.execute("SELECT LAST_INSERT_ID() AS id")
                locations[loc_key] = dw_cur.fetchone()["id"]

            loc_id = locations[loc_key]

            # --- PostingDate ---
            post_date = parse_date(row.get("posting_date")) or now
            if post_date not in dates:
                dw_cur.execute("INSERT INTO PostingDate (posting_date) VALUES (%s)", (post_date,))
                dw_conn.commit()
                dw_cur.execute("SELECT LAST_INSERT_ID() AS id")
                dates[post_date] = dw_cur.fetchone()["id"]

            date_id = dates[post_date]

            # --- SCD2 LOGIC ---
            # Kiểm tra bản ghi hiện tại
            dw_cur.execute(
                "SELECT * FROM PropertyListing WHERE `key`=%s AND isCurrent=1",
                (safe_str(row["key"]),)
            )
            current = dw_cur.fetchone()

            price = safe_num(row["price"])
            area = safe_num(row["area"])
            bedrooms = int(row["bedrooms"] or 1)

            if current:
                # Nếu giá hoặc diện tích thay đổi → đóng bản ghi cũ
                if float(current["price"]) != price or float(current["area"]) != area or int(current["bedrooms"]) != bedrooms:
                    dw_cur.execute(
                        "UPDATE PropertyListing SET isCurrent=0, endDay=%s WHERE sk=%s",
                        (now, current["sk"])
                    )
                    dw_conn.commit()
                else:
                    continue  # Không thay đổi → skip

            # Insert bản ghi mới
            dw_cur.execute("""
                INSERT INTO PropertyListing
                (`key`, url, create_date, name, price, area, bedrooms, floors, description, street_width,
                property_type_id, location_id, date_id, startDay, endDay, isCurrent)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                safe_str(row["key"]),
                safe_str(row["url"]),
                parse_date(row["create_date"]),
                safe_str(row["name"]),
                safe_num(row["price"]),
                safe_num(row["area"]),
                parse_int(row["bedrooms"], 1),
                parse_int(row["floors"], 0),  # <-- sửa ở đây
                safe_str(row["description"]),
                safe_str(row["street_width"]),
                pt_id,
                loc_id,
                date_id,
                now,
                None,
                1
            ))
            dw_conn.commit()

        dw_cur.close()
        dw_conn.close()
        st.success("✅ ETL SCD2 thành công! Dữ liệu đã được đưa vào PropertyListing.")

# ============================== MODULE 3 – DASHBOARD ==============================
if page == "3️⃣ Dashboard phân tích":
    st.header("📊 Dashboard phân tích thị trường BĐS")

    dw_conn = mysql.connector.connect(**dw_config)
    
    # Join bảng Location để lấy district, ward, street
    df = pd.read_sql("""
        SELECT p.*, l.district, l.ward, l.street, l.city, d.posting_date
        FROM PropertyListing p
        LEFT JOIN Location l ON p.location_id = l.location_id
        LEFT JOIN PostingDate d ON p.date_id = d.date_id
        WHERE p.isCurrent=1
    """, dw_conn)
    dw_conn.close()

    st.subheader("📍 Lọc dữ liệu")
    
    if "district" in df.columns:
        districts = sorted(df["district"].dropna().unique())
        q = st.selectbox("Chọn quận:", ["Tất cả"] + districts)
        df2 = df if q == "Tất cả" else df[df["district"] == q]
    else:
        df2 = df.copy()
    
    st.dataframe(df2.head(), use_container_width=True)

    # Giá số
    df2["price_num"] = pd.to_numeric(df2["price"].fillna(0), errors="coerce")

    st.subheader("📈 Phân phối giá")
    hist = alt.Chart(df2).mark_bar().encode(
        x=alt.X("price_num:Q", bin=alt.Bin(maxbins=60), title="Giá (VNĐ)"),
        y=alt.Y("count()", title="Số lượng tin")
    ).properties(height=350)
    st.altair_chart(hist, use_container_width=True)

    st.subheader("📦 Boxplot giá")
    box = alt.Chart(df2).mark_boxplot().encode(
        y=alt.Y("price_num:Q", title="Giá (VNĐ)")
    ).properties(height=300)
    st.altair_chart(box, use_container_width=True)

    # Trend giá theo ngày
    if "posting_date" in df2.columns:
        try:
            # Chuyển về datetime, bỏ các giá trị lỗi
            df2["posting_date"] = pd.to_datetime(df2["posting_date"], errors="coerce")
            df2 = df2.dropna(subset=["posting_date"])  # loại bỏ NULL

            # Tính giá trung bình theo ngày
            trend = (
                df2.groupby(df2["posting_date"].dt.normalize())["price_num"]
                .mean()
                .reset_index()
                .rename(columns={"posting_date": "date"})
            )

            st.subheader("📅 Trend giá theo ngày")
            line = alt.Chart(trend).mark_line(point=True).encode(
                x=alt.X("date:T", title="Ngày"),
                y=alt.Y("price_num:Q", title="Giá TB")
            ).properties(height=350)
            st.altair_chart(line, use_container_width=True)
        except Exception as e:
            st.info(f"Không vẽ được biểu đồ trend: {e}")

