from datetime import date
import glob
import streamlit as st
import subprocess
import pandas as pd
import mysql.connector
import altair as alt
import json
import os

# ======================= 1. CONFIGURATION =======================
st.set_page_config(
    page_title="Real Estate Daily Monitor",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load Config
try:
    PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))

    # Nếu index.py nằm trong /template → lùi 1 cấp ra thư mục cha
    config_path = os.path.join(PROJECT_ROOT, "..", "config", "config.json")
    config_path = os.path.abspath(config_path)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    staging_cfg = config["staging"]
    dw_cfg = config["datawarehouse"]
except FileNotFoundError:
    st.error("Không tìm thấy file config/config.json. Vui lòng kiểm tra lại!")
    st.stop()

# Database Connection 
def query_dw(sql, params=None):
    try:
        conn = mysql.connector.connect(**dw_cfg)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# Helper function để chạy script Python và hiện log
def run_etl_script(script_path, description):
    with st.spinner(f"Đang chạy: {description}..."):
        try:
            # Kiểm tra file có tồn tại không trước khi chạy
            if not os.path.exists(script_path):
                st.error(f"Không tìm thấy file: {script_path}")
                return

            # Chạy script
            # Thêm cấu hình encoding
            my_env = os.environ.copy()
            my_env["PYTHONIOENCODING"] = "utf-8"

            # Thêm tham số env=my_env và encoding="utf-8"
            out = subprocess.run(
                ["python", script_path], 
                capture_output=True, 
                text=True, 
                encoding="utf-8", 
                env=my_env
            )
            
            if out.returncode == 0:
                st.success(f"{description} thành công!")
                with st.expander("Xem chi tiết Log (Output)"):
                    st.code(out.stdout)
            else:
                st.error(f"{description} thất bại!")
                with st.expander("Xem lỗi (Error Log)"):
                    st.code(out.stderr)
        except Exception as e:
            st.error(f"Lỗi hệ thống: {e}")

# Hàm kiểm tra file đã Crawl chưa (Check File System)
def check_crawled_file_exists(today_date):

    patterns = [
        f"*{today_date.strftime('%Y-%m-%d')}*", 
        f"*{today_date.strftime('%d_%m_%Y')}*"
    ]
    
    found_files = []
    # Tìm file trong thư mục 
    search_dirs = [".", "dataset", "data", "craw_data"]
    
    for folder in search_dirs:
        if os.path.exists(folder):
            for pattern in patterns:
                # Tìm tất cả file khớp pattern (vd: batdongsan_2023-11-23.csv)
                full_path = os.path.join(folder, pattern)
                files = glob.glob(full_path)
                found_files.extend(files)
    
    return len(found_files) > 0, found_files
# ======================= 2. SIDEBAR NAVIGATION =======================
with st.sidebar:
    st.title("Real Estate DW")
    st.caption("Hệ thống giám sát & ETL Local")
    st.markdown("---")
# --- 2.1. Ngày xem ---
    today = date.today()
    st.markdown(f"### Date: **{today.strftime('%d/%m/%Y')}**") 
    
    # Có file CSV/Excel chưa?
    has_file, file_list = check_crawled_file_exists(today)
    
    # Có dữ liệu trong DB chưa?
    check_sql = """
        SELECT COUNT(*) as cnt 
        FROM PropertyListing
        WHERE create_date = %s
    """
    df_check = query_dw(check_sql, params=(today,))
    db_count = df_check.iloc[0]['cnt'] if not df_check.empty else 0
    has_db_data = db_count > 0

    # Hiển thị trạng thái bên trái
    st.markdown("#### Trạng thái:")
    if has_file:
        st.success(f"Đã có File Crawl ({len(file_list)} file)")
    else:
        st.warning("Chưa thấy File Crawl hôm nay")
        
    if has_db_data:
        st.success(f"DB Warehouse: {db_count} dòng")
    else:
        st.error("DB Warehouse: Trống")

    st.markdown("---")
    # --- 2.3. MENU ---
    menu = st.radio(
        "Điều hướng:",
        ["ETL Pipeline", "Dashboard", "Data Marts"],
        index=0
    )
    
    st.markdown("---")
    st.caption("Project: Data Warehouse"
               " -Team: 3")

# ======================= 3. MAIN CONTENT =======================

st.title(f"{menu}") # Tiêu đề động dựa theo menu chọn

# ------------------- TAB: ETL PIPELINE -------------------
if menu == "ETL Pipeline":
    
    st.markdown("### Quản lý quy trình xử lý dữ liệu")
    st.info("Bấm vào các bước bên dưới để chạy quy trình ETL.")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("#### 1. Giai đoạn Staging")
        if st.button("1. Tạo Database Staging", use_container_width=True):
            run_etl_script("create_database/create_table_stagging.py", "Tạo DB Staging")
            
        if st.button("2. Crawl Data", use_container_width=True):
            run_etl_script("craw_data/stagging.py", "Crawl Data")

        if st.button("3. Load vào Staging DB", use_container_width=True):
            run_etl_script("loadData/load_data_stagging.py", "Load to Staging")

    with col2:
        st.markdown("#### 2. Giai đoạn Data Warehouse")
        if st.button("4. Tạo Database Data Warehouse", use_container_width=True):
            run_etl_script("create_database/create_table_dw.py", "Tạo DB Data Warehouse")

        if st.button("5. Load vào Data Warehouse", use_container_width=True):
            run_etl_script("loadData/load_data_datawarehouse.py", "Load to Data Warehouse")
# ------------------- TAB: DASHBOARD -------------------
elif menu == "Dashboard":
    st.markdown("### Thống kê Bất động sản")
    
    # Filter Section
    if not has_db_data:
        st.warning(f"**Cảnh báo:** Chưa có dữ liệu trong Database cho ngày **{today}**. Biểu đồ có thể bị trống.")
        
    col_metrics, col_padding = st.columns([2, 1])
    with col_metrics:
            metric_choice = st.selectbox(
                "Chọn tiêu chí thống kê:",
                ["Giá trung bình theo Quận", "Số lượng bài đăng theo Quận", "Diện tích trung bình theo Quận"]
            )

    st.divider()

    # Logic hiển thị biểu đồ
    if metric_choice == "Giá trung bình theo Quận":
        sql = """
            SELECT L.district, AVG(P.price) AS avg_price
            FROM PropertyListing P
            JOIN Location L ON P.location_id = L.location_id
            JOIN PostingDate D ON P.date_id = D.date_id
            WHERE D.posting_date = %s 
            GROUP BY L.district
            ORDER BY avg_price DESC;
        """
        df = query_dw(sql, params=(today,))
        if not df.empty:
            chart = alt.Chart(df).mark_bar(color='#4c78a8').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("avg_price:Q", title="Giá TB (VNĐ)"),
                tooltip=["district", alt.Tooltip("avg_price", format=",.0f")]
            ).properties(title=f"Giá BĐS trung bình (Dữ liệu: {today})")
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("Hôm nay chưa có dữ liệu về Giá.")

    elif metric_choice == "Số lượng bài đăng theo Quận":
        sql = """
            SELECT L.district, COUNT(*) AS cnt
            FROM PropertyListing P
            JOIN Location L ON P.location_id = L.location_id
            JOIN PostingDate D ON P.date_id = D.date_id
            WHERE D.posting_date = %s
            GROUP BY L.district
            ORDER BY cnt DESC;
        """
        df = query_dw(sql, params=(today,))
        if not df.empty:
            chart = alt.Chart(df).mark_bar(color='#f58518').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("cnt:Q", title="Số lượng tin"),
                tooltip=["district", "cnt"]
            ).properties(title=f"Số lượng tin đăng mới ({today})")
            st.altair_chart(chart, use_container_width=True)

    elif metric_choice == "Diện tích trung bình theo Quận":
        sql = """
            SELECT L.district, AVG(P.area) AS avg_area
            FROM PropertyListing P
            JOIN Location L ON P.location_id = L.location_id
            JOIN PostingDate D ON P.date_id = D.date_id
            WHERE D.posting_date = %s
            GROUP BY L.district
            ORDER BY avg_area DESC;
        """
        df = query_dw(sql, params=(today,))
        if not df.empty:
            chart = alt.Chart(df).mark_line(point=True, color='#e45756').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("avg_area:Q", title="Diện tích TB (m2)"),
                tooltip=["district", alt.Tooltip("avg_area", format=".1f")]
            )
            st.altair_chart(chart, use_container_width=True)
# ------------------- TAB: DATA MARTS -------------------
elif menu == "Data Marts":
    # CHẶN NẾU CHƯA CÓ DỮ LIỆU
    if not has_db_data:
        st.warning("**Lưu ý:** Database Warehouse chưa có dữ liệu ngày hôm nay. Các phân tích dưới đây có thể trống.")

    st.divider()

    col_left, col_right = st.columns(2)

    # --- 1. Tương quan Giá & Diện tích (Chỉ hôm nay) ---
    with col_left:
        st.markdown("#### 1. Tương quan Giá - Diện tích")
        sql_scatter = """
            SELECT 
                CAST(price AS DECIMAL(15,2)) as price, 
                CAST(area AS DECIMAL(10,2)) as area, 
                T.type_name
            FROM PropertyListing P
            JOIN PropertyType T ON P.property_type_id = T.property_type_id
            WHERE create_date = %s 
              AND CAST(area AS DECIMAL(10,2)) > 0 
              AND CAST(price AS DECIMAL(15,2)) > 0
            LIMIT 500;
        """
        df_scatter = query_dw(sql_scatter, params=(today,))
        
        if not df_scatter.empty:
            chart_scatter = alt.Chart(df_scatter).mark_circle(size=60).encode(
                x=alt.X('area:Q', title='Diện tích (m2)', scale=alt.Scale(zero=False)),
                y=alt.Y('price:Q', title='Giá (VNĐ)', scale=alt.Scale(zero=False)),
                color='type_name:N',
                tooltip=['type_name', 'area', 'price']
            ).interactive()
            st.altair_chart(chart_scatter, use_container_width=True)
        else:
            st.info("Không đủ dữ liệu vẽ biểu đồ tương quan.")

    # --- 2. Phân bổ Loại hình (Chỉ hôm nay) ---
    with col_right:
        st.markdown("#### 2. Tỷ trọng Loại hình BĐS")
        sql_type = """
            SELECT T.type_name, COUNT(*) as total_count
            FROM PropertyListing P
            JOIN PropertyType T ON P.property_type_id = T.property_type_id
            WHERE create_date = %s
            GROUP BY T.type_name
            ORDER BY total_count DESC;
        """
        df_type = query_dw(sql_type, params=(today,))
        
        if not df_type.empty:
            chart_pie = alt.Chart(df_type).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="total_count", type="quantitative"),
                color=alt.Color(field="type_name", type="nominal"),
                tooltip=['type_name', 'total_count']
            )
            st.altair_chart(chart_pie, use_container_width=True)
        else:
            st.info("Không có dữ liệu loại hình.")
    
    st.divider()

    # --- 3. Đơn giá theo Quận (Chỉ hôm nay) ---
    st.markdown("#### 3. Top Quận có Đơn giá cao nhất hôm nay (VNĐ/m²)")
    sql_unit_price = """
        SELECT 
            L.district, 
            AVG(CAST(price AS DECIMAL(15,2)) / CAST(area AS DECIMAL(10,2))) as price_per_m2
        FROM PropertyListing P
        JOIN Location L ON P.location_id = L.location_id
        WHERE create_date = %s 
          AND CAST(area AS DECIMAL(10,2)) > 0
        GROUP BY L.district
        ORDER BY price_per_m2 DESC
        LIMIT 15;
    """
    df_unit = query_dw(sql_unit_price, params=(today,))
    
    if not df_unit.empty:
        chart_unit = alt.Chart(df_unit).mark_bar(color='#8E44AD').encode(
            x=alt.X('district:N', sort='-y', title='Quận'),
            y=alt.Y('price_per_m2:Q', title='Đơn giá TB (VNĐ/m2)'),
            tooltip=['district', alt.Tooltip('price_per_m2', format=',.0f')]
        )
        st.altair_chart(chart_unit, use_container_width=True)
    else:
        st.warning("Chưa tính được đơn giá hôm nay.")

    # --- 4. Context: Xu hướng thị trường (Toàn thời gian) ---
    st.markdown("---")
    st.markdown("#### 4. Bối cảnh: Xu hướng thị trường (Lịch sử)")
    st.caption("Biểu đồ so sánh ngày hôm nay (chấm đỏ) với lịch sử trước đó.")
    
    # Không lọc WHERE date ở đây để xem xu hướng toàn cục
    sql_trend = """
        SELECT 
            posting_date, 
            AVG(CAST(price AS DECIMAL(15,2))) AS avg_price
        FROM PropertyListing P
        JOIN PostingDate D ON P.date_id = D.date_id
        GROUP BY posting_date
        ORDER BY posting_date ASC;
    """
    df_trend = query_dw(sql_trend)
    
    if not df_trend.empty:
        base = alt.Chart(df_trend).encode(x=alt.X('posting_date:T', title='Ngày'))
        
        # Đường lịch sử
        line = base.mark_line(color='gray').encode(
            y=alt.Y('avg_price:Q', title='Giá TB')
        )
        
        # Lọc trong dataframe của pandas để lấy dòng có ngày == today
        df_today_trend = df_trend[df_trend['posting_date'].astype(str) == str(today)]
        
        points = alt.Chart(df_today_trend).mark_point(
            color='red', size=200, filled=True
        ).encode(
            x='posting_date:T',
            y='avg_price:Q',
            tooltip=['posting_date', 'avg_price']
        )

        st.altair_chart(line + points, use_container_width=True)