from datetime import date
import glob
from dotenv import load_dotenv #pip install python-dotenv
import streamlit as st
import subprocess
import pandas as pd
import mysql.connector
import altair as alt
import json
import os,sys
from dotenv import load_dotenv

#Load biến môi trường trước
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

# Chạy gửi mail báo lỗi tại local
#env_path = os.path.join(os.path.dirname(__file__), '.env')
env_path = os.path.join(ROOT_DIR, 'template', '.env')
print(env_path)
load_dotenv(env_path)

from config.config import cfg
from template.notification import send_error_email


# ======================= 1. CONFIGURATION =======================
st.set_page_config(
    page_title="Real Estate Daily Monitor",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load Config
try:
    
    dm_cfg = cfg["datamart"]  # Sử dụng Data Mart
except FileNotFoundError:
    st.error("Không tìm thấy file config/config.json. Vui lòng kiểm tra lại!")
    st.stop()

# ---------------- DATABASE CONNECTION ----------------
def query_dm(sql, params=None):
    """Query Data Mart và trả về DataFrame"""
    try:
        conn = mysql.connector.connect(**dm_cfg)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Lỗi query Data Mart: {e}")
        return pd.DataFrame()


# function để chạy script Python và hiện log
def run_etl_script(script_path, description):
    with st.spinner(f"Đang chạy: {description}..."):
        try:
            if not os.path.exists(script_path):
                st.error(f"Không tìm thấy file: {script_path}")
                return
            my_env = os.environ.copy()
            my_env["PYTHONIOENCODING"] = "utf-8"
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

                # Gửi mail tự động
                send_error_email(script_path, out.stderr) 
                st.toast("Đã gửi email cảnh báo lỗi cho Admin!")

                with st.expander("Xem lỗi (Error Log)"):
                    st.code(out.stderr)
        except Exception as e:
            st.error(f"Lỗi hệ thống: {e}")
            send_error_email(script_path, str(e)) #gửi mail

# ---------------- Kiểm tra file crawl ----------------
def check_crawled_file_exists(today_date):
    patterns = [
        f"*{today_date.strftime('%Y-%m-%d')}*",
        f"*{today_date.strftime('%d_%m_%Y')}*"
    ]
    found_files = []
    search_dirs = [".", "dataset", "data", "craw_data"]
    for folder in search_dirs:
        if os.path.exists(folder):
            for pattern in patterns:
                full_path = os.path.join(folder, pattern)
                found_files.extend(glob.glob(full_path))
    return len(found_files) > 0, found_files

# ======================= 2. SIDEBAR NAVIGATION =======================
with st.sidebar:
    st.title("Real Estate DW")
    st.caption("Hệ thống giám sát & ETL Local")
    st.markdown("---")

    # today = date(2025, 11, 25)
    today = date.today()
    st.markdown(f"### Date: **{today.strftime('%d/%m/%Y')}**") 

    has_file, file_list = check_crawled_file_exists(today)

    # Kiểm tra DB Data Mart có dữ liệu hôm nay không
    check_sql = """
        SELECT COUNT(*) AS cnt
        FROM FactProperty_DM f
        JOIN DimPostingDate_DM d ON f.date_id = d.date_id
        WHERE d.posting_date = %s
    """
    df_check = query_dm(check_sql, params=(today,))
    has_db_data = df_check.iloc[0]['cnt'] > 0 if not df_check.empty else False

    st.markdown("#### Trạng thái:")
    if has_file:
        st.success(f"Đã có File Crawl ({len(file_list)} file)")
    else:
        st.warning("Chưa thấy File Crawl hôm nay")
        
    if has_db_data:
        st.success(f"Data Mart: {df_check.iloc[0]['cnt']} dòng")
    else:
        st.error("Data Mart: Trống")

    st.markdown("---")
    menu = st.radio(
        "Điều hướng:",
        ["ETL Pipeline", "Dashboard", "Data Marts"],
        index=0
    )
    st.markdown("---")
    st.caption("Project: Data Warehouse - Team: 3")

# ======================= 3. MAIN CONTENT =======================
st.title(f"{menu}")

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
        if st.button("3. Load dữ liệu thô vào Staging DB", use_container_width=True):
            run_etl_script("loadData/load_data_stagging.py", "Load to Staging")
        if st.button("4. Transform & Load to Staging DB", use_container_width=True):
            run_etl_script("transform/transform_staging.py", "Transform & Load to Staging DB")

    with col2:
        st.markdown("#### 2. Giai đoạn Data Warehouse")
        if st.button("5. Tạo Database Data Warehouse", use_container_width=True):
            run_etl_script("create_database/create_table_dw.py", "Tạo DB Data Warehouse")
        if st.button("6. Load vào Data Warehouse", use_container_width=True):
            run_etl_script("loadData/load_data_datawarehouse.py", "Load to Data Warehouse")
        if st.button("7. Load vào Data Mart", use_container_width=True):
            run_etl_script("loadData/load_data_mart.py", "Load to Data Mart")

# ------------------- CHỌN NGÀY CHUNG -------------------
st.markdown("### Chọn ngày để hiển thị dữ liệu")
selected_date = st.date_input("Chọn ngày:", value=pd.to_datetime(today))
selected_dt = pd.to_datetime(selected_date).normalize()

# =================== TAB: DASHBOARD ===================
if menu == "Dashboard":
    st.markdown("### Thống kê Bất động sản")
    if not has_db_data:
        st.warning(f"Chưa có dữ liệu Data Mart hôm nay ({selected_date}). Biểu đồ có thể trống.")
        
    col_metrics, col_padding = st.columns([2, 1])
    with col_metrics:
        metric_choice = st.selectbox(
            "Chọn tiêu chí thống kê:",
            ["Giá trung bình theo Quận", "Số lượng bài đăng theo Quận", "Diện tích trung bình theo Quận"]
        )

    st.divider()

    if metric_choice == "Giá trung bình theo Quận":
        sql = """
            SELECT l.district, AVG(f.price) AS avg_price
            FROM FactProperty_DM f
            JOIN DimLocation_DM l ON f.location_id = l.location_id
            JOIN DimPostingDate_DM d ON f.date_id = d.date_id
            WHERE d.posting_date = %s
            GROUP BY l.district
            ORDER BY avg_price DESC;
        """
        df = query_dm(sql, params=(selected_dt,))
        if not df.empty:
            chart = alt.Chart(df).mark_bar(color='#4c78a8').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("avg_price:Q", title="Giá TB (VNĐ)"),
                tooltip=["district", alt.Tooltip("avg_price", format=",.0f")]
            ).properties(title=f"Giá BĐS trung bình ({selected_date})")
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("Chưa có dữ liệu về Giá.")

    elif metric_choice == "Số lượng bài đăng theo Quận":
        sql = """
            SELECT l.district, COUNT(*) AS cnt
            FROM FactProperty_DM f
            JOIN DimLocation_DM l ON f.location_id = l.location_id
            JOIN DimPostingDate_DM d ON f.date_id = d.date_id
            WHERE d.posting_date = %s
            GROUP BY l.district
            ORDER BY cnt DESC;
        """
        df = query_dm(sql, params=(selected_dt,))
        if not df.empty:
            chart = alt.Chart(df).mark_bar(color='#f58518').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("cnt:Q", title="Số lượng tin"),
                tooltip=["district", "cnt"]
            ).properties(title=f"Số lượng tin đăng ({selected_date})")
            st.altair_chart(chart, use_container_width=True)

    elif metric_choice == "Diện tích trung bình theo Quận":
        sql = """
            SELECT l.district, AVG(f.area) AS avg_area
            FROM FactProperty_DM f
            JOIN DimLocation_DM l ON f.location_id = l.location_id
            JOIN DimPostingDate_DM d ON f.date_id = d.date_id
            WHERE d.posting_date = %s
            GROUP BY l.district
            ORDER BY avg_area DESC;
        """
        df = query_dm(sql, params=(selected_dt,))
        if not df.empty:
            chart = alt.Chart(df).mark_line(point=True, color='#e45756').encode(
                x=alt.X("district:N", title="Quận", sort="-y"),
                y=alt.Y("avg_area:Q", title="Diện tích TB (m2)"),
                tooltip=["district", alt.Tooltip("avg_area", format=".1f")]
            ).properties(title=f"Diện tích trung bình ({selected_date})")
            st.altair_chart(chart, use_container_width=True)

# =================== TAB: DATA MARTS ===================
elif menu == "Data Marts":
    if not has_db_data:
        st.warning(f"Database Data Mart chưa có dữ liệu ngày {selected_date}. Một số phân tích có thể trống.")

    st.divider()
    col_left, col_right = st.columns(2)

    # 1. Tương quan Giá - Diện tích
    with col_left:
        st.markdown("#### 1. Tương quan Giá - Diện tích")
        sql_scatter = """
            SELECT CAST(f.price AS DECIMAL(15,2)) AS price,
                   CAST(f.area AS DECIMAL(10,2)) AS area,
                   pt.type_name
            FROM FactProperty_DM f
            JOIN DimPropertyType_DM pt ON f.property_type_id = pt.property_type_id
            JOIN DimPostingDate_DM d ON f.date_id = d.date_id
            WHERE d.posting_date = %s
              AND f.area > 0 AND f.price > 0
            LIMIT 500;
        """
        df_scatter = query_dm(sql_scatter, params=(selected_dt,))
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

    # 2. Tỷ trọng Loại hình
    with col_right:
        st.markdown("#### 2. Tỷ trọng Loại hình BĐS")
        sql_type = """
            SELECT pt.type_name, COUNT(*) AS total_count
            FROM FactProperty_DM f
            JOIN DimPropertyType_DM pt ON f.property_type_id = pt.property_type_id
            JOIN DimPostingDate_DM d ON f.date_id = d.date_id
            WHERE d.posting_date = %s
            GROUP BY pt.type_name
            ORDER BY total_count DESC;
        """
        df_type = query_dm(sql_type, params=(selected_dt,))
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

    # 3. Top Quận đơn giá cao nhất
    st.markdown(f"#### 3. Top Quận có Đơn giá cao nhất ({selected_date})")
    sql_unit_price = """
        SELECT l.district, AVG(f.price / f.area) AS price_per_m2
        FROM FactProperty_DM f
        JOIN DimLocation_DM l ON f.location_id = l.location_id
        JOIN DimPostingDate_DM d ON f.date_id = d.date_id
        WHERE d.posting_date = %s AND f.area > 0
        GROUP BY l.district
        ORDER BY price_per_m2 DESC
        LIMIT 15;
    """
    df_unit = query_dm(sql_unit_price, params=(selected_dt,))
    if not df_unit.empty:
        chart_unit = alt.Chart(df_unit).mark_bar(color='#8E44AD').encode(
            x=alt.X('district:N', sort='-y', title='Quận'),
            y=alt.Y('price_per_m2:Q', title='Đơn giá TB (VNĐ/m2)'),
            tooltip=['district', alt.Tooltip('price_per_m2', format=',.0f')]
        )
        st.altair_chart(chart_unit, use_container_width=True)
    else:
        st.warning("Chưa tính được đơn giá ngày này.")

    # 4. Xu hướng thị trường (lịch sử)
    st.markdown("---")
    st.markdown("#### 4. Bối cảnh: Xu hướng thị trường (Lịch sử)")

    # Lấy dữ liệu trung bình giá 30 ngày gần nhất
    sql_trend = """
        SELECT d.posting_date, AVG(f.price) AS avg_price
        FROM FactProperty_DM f
        JOIN DimPostingDate_DM d ON f.date_id = d.date_id
        GROUP BY d.posting_date
        ORDER BY d.posting_date DESC
        LIMIT 30;
    """
    df_trend = query_dm(sql_trend)
    if not df_trend.empty:
        df_trend['posting_date'] = pd.to_datetime(df_trend['posting_date']).dt.normalize()
        df_trend = df_trend.sort_values('posting_date')
        df_selected = df_trend[df_trend['posting_date'] == selected_dt]

        # Vẽ line chart
        base = alt.Chart(df_trend).encode(x=alt.X('posting_date:T', title='Ngày'))
        line = base.mark_line(color='gray').encode(y=alt.Y('avg_price:Q', title='Giá TB (VNĐ)'))

        # Highlight ngày chọn
        points = alt.Chart(df_selected).mark_point(color='red', size=200, filled=True).encode(
            x='posting_date:T',
            y='avg_price:Q',
            tooltip=[alt.Tooltip('posting_date:T', title='Ngày'), alt.Tooltip('avg_price:Q', title='Giá TB')]
        )

        st.altair_chart(line + points, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu để vẽ biểu đồ xu hướng lịch sử.")


