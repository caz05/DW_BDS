import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import os
import re

# ------------------ HÀM LOẠI BỎ ICON / EMOJI ------------------
def clean_text(text):
    cleaned = re.sub(r'[^a-zA-Z0-9À-ỹà-ỹ\s.,;:!?()-]', '', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

# ------------------ HÀM HỖ TRỢ ------------------
def parse_datetime(dt_str):
    try:
        dt_obj = datetime.fromisoformat(dt_str)
        return dt_obj.strftime('%Y-%m-%d')
    except:
        return 'N/A'

def get_property_type(title, description):
    title = title.lower()
    description = description.lower()
    if "căn hộ" in title or "căn hộ" in description:
        return "Căn hộ"
    elif "nhà phố" in title or "nhà phố" in description:
        return "Nhà phố"
    elif "biệt thự" in title or "biệt thự" in description:
        return "Biệt thự"
    elif "đất nền" in title or "đất nền" in description:
        return "Đất nền"
    else:
        return "Khác"

def parse_location(address):
    parts = [x.strip() for x in address.split(',')]
    ward = parts[0] if len(parts) > 0 else ''
    district = parts[1] if len(parts) > 1 else ''
    city = parts[2] if len(parts) > 2 else 'Hồ Chí Minh'
    return ward, district, city

# ------------------ HÀM CRAWL 1 TRANG ------------------
def crawl_page(page_num):
    crawl_date = datetime.now().strftime('%Y-%m-%d')
    if page_num == 1:
        url = "https://alonhadat.com.vn/can-ban-nha-dat/ho-chi-minh"
    else:
        url = f"https://alonhadat.com.vn/can-ban-nha-dat/ho-chi-minh/trang-{page_num}"

    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(url, headers=headers)
    resp.encoding = 'utf-8'
    if resp.status_code != 200:
        print(f"Không lấy được trang {page_num} — status: {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    section = soup.find('section', class_='list-property-box')
    if not section:
        return []

    results = []
    for item in section.find_all('article', class_='property-item'):
        p = {}

        # ===== LINK + ID =====
        link = item.find('a', href=True)
        if link:
            url = link['href']
            p['URL'] = "https://alonhadat.com.vn" + url
            try:
                p['Key'] = url.split('-')[-1].replace('.html', '')
            except:
                p['Key'] = 'N/A'
        else:
            p['URL'] = 'N/A'
            p['Key'] = 'N/A'

        title = item.find('h3', class_='property-title')
        p['Tên'] = clean_text(title.get_text(strip=True)) if title else 'N/A'

        price = item.find('span', class_='price')
        pt = price.find('span', itemprop='price') if price else None
        p['Giá'] = pt.get_text(strip=True) if pt else 'N/A'

        area = item.find('span', class_='area')
        at = area.find('span', itemprop='value') if area else None
        p['Diện tích'] = f"{at.get_text(strip=True)} m²" if at else 'N/A'

        address = item.find('p', class_='new-address')
        if address:
            parts = [x.get_text(strip=True) for x in address.find_all('span') if x.get_text(strip=True)]
            full_address = ', '.join(parts)
            p['Địa chỉ'] = full_address
            ward, district, city = parse_location(full_address)
            p['Phường'] = ward
            p['Quận'] = district
            p['Thành phố'] = city
        else:
            p['Địa chỉ'] = p['Phường'] = p['Quận'] = p['Thành phố'] = 'N/A'

        bedrooms = item.find('span', class_='bedroom')
        vb = bedrooms.find('span', itemprop='value') if bedrooms else None
        p['Phòng ngủ'] = vb.get_text(strip=True) if vb else 'N/A'

        floors = item.find('span', class_='floors')
        p['Tầng'] = floors.get_text(strip=True) if floors else 'N/A'

        street = item.find('span', class_='street-width')
        p['Lộ giới'] = street.get_text(strip=True) if street else 'N/A'

        desc = item.find('p', class_='brief')
        if desc:
            vd = desc.find('span', class_='view-detail')
            if vd: vd.decompose()
            txt = desc.get_text(strip=True)
            txt = txt[:100] + '...' if len(txt) > 100 else txt
            p['Mô tả'] = clean_text(txt)
        else:
            p['Mô tả'] = 'N/A'

        created = item.find('time', class_='created-date')
        p['Ngày đăng'] = parse_datetime(created['datetime']) if created and created.has_attr('datetime') else 'N/A'

        p['Loại nhà'] = get_property_type(p['Tên'], p['Mô tả'])
        p['Ngày cào'] = crawl_date

        results.append(p)

    return results

# ------------------ CRAWL TẤT CẢ ------------------
def crawl_all(pages=5, delay=1.0):
    all_props = []
    for i in range(1, pages + 1):
        print(f"Đang crawl trang {i}...")
        data = crawl_page(i)
        if not data:
            print(f"Trang {i} không có dữ liệu hoặc đã hết.")
            break
        all_props.extend(data)
        time.sleep(delay)
    return all_props

# ------------------ MAIN ------------------
props = crawl_all(pages=10, delay=1.5)
df = pd.DataFrame(props)

# ===== TÊN FILE THEO NGÀY =====
today_str = datetime.now().strftime('%d_%m_%Y')
file_name = f"bds_{today_str}.xlsx"
file_path = os.path.join("data", file_name)

# ===== GỘP DỮ LIỆU CŨ + MỚI VÀ LỌC TRÙNG THEO KEY =====
if os.path.exists(file_path):
    old_df = pd.read_excel(file_path)
    merged_df = pd.concat([old_df, df], ignore_index=True)
    final_df = merged_df.drop_duplicates(subset="Key", keep="last")
else:
    final_df = df

# ===== LƯU FILE =====
if not os.path.exists("data"):
    os.makedirs("data")

final_df.to_excel(file_path, index=False, engine="xlsxwriter")
print(f"✅ Staging đã lưu snapshot mới: {file_path}")
