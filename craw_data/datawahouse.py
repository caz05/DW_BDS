import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime


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
    elif "biệt thự" in title:
        return "Biệt thự"
    elif "đất nền" in title or "đất nền" in description:
        return "Đất nền"
    else:
        return "Khác"


def parse_location(address):
    """Tách địa chỉ thành Đường, Phường, Quận, Thành phố"""
    ward = ''
    district = ''
    city = ''
    street = ''

    parts = [p.strip() for p in address.split(',') if p.strip()]
    parts_lower = [p.lower() for p in parts]

    ward_tokens = ['phường', 'p.', 'phuong', 'xã', 'xa']
    district_tokens = ['quận', 'q.', 'quan', 'huyện', 'h.']
    street_tokens = ['đường', 'duong', 'đ.', 'đường số', 'đường vào', 'đường lớn', 'đường nhỏ']
    city_tokens = ['thành phố', 'tp', 'tp.', 'thanh pho', 'tp hcm', 'hồ chí minh', 'ho chi minh']

    for i, p in enumerate(parts):
        low = parts_lower[i]
        if any(tok in low for tok in city_tokens):
            city = p
            continue
        if any(tok in low for tok in district_tokens):
            district = p
            continue
        if any(tok in low for tok in ward_tokens):
            ward = p
            continue
        if any(tok in low for tok in street_tokens):
            street = p
            continue

    if not city:
        city = "Hồ Chí Minh"

    # Nếu thiếu phường/quận thì lấy theo thứ tự từ cuối
    if not district or not ward:
        candidates = [p for p in parts if p and p != city]
        if len(candidates) >= 1 and not district:
            district = candidates[-1]
        if len(candidates) >= 2 and not ward:
            ward = candidates[-2]

    # Nếu chưa có đường thì lấy phần đầu
    if not street:
        if parts:
            first = parts[0]
            if first not in (ward, district, city):
                street = first

    # Xử lý trùng lặp
    if ward == district:
        ward = ''

    ward = ward.strip() if ward else ''
    district = district.strip() if district else ''
    city = city.strip() if city else 'Hồ Chí Minh'
    street = street.strip() if street else ''

    return street, ward, district, city


def crawl_page(page_num):
    url = (
        "https://alonhadat.com.vn/can-ban-nha-dat/ho-chi-minh"
        if page_num == 1
        else f"https://alonhadat.com.vn/can-ban-nha-dat/ho-chi-minh/trang-{page_num}"
    )
    resp = requests.get(url)
    resp.encoding = "utf-8"
    if resp.status_code != 200:
        print(f"Không lấy được trang {page_num} — status: {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    section = soup.find("section", class_="list-property-box")
    if not section:
        return []

    results = []
    for item in section.find_all("article", class_="property-item"):
        p = {}
        title = item.find("h3", class_="property-title")
        p["Tên"] = title.get_text(strip=True) if title else "N/A"

        price = item.find("span", class_="price")
        pt = price.find("span", itemprop="price") if price else None
        p["Giá"] = pt.get_text(strip=True) if pt else "N/A"

        area = item.find("span", class_="area")
        at = area.find("span", itemprop="value") if area else None
        p["DT"] = f"{at.get_text(strip=True)} m²" if at else "N/A"

        # ✅ Lấy đúng class 'old-address'
        address = item.find("p", class_="old-address")
        if address:
            parts = [x.get_text(strip=True) for x in address.find_all("span") if x.get_text(strip=True)]
            full_address = ", ".join(parts)
            p["Địa chỉ"] = full_address
            street, ward, district, city = parse_location(full_address)
            p["Đường"] = street if street else "N/A"
            p["Phường"] = ward if ward else "N/A"
            p["Quận"] = district if district else "N/A"
            p["Thành phố"] = city if city else "N/A"
        else:
            p["Địa chỉ"] = p["Đường"] = p["Phường"] = p["Quận"] = p["Thành phố"] = "N/A"

        bedrooms = item.find("span", class_="bedroom")
        vb = bedrooms.find("span", itemprop="value") if bedrooms else None
        p["PN"] = vb.get_text(strip=True) if vb else "N/A"

        floors = item.find("span", class_="floors")
        p["Tầng"] = floors.get_text(strip=True) if floors else "N/A"

        street_width = item.find("span", class_="street-width")
        p["Lộ giới"] = street_width.get_text(strip=True) if street_width else "N/A"

        desc = item.find("p", class_="brief")
        if desc:
            vd = desc.find("span", class_="view-detail")
            if vd:
                vd.decompose()
            txt = desc.get_text(strip=True)
            p["Mô tả"] = txt[:100] + "..." if len(txt) > 100 else txt
        else:
            p["Mô tả"] = "N/A"

        created = item.find("time", class_="created-date")
        p["Ngày đăng"] = parse_datetime(created["datetime"]) if created and created.has_attr("datetime") else "N/A"

        p["Loại nhà"] = get_property_type(p["Tên"], p["Mô tả"])
        results.append(p)

    return results


def crawl_all(pages=5, delay=1.0):
    all_props = []
    for i in range(1, pages + 1):
        print(f"Crawling trang {i}")
        data = crawl_page(i)
        if not data:
            print(f"Trang {i} không có dữ liệu hoặc kết thúc phân trang.")
            break
        all_props.extend(data)
        time.sleep(delay)
    return all_props


# ===== Main =====
props = crawl_all(pages=10, delay=1.5)
df = pd.DataFrame(props)

# ===== Tạo các bảng dimension và fact =====
dim_date = df[['Ngày đăng']].drop_duplicates().reset_index(drop=True)
dim_date['date_id'] = dim_date.index + 1

dim_location = df[['Đường','Phường','Quận','Thành phố']].drop_duplicates().reset_index(drop=True)
dim_location['location_id'] = dim_location.index + 1

dim_property_type = df[['Loại nhà']].drop_duplicates().reset_index(drop=True)
dim_property_type['property_type_id'] = dim_property_type.index + 1

fact = df.merge(dim_date, on='Ngày đăng', how='left') \
         .merge(dim_location, on=['Đường','Phường','Quận','Thành phố'], how='left') \
         .merge(dim_property_type, on='Loại nhà', how='left')

fact = fact[['Tên','Giá','DT','PN','Tầng','Lộ giới','Mô tả',
             'date_id','location_id','property_type_id']]

# ===== Lưu tất cả vào 1 file Excel nhiều sheet =====
with pd.ExcelWriter('DataWarehousenew_BDS.xlsx', engine='xlsxwriter') as writer:
    dim_date.to_excel(writer, sheet_name='Dim_Date', index=False)
    dim_location.to_excel(writer, sheet_name='Dim_Location', index=False)
    dim_property_type.to_excel(writer, sheet_name='Dim_PropertyType', index=False)
    fact.to_excel(writer, sheet_name='Fact_Property', index=False)

print("✅ Đã crawl xong và lưu tất cả bảng vào DataWarehouse_BDS.xlsx với nhiều sheet (địa chỉ đã đúng).")
