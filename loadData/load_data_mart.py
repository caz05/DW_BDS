import json
import mysql.connector
from datetime import date

# -------------------------
# Load DB config
# -------------------------
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

dw_cfg = cfg["datawarehouse"]
dm_cfg = cfg["datamart"]

# -------------------------
# Connect DW & DM
# -------------------------
dw_conn = mysql.connector.connect(**dw_cfg)
dm_conn = mysql.connector.connect(**dm_cfg)

dw_cur = dw_conn.cursor(dictionary=True)
dm_cur = dm_conn.cursor()

print("✅ Connected to DW and DM")

# -------------------------
# Helper functions for dimensions
# -------------------------
def get_or_create_property_type(type_name):
    if not type_name:
        type_name = "Unknown"
    dm_cur.execute("SELECT property_type_id FROM DimPropertyType_DM WHERE type_name=%s", (type_name,))
    r = dm_cur.fetchone()
    if r:
        return r[0]
    dm_cur.execute("INSERT INTO DimPropertyType_DM (type_name) VALUES (%s)", (type_name,))
    dm_conn.commit()
    return dm_cur.lastrowid

def get_or_create_location(street, ward, district, city, old_address=None):
    street = street or ""
    ward = ward or ""
    district = district or ""
    city = city or ""
    dm_cur.execute("""
        SELECT location_id FROM DimLocation_DM
        WHERE street=%s AND ward=%s AND district=%s AND city=%s LIMIT 1
    """, (street, ward, district, city))
    r = dm_cur.fetchone()
    if r:
        return r[0]
    dm_cur.execute("""
        INSERT INTO DimLocation_DM (street, ward, district, city, old_address)
        VALUES (%s,%s,%s,%s,%s)
    """, (street, ward, district, city, old_address))
    dm_conn.commit()
    return dm_cur.lastrowid

def get_or_create_date(posting_date):
    if not posting_date:
        return None
    dm_cur.execute("SELECT date_id FROM DimPostingDate_DM WHERE posting_date=%s LIMIT 1", (posting_date,))
    r = dm_cur.fetchone()
    if r:
        return r[0]
    dm_cur.execute(
        "INSERT INTO DimPostingDate_DM (posting_date, year, month, day) VALUES (%s,%s,%s,%s)",
        (posting_date, posting_date.year, posting_date.month, posting_date.day)
    )
    dm_conn.commit()
    return dm_cur.lastrowid

# -------------------------
# Fetch current listings from DW
# -------------------------
dw_cur.execute("""
SELECT p.sk, p.`key` AS listing_key, p.url, p.name, pt.type_name AS property_type,
       p.price, p.area, l.old_address, l.street, l.ward, l.district, l.city,
       p.bedrooms, p.floors, p.street_width, pd.posting_date, p.create_date,
       p.startDay, p.endDay, p.isCurrent
FROM PropertyListing p
LEFT JOIN PropertyType pt ON p.property_type_id = pt.property_type_id
LEFT JOIN Location l ON p.location_id = l.location_id
LEFT JOIN PostingDate pd ON p.date_id = pd.date_id
WHERE p.isCurrent=1
""")
rows = dw_cur.fetchall()
print(f"Fetched {len(rows)} rows from DW")

# -------------------------
# Insert into FactProperty_DM (including startDay, endDay, isCurrent)
# -------------------------
insert_fact_sql = """
INSERT INTO FactProperty_DM (
    listing_key, name, property_type_id, location_id, date_id,
    price, area, price_per_m2, bedrooms, floors, street_width,
    posting_date, create_date, startDay, endDay, isCurrent
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    name=VALUES(name),
    property_type_id=VALUES(property_type_id),
    location_id=VALUES(location_id),
    date_id=VALUES(date_id),
    price=VALUES(price),
    area=VALUES(area),
    price_per_m2=VALUES(price_per_m2),
    bedrooms=VALUES(bedrooms),
    floors=VALUES(floors),
    street_width=VALUES(street_width),
    posting_date=VALUES(posting_date),
    create_date=VALUES(create_date),
    startDay=VALUES(startDay),
    endDay=VALUES(endDay),
    isCurrent=VALUES(isCurrent)
"""

count = 0
for r in rows:
    price = float(r['price']) if r['price'] is not None else None
    area = float(r['area']) if r['area'] is not None else None
    price_per_m2 = price / area if price and area and area > 0 else None

    property_type_id = get_or_create_property_type(r['property_type'])
    location_id = get_or_create_location(r['street'], r['ward'], r['district'], r['city'], r['old_address'])
    date_id = get_or_create_date(r['posting_date'])

    dm_cur.execute(insert_fact_sql, (
        r['listing_key'], r['name'], property_type_id, location_id, date_id,
        price, area, price_per_m2, r['bedrooms'], r['floors'], r['street_width'],
        r['posting_date'], r['create_date'], r['startDay'], r['endDay'], r['isCurrent']
    ))
    count += 1

dm_conn.commit()
print(f"✅ Loaded {count} rows into FactProperty_DM with startDay/endDay/isCurrent")

# -------------------------
# Close connections
# -------------------------
dw_cur.close()
dw_conn.close()
dm_cur.close()
dm_conn.close()
print("All done!")
