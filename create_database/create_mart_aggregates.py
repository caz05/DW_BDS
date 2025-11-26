import mysql.connector
import json
from datetime import date

with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

dm_cfg = cfg["datamart"]

conn = mysql.connector.connect(**dm_cfg)
cur = conn.cursor()

print("Building aggregates...")

# Truncate target aggregates
cur.execute("TRUNCATE TABLE MartAgg_District")
cur.execute("TRUNCATE TABLE MartAgg_TypeMonth")

snapshot_date = date.today()

# -------------------------
# Aggregate by district
# -------------------------
cur.execute("""
INSERT INTO MartAgg_District (city, district, listing_count, avg_price, avg_area, avg_price_per_m2, snapshot_date)
SELECT l.city, l.district,
       COUNT(f.fact_id) AS listing_count,
       AVG(f.price) AS avg_price,
       AVG(f.area) AS avg_area,
       AVG(f.price_per_m2) AS avg_price_per_m2,
       %s
FROM FactProperty_DM f
JOIN DimLocation_DM l ON f.location_id = l.location_id
WHERE f.isCurrent = 1
GROUP BY l.city, l.district
""", (snapshot_date,))

# -------------------------
# Aggregate by property type and month
# -------------------------
cur.execute("""
INSERT INTO MartAgg_TypeMonth (property_type, year, month, listing_count, avg_price, avg_area, avg_price_per_m2, snapshot_date)
SELECT pt.type_name AS property_type, YEAR(f.posting_date) AS year, MONTH(f.posting_date) AS month,
       COUNT(f.fact_id) AS listing_count,
       AVG(f.price) AS avg_price,
       AVG(f.area) AS avg_area,
       AVG(f.price_per_m2) AS avg_price_per_m2,
       %s
FROM FactProperty_DM f
JOIN DimPropertyType_DM pt ON f.property_type_id = pt.property_type_id
WHERE f.isCurrent = 1 AND f.posting_date IS NOT NULL
GROUP BY pt.type_name, YEAR(f.posting_date), MONTH(f.posting_date)
""", (snapshot_date,))

conn.commit()
print("✅ Aggregates built.")

cur.close()
conn.close()
