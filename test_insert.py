import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import os
import time

# ==============================================================================
# 1. CẤU HÌNH KẾT NỐI
# ==============================================================================
DB_HOST = "100.117.51.34"

# Ops Database (PostgreSQL)
DB_OPS_URL = f"postgresql://postgres:hung12345@{DB_HOST}:5433/Uber_ops"
engine_ops = create_engine(DB_OPS_URL)

# CRM Database (MariaDB)
DB_CRM_URL = f"mysql+pymysql://root:hung12345@{DB_HOST}:3307/Uber_crm"
engine_crm = create_engine(DB_CRM_URL)

print(f"🔗 Đã kết nối Server: {DB_HOST}")

# ==============================================================================
# 2. ĐỊNH NGHĨA DANH SÁCH THÁNG CẦN CHẠY (TỪ 06/2024 ĐẾN 07/2025)
# ==============================================================================
months_to_process = []

# Năm 2024: Tháng 7 đến 12
for m in range(7, 13):
    months_to_process.append(f"2024_{m:02d}")

# Năm 2025: Tháng 1 đến 7
for m in range(1, 8):
    months_to_process.append(f"2025_{m:02d}")

print(f"📅 Danh sách tháng sẽ xử lý ({len(months_to_process)} tháng):")
print(months_to_process)

# ==============================================================================
# 3. SCHEMA & MAPPING (GIỮ NGUYÊN)
# ==============================================================================
DB_SCHEMAS = {
    "vehicles": ["vehicle_id", "license_plate", "make_model", "color", "capacity"],
    "drivers": ["driver_id", "legal_name", "license_number", "date_of_birth", "driver_status", "vehicle_id"],
    "trips": [
        "trip_id", "driver_id", "customer_id", "vendorid", "tpep_pickup_datetime", 
        "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid", 
        "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", 
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
        "improvement_surcharge", "total_amount", "congestion_surcharge", 
        "airport_fee", "cbd_congestion_fee"
    ],
    "customers": ["customer_id", "display_name", "phone_number", "email", "registration_date", "customer_segment"],
    "promotions": ["promotion_id", "promo_code", "description", "discount_value", "discount_type", "start_date", "end_date", "is_active"],
    "driver_performance": ["driver_id", "period_date", "total_trips", "total_revenue", "average_rating", "acceptance_rate", "cancellation_rate", "online_hours", "driver_tier"],
    "trip_feedback": ["feedback_id", "trip_id", "customer_id", "rating", "feedback_text", "feedback_date", "used_promotion_id"]
}

RENAME_MAPPING = {
    "VendorID": "vendorid", "RatecodeID": "ratecodeid",
    "PULocationID": "pulocationid", "DOLocationID": "dolocationid",
    "Airport_fee": "airport_fee"
}

# ==============================================================================
# 4. HÀM INSERT THÔNG MINH
# ==============================================================================
def insert_smart(file_path, table_name, engine, current_month_str, chunksize=10000):
    # Fallback tìm file
    if not os.path.exists(file_path):
        if "_" in file_path: alt_path = file_path.replace("_", "-")
        else: alt_path = file_path.replace("-", "_")
        
        if os.path.exists(alt_path):
            file_path = alt_path
        else:
            # File không tồn tại là bình thường với một số bảng (tùy dữ liệu), nên chỉ in warning nhẹ
            # print(f"   [Skipped] Không thấy file cho bảng {table_name}")
            return

    print(f"   📂 Đọc file: {os.path.basename(file_path)}")

    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()

        # Rename
        df.rename(columns=RENAME_MAPPING, inplace=True)
        
        # Trip ID Logic
        if table_name == "trips" and "trip_id" not in df.columns:
            try:
                # Lấy số từ chuỗi tháng (VD: 2024_06 -> 202406)
                prefix = int(current_month_str.replace("_", "").replace("-", ""))
            except:
                prefix = 999999
            start_id = prefix * 10_000_000
            df['trip_id'] = range(start_id, start_id + len(df))

        # Schema Filter
        if table_name in DB_SCHEMAS:
            valid_cols = DB_SCHEMAS[table_name]
            available_cols = [c for c in valid_cols if c in df.columns]
            df = df[available_cols]
        else:
            print(f"   ❌ Lỗi: Không tìm thấy schema {table_name}")
            return

        # Insert
        row_count = len(df)
        print(f"      -> Nạp {row_count:,} dòng...")
        
        start_time = time.time()
        df.to_sql(
            name=table_name, con=engine, if_exists='append',
            index=False, chunksize=chunksize, method='multi'
        )
        elapsed = time.time() - start_time
        print(f"      ✅ Xong trong {elapsed:.1f}s")

    except Exception as e:
        print(f"      ❌ FAILED {table_name}: {e}")

# ==============================================================================
# 5. VÒNG LẶP CHÍNH (MAIN LOOP)
# ==============================================================================
total_start = time.time()

for month in months_to_process:
    print("\n" + "="*60)
    print(f"🚀 ĐANG XỬ LÝ THÁNG: {month}")
    print("="*60)
    
    TARGET_MONTH_HYPHEN = month.replace("_", "-")
    
    # --- OPS DB ---
    # Master Data
    insert_smart(f"Dữ liệu/01_vehicles/vehicles_{month}.parquet", "vehicles", engine_ops, month)
    insert_smart(f"Dữ liệu/02_driver/drivers_{month}.parquet", "drivers", engine_ops, month)
    
    # Trips (Nặng nhất -> Chunk 5000 để an toàn RAM)
    # Thử tìm file tên gạch ngang trước (do colab hay tạo ra)
    trip_path = f"Dữ liệu/05_Trips/cleaned_yellow_tripdata_{TARGET_MONTH_HYPHEN}.parquet"
    if not os.path.exists(trip_path):
        trip_path = f"Dữ liệu/05_Trips/cleaned_yellow_tripdata_{month}.parquet"
    
    insert_smart(trip_path, "trips", engine_ops, month, chunksize=5000)

    # --- CRM DB ---
    insert_smart(f"Dữ liệu/03_customer/customers_{month}.parquet", "customers", engine_crm, month)
    insert_smart(f"Dữ liệu/04_promotions/promotions_{month}.parquet", "promotions", engine_crm, month)
    
    # Satellite Data
    insert_smart(f"Dữ liệu/06_Driver_performance/driver_performance_{TARGET_MONTH_HYPHEN}.parquet", "driver_performance", engine_crm, month)
    insert_smart(f"Dữ liệu/07_TRIP_FEEDBACK/trip_feedback_{TARGET_MONTH_HYPHEN}.parquet", "trip_feedback", engine_crm, month)

total_end = time.time()
print("\n" + "="*60)
print(f"🎉 HOÀN TẤT TOÀN BỘ! Tổng thời gian: {(total_end - total_start)/60:.1f} phút")
print("="*60)