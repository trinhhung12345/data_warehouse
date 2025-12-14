import time
import pandas as pd
import random
from sqlalchemy import create_engine, text
from datetime import datetime

# ==============================================================================
# 1. CẤU HÌNH KẾT NỐI
# ==============================================================================
SERVER_IP = "100.102.253.78"  # Cập nhật IP của bạn nếu cần (hoặc 127.0.0.1 nếu chạy trên máy chứa DB)

# Ops (PostgreSQL)
DB_OPS_URL = f"postgresql://postgres:hung12345@{SERVER_IP}:5433/Uber_ops"
engine_ops = create_engine(DB_OPS_URL)

# CRM (MariaDB)
DB_CRM_URL = f"mysql+pymysql://root:hung12345@{SERVER_IP}:3307/Uber_crm"
engine_crm = create_engine(DB_CRM_URL)

print(f"🔗 Đã kết nối Ops & CRM tại {SERVER_IP}...")

# ==============================================================================
# 2. KHỞI TẠO DANH SÁCH ID HỢP LỆ (CACHE ĐỂ CHẠY NHANH)
# ==============================================================================
# Lấy danh sách Zone ID thực tế từ bảng taxi_zones để tránh lỗi Foreign Key
try:
    with engine_ops.connect() as conn:
        valid_zones = [r[0] for r in conn.execute(text("SELECT zone_id FROM taxi_zones")).fetchall()]
        print(f"✅ Đã tải {len(valid_zones)} Zone ID hợp lệ.")
except:
    # Fallback nếu không kết nối được hoặc lỗi
    print("⚠️ Không lấy được danh sách Zone, dùng danh sách mặc định an toàn (1-100).")
    valid_zones = list(range(1, 101))

# ==============================================================================
# 3. CÁC HÀM SINH DỮ LIỆU
# ==============================================================================

def generate_fake_trip():
    # Lấy Max Trip ID hiện tại để +1
    try:
        with engine_ops.connect() as conn:
            max_id = conn.execute(text("SELECT MAX(trip_id) FROM trips")).scalar()
        new_id = int(max_id) + 1 if max_id else 2025070000001
    except:
        new_id = int(time.time() * 1000) # Fallback nếu lỗi

    # Random ngày giờ (Tháng 7/2025)
    random_day = random.randint(1, 30)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    pickup_time = datetime(2025, 7, random_day, random_hour, random_minute)
    dropoff_time = pickup_time + pd.Timedelta(minutes=random.randint(10, 40))
    
    # Chọn ngẫu nhiên Zone từ danh sách hợp lệ
    pu_zone = random.choice(valid_zones)
    do_zone = random.choice(valid_zones)
    
    trip = {
        'trip_id': new_id,
        'driver_id': random.randint(1, 100),
        'customer_id': random.randint(1, 1000),
        'vendorid': 1,
        'tpep_pickup_datetime': pickup_time,
        'tpep_dropoff_datetime': dropoff_time,
        'passenger_count': random.randint(1, 4),
        'trip_distance': round(random.uniform(1.0, 15.0), 2),
        'ratecodeid': 1,
        'store_and_fwd_flag': 'N',
        'pulocationid': pu_zone,  # Dùng ID hợp lệ
        'dolocationid': do_zone,  # Dùng ID hợp lệ
        'payment_type': 1,
        'fare_amount': round(random.uniform(10.0, 80.0), 2),
        'extra': 0, 'mta_tax': 0.5, 'tip_amount': round(random.uniform(0, 10.0), 2), 
        'tolls_amount': 0, 'improvement_surcharge': 0.3, 'total_amount': 0, 
        'congestion_surcharge': 2.5, 'airport_fee': 0
    }
    trip['total_amount'] = trip['fare_amount'] + trip['tip_amount'] + 3.3
    
    return trip

def insert_crm_data(trip_data):
    # (Giữ nguyên logic cũ)
    has_feedback = random.choice([True, False])
    if has_feedback:
        promo_id = random.randint(1, 20) if random.random() < 0.3 else None
        rating = random.randint(3, 5)
        feedback = {
            'trip_id': trip_data['trip_id'],
            'customer_id': trip_data['customer_id'],
            'rating': rating,
            'feedback_text': 'Demo Live Insert',
            'feedback_date': datetime.now(),
            'used_promotion_id': promo_id
        }
        try:
            df_feed = pd.DataFrame([feedback])
            df_feed.to_sql('trip_feedback', engine_crm, if_exists='append', index=False)
            print(f"      ↳ CRM Feedback: Rating {rating}⭐ | PromoID: {promo_id}")
        except Exception as e:
            print(f"      ⚠️ Lỗi insert CRM: {e}")

# ==============================================================================
# 4. CHẠY DEMO
# ==============================================================================
def run_demo():
    print("\n🎬 BẮT ĐẦU DEMO LIVE STREAMING (Fixed Zone ID)...")
    print("------------------------------------------------")
    
    try:
        while True:
            try:
                trip = generate_fake_trip()
                
                df_trip = pd.DataFrame([trip])
                df_trip.to_sql('trips', engine_ops, if_exists='append', index=False)
                
                print(f"➕ [OPS] New Trip: {trip['trip_id']} (Zone: {trip['pulocationid']} -> {trip['dolocationid']})")
                
                insert_crm_data(trip)
                time.sleep(3) # Tốc độ vừa phải
            except Exception as e:
                print(f"❌ Lỗi vòng lặp: {e}")
                time.sleep(1) # Nghỉ chút rồi thử lại
            
    except KeyboardInterrupt:
        print("\n🛑 Dừng Demo.")

if __name__ == "__main__":
    run_demo()