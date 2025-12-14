import time
import pandas as pd
import random
from sqlalchemy import create_engine, text
from datetime import datetime

# Kết nối vào Source (Ops)
DB_OPS_URL = "postgresql://postgres:hung12345@100.102.253.78:5433/Uber_ops"
engine_ops = create_engine(DB_OPS_URL)

def generate_fake_trip():
    # Lấy Max Trip ID hiện tại để +1
    with engine_ops.connect() as conn:
        max_id = conn.execute(text("SELECT MAX(trip_id) FROM trips")).scalar()
    
    new_id = int(max_id) + 1

    random_day = random.randint(1, 31)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    pickup_time = datetime(2025, 7, random_day, random_hour, random_minute)
    dropoff_time = datetime(2025, 7, random_day, random_hour + random.randint(0, 1), (random_minute + random.randint(5, 30)) % 60)
    
    # Tạo data giả (Lấy Driver/Customer ID nhỏ để chắc chắn đã có trong Dim)
    trip = {
        'trip_id': new_id,
        'driver_id': random.randint(1, 100),
        'customer_id': random.randint(1, 1000),
        'vendorid': 1,
        'tpep_pickup_datetime': pickup_time,
        'tpep_dropoff_datetime': dropoff_time,
        'passenger_count': 1,
        'trip_distance': round(random.uniform(1.0, 10.0), 2),
        'ratecodeid': 1,
        'store_and_fwd_flag': 'N',
        'pulocationid': random.randint(1, 263),
        'dolocationid': random.randint(1, 263),
        'payment_type': 1,
        'fare_amount': round(random.uniform(10.0, 50.0), 2),
        'extra': 0, 'mta_tax': 0.5, 'tip_amount': 2.0, 'tolls_amount': 0,
        'improvement_surcharge': 0.3, 'total_amount': 0, 'congestion_surcharge': 2.5,
        'airport_fee': 0
    }
    trip['total_amount'] = trip['fare_amount'] + 5.3
    
    return trip

def run_demo():
    print("🎬 BẮT ĐẦU DEMO LIVE STREAMING...")
    print("Đang bơm dữ liệu vào Uber_ops...")
    
    try:
        while True:
            trip = generate_fake_trip()
            
            # Insert vào Ops
            df = pd.DataFrame([trip])
            df.to_sql('trips', engine_ops, if_exists='append', index=False)
            
            print(f"   ➕ Đã tạo chuyến đi mới: {trip['trip_id']} (Driver: {trip['driver_id']})")
            
            # Ngủ 2 giây để mọi người kịp nhìn thấy log bên Producer/Consumer nhảy
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Dừng Demo.")

if __name__ == "__main__":
    run_demo()