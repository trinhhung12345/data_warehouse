# etl_producer.py
import time
import pandas as pd
import json
from sqlalchemy import text
from datetime import datetime
from etl_config import r_client, engine_ops, STREAM_KEY

LAST_ID_KEY = "etl:state:last_trip_id"
BATCH_SIZE = 1000  # Lấy 1000 dòng mỗi lần (An toàn cho RAM)

def serialize_date(o):
    if isinstance(o, (datetime, pd.Timestamp)):
        return o.strftime('%Y-%m-%d %H:%M:%S')

def producer():
    print("🚀 [PRODUCER] Bắt đầu chạy Extract...")
    
    # 1. Lấy vị trí cũ
    last_id = r_client.get(LAST_ID_KEY)
    last_id = int(last_id) if last_id else 0
    print(f"   -> Tiếp tục từ Trip ID: {last_id}")

    while True:
        # 2. Query JOIN để lấy đủ dữ liệu (Trips + Vehicle từ Drivers)
        # Lưu ý: Chúng ta lấy vehicle_id từ bảng drivers
        sql = text(f"""
            SELECT 
                t.trip_id, t.driver_id, t.customer_id, t.vendorid,
                t.tpep_pickup_datetime, t.tpep_dropoff_datetime,
                t.passenger_count, t.trip_distance, t.ratecodeid,
                t.pulocationid, t.dolocationid, t.payment_type,
                t.fare_amount, t.extra, t.mta_tax, t.tip_amount, 
                t.tolls_amount, t.improvement_surcharge, t.total_amount,
                t.congestion_surcharge,
                d.vehicle_id  -- Lấy thêm cột này
            FROM trips t
            LEFT JOIN drivers d ON t.driver_id = d.driver_id
            WHERE t.trip_id > :last_id
            ORDER BY t.trip_id ASC
            LIMIT :batch_size
        """)
        
        try:
            with engine_ops.connect() as conn:
                df = pd.read_sql(sql, conn, params={"last_id": last_id, "batch_size": BATCH_SIZE})

            if df.empty:
                print("💤 [PRODUCER] Hết dữ liệu mới. Đợi 5s...")
                time.sleep(5)
                continue

            # 3. Đẩy vào Redis Stream
            pipeline = r_client.pipeline()
            max_id_in_batch = last_id
            
            count = 0
            for _, row in df.iterrows():
                # Chuyển row thành dict
                data = row.to_dict()
                
                # Xử lý Datetime và None thành String (Redis yêu cầu)
                for k, v in data.items():
                    if isinstance(v, (datetime, pd.Timestamp)):
                        data[k] = str(v)
                    elif v is None:
                        data[k] = "" # Gửi chuỗi rỗng thay vì None
                    else:
                        data[k] = str(v) # Chuyển số thành string
                
                pipeline.xadd(STREAM_KEY, data)
                max_id_in_batch = row['trip_id']
                count += 1

            pipeline.execute()
            
            # 4. Lưu trạng thái
            r_client.set(LAST_ID_KEY, int(max_id_in_batch))
            last_id = max_id_in_batch
            print(f"📦 [PRODUCER] Đã đẩy {count} dòng. Last ID: {last_id}")
            
            # Chạy liên tục, không sleep nếu đang có nhiều dữ liệu
            
        except Exception as e:
            print(f"❌ [PRODUCER] Lỗi: {e}")
            time.sleep(5)

if __name__ == "__main__":
    producer()