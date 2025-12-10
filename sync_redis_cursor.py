import redis
from sqlalchemy import create_engine, text

# Cấu hình
REDIS_HOST = '100.117.51.34'
REDIS_PASS = 'hung12345'
DB_DWH_URL = f"postgresql://postgres:hung12345@{REDIS_HOST}:5432/Uber_data_warehouse"

# Kết nối
r = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASS, decode_responses=True)
engine_dwh = create_engine(DB_DWH_URL)

try:
    print("🔄 Đang lấy Max SourceTripID từ DWH...")
    with engine_dwh.connect() as conn:
        # Lấy ID gốc lớn nhất đã nạp thành công
        # Lưu ý: Cột này là SourceTripID (đã tạo ở bước audit trước đó)
        # Nếu chưa có cột SourceTripID, bạn có thể tạm dùng logic khác hoặc chấp nhận chạy lại từ 0
        res = conn.execute(text("SELECT MAX(SourceTripID) FROM FactTrip")).scalar()
    
    max_id = int(res) if res else 0
    print(f"✅ Tìm thấy Max ID trong kho là: {max_id}")
    
    # Set vào Redis
    r.set("etl:state:last_trip_id", max_id)
    print(f"✅ Đã cập nhật Redis Cursor: etl:state:last_trip_id = {max_id}")
    print("🚀 Bây giờ bạn có thể bật Producer để chạy tiếp phần còn lại!")

except Exception as e:
    print(f"❌ Lỗi: {e}")
    print("Gợi ý: Nếu bảng FactTrip chưa có cột SourceTripID, hãy chạy Producer từ đầu (chấp nhận chậm một chút).")