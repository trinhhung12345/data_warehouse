import redis
# Cấu hình IP Tailscale của bạn
r = redis.Redis(host='100.117.51.34', port=6379, password='hung12345', decode_responses=True)

# Xóa các key liên quan đến ETL
r.delete("stream:fact_trips_real")      # Xóa hàng đợi
r.delete("etl:state:last_trip_id")      # Reset bộ đếm Producer về 0
r.delete("dwh_group")                   # Xóa consumer group (nếu cần thiết)

print("✅ Đã Reset trạng thái ETL! Bạn có thể chạy lại Producer.")