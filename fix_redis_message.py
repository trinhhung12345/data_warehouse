import redis
# Cấu hình IP của bạn
r = redis.Redis(host='100.117.51.34', port=6379, password='hung12345')

stream_key = "stream:fact_trips_real"

# Xóa toàn bộ tin nhắn đã cũ, chỉ giữ lại 0 tin
# Lệnh này sẽ làm Producer thấy Queue trống và chạy lại ngay
r.xtrim(stream_key, maxlen=0)

print("✅ Đã dọn sạch thùng rác Redis! Producer sẽ chạy lại ngay.")