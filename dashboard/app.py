from taipy.gui import Gui, notify
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime

# ==============================================================================
# 1. CẤU HÌNH DATABASE
# ==============================================================================
DB_HOST = "100.117.51.34"
URL_DWH = f"postgresql://postgres:hung12345@{DB_HOST}:5432/Uber_data_warehouse"
engine_dwh = create_engine(URL_DWH)

# ==============================================================================
# 2. KHỞI TẠO BIẾN (GLOBAL VARIABLES) - QUAN TRỌNG
# ==============================================================================
# Phải khai báo trước khi dùng trong Markdown
logo_path = "assets/uber_radar.gif" 

# Ngày mặc định
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)

# Các biến hiển thị trên UI (Khởi tạo giá trị rỗng trước)
total_revenue = "$0"
total_trips = "0"
avg_rating = "0.0 ⭐"

# DataFrame rỗng ban đầu
df_revenue_trend = pd.DataFrame({"date": [], "revenue": []})
df_top_zones = pd.DataFrame({"trips": [], "zone": []})

# ==============================================================================
# 3. HÀM DATA
# ==============================================================================
def load_data(start, end):
    sk = int(start.strftime('%Y%m%d'))
    ek = int(end.strftime('%Y%m%d'))
    print(f"🔄 Loading data {sk}-{ek}...")
    
    with engine_dwh.connect() as conn:
        # KPI
        sql_kpi = text(f"""
            SELECT SUM(TotalAmount), COUNT(*), AVG(NULLIF(AverageRating, 0)) 
            FROM FactTrip WHERE DateKey BETWEEN {sk} AND {ek}
        """)
        res = conn.execute(sql_kpi).fetchone()
        
        # Trend
        sql_trend = text(f"""
            SELECT dd.FullDate as date, SUM(ft.TotalAmount) as revenue
            FROM FactTrip ft JOIN DimDate dd ON ft.DateKey = dd.DateKey
            WHERE ft.DateKey BETWEEN {sk} AND {ek}
            GROUP BY dd.FullDate ORDER BY dd.FullDate
        """)
        df_trend = pd.read_sql(sql_trend, conn)
        
        # Zones
        sql_zones = text(f"""
            SELECT dl.ZoneName as zone, COUNT(*) as trips
            FROM FactTrip ft JOIN DimLocation dl ON ft.PickupLocationKey = dl.LocationKey
            WHERE ft.DateKey BETWEEN {sk} AND {ek}
            GROUP BY dl.ZoneName ORDER BY trips DESC LIMIT 10
        """)
        df_zones = pd.read_sql(sql_zones, conn)
        
    return res, df_trend, df_zones

# Hàm Callback khi bấm nút
def on_filter(state):
    res, trend, zones = load_data(state.start_date, state.end_date)
    
    # Cập nhật State
    state.total_revenue = f"${res[0]:,.2f}" if res[0] else "$0"
    state.total_trips = f"{res[1]:,}" if res[1] else "0"
    state.avg_rating = f"{res[2]:.2f} ⭐" if res[2] else "N/A"
    state.df_revenue_trend = trend
    state.df_top_zones = zones
    notify(state, "success", "Updated!")

# ==============================================================================
# 4. CHẠY LẦN ĐẦU ĐỂ LẤY DỮ LIỆU THẬT
# ==============================================================================
# Gọi hàm load data ngay khi khởi động để cập nhật vào biến Global
_init_res, _init_trend, _init_zones = load_data(start_date, end_date)

if _init_res[0]:
    total_revenue = f"${_init_res[0]:,.2f}"
    total_trips = f"{_init_res[1]:,}"
    avg_rating = f"{_init_res[2]:.2f} ⭐"
df_revenue_trend = _init_trend
df_top_zones = _init_zones

# ==============================================================================
# 5. GIAO DIỆN (MARKDOWN)
# ==============================================================================
# Lưu ý: Taipy rất nhạy cảm với khoảng trắng trong Markdown, hãy giữ indent thẳng hàng
page = """
<|layout|columns=1 4|
<|text-center|
<|{logo_path}|image|width=100px|>
|>

<|
# UBER DASHBOARD
### Real-time Analytics
|>
|>

<|layout|columns=1 1 1|
<|
**Từ ngày:**
<|{start_date}|date|>
|>

<|
**Đến ngày:**
<|{end_date}|date|>
|>

<|
<br/>
<|Lọc Dữ Liệu|button|on_action=on_filter|>
|>
|>

<|layout|columns=1 1 1|gap=20px|
<|card|
## Doanh Thu
### <|{total_revenue}|text|>
|>

<|card|
## Số Chuyến
### <|{total_trips}|text|>
|>

<|card|
## Rating
### <|{avg_rating}|text|>
|>
|>

<|layout|columns=1 1|
<|
### Xu hướng Doanh Thu
<|{df_revenue_trend}|chart|type=line|x=date|y=revenue|color=#00D084|>
|>

<|
### Top Khu Vực
<|{df_top_zones}|chart|type=bar|x=trips|y=zone|orientation=h|color=#FF6B6B|>
|>
|>
"""

if __name__ == "__main__":
    # debug=True giúp hiện chi tiết lỗi nếu có
    Gui(page).run(host="0.0.0.0", port=8050, title="Uber Dashboard", dark_mode=True, debug=True)