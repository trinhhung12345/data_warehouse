import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from etl_config import engine_ops, engine_crm, engine_dwh

# ==============================================================================
# HÀM HỖ TRỢ: SCD TYPE 2 (PHIÊN BẢN ĐÃ FIX LỖI MERGE COLUMN)
# ==============================================================================
def sync_scd2_table(source_df, dim_table, business_key, compare_cols):
    if source_df.empty: return

    # 1. Đọc dữ liệu hiện tại trong DWH
    cols_to_select = [business_key] + compare_cols
    cols_str = ", ".join(cols_to_select)
    
    sql = f"SELECT {cols_str} FROM {dim_table} WHERE IsCurrent = True"
    with engine_dwh.connect() as conn:
        dwh_df = pd.read_sql(sql, conn)
    
    # Chuẩn hóa tên cột
    source_df.columns = [c.lower() for c in source_df.columns]
    dwh_df.columns = [c.lower() for c in dwh_df.columns]
    
    business_key = business_key.lower()
    compare_cols = [c.lower() for c in compare_cols]

    # 2. PHÂN LOẠI DỮ LIỆU
    merged = pd.merge(source_df, dwh_df, on=business_key, how='left', suffixes=('_src', '_dwh'), indicator=True)
    
    # A. NEW RECORDS (Có trong Source nhưng chưa có trong DWH)
    new_ids = merged[merged['_merge'] == 'left_only'][business_key].tolist()
    
    # B. CHANGED RECORDS (Có cả 2 nhưng dữ liệu khác nhau)
    mask_changed = False
    for col in compare_cols:
        mask_changed |= (merged[f'{col}_src'].fillna('') != merged[f'{col}_dwh'].fillna(''))
    
    changed_ids = merged[(merged['_merge'] == 'both') & mask_changed][business_key].tolist()

    # 3. THỰC HIỆN UPDATE DWH
    
    # --- XỬ LÝ DÒNG MỚI (INSERT) ---
    if new_ids:
        print(f"   + Phát hiện {len(new_ids)} dòng mới cho {dim_table}. Đang Insert...")
        
        # FIX LỖI: Lấy dữ liệu từ source_df gốc dựa trên ID (tránh lỗi tên cột _src)
        insert_df = source_df[source_df[business_key].isin(new_ids)].copy()
        
        insert_df['startdate'] = datetime.now()
        insert_df['enddate'] = None
        insert_df['iscurrent'] = True
        
        insert_df.to_sql(dim_table, engine_dwh, if_exists='append', index=False)

    # --- XỬ LÝ DÒNG THAY ĐỔI (UPDATE & INSERT) ---
    if changed_ids:
        print(f"   + Phát hiện {len(changed_ids)} dòng thay đổi trong {dim_table}. Cập nhật lịch sử...")
        
        # B1: Đóng dòng cũ
        ids_str = ",".join([str(x) for x in changed_ids])
        expire_sql = text(f"""
            UPDATE {dim_table} 
            SET EndDate = NOW(), IsCurrent = False 
            WHERE {business_key} IN ({ids_str}) AND IsCurrent = True
        """)
        
        with engine_dwh.begin() as conn:
            conn.execute(expire_sql)
            
        # B2: Thêm dòng mới (Lấy từ source_df gốc)
        insert_df = source_df[source_df[business_key].isin(changed_ids)].copy()
        
        insert_df['startdate'] = datetime.now()
        insert_df['enddate'] = None
        insert_df['iscurrent'] = True
        
        insert_df.to_sql(dim_table, engine_dwh, if_exists='append', index=False)

# ==============================================================================
# HÀM ĐỒNG BỘ CỤ THỂ TỪNG BẢNG
# ==============================================================================

def sync_drivers():
    # Lấy dữ liệu nguồn Ops
    # Map tên cột ngay tại nguồn cho giống DWH
    df = pd.read_sql("""
        SELECT driver_id as driverid, legal_name as drivername, 
               license_number as licensenumber, driver_status as driverstatus 
        FROM drivers
    """, engine_ops)
    
    # So sánh và đồng bộ (Theo dõi thay đổi ở cột driverstatus và drivername)
    sync_scd2_table(df, 'dimdriver', 'driverid', ['driverstatus', 'drivername'])

def sync_customers():
    # Lấy dữ liệu nguồn CRM
    # Lưu ý: Bảng Customer thường rất lớn, thực tế nên filter theo updated_at > last_run
    # Nhưng với demo này ta load full (600k dòng vẫn nhanh)
    df = pd.read_sql("""
        SELECT customer_id as customerid, display_name as customername, 
               phone_number as phonenumber, email, customer_segment as customersegment,
               registration_date as registrationdate
        FROM customers
    """, engine_crm)
    
    # Tính toán thêm cột dẫn xuất nếu cần (như AgeOnPlatform), ở đây làm đơn giản
    # Customer thường ít thay đổi thông tin cá nhân, chủ yếu là thêm mới
    sync_scd2_table(df, 'dimcustomer', 'customerid', ['customersegment'])

def sync_vehicles():
    df = pd.read_sql("""
        SELECT vehicle_id as vehicleid, make_model as vehiclemakemodel, 
               color as vehiclecolor, capacity as vehiclecapacity 
        FROM vehicles
    """, engine_ops)
    
    sync_scd2_table(df, 'dimvehicle', 'vehicleid', ['vehiclecolor'])

def sync_promotions():
    # Bảng này của bạn schema khác (không có IsCurrent), nên ta chỉ check INSERT MỚI
    # (SCD Type 1 - Insert Only)
    df_src = pd.read_sql("""
        SELECT promotion_id as promotionid, promo_code as promotioncode, 
               description, discount_value as discountvalue, discount_type as discounttype,
               start_date as startdate, end_date as enddate
        FROM promotions
    """, engine_crm)
    
    # Logic riêng cho bảng không có IsCurrent
    with engine_dwh.connect() as conn:
        existing_ids = pd.read_sql("SELECT promotionid FROM dimpromotion", conn)['promotionid'].tolist()
    
    # Lọc những ID chưa có
    new_promos = df_src[~df_src['promotionid'].isin(existing_ids)].copy()
    
    if not new_promos.empty:
        print(f"   + Phát hiện {len(new_promos)} Promotion mới.")
        # Tính toán cột thiếu
        new_promos['campaign'] = 'General'
        new_promos['promotionname'] = new_promos['promotioncode']
        new_promos['startdate'] = pd.to_datetime(new_promos['startdate'])
        new_promos['enddate'] = pd.to_datetime(new_promos['enddate'])
        new_promos['durationindays'] = (new_promos['enddate'] - new_promos['startdate']).dt.days
        new_promos['promotionstatus'] = 'Active'

        new_promos.to_sql('dimpromotion', engine_dwh, if_exists='append', index=False)

# ==============================================================================
# MAIN LOOP
# ==============================================================================
def dim_sync_worker():
    print("🔄 [DIM SYNC] Bắt đầu đồng bộ Dimensions (Chu kỳ 60s)...")
    
    while True:
        try:
            print(f"\n⏰ {datetime.now()} - Đang kiểm tra thay đổi Master Data...")
            
            # 1. Sync Drivers
            sync_drivers()
            
            # 2. Sync Vehicles
            sync_vehicles()
            
            # 3. Sync Promotions
            sync_promotions()
            
            # 4. Sync Customers (Nặng nhất để cuối)
            sync_customers()
            
            print("✅ Đã đồng bộ xong. Ngủ 60 giây.")
            time.sleep(60)
            
        except Exception as e:
            print(f"❌ Lỗi Dim Sync: {e}")
            time.sleep(10)

if __name__ == "__main__":
    dim_sync_worker()