import time
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl_config import r_client, engine_dwh, engine_crm, STREAM_KEY, GROUP_NAME, CONSUMER_NAME

# Tạo Consumer Group
try:
    r_client.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
except:
    pass

# --- HÀM 1: BULK LOOKUP KEYS (DIMENSIONS) ---
def get_lookup_map(id_list, table_name, id_col, key_col):
    if not id_list: return {}
    valid_ids = [str(x) for x in id_list if x and str(x) != 'nan' and str(x) != '0' and str(x) != '-1']
    if not valid_ids: return {}

    ids_str = ",".join([f"'{x}'" for x in valid_ids])
    sql = f"SELECT {id_col}, {key_col} FROM {table_name} WHERE {id_col} IN ({ids_str})"
    
    if table_name.lower() != 'dimpromotion':
        sql += " AND IsCurrent = True"
    
    try:
        with engine_dwh.connect() as conn:
            lookup_df = pd.read_sql(sql, conn)
        return dict(zip(lookup_df[id_col].astype(str), lookup_df[key_col]))
    except Exception as e:
        print(f"⚠️ Lỗi lookup {table_name}: {e}")
        return {}

# --- HÀM 2: LẤY PROMO TỪ CRM ---
def get_promo_from_crm(real_trip_ids):
    if not real_trip_ids: return {}
    
    mapping_real_to_short = {}
    short_ids = []
    
    for rid in real_trip_ids:
        try:
            short_id = int(rid) % 10_000_000
            mapping_real_to_short[str(rid)] = str(short_id)
            short_ids.append(str(short_id))
        except:
            continue
            
    if not short_ids: return {}
    ids_str = ",".join(short_ids)
    
    sql = text(f"SELECT trip_id, used_promotion_id FROM trip_feedback WHERE trip_id IN ({ids_str}) AND used_promotion_id IS NOT NULL")
    
    try:
        with engine_crm.connect() as conn:
            df_feedback = pd.read_sql(sql, conn)
        
        if df_feedback.empty: return {}
        
        short_to_promo = dict(zip(df_feedback['trip_id'].astype(str), df_feedback['used_promotion_id'].astype(int).astype(str)))
        
        final_map = {}
        for rid, sid in mapping_real_to_short.items():
            if sid in short_to_promo:
                final_map[rid] = short_to_promo[sid]
        return final_map
    except Exception as e:
        print(f"⚠️ Lỗi lookup CRM Feedback: {e}")
        return {}

# --- HÀM 3: LẤY DRIVER PERFORMANCE TỪ CRM (MỚI THÊM) ---
def get_driver_performance(df_batch):
    """
    Lấy AverageRating và AcceptanceRate từ bảng driver_performance (CRM)
    Dựa vào driver_id và tháng của chuyến đi.
    """
    if df_batch.empty: return df_batch

    # 1. Tạo cột 'month_key' dạng 'YYYY-MM-01' để khớp với period_date trong DB
    # df_batch['tpep_pickup_datetime'] đã được convert sang datetime ở bước trước
    df_batch['temp_period'] = df_batch['tpep_pickup_datetime'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m-%d')
    
    # Lấy danh sách driver và các tháng cần query
    drivers = df_batch['driver_id'].unique().tolist()
    periods = df_batch['temp_period'].unique().tolist()
    
    if not drivers: return df_batch

    drivers_str = ",".join([str(x) for x in drivers if str(x) != 'nan'])
    periods_str = ",".join([f"'{x}'" for x in periods])
    
    # 2. Query CRM
    sql = text(f"""
        SELECT driver_id, period_date, average_rating, acceptance_rate
        FROM driver_performance
        WHERE driver_id IN ({drivers_str})
          AND period_date IN ({periods_str})
    """)
    
    try:
        with engine_crm.connect() as conn:
            df_perf = pd.read_sql(sql, conn)
        
        if df_perf.empty:
            df_batch['AverageRating'] = None
            df_batch['AcceptanceRate'] = None
            return df_batch

        # 3. Chuẩn hóa để Merge
        df_perf['period_date'] = pd.to_datetime(df_perf['period_date']).dt.strftime('%Y-%m-%d')
        df_perf['driver_id'] = df_perf['driver_id'].astype(str)
        
        # Tạo key để map: "driver_id|YYYY-MM-DD"
        df_perf['join_key'] = df_perf['driver_id'] + "|" + df_perf['period_date']
        df_batch['join_key'] = df_batch['driver_id'].astype(str) + "|" + df_batch['temp_period']
        
        # Map dữ liệu vào Dictionary
        rating_map = dict(zip(df_perf['join_key'], df_perf['average_rating']))
        acceptance_map = dict(zip(df_perf['join_key'], df_perf['acceptance_rate']))
        
        # 4. Map vào DataFrame chính
        df_batch['AverageRating'] = df_batch['join_key'].map(rating_map)
        df_batch['AcceptanceRate'] = df_batch['join_key'].map(acceptance_map)
        
        # Dọn dẹp cột tạm
        df_batch.drop(columns=['temp_period', 'join_key'], inplace=True, errors='ignore')
        
    except Exception as e:
        print(f"⚠️ Lỗi lookup Driver Performance: {e}")
        df_batch['AverageRating'] = None
        df_batch['AcceptanceRate'] = None
        
    return df_batch

# --- HÀM 4: XỬ LÝ BATCH ---
def process_batch_data(df):
    df.columns = [c.lower() for c in df.columns]
    
    # Convert Datetime & Numeric
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')
    
    numeric_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'trip_distance']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Tính toán
    df['DateKey'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d').astype(int)
    df['TripDuration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds().fillna(0).astype(int)

    # 1. LẤY DRIVER PERFORMANCE (Logic Mới)
    df = get_driver_performance(df)

    # 2. LOOKUP KEYS
    unique_drivers = df['driver_id'].astype(str).unique().tolist()
    unique_customers = df['customer_id'].astype(str).unique().tolist()
    unique_vehicles = df['vehicle_id'].astype(str).unique().tolist()
    unique_locs = set(df['pulocationid'].astype(str).unique().tolist() + df['dolocationid'].astype(str).unique().tolist())

    driver_map = get_lookup_map(unique_drivers, 'DimDriver', 'driverid', 'driverkey')
    customer_map = get_lookup_map(unique_customers, 'DimCustomer', 'customerid', 'customerkey')
    vehicle_map = get_lookup_map(unique_vehicles, 'DimVehicle', 'vehicleid', 'vehiclekey')
    location_map = get_lookup_map(list(unique_locs), 'DimLocation', 'locationid', 'locationkey')

    unique_trips = df['trip_id'].astype(str).unique().tolist()
    trip_to_promo_map = get_promo_from_crm(unique_trips)
    df['temp_promo_id'] = df['trip_id'].astype(str).map(trip_to_promo_map)
    unique_promo_ids = df['temp_promo_id'].dropna().unique().tolist()
    promo_key_map = get_lookup_map(unique_promo_ids, 'DimPromotion', 'promotionid', 'promotionkey')

    # Mapping
    df['DriverKey'] = df['driver_id'].astype(str).map(driver_map).fillna(-1).astype(int)
    df['CustomerKey'] = df['customer_id'].astype(str).map(customer_map).fillna(-1).astype(int)
    df['VehicleKey'] = df['vehicle_id'].astype(str).map(vehicle_map).fillna(-1).astype(int)
    df['PickupLocationKey'] = df['pulocationid'].astype(str).map(location_map).fillna(-1).astype(int)
    df['DropoffLocationKey'] = df['dolocationid'].astype(str).map(location_map).fillna(-1).astype(int)
    df['PromotionKey'] = df['temp_promo_id'].map(promo_key_map).fillna(-1).astype(int)

    # 3. PREPARE FINAL DF
    final_rename = {
        'trip_id':'sourcetripid',
        'DateKey': 'datekey',
        'PickupLocationKey': 'pickuplocationkey',
        'DropoffLocationKey': 'dropofflocationkey',
        'DriverKey': 'driverkey',
        'VehicleKey': 'vehiclekey',
        'CustomerKey': 'customerkey',
        'PromotionKey': 'promotionkey',
        'fare_amount': 'fareamount',
        'extra': 'extra',
        'mta_tax': 'mtatax',
        'tip_amount': 'tipamount',
        'tolls_amount': 'tollsamount',
        'improvement_surcharge': 'improvementsurcharge',
        'total_amount': 'totalamount',
        'congestion_surcharge': 'congestionsurcharge',
        'trip_distance': 'tripdistance',
        'TripDuration': 'tripduration',
        'AverageRating': 'averagerating',       # Cột mới
        'AcceptanceRate': 'acceptancerate'      # Cột mới
    }
    
    df = df.rename(columns=final_rename)
    target_cols = list(final_rename.values())
    # Chỉ lấy cột có trong DF
    available_cols = [c for c in target_cols if c in df.columns]
    
    return df[available_cols]

def consumer():
    print("📥 [CONSUMER] Bắt đầu lắng nghe Redis...")
    while True:
        try:
            entries = r_client.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=1000, block=2000)
            if not entries: continue

            stream, messages = entries[0]
            print(f"⚡ Xử lý {len(messages)} dòng...")

            rows = []
            msg_ids = []
            for msg_id, data in messages:
                rows.append(data)
                msg_ids.append(msg_id)
            
            df_batch = pd.DataFrame(rows)
            if df_batch.empty:
                r_client.xack(STREAM_KEY, GROUP_NAME, *msg_ids)
                continue

            df_fact = process_batch_data(df_batch)
            
            if not df_fact.empty:
                with engine_dwh.begin() as conn:
                    df_fact.to_sql('facttrip', conn, if_exists='append', index=False)
                print(f"✅ Đã nạp {len(df_fact)} dòng. (Rating: {df_fact['averagerating'].iloc[0]})")
            
            r_client.xack(STREAM_KEY, GROUP_NAME, *msg_ids)
            
        except Exception as e:
            print(f"❌ [CONSUMER] Lỗi: {e}")
            time.sleep(5)

if __name__ == "__main__":
    consumer()