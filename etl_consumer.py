import time
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl_config import r_client, engine_dwh, engine_crm, STREAM_KEY, GROUP_NAME, CONSUMER_NAME

# --- THƯ VIỆN GIAO DIỆN RICH ---
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich import box
from datetime import datetime

# Tạo Consumer Group
try:
    r_client.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
except:
    pass

# ==============================================================================
# 1. CÁC HÀM XỬ LÝ LOGIC (KHÔNG ĐỔI)
# ==============================================================================

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
        return {}

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
        return {}

def get_driver_performance(df_batch):
    if df_batch.empty: return df_batch
    df_batch['temp_period'] = df_batch['tpep_pickup_datetime'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m-%d')
    drivers = df_batch['driver_id'].unique().tolist()
    periods = df_batch['temp_period'].unique().tolist()
    
    if not drivers: return df_batch

    drivers_str = ",".join([str(x) for x in drivers if str(x) != 'nan'])
    periods_str = ",".join([f"'{x}'" for x in periods])
    
    sql = text(f"""
        SELECT driver_id, period_date, average_rating, acceptance_rate
        FROM driver_performance
        WHERE driver_id IN ({drivers_str}) AND period_date IN ({periods_str})
    """)
    
    try:
        with engine_crm.connect() as conn:
            df_perf = pd.read_sql(sql, conn)
        
        if df_perf.empty:
            df_batch['AverageRating'] = None
            df_batch['AcceptanceRate'] = None
            return df_batch

        df_perf['period_date'] = pd.to_datetime(df_perf['period_date']).dt.strftime('%Y-%m-%d')
        df_perf['driver_id'] = df_perf['driver_id'].astype(str)
        df_perf['join_key'] = df_perf['driver_id'] + "|" + df_perf['period_date']
        df_batch['join_key'] = df_batch['driver_id'].astype(str) + "|" + df_batch['temp_period']
        
        rating_map = dict(zip(df_perf['join_key'], df_perf['average_rating']))
        acceptance_map = dict(zip(df_perf['join_key'], df_perf['acceptance_rate']))
        
        df_batch['AverageRating'] = df_batch['join_key'].map(rating_map)
        df_batch['AcceptanceRate'] = df_batch['join_key'].map(acceptance_map)
        df_batch.drop(columns=['temp_period', 'join_key'], inplace=True, errors='ignore')
        
    except Exception as e:
        df_batch['AverageRating'] = None
        df_batch['AcceptanceRate'] = None
        
    return df_batch

def process_batch_data(df):
    sample_debug = {}
    df.columns = [c.lower() for c in df.columns]
    
    if not df.empty:
        try:
            ops_id = str(df.iloc[0]['trip_id'])
            crm_id = str(int(ops_id) % 10_000_000)
            sample_debug = {"ops": ops_id, "crm": crm_id}
        except:
            sample_debug = {"ops": "N/A", "crm": "N/A"}

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')
    
    numeric_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'trip_distance']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df['DateKey'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d').astype(int)
    df['TripDuration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds().fillna(0).astype(int)

    df = get_driver_performance(df)

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

    df['DriverKey'] = df['driver_id'].astype(str).map(driver_map).fillna(-1).astype(int)
    df['CustomerKey'] = df['customer_id'].astype(str).map(customer_map).fillna(-1).astype(int)
    df['VehicleKey'] = df['vehicle_id'].astype(str).map(vehicle_map).fillna(-1).astype(int)
    df['PickupLocationKey'] = df['pulocationid'].astype(str).map(location_map).fillna(-1).astype(int)
    df['DropoffLocationKey'] = df['dolocationid'].astype(str).map(location_map).fillna(-1).astype(int)
    df['PromotionKey'] = df['temp_promo_id'].map(promo_key_map).fillna(-1).astype(int)

    final_rename = {
        'trip_id': 'sourcetripid',
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
        'AverageRating': 'averagerating',
        'AcceptanceRate': 'acceptancerate'
    }
    
    df = df.rename(columns=final_rename)
    target_cols = list(final_rename.values())
    available_cols = [c for c in target_cols if c in df.columns]
    
    return df[available_cols], sample_debug

# ==============================================================================
# 2. GIAO DIỆN DASHBOARD (RICH) - ĐÃ SỬA LỖI LAYOUT
# ==============================================================================
def generate_dashboard(total, last_rating, last_fare, status, last_error, debug_info):
    # 1. Bảng Main Stats
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    
    grid.add_row(
        Panel(f"[bold green]{total:,}[/bold green]", title="🚀 Total Processed", border_style="green"),
        Panel(f"[bold yellow]{last_rating}[/bold yellow]", title="⭐ Avg Rating (Batch)", border_style="yellow"),
        Panel(f"[bold cyan]${last_fare}[/bold cyan]", title="💰 Avg Fare (Batch)", border_style="cyan"),
    )

    # 2. Bảng Debug ID
    debug_table = Table(show_header=True, expand=True, box=box.SIMPLE)
    debug_table.add_column("Level", style="dim")
    debug_table.add_column("ID / Key", style="bold white")
    debug_table.add_column("System", style="italic cyan")

    ops_id = debug_info.get('ops', 'N/A')
    crm_id = debug_info.get('crm', 'N/A')
    dwh_key = debug_info.get('dwh', 'Waiting...')

    debug_table.add_row("1. Source (Ops)", ops_id, "PostgreSQL (Uber_ops)")
    debug_table.add_row("2. Map Logic", f"{crm_id} (Modulo)", "Python Logic / Uber_CRM")
    debug_table.add_row("3. Warehouse", dwh_key, "FactTrip (Auto Increment)")

    # 3. Status Panel
    status_panel = Panel(
        status, 
        title="[bold]🔄 Current Status[/bold]", 
        border_style="cyan" if last_error == "None" else "red"
    )
    
    # 4. TẠO LAYOUT CHÍNH (SỬA LỖI TYPEERROR)
    main_layout = Layout()
    
    # Chia layout theo cột dọc (split_column)
    # Danh sách các phần tử cần hiển thị
    layout_elements = [
        Layout(grid, size=5),          # Phần thống kê
        Layout(status_panel, size=3),  # Phần trạng thái
        Layout(Panel(debug_table, title="🔍 [bold magenta]ID TRACING (Live Sample)[/bold magenta]", border_style="magenta"), size=9) # Phần debug
    ]
    
    # Nếu có lỗi, thêm Panel lỗi vào cuối
    if last_error != "None":
        error_panel = Panel(f"[red]{last_error}[/red]", title="❌ Last Error", border_style="red")
        layout_elements.append(Layout(error_panel, size=5))

    # Thực hiện chia
    main_layout.split_column(*layout_elements)

    # Bọc tất cả trong Panel khung
    return Panel(
        main_layout, 
        title="ETL CONSUMER MONITOR", 
        border_style="yellow"
    )

def consumer():
    total_processed = 0
    last_rating = "0.0"
    last_fare = "0.0"
    status_msg = "[grey]Waiting for stream...[/grey]"
    last_error = "None"
    debug_info = {"ops": "...", "crm": "...", "dwh": "..."}

    with Live(generate_dashboard(total_processed, last_rating, last_fare, status_msg, last_error, debug_info), refresh_per_second=4) as live:
        while True:
            try:
                entries = r_client.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=1000, block=2000)
                
                if not entries:
                    status_msg = "[grey]💤 Idle...[/grey]"
                    live.update(generate_dashboard(total_processed, last_rating, last_fare, status_msg, last_error, debug_info))
                    continue

                stream, messages = entries[0]
                status_msg = f"[bold blue]⚡ Processing {len(messages)} rows...[/bold blue]"
                live.update(generate_dashboard(total_processed, last_rating, last_fare, status_msg, last_error, debug_info))

                rows = []
                msg_ids = []
                for msg_id, data in messages:
                    rows.append(data)
                    msg_ids.append(msg_id)
                
                df_batch = pd.DataFrame(rows)
                
                if df_batch.empty:
                    r_client.xack(STREAM_KEY, GROUP_NAME, *msg_ids)
                    continue

                # Xử lý và nhận lại debug info
                df_fact, sample_debug = process_batch_data(df_batch)
                
                # Cập nhật Debug Info từ Python
                debug_info.update(sample_debug)

                if not df_fact.empty:
                    with engine_dwh.begin() as conn:
                        df_fact.to_sql('facttrip', conn, if_exists='append', index=False)
                        
                        # Query lấy Max Key thực tế trong DB để hiển thị
                        try:
                            max_key = conn.execute(text("SELECT MAX(TripKey) FROM FactTrip")).scalar()
                            debug_info["dwh"] = f"{max_key:,}"
                        except:
                            debug_info["dwh"] = "N/A"

                    # Update Stats
                    total_processed += len(df_fact)
                    if 'averagerating' in df_fact.columns:
                        last_rating = f"{df_fact['averagerating'].mean():.2f}"
                    if 'fareamount' in df_fact.columns:
                        last_fare = f"{df_fact['fareamount'].mean():.2f}"
                    
                    status_msg = f"[bold green]✅ Success (+{len(df_fact)})[/bold green]"
                
                r_client.xack(STREAM_KEY, GROUP_NAME, *msg_ids)
                last_error = "None"
                live.update(generate_dashboard(total_processed, last_rating, last_fare, status_msg, last_error, debug_info))
                
            except Exception as e:
                last_error = str(e)[0:200]
                status_msg = "[bold red]❌ Error[/bold red]"
                live.update(generate_dashboard(total_processed, last_rating, last_fare, status_msg, last_error, debug_info))
                time.sleep(5)

if __name__ == "__main__":
    consumer()