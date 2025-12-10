import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from etl_config import engine_ops, engine_crm, engine_dwh

# --- THƯ VIỆN GIAO DIỆN RICH ---
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich import box

# ==============================================================================
# 1. HÀM HỖ TRỢ: SCD TYPE 2 (TRẢ VỀ SỐ LƯỢNG THAY VÌ IN RA)
# ==============================================================================
def sync_scd2_table(source_df, dim_table, business_key, compare_cols):
    """
    Trả về tuple: (số_dòng_thêm_mới, số_dòng_cập_nhật)
    """
    added_count = 0
    updated_count = 0

    if source_df.empty: return 0, 0

    # 1. Đọc dữ liệu hiện tại trong DWH
    cols_to_select = [business_key] + compare_cols
    cols_str = ", ".join(cols_to_select)
    
    try:
        sql = f"SELECT {cols_str} FROM {dim_table} WHERE IsCurrent = True"
        with engine_dwh.connect() as conn:
            dwh_df = pd.read_sql(sql, conn)
    except:
        # Trường hợp bảng chưa có hoặc lỗi
        dwh_df = pd.DataFrame(columns=cols_to_select)
    
    # Chuẩn hóa tên cột
    source_df.columns = [c.lower() for c in source_df.columns]
    dwh_df.columns = [c.lower() for c in dwh_df.columns]
    
    business_key = business_key.lower()
    compare_cols = [c.lower() for c in compare_cols]

    # 2. PHÂN LOẠI DỮ LIỆU
    merged = pd.merge(source_df, dwh_df, on=business_key, how='left', suffixes=('_src', '_dwh'), indicator=True)
    
    # A. NEW RECORDS
    new_ids = merged[merged['_merge'] == 'left_only'][business_key].tolist()
    
    # B. CHANGED RECORDS
    mask_changed = False
    for col in compare_cols:
        mask_changed |= (merged[f'{col}_src'].fillna('') != merged[f'{col}_dwh'].fillna(''))
    
    changed_ids = merged[(merged['_merge'] == 'both') & mask_changed][business_key].tolist()

    # 3. THỰC HIỆN UPDATE DWH
    
    # --- INSERT NEW ---
    if new_ids:
        insert_df = source_df[source_df[business_key].isin(new_ids)].copy()
        insert_df['startdate'] = datetime.now()
        insert_df['enddate'] = None
        insert_df['iscurrent'] = True
        
        insert_df.to_sql(dim_table, engine_dwh, if_exists='append', index=False)
        added_count = len(new_ids)

    # --- UPDATE CHANGED ---
    if changed_ids:
        # B1: Expire old
        ids_str = ",".join([str(x) for x in changed_ids])
        expire_sql = text(f"UPDATE {dim_table} SET EndDate = NOW(), IsCurrent = False WHERE {business_key} IN ({ids_str}) AND IsCurrent = True")
        with engine_dwh.begin() as conn:
            conn.execute(expire_sql)
            
        # B2: Insert new
        insert_df = source_df[source_df[business_key].isin(changed_ids)].copy()
        insert_df['startdate'] = datetime.now()
        insert_df['enddate'] = None
        insert_df['iscurrent'] = True
        
        insert_df.to_sql(dim_table, engine_dwh, if_exists='append', index=False)
        updated_count = len(changed_ids)
        
    return added_count, updated_count

# ==============================================================================
# 2. HÀM ĐỒNG BỘ CỤ THỂ (WRAPPER)
# ==============================================================================

def sync_drivers():
    df = pd.read_sql("SELECT driver_id as driverid, legal_name as drivername, license_number as licensenumber, driver_status as driverstatus FROM drivers", engine_ops)
    return sync_scd2_table(df, 'dimdriver', 'driverid', ['driverstatus', 'drivername'])

def sync_customers():
    df = pd.read_sql("SELECT customer_id as customerid, display_name as customername, phone_number as phonenumber, email, customer_segment as customersegment, registration_date as registrationdate FROM customers", engine_crm)
    return sync_scd2_table(df, 'dimcustomer', 'customerid', ['customersegment'])

def sync_vehicles():
    df = pd.read_sql("SELECT vehicle_id as vehicleid, make_model as vehiclemakemodel, color as vehiclecolor, capacity as vehiclecapacity FROM vehicles", engine_ops)
    return sync_scd2_table(df, 'dimvehicle', 'vehicleid', ['vehiclecolor'])

def sync_promotions():
    df_src = pd.read_sql("SELECT promotion_id as promotionid, promo_code as promotioncode, description, discount_value as discountvalue, discount_type as discounttype, start_date as startdate, end_date as enddate FROM promotions", engine_crm)
    
    with engine_dwh.connect() as conn:
        existing_ids = pd.read_sql("SELECT promotionid FROM dimpromotion", conn)['promotionid'].tolist()
    
    new_promos = df_src[~df_src['promotionid'].isin(existing_ids)].copy()
    added_count = 0
    
    if not new_promos.empty:
        new_promos['campaign'] = 'General'
        new_promos['promotionname'] = new_promos['promotioncode']
        new_promos['startdate'] = pd.to_datetime(new_promos['startdate'])
        new_promos['enddate'] = pd.to_datetime(new_promos['enddate'])
        new_promos['durationindays'] = (new_promos['enddate'] - new_promos['startdate']).dt.days
        new_promos['promotionstatus'] = 'Active'
        new_promos.to_sql('dimpromotion', engine_dwh, if_exists='append', index=False)
        added_count = len(new_promos)
        
    return added_count, 0 # Promotion không có update SCD2 trong thiết kế này

# ==============================================================================
# 3. GIAO DIỆN DASHBOARD
# ==============================================================================
def generate_dashboard(state_data, status_msg, last_error):
    # 1. Bảng Trạng Thái Dimensions
    table = Table(show_header=True, expand=True, box=box.SIMPLE_HEAD, padding=(0, 1))
    table.add_column("Dimension Table", style="bold cyan")
    table.add_column("Last Sync", style="dim")
    table.add_column("Added", justify="right", style="green")
    table.add_column("Updated", justify="right", style="yellow")
    table.add_column("Status", justify="center")

    for dim, data in state_data.items():
        icon = "✅" if data['status'] == 'OK' else "⏳"
        if data['status'] == 'Error': icon = "❌"
        
        added_str = f"+{data['added']}" if data['added'] > 0 else "-"
        updated_str = f"~{data['updated']}" if data['updated'] > 0 else "-"
        
        table.add_row(
            dim, 
            data['last_sync'], 
            added_str, 
            updated_str, 
            icon
        )

    # 2. Header Panel
    header = Panel(
        f"[bold white]DIMENSION SYNC SERVICE[/bold white] | [dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
        style="magenta", box=box.HEAVY_HEAD
    )

    # 3. Status Panel
    status_style = "blue"
    if "Sleeping" in status_msg: status_style = "grey50"
    
    status_panel = Panel(status_msg, title="[bold]System Activity[/bold]", border_style=status_style)

    # 4. Layout
    layout = Layout()
    elements = [
        Layout(header, size=3),
        Layout(Panel(table, border_style="white"), size=8), # Bảng chứa 4 dòng dim
        Layout(status_panel, size=3)
    ]

    if last_error != "None":
        error_panel = Panel(f"[red]{last_error}[/red]", title="Error Log", border_style="red")
        elements.append(Layout(error_panel, size=4))

    layout.split_column(*elements)
    return layout

# ==============================================================================
# 4. MAIN LOOP
# ==============================================================================
def dim_sync_worker():
    console = Console()
    console.clear()
    
    # Khởi tạo trạng thái ban đầu
    dims = ["DimDriver", "DimVehicle", "DimCustomer", "DimPromotion"]
    state_data = {
        d: {"last_sync": "Waiting...", "added": 0, "updated": 0, "status": "Pending"} 
        for d in dims
    }
    
    status_msg = "Initializing..."
    last_error = "None"

    with Live(generate_dashboard(state_data, status_msg, last_error), console=console, screen=True, refresh_per_second=4) as live:
        while True:
            try:
                # --- SYNC DRIVER ---
                status_msg = "[bold cyan]🔄 Syncing Drivers...[/bold cyan]"
                live.update(generate_dashboard(state_data, status_msg, last_error))
                a, u = sync_drivers()
                state_data["DimDriver"] = {"last_sync": datetime.now().strftime('%H:%M:%S'), "added": a, "updated": u, "status": "OK"}
                
                # --- SYNC VEHICLE ---
                status_msg = "[bold cyan]🔄 Syncing Vehicles...[/bold cyan]"
                live.update(generate_dashboard(state_data, status_msg, last_error))
                a, u = sync_vehicles()
                state_data["DimVehicle"] = {"last_sync": datetime.now().strftime('%H:%M:%S'), "added": a, "updated": u, "status": "OK"}

                # --- SYNC PROMOTION ---
                status_msg = "[bold cyan]🔄 Syncing Promotions...[/bold cyan]"
                live.update(generate_dashboard(state_data, status_msg, last_error))
                a, u = sync_promotions()
                state_data["DimPromotion"] = {"last_sync": datetime.now().strftime('%H:%M:%S'), "added": a, "updated": u, "status": "OK"}

                # --- SYNC CUSTOMER ---
                status_msg = "[bold cyan]🔄 Syncing Customers (Large Dataset)...[/bold cyan]"
                live.update(generate_dashboard(state_data, status_msg, last_error))
                a, u = sync_customers()
                state_data["DimCustomer"] = {"last_sync": datetime.now().strftime('%H:%M:%S'), "added": a, "updated": u, "status": "OK"}

                # --- SLEEP COUNTDOWN ---
                last_error = "None"
                sleep_time = 60
                for i in range(sleep_time, 0, -1):
                    status_msg = f"[grey]💤 Sleeping... Next sync in {i}s[/grey]"
                    live.update(generate_dashboard(state_data, status_msg, last_error))
                    time.sleep(1)

            except Exception as e:
                last_error = str(e)[0:100] + "..."
                status_msg = "[bold red]❌ Error Paused[/bold red]"
                live.update(generate_dashboard(state_data, status_msg, last_error))
                time.sleep(10)

if __name__ == "__main__":
    try:
        dim_sync_worker()
    except KeyboardInterrupt:
        print("\nStopped.")