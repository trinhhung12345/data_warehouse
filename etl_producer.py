import time
import pandas as pd
import json
from sqlalchemy import text
from datetime import datetime
from etl_config import r_client, engine_ops, STREAM_KEY

# --- THƯ VIỆN GIAO DIỆN RICH ---
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich import box

LAST_ID_KEY = "etl:state:last_trip_id"
BATCH_SIZE = 1000

# CẤU HÌNH NGƯỠNG AN TOÀN
THRESHOLD_WARNING = 50000   # Bắt đầu phanh nhẹ
THRESHOLD_CRITICAL = 100000 # Phanh gấp
MAX_SAFETY_CAP = 200000     # Giới hạn cứng để không sập Redis (Consumer quá chậm thì chấp nhận mất)

# ==============================================================================
# 1. HÀM TẠO GIAO DIỆN COMPACT
# ==============================================================================
def generate_dashboard(total_pushed, last_id, batch_range, status, last_error, pending_count):
    # 1. Bảng Thống Kê
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    
    # Màu sắc Pending
    pending_color = "green"
    if pending_count > THRESHOLD_CRITICAL: pending_color = "red"
    elif pending_count > THRESHOLD_WARNING: pending_color = "yellow"

    grid.add_row(
        Panel(f"[bold green]{total_pushed:,}[/bold green]", title="📦 Total Pushed", border_style="green"),
        Panel(f"[bold {pending_color}]{pending_count:,}[/bold {pending_color}]", title="⏳ Queue Size", border_style=pending_color),
        Panel(f"[bold cyan]{last_id}[/bold cyan]", title="🔖 Cursor", border_style="cyan"),
    )

    # 2. Bảng Debug
    debug_table = Table(show_header=False, expand=True, box=None, padding=(0, 1))
    debug_table.add_column("Metric", style="dim", width=15)
    debug_table.add_column("Value", style="bold white")
    
    start_batch, end_batch = batch_range
    debug_table.add_row("Range:", f"{start_batch} -> {end_batch}")
    debug_table.add_row("Target:", STREAM_KEY)

    # 3. Status Panel
    status_style = "blue"
    if "Idle" in status: status_style = "grey50"
    if "Error" in status: status_style = "red"
    if "Slowing" in status: status_style = "yellow"

    status_panel = Panel(status, title="[bold]Status[/bold]", border_style=status_style)

    # 4. Header
    header = Panel(
        f"[bold white]ETL PRODUCER (Safe Mode)[/bold white] | [dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
        style="blue", box=box.HEAVY_HEAD
    )

    # 5. Layout
    layout = Layout()
    layout_elements = [
        Layout(header, size=3),
        Layout(grid, size=4),
        Layout(status_panel, size=3),
        Layout(Panel(debug_table, title="Debugger", border_style="magenta"), size=5)
    ]

    if last_error != "None":
        error_panel = Panel(f"[red]{last_error}[/red]", title="Error", border_style="red")
        layout_elements.append(Layout(error_panel, size=4))

    layout.split_column(*layout_elements)
    return layout

# ==============================================================================
# 2. HÀM CHÍNH (SAFE PRODUCER)
# ==============================================================================
def producer():
    console = Console()
    console.clear() 
    
    total_pushed = 0
    last_id_redis = r_client.get(LAST_ID_KEY)
    last_id = int(last_id_redis) if last_id_redis else 0
    
    batch_range = ("N/A", "N/A")
    status_msg = "[grey]Initializing...[/grey]"
    last_error = "None"
    pending_count = 0

    with Live(
        generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count), 
        console=console, screen=True, refresh_per_second=2
    ) as live:
        
        while True:
            try:
                # --- CHECK 1: SMART THROTTLING (PHANH THÔNG MINH) ---
                pending_count = r_client.xlen(STREAM_KEY)
                
                # Logic điều chỉnh tốc độ
                sleep_duration = 0
                
                if pending_count > THRESHOLD_CRITICAL:
                    # Nguy hiểm: Ngủ lâu để Consumer dọn dẹp
                    status_msg = f"[bold red]✋ Queue High ({pending_count:,}). Slowing down 5s...[/bold red]"
                    sleep_duration = 5
                elif pending_count > THRESHOLD_WARNING:
                    # Cảnh báo: Ngủ nhẹ
                    status_msg = f"[bold yellow]⚠️ Queue Warning ({pending_count:,}). Throttling 1s...[/bold yellow]"
                    sleep_duration = 1
                
                # Cập nhật giao diện nếu đang bị delay
                if sleep_duration > 0:
                    live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                    time.sleep(sleep_duration)

                # --- GIAI ĐOẠN 2: SCANNING ---
                status_msg = f"[bold yellow]🔍 Scanning > {last_id}...[/bold yellow]"
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                
                sql = text(f"""
                    SELECT 
                        t.trip_id, t.driver_id, t.customer_id, t.vendorid,
                        t.tpep_pickup_datetime, t.tpep_dropoff_datetime,
                        t.passenger_count, t.trip_distance, t.ratecodeid,
                        t.pulocationid, t.dolocationid, t.payment_type,
                        t.fare_amount, t.extra, t.mta_tax, t.tip_amount, 
                        t.tolls_amount, t.improvement_surcharge, t.total_amount,
                        t.congestion_surcharge,
                        d.vehicle_id 
                    FROM trips t
                    LEFT JOIN drivers d ON t.driver_id = d.driver_id
                    WHERE t.trip_id > :last_id
                    ORDER BY t.trip_id ASC
                    LIMIT :batch_size
                """)
                
                with engine_ops.connect() as conn:
                    df = pd.read_sql(sql, conn, params={"last_id": int(last_id), "batch_size": BATCH_SIZE})

                if df.empty:
                    status_msg = "[grey]💤 Idle. Waiting 5s...[/grey]"
                    batch_range = ("Waiting", "Waiting")
                    live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                    time.sleep(5)
                    continue

                current_min_id = df['trip_id'].min()
                current_max_id = df['trip_id'].max()
                batch_range = (f"{current_min_id}", f"{current_max_id}")

                # --- GIAI ĐOẠN 3: PUSHING ---
                status_msg = f"[bold blue]📦 Pushing {len(df)} rows...[/bold blue]"
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))

                pipeline = r_client.pipeline()
                for _, row in df.iterrows():
                    data = row.to_dict()
                    for k, v in data.items():
                        if isinstance(v, (datetime, pd.Timestamp)):
                            data[k] = str(v)
                        elif v is None:
                            data[k] = ""
                        else:
                            data[k] = str(v)
                    
                    # 🔥 CẤU HÌNH AN TOÀN: 
                    # Vẫn dùng maxlen nhưng để rất lớn (200k) để tránh sập RAM
                    # Logic Throttling ở trên sẽ giữ queue không bao giờ chạm tới mức này
                    pipeline.xadd(STREAM_KEY, data, maxlen=MAX_SAFETY_CAP, approximate=True)

                pipeline.execute()
                
                last_id = int(current_max_id) 
                r_client.set(LAST_ID_KEY, last_id)
                total_pushed += len(df)
                
                status_msg = f"[bold green]✅ Pushed (+{len(df)})[/bold green]"
                last_error = "None"
                # Cập nhật lại pending count ước tính
                pending_count += len(df)
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                
            except Exception as e:
                last_error = str(e)[0:100] + "..."
                status_msg = "[bold red]❌ Error[/bold red]"
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                time.sleep(5)

if __name__ == "__main__":
    try:
        producer()
    except KeyboardInterrupt:
        print("\n[bold red]Stopped by user[/bold red]")