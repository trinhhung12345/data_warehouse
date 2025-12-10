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
MAX_PENDING_SIZE = 50000  # <--- GIỚI HẠN AN TOÀN: Chỉ cho phép tối đa 50k tin chờ

# ==============================================================================
# 1. HÀM TẠO GIAO DIỆN COMPACT
# ==============================================================================
def generate_dashboard(total_pushed, last_id, batch_range, status, last_error, pending_count):
    # 1. Bảng Thống Kê
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    
    # Màu của Pending: Xanh (ít) -> Đỏ (đầy)
    pending_color = "green"
    if pending_count > MAX_PENDING_SIZE * 0.8: pending_color = "red"
    elif pending_count > MAX_PENDING_SIZE * 0.5: pending_color = "yellow"

    grid.add_row(
        Panel(f"[bold green]{total_pushed:,}[/bold green]", title="📦 Total Pushed", border_style="green"),
        Panel(f"[bold {pending_color}]{pending_count:,}[/bold {pending_color}] / {MAX_PENDING_SIZE}", title="⏳ Redis Queue", border_style=pending_color),
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
    if "Full" in status: status_style = "yellow" # Trạng thái mới: Full

    status_panel = Panel(status, title="[bold]Status[/bold]", border_style=status_style)

    # 4. Header
    header = Panel(
        f"[bold white]ETL PRODUCER (Smart Flow)[/bold white] | [dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
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
# 2. HÀM CHÍNH (SMART PRODUCER)
# ==============================================================================
def producer():
    console = Console()
    console.clear() 
    
    total_pushed = 0
    last_id = r_client.get(LAST_ID_KEY)
    last_id = int(last_id) if last_id else 0
    
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
                # --- CHECK 1: BACKPRESSURE CONTROL ---
                # Kiểm tra độ dài hàng đợi trong Redis
                pending_count = r_client.xlen(STREAM_KEY)
                
                # Nếu Redis đang gánh quá nhiều (> 50k tin), Producer tạm dừng
                if pending_count >= MAX_PENDING_SIZE:
                    status_msg = f"[bold yellow]✋ Queue Full ({pending_count:,}). Pausing 2s...[/bold yellow]"
                    live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error, pending_count))
                    time.sleep(2)
                    continue # Bỏ qua vòng lặp này, không query DB nữa

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
                    df = pd.read_sql(sql, conn, params={"last_id": last_id, "batch_size": BATCH_SIZE})

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
                    pipeline.xadd(STREAM_KEY, data)

                pipeline.execute()
                
                r_client.set(LAST_ID_KEY, int(current_max_id))
                last_id = current_max_id
                total_pushed += len(df)
                
                # --- GIAI ĐOẠN 4: SUCCESS ---
                status_msg = f"[bold green]✅ Pushed (+{len(df)})[/bold green]"
                last_error = "None"
                # Cập nhật lại số pending để hiển thị chính xác
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