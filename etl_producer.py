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

# ==============================================================================
# 1. HÀM TẠO GIAO DIỆN COMPACT (ĐÃ BỎ KHUNG NGOÀI)
# ==============================================================================
def generate_dashboard(total_pushed, last_id, batch_range, status, last_error):
    # 1. Bảng Thống Kê (Thu gọn chiều cao)
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    
    # Dùng box=box.ROUNDED để nhìn mềm mại hơn, giảm padding
    grid.add_row(
        Panel(f"[bold green]{total_pushed:,}[/bold green]", title="📦 Total", border_style="green"),
        Panel(f"[bold cyan]{last_id}[/bold cyan]", title="🔖 Cursor", border_style="cyan"),
        Panel(f"[bold yellow]{BATCH_SIZE}[/bold yellow]", title="⚙️ Batch", border_style="yellow"),
    )

    # 2. Bảng Debug (Thu gọn)
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
    if "Success" in status: status_style = "green"

    status_panel = Panel(
        status, 
        title="[bold]Status[/bold]", 
        border_style=status_style
    )

    # 4. Header Panel (Thay cho khung bao ngoài)
    header = Panel(
        f"[bold white]ETL PRODUCER MONITOR[/bold white] | [dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
        style="blue", box=box.HEAVY_HEAD
    )

    # 5. TẠO LAYOUT
    layout = Layout()
    
    # Chia layout thành các phần nhỏ, tính toán size kỹ lưỡng
    layout_elements = [
        Layout(header, size=3),          # Tiêu đề (3 dòng)
        Layout(grid, size=4),            # Thống kê (4 dòng)
        Layout(status_panel, size=3),    # Trạng thái (3 dòng)
        Layout(Panel(debug_table, title="Debugger", border_style="magenta"), size=5) # Debug (5 dòng)
    ]

    # Nếu có lỗi thì chèn vào
    if last_error != "None":
        error_panel = Panel(f"[red]{last_error}[/red]", title="Error", border_style="red")
        layout_elements.append(Layout(error_panel, size=4))

    layout.split_column(*layout_elements)

    # TRẢ VỀ LAYOUT TRỰC TIẾP (KHÔNG BỌC PANEL NGOÀI NỮA)
    return layout

# ==============================================================================
# 2. HÀM CHÍNH
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

    # screen=True để chiếm toàn màn hình, tránh trôi dòng
    with Live(
        generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error), 
        console=console, 
        screen=True,  
        refresh_per_second=4
    ) as live:
        
        while True:
            # --- GIAI ĐOẠN 1: SCANNING ---
            status_msg = f"[bold yellow]🔍 Scanning > {last_id}...[/bold yellow]"
            live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error))
            
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
            
            try:
                with engine_ops.connect() as conn:
                    df = pd.read_sql(sql, conn, params={"last_id": last_id, "batch_size": BATCH_SIZE})

                if df.empty:
                    # --- GIAI ĐOẠN 2: IDLE ---
                    status_msg = "[grey]💤 Idle. Waiting 5s...[/grey]"
                    batch_range = ("Waiting", "Waiting")
                    live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error))
                    time.sleep(5)
                    continue

                current_min_id = df['trip_id'].min()
                current_max_id = df['trip_id'].max()
                batch_range = (f"{current_min_id}", f"{current_max_id}")

                # --- GIAI ĐOẠN 3: PUSHING ---
                status_msg = f"[bold blue]📦 Pushing {len(df)} rows...[/bold blue]"
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error))

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
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error))
                
            except Exception as e:
                # --- GIAI ĐOẠN 5: ERROR ---
                last_error = str(e)[0:100] + "..."
                status_msg = "[bold red]❌ Error[/bold red]"
                live.update(generate_dashboard(total_pushed, last_id, batch_range, status_msg, last_error))
                time.sleep(5)

if __name__ == "__main__":
    try:
        producer()
    except KeyboardInterrupt:
        print("\n[bold red]Stopped by user[/bold red]")