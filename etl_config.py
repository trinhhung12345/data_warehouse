# etl_config.py
from sqlalchemy import create_engine
import redis

# --- CẤU HÌNH IP & PORT ---
SERVER_IP = "100.117.51.34" # IP Tailscale của bạn
REDIS_PORT = 6379
REDIS_PASS = "hung12345"

# --- KẾT NỐI REDIS ---
# decode_responses=True để nhận String thay vì Bytes
r_client = redis.Redis(host=SERVER_IP, port=REDIS_PORT, password=REDIS_PASS, decode_responses=True)

# --- KẾT NỐI DATABASE ---
# Ops (Nguồn): PostgreSQL
URL_OPS = f"postgresql://postgres:hung12345@{SERVER_IP}:5433/Uber_ops"
engine_ops = create_engine(URL_OPS)

# CRM (MariaDB) - THÊM CÁI NÀY
URL_CRM = f"mysql+pymysql://root:hung12345@{SERVER_IP}:3307/Uber_crm"
engine_crm = create_engine(URL_CRM)

# DWH (Đích): PostgreSQL
URL_DWH = f"postgresql://postgres:hung12345@{SERVER_IP}:5432/Uber_data_warehouse"
engine_dwh = create_engine(URL_DWH)

# --- THÔNG SỐ REDIS STREAM ---
STREAM_KEY = "stream:fact_trips_real"
GROUP_NAME = "dwh_group"
CONSUMER_NAME = "worker_jet"