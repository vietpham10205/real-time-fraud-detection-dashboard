import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import time

st.set_page_config(
    page_title="Real-time Fleet Intelligence Pipeline",
    page_icon="🚕",
    layout="wide"
)

# Database connection
DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

def get_db_connection():
    try:
        return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    except Exception as e:
        return None

# Load Custom CSS for beautiful styling
st.markdown("""
<style>
    .reportview-container {
        background: #111111
    }
    .main .block-container {
        padding-top: 2rem;
    }
    h1 {
        color: #F8D030;
        font-family: 'Inter', sans-serif;
    }
    .metric-card {
        background-color: #222222;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #F8D030;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    .sys-card {
        background-color: #1a2a3a;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #00aaff;
        margin-bottom: 15px;
    }
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: white;
    }
    .sys-value {
        font-size: 24px;
        font-weight: bold;
        color: #00aaff;
    }
    .metric-title {
        font-size: 13px;
        color: #AAAAAA;
        text-transform: uppercase;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚕 NYC Taxi: Real-time Fleet Intelligence Pipeline")
st.markdown("**Kiến trúc:** `Parquet -> Kafka -> Spark Structured Streaming (Isolation Forest) -> Distributed DB (PostgreSQL) -> Dashboard`")

placeholder = st.empty()

while True:
    conn = get_db_connection()
    if not conn:
        with placeholder.container():
            st.error("Cannot connect to Database. Waiting for Spark Pipeline to initialize...")
        time.sleep(2)
        continue

    # Fetch Data
    try:
        df_normal = pd.read_sql("SELECT * FROM normal_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_anomalies = pd.read_sql("SELECT * FROM anomalous_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_sys = pd.read_sql("SELECT * FROM pipeline_metrics ORDER BY inserted_at DESC LIMIT 1", conn)
        df_sys_hist = pd.read_sql("SELECT SUM(estimated_cost_usd) as total_cost, SUM(records_processed) as total_recs FROM pipeline_metrics", conn)
        df_live = pd.read_sql("SELECT VendorID, tpep_pickup_datetime, trip_distance, fare_amount, avg_speed_kmh, anomaly_score FROM normal_trips ORDER BY inserted_at DESC LIMIT 5", conn)
    except Exception as e:
        with placeholder.container():
            st.warning("Database tables not ready yet...")
        time.sleep(2)
        continue
    finally:
        conn.close()

    total_normal = len(df_normal)
    total_anomalies = len(df_anomalies)
    
    with placeholder.container():
        # --- ROW 1: SYSTEM MONITORING ---
        st.subheader("⚙️ System Health & Performance (Big Data Metrics)")
        sys1, sys2, sys3, sys4 = st.columns(4)
        
        current_tps = round(df_sys['throughput_eps'].iloc[0], 2) if not df_sys.empty else 0
        latency_ms = round(df_sys['processing_time_ms'].iloc[0], 2) if not df_sys.empty else 0
        total_records = int(df_sys_hist['total_recs'].iloc[0]) if not df_sys_hist.empty and pd.notna(df_sys_hist['total_recs'].iloc[0]) else 0
        total_cost = round(df_sys_hist['total_cost'].iloc[0], 5) if not df_sys_hist.empty and pd.notna(df_sys_hist['total_cost'].iloc[0]) else 0
        
        with sys1:
            st.markdown(f'<div class="sys-card"><div class="metric-title">Tốc độ truyền (Throughput)</div><div class="sys-value">{current_tps} rows/s</div></div>', unsafe_allow_html=True)
        with sys2:
            st.markdown(f'<div class="sys-card"><div class="metric-title">Độ trễ xử lý (Latency)</div><div class="sys-value">{latency_ms} ms</div></div>', unsafe_allow_html=True)
        with sys3:
            st.markdown(f'<div class="sys-card"><div class="metric-title">Tổng bản ghi đã phân tích</div><div class="sys-value">{total_records}</div></div>', unsafe_allow_html=True)
        with sys4:
            st.markdown(f'<div class="sys-card" style="border-left-color:#00ff88"><div class="metric-title">Chi phí Cloud ước tính</div><div class="sys-value" style="color:#00ff88">${total_cost}</div></div>', unsafe_allow_html=True)

        st.write("---")

        # --- ROW 2: LIVE DATA STREAM ---
        st.subheader("🌊 Live Data Stream (Luồng dữ liệu thời gian thực)")
        if not df_live.empty:
            st.dataframe(df_live, use_container_width=True)
        else:
            st.info("Đang chờ dữ liệu...")
            
        st.write("---")

        # --- ROW 3: BUSINESS METRICS ---
        st.subheader("🚕 Business Analytics & AI (Isolation Forest)")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-title">Chuyến đi bình thường</div><div class="metric-value">{total_normal}</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card" style="border-left-color: #FF4B4B;"><div class="metric-title">Cảnh báo Bất thường</div><div class="metric-value" style="color: #FF4B4B;">{total_anomalies}</div></div>', unsafe_allow_html=True)
        with col3:
            avg_fare = round(df_normal['fare_amount'].mean(), 2) if not df_normal.empty else 0
            st.markdown(f'<div class="metric-card"><div class="metric-title">Giá cước TB (Bình thường)</div><div class="metric-value">${avg_fare}</div></div>', unsafe_allow_html=True)
        with col4:
            avg_fare_anomaly = round(df_anomalies['fare_amount'].mean(), 2) if not df_anomalies.empty else 0
            st.markdown(f'<div class="metric-card" style="border-left-color: #FF4B4B;"><div class="metric-title">Giá cước TB (Bất thường)</div><div class="metric-value" style="color: #FF4B4B;">${avg_fare_anomaly}</div></div>', unsafe_allow_html=True)
            
        st.write("---")
        
        # --- ROW 4: CHARTS ---
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.subheader("📊 Tốc độ vs Khoảng cách")
            if not df_normal.empty and not df_anomalies.empty:
                df_normal['Type'] = 'Normal'
                df_anomalies['Type'] = 'Anomaly'
                df_combined = pd.concat([df_normal, df_anomalies])
                
                fig = px.scatter(df_combined, x="trip_distance", y="avg_speed_kmh", color="Type",
                                 color_discrete_map={"Normal": "#F8D030", "Anomaly": "#FF4B4B"},
                                 hover_data=['fare_amount', 'trip_duration_sec'])
                fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Chưa đủ dữ liệu để vẽ biểu đồ.")
                
        with col_chart2:
            st.subheader("💰 Giá cước vs Thời gian chuyến đi")
            if not df_normal.empty and not df_anomalies.empty:
                if 'Type' not in df_combined.columns:
                     df_normal['Type'] = 'Normal'
                     df_anomalies['Type'] = 'Anomaly'
                     df_combined = pd.concat([df_normal, df_anomalies])
                fig2 = px.scatter(df_combined, x="trip_duration_sec", y="fare_amount", color="Type",
                                 color_discrete_map={"Normal": "#F8D030", "Anomaly": "#FF4B4B"})
                fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Chưa đủ dữ liệu để vẽ biểu đồ.")
                
        st.write("---")
        st.subheader("🚨 Danh sách chuyến đi có dấu hiệu thu phí sai luật / Bất thường")
        if not df_anomalies.empty:
            display_df = df_anomalies[['tpep_pickup_datetime', 'trip_distance', 'fare_amount', 'avg_speed_kmh', 'anomaly_score', 'reason']]
            st.dataframe(display_df.style.applymap(lambda x: "background-color: #551111; color: white;"), use_container_width=True)
        else:
            st.success("Hệ thống chưa phát hiện bất thường nào.")

    time.sleep(2)
