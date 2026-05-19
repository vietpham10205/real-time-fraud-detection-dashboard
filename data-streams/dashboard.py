import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(
    page_title="Multi-Source Fleet Intelligence",
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

# Custom CSS
st.markdown("""
<style>
    .reportview-container { background: #111111 }
    .main .block-container { padding-top: 2rem; }
    h1 { color: #F8D030; font-family: 'Inter', sans-serif; }
    .metric-card {
        background-color: #222222;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #F8D030;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    .metric-card-green {
        background-color: #222222;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #00E676;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    .sys-card {
        background-color: #1a2a3a;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #00aaff;
        margin-bottom: 15px;
    }
    .metric-value { font-size: 32px; font-weight: bold; color: white; }
    .sys-value { font-size: 24px; font-weight: bold; color: #00aaff; }
    .metric-title { font-size: 13px; color: #AAAAAA; text-transform: uppercase; }
</style>
""", unsafe_allow_html=True)

st.title("🚕 NYC Taxi: Multi-Source Real-Time Pipeline")
st.markdown("**Kiến trúc:** `2 Producers (Yellow/Green) -> Kafka -> Spark Structured Streaming -> PostgreSQL -> Streamlit Dashboard`")

# --- REAL-TIME FRAGMENT ---
@st.fragment(run_every=3)
def update_dashboard():
    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to Database. Waiting for Spark Pipeline to initialize...")
        return

    try:
        # Fetch System Metrics
        df_sys_hist = pd.read_sql("SELECT batch_id, inserted_at, processing_time_ms, throughput_eps, estimated_cost_usd, records_processed FROM pipeline_metrics ORDER BY inserted_at DESC LIMIT 50", conn)
        
        # Fetch Aggregated Stats
        df_counts = pd.read_sql(
            """
            SELECT 
                taxi_type, 
                COUNT(*) as total_normal, 
                AVG(fare_amount) as avg_fare 
            FROM normal_trips 
            GROUP BY taxi_type
            """, conn
        )
        
        df_anomalies_counts = pd.read_sql(
            """
            SELECT 
                taxi_type, 
                COUNT(*) as total_anomalies
            FROM anomalous_trips 
            GROUP BY taxi_type
            """, conn
        )

        df_normal = pd.read_sql("SELECT * FROM normal_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_anomalies = pd.read_sql("SELECT * FROM anomalous_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_live = pd.read_sql("SELECT taxi_type, VendorID, pickup_datetime, trip_distance, fare_amount, avg_speed_kmh, anomaly_score FROM normal_trips ORDER BY inserted_at DESC LIMIT 6", conn)
        
    except Exception as e:
        st.warning("Database tables not ready or missing columns...")
        return
    finally:
        conn.close()

    # Calculate global metrics
    current_tps = round(df_sys_hist['throughput_eps'].iloc[0], 2) if not df_sys_hist.empty else 0
    latency_ms = round(df_sys_hist['processing_time_ms'].iloc[0], 2) if not df_sys_hist.empty else 0
    total_records = df_sys_hist['records_processed'].sum() if not df_sys_hist.empty else 0
    total_cost = round(df_sys_hist['estimated_cost_usd'].sum(), 5) if not df_sys_hist.empty else 0

    # --- ROW 1: SYSTEM MONITORING ---
    st.subheader("⚙️ System Performance (Real-time Cluster Metrics)")
    sys1, sys2, sys3, sys4 = st.columns(4)
    
    with sys1:
        st.markdown(f'<div class="sys-card"><div class="metric-title">Tốc độ truyền (Throughput)</div><div class="sys-value">{current_tps} rows/s</div></div>', unsafe_allow_html=True)
    with sys2:
        st.markdown(f'<div class="sys-card"><div class="metric-title">Độ trễ xử lý (Latency)</div><div class="sys-value">{latency_ms} ms</div></div>', unsafe_allow_html=True)
    with sys3:
        st.markdown(f'<div class="sys-card"><div class="metric-title">Lưu lượng phân tích (50 Batch gần nhất)</div><div class="sys-value">{total_records} records</div></div>', unsafe_allow_html=True)
    with sys4:
        st.markdown(f'<div class="sys-card" style="border-left-color:#00ff88"><div class="metric-title">Chi phí xử lý đám mây</div><div class="sys-value" style="color:#00ff88">${total_cost}</div></div>', unsafe_allow_html=True)

    # System Line Charts
    if not df_sys_hist.empty:
        df_sys_hist = df_sys_hist.sort_values("inserted_at")
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            fig_tps = px.line(df_sys_hist, x="inserted_at", y="throughput_eps", title="Throughput over time (Rows/sec)")
            fig_tps.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig_tps, use_container_width=True)
        with chart_col2:
            fig_lat = px.line(df_sys_hist, x="inserted_at", y="processing_time_ms", title="Latency over time (ms)")
            fig_lat.update_traces(line_color='#FF4B4B')
            fig_lat.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig_lat, use_container_width=True)

    st.write("---")

    # --- ROW 2: LIVE DATA STREAM ---
    st.subheader("🌊 Live Data Stream (Đa luồng dữ liệu)")
    if not df_live.empty:
        def style_taxi_type(val):
            color = '#F8D030' if val == 'yellow' else '#00E676'
            return f'color: {color}; font-weight: bold;'
        
        st.dataframe(df_live.style.map(style_taxi_type, subset=['taxi_type']), use_container_width=True)
    else:
        st.info("Đang chờ dữ liệu từ Producer...")
        
    st.write("---")

    # --- ROW 3: BUSINESS METRICS (COMPARISON) ---
    st.subheader("🚕 Business Analytics: Yellow vs Green Taxi")
    
    yellow_normal = int(df_counts[df_counts['taxi_type'] == 'yellow']['total_normal'].iloc[0]) if not df_counts[df_counts['taxi_type'] == 'yellow'].empty else 0
    green_normal = int(df_counts[df_counts['taxi_type'] == 'green']['total_normal'].iloc[0]) if not df_counts[df_counts['taxi_type'] == 'green'].empty else 0
    
    yellow_avg_fare = round(df_counts[df_counts['taxi_type'] == 'yellow']['avg_fare'].iloc[0], 2) if not df_counts[df_counts['taxi_type'] == 'yellow'].empty else 0
    green_avg_fare = round(df_counts[df_counts['taxi_type'] == 'green']['avg_fare'].iloc[0], 2) if not df_counts[df_counts['taxi_type'] == 'green'].empty else 0
    
    yellow_anomalies = int(df_anomalies_counts[df_anomalies_counts['taxi_type'] == 'yellow']['total_anomalies'].iloc[0]) if not df_anomalies_counts[df_anomalies_counts['taxi_type'] == 'yellow'].empty else 0
    green_anomalies = int(df_anomalies_counts[df_anomalies_counts['taxi_type'] == 'green']['total_anomalies'].iloc[0]) if not df_anomalies_counts[df_anomalies_counts['taxi_type'] == 'green'].empty else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-title">Tổng chuyến Yellow Taxi</div><div class="metric-value">{yellow_normal}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card-green"><div class="metric-title">Tổng chuyến Green Taxi</div><div class="metric-value">{green_normal}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><div class="metric-title">Giá TB Yellow vs Green</div><div class="metric-value" style="font-size:24px">${yellow_avg_fare} / ${green_avg_fare}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card" style="border-left-color: #FF4B4B;"><div class="metric-title">Cảnh báo (Yellow / Green)</div><div class="metric-value" style="color: #FF4B4B; font-size:24px">{yellow_anomalies} / {green_anomalies}</div></div>', unsafe_allow_html=True)
        
    st.write("---")
    
    # --- ROW 4: CHARTS ---
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("📊 Tốc độ vs Khoảng cách (Phân loại theo Taxi)")
        if not df_normal.empty:
            fig = px.scatter(df_normal, x="trip_distance", y="avg_speed_kmh", color="taxi_type",
                             color_discrete_map={"yellow": "#F8D030", "green": "#00E676"},
                             hover_data=['fare_amount'])
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa đủ dữ liệu để vẽ biểu đồ.")
            
    with col_chart2:
        st.subheader("💰 Giá cước vs Thời gian chuyến đi")
        if not df_normal.empty:
            fig2 = px.scatter(df_normal, x="trip_duration_sec", y="fare_amount", color="taxi_type",
                             color_discrete_map={"yellow": "#F8D030", "green": "#00E676"})
            fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Chưa đủ dữ liệu để vẽ biểu đồ.")
            
    st.write("---")
    st.subheader("🚨 Bảng tóm tắt các chuyến đi có dấu hiệu Bất thường")
    if not df_anomalies.empty:
        display_df = df_anomalies[['taxi_type', 'pickup_datetime', 'trip_distance', 'fare_amount', 'avg_speed_kmh', 'anomaly_score', 'reason']]
        st.dataframe(display_df.style.map(lambda _: "background-color: #551111; color: #FFAAAA;", subset=['reason']), use_container_width=True)
    else:
        st.success("Hệ thống chưa phát hiện bất thường nào.")

# Run the update function
update_dashboard()
