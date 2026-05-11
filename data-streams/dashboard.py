import streamlit as st
import json
import pandas as pd
import time
import sys
import os
from datetime import datetime

# --- FIX LỖI IMPORT SHADOWING ---
current_dir = os.path.abspath(os.getcwd())
if current_dir in sys.path:
    sys.path.remove(current_dir)
if '' in sys.path:
    sys.path.remove('')

from kafka import KafkaConsumer

st.set_page_config(page_title="Hệ Thống Theo Dõi Gian Lận", page_icon="🕵️", layout="wide")

st.markdown("""
<style>
    .big-font { font-size:30px !important; font-weight: bold; color: #FF4B4B; }
    .metric-card { background-color: #262730; padding: 20px; border-radius: 10px; text-align: center; }
</style>
""", unsafe_allow_html=True)

st.title("🚀 Real-Time Dashboard: Hệ thống AI Phát Hiện Gian Lận")
st.markdown("Kiến trúc Lai (Hybrid Architecture): Kết hợp **Z-Score (Thống kê)** và **Isolation Forest (Machine Learning)**.")

@st.cache_resource
def create_kafka_consumer():
    try:
        # Nghe cả 2 topic: 'ratings' (dữ liệu thô liên tục) và 'movie_anomalies' (dữ liệu đã báo lỗi)
        consumer = KafkaConsumer(
            'movie_anomalies', 'ratings',
            bootstrap_servers=['localhost:9093'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='admin',
            sasl_plain_password='admin',
            consumer_timeout_ms=1000
        )
        return consumer
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Kafka: {e}")
        return None

consumer = create_kafka_consumer()

# State variables
if "total_anomalies" not in st.session_state: st.session_state.total_anomalies = 0
if "ml_count" not in st.session_state: st.session_state.ml_count = 0
if "zscore_count" not in st.session_state: st.session_state.zscore_count = 0
if "recent_alerts" not in st.session_state: st.session_state.recent_alerts = []
if "total_processed" not in st.session_state: st.session_state.total_processed = 0
if "live_stream" not in st.session_state: st.session_state.live_stream = []

# Tinh toc do tuc thoi (Speed)
if "last_time" not in st.session_state: st.session_state.last_time = time.time()
if "previous_count" not in st.session_state: st.session_state.previous_count = 0
if "current_speed" not in st.session_state: st.session_state.current_speed = 0.0

# ==================================
# GIAO DIỆN UI (LAYOUT)
# ==================================
col1, col2, col3, col4 = st.columns(4)
metric_total_processed = col1.empty()
metric_total_anomalies = col2.empty()
metric_ml = col3.empty()
metric_zscore = col4.empty()

with metric_total_processed: st.metric(label="Tổng Dữ Liệu Đang Chảy", value=0)
with metric_total_anomalies: st.metric(label="Tổng Cảnh Báo", value=0)
with metric_ml: st.metric(label="🤖 AI Phát Hiện (Isolation Forest)", value=0)
with metric_zscore: st.metric(label="🔥 Đột biến (Z-Score)", value=0)

col_chart, col_speed = st.columns([3, 1])
with col_chart:
    st.markdown("### 📊 Biểu đồ phân tích thuật toán")
    chart_placeholder = st.empty()
    chart_data = pd.DataFrame({"Thuật Toán": ["AI (ML)", "Z-Score"], "Số Lượng": [0, 0]}).set_index("Thuật Toán")
    chart_placeholder.bar_chart(chart_data)
with col_speed:
    st.markdown("### ⚡ Tốc độ xử lý (Dữ liệu/giây)")
    speed_placeholder = st.empty()
    speed_placeholder.metric(label="", value="0 msg/s")

# Hai bảng hiển thị song song
col_live, col_alerts = st.columns(2)
with col_live:
    st.markdown("### 🌊 Luồng dữ liệu thô liên tục (Live)")
    live_table_placeholder = st.empty()
    live_table_placeholder.dataframe(pd.DataFrame(columns=["Thời gian", "Người dùng", "Phim", "Điểm số"]), use_container_width=True)

with col_alerts:
    st.markdown("### 🚨 Các cảnh báo bất thường (Alerts)")
    alert_table_placeholder = st.empty()
    alert_table_placeholder.dataframe(pd.DataFrame(columns=["Thời gian", "Người dùng", "Phim", "Điểm số", "Loại Cảnh Báo"]), use_container_width=True)

run_dashboard = st.checkbox("Bắt đầu theo dõi luồng dữ liệu (Live Stream)", value=True)

# ==================================
# XỬ LÝ DỮ LIỆU THỜI GIAN THỰC
# ==================================
if run_dashboard and consumer:
    while run_dashboard:
        try:
            records = consumer.poll(timeout_ms=1000)
            has_new_data = False
            
            for topic_partition, messages in records.items():
                for message in messages:
                    data = message.value
                    topic = message.topic
                    
                    # 1. Luồng dữ liệu thô liên tục
                    if topic == 'ratings':
                        st.session_state.total_processed += 1
                        st.session_state.live_stream.insert(0, {
                            "Thời gian": datetime.now().strftime("%H:%M:%S"),
                            "Người dùng": data.get('userId'),
                            "Phim": data.get('movie', {}).get('title', 'Unknown') if isinstance(data.get('movie'), dict) else "Unknown",
                            "Điểm số": data.get('rating')
                        })
                        st.session_state.live_stream = st.session_state.live_stream[:10]
                        has_new_data = True
                        
                    # 2. Cảnh báo gian lận từ Spark (Kiến trúc Lai)
                    elif topic == 'movie_anomalies':
                        st.session_state.total_anomalies += 1
                        anomaly_type_str = data.get('anomaly_type', '')
                        
                        # Phân loại dựa trên chuỗi trả về từ Spark
                        if "ML" in anomaly_type_str or "Isolation Forest" in anomaly_type_str:
                            st.session_state.ml_count += 1
                        elif "ĐỘT BIẾN" in anomaly_type_str or "Z-Score" in anomaly_type_str:
                            st.session_state.zscore_count += 1
                            
                        st.session_state.recent_alerts.insert(0, {
                            "Thời gian": data.get('event_time'),
                            "Người dùng": data.get('userId'),
                            "Phim": data.get('title', 'Unknown'),
                            "Điểm số": data.get('rating_val'),
                            "Loại Cảnh Báo": anomaly_type_str
                        })
                        st.session_state.recent_alerts = st.session_state.recent_alerts[:10]
                        has_new_data = True

            # Tính tốc độ tức thời (Cập nhật mỗi giây)
            current_time = time.time()
            time_diff = current_time - st.session_state.last_time
            if time_diff >= 1.0:
                msg_diff = st.session_state.total_processed - st.session_state.previous_count
                st.session_state.current_speed = round(msg_diff / time_diff, 1)
                st.session_state.previous_count = st.session_state.total_processed
                st.session_state.last_time = current_time
                has_new_data = True # Ép UI cập nhật tốc độ

            # Cập nhật UI
            if has_new_data:
                speed_placeholder.metric(label="", value=f"{st.session_state.current_speed} msg/s")

                with metric_total_processed: st.metric(label="Tổng Dữ Liệu Đang Chảy", value=st.session_state.total_processed)
                with metric_total_anomalies: st.metric(label="Tổng Cảnh Báo", value=st.session_state.total_anomalies)
                with metric_ml: st.metric(label="🤖 AI (Isolation Forest)", value=st.session_state.ml_count)
                with metric_zscore: st.metric(label="🔥 Đột biến (Z-Score)", value=st.session_state.zscore_count)

                chart_data = pd.DataFrame({"Thuật Toán": ["AI (Isolation Forest)", "Thống Kê (Z-Score)"], "Số Lượng": [st.session_state.ml_count, st.session_state.zscore_count]}).set_index("Thuật Toán")
                chart_placeholder.bar_chart(chart_data)

                live_table_placeholder.dataframe(pd.DataFrame(st.session_state.live_stream), use_container_width=True)
                alert_table_placeholder.dataframe(pd.DataFrame(st.session_state.recent_alerts), use_container_width=True)

        except Exception as e:
            pass
            
        time.sleep(0.5)
