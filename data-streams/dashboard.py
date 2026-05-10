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

st.title("🚀 Real-Time Dashboard: Phát Hiện Gian Lận Đánh Giá Phim")
st.markdown("Hiển thị đồng thời **Luồng dữ liệu tổng (Live Stream)** và **Các giao dịch gian lận (Alerts)**.")

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

if "total_anomalies" not in st.session_state: st.session_state.total_anomalies = 0
if "inflation_count" not in st.session_state: st.session_state.inflation_count = 0
if "bombing_count" not in st.session_state: st.session_state.bombing_count = 0
if "recent_alerts" not in st.session_state: st.session_state.recent_alerts = []
if "total_processed" not in st.session_state: st.session_state.total_processed = 0
if "live_stream" not in st.session_state: st.session_state.live_stream = []
if "start_time" not in st.session_state: st.session_state.start_time = time.time()

# ==================================
# GIAO DIỆN UI (LAYOUT)
# ==================================
col1, col2, col3, col4 = st.columns(4)
metric_total_processed = col1.empty()
metric_total_anomalies = col2.empty()
metric_inflation = col3.empty()
metric_bombing = col4.empty()

with metric_total_processed: st.metric(label="Tổng Dữ Liệu Đang Chảy", value=0)
with metric_total_anomalies: st.metric(label="Tổng Lượt Đánh Giá Ảo", value=0)
with metric_inflation: st.metric(label="📈 Nâng khống điểm (5.0)", value=0)
with metric_bombing: st.metric(label="📉 Hạ bệ/Dìm giá (<=1.0)", value=0)

col_chart, col_speed = st.columns([3, 1])
with col_chart:
    st.markdown("### 📊 Biểu đồ so sánh các loại gian lận")
    chart_placeholder = st.empty()
    chart_data = pd.DataFrame({"Loại Gian Lận": ["Nâng khống điểm", "Cố tình hạ bệ"], "Số Lượng": [0, 0]}).set_index("Loại Gian Lận")
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
    alert_table_placeholder.dataframe(pd.DataFrame(columns=["Thời gian", "Người dùng", "Phim", "Điểm số", "Loại Vi Phạm"]), use_container_width=True)

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
                    
                    # 1. Nếu là luồng dữ liệu thô liên tục (Tất cả đánh giá)
                    if topic == 'ratings':
                        st.session_state.total_processed += 1
                        st.session_state.live_stream.insert(0, {
                            "Thời gian": datetime.now().strftime("%H:%M:%S"),
                            "Người dùng": data.get('userId'),
                            "Phim": data.get('movie', {}).get('title', 'Unknown') if isinstance(data.get('movie'), dict) else "Unknown",
                            "Điểm số": data.get('rating')
                        })
                        st.session_state.live_stream = st.session_state.live_stream[:10] # Giữ 10 dòng
                        has_new_data = True
                        
                    # 2. Nếu là cảnh báo gian lận từ Spark
                    elif topic == 'movie_anomalies':
                        st.session_state.total_anomalies += 1
                        if "Nâng khống" in data.get('anomaly_type', ''):
                            st.session_state.inflation_count += 1
                        elif "hạ điểm" in data.get('anomaly_type', ''):
                            st.session_state.bombing_count += 1
                            
                        st.session_state.recent_alerts.insert(0, {
                            "Thời gian": data.get('event_time'),
                            "Người dùng": data.get('userId'),
                            "Phim": data.get('title', 'Unknown'),
                            "Điểm số": data.get('rating_val'),
                            "Loại Vi Phạm": data.get('anomaly_type')
                        })
                        st.session_state.recent_alerts = st.session_state.recent_alerts[:10] # Giữ 10 dòng
                        has_new_data = True

            # Cập nhật UI nếu có dữ liệu mới
            if has_new_data:
                # Tính tốc độ
                elapsed = time.time() - st.session_state.start_time
                speed = int(st.session_state.total_processed / elapsed) if elapsed > 0 else 0
                speed_placeholder.metric(label="", value=f"{speed} msg/s")

                with metric_total_processed: st.metric(label="Tổng Dữ Liệu Đang Chảy", value=st.session_state.total_processed)
                with metric_total_anomalies: st.metric(label="Tổng Lượt Đánh Giá Ảo", value=st.session_state.total_anomalies)
                with metric_inflation: st.metric(label="📈 Nâng khống điểm (5.0)", value=st.session_state.inflation_count)
                with metric_bombing: st.metric(label="📉 Hạ bệ/Dìm giá (<=1.0)", value=st.session_state.bombing_count)

                chart_data = pd.DataFrame({"Loại Gian Lận": ["Nâng khống điểm", "Cố tình hạ bệ"], "Số Lượng": [st.session_state.inflation_count, st.session_state.bombing_count]}).set_index("Loại Gian Lận")
                chart_placeholder.bar_chart(chart_data)

                live_table_placeholder.dataframe(pd.DataFrame(st.session_state.live_stream), use_container_width=True)
                alert_table_placeholder.dataframe(pd.DataFrame(st.session_state.recent_alerts), use_container_width=True)

        except Exception as e:
            pass
            
        time.sleep(0.5)
