# Real-Time Fraud Detection Dashboard 🚀

Dự án này là hệ thống xử lý dữ liệu luồng thời gian thực (Real-Time Data Streaming) sử dụng **Apache Kafka** và **Apache Spark Structured Streaming** nhằm mục đích mô phỏng và phát hiện các hành vi gian lận (Review Bombing, Rating Inflation) trên tập dữ liệu MovieLens.
Hệ thống cũng tích hợp một giao diện trực quan hóa **Streamlit** để theo dõi luồng dữ liệu thô và các cảnh báo gian lận ngay lập tức.

## 🛠️ Công nghệ sử dụng
- **Apache Kafka & Zookeeper**: Quản lý và phân phối luồng dữ liệu liên tục.
- **Apache Spark (PySpark)**: Động cơ xử lý và phân tích dữ liệu thời gian thực.
- **Streamlit**: Xây dựng giao diện Web App (Dashboard) hiển thị biểu đồ và bảng dữ liệu.
- **Docker & Docker Compose**: Ảo hóa môi trường chạy Kafka và Zookeeper.
- **Python**: Ngôn ngữ lập trình chính.

## 🚀 Cách khởi chạy hệ thống
Yêu cầu hệ thống: Đã cài đặt Docker Desktop, Python 3.x, và Java (JDK 8/11 cho Spark).

**1. Khởi động Kafka & Sinh dữ liệu (Terminal 1)**
```powershell
docker-compose up -d
```

**2. Khởi động Lõi phân tích Spark (Terminal 2)**
```powershell
python spark_anomaly_detection.py
```

**3. Bật Giao diện Dashboard (Terminal 3)**
```powershell
streamlit run dashboard.py
```

## 📄 License & Attribution (Ghi nhận bản quyền)
Dự án này sử dụng cơ sở hạ tầng phân phối luồng dữ liệu ban đầu từ kho lưu trữ mã nguồn mở **[data-streams](https://github.com/memgraph/data-streams)**.
- **License**: MIT License (Kèm theo trong file `LICENSE`).
- Toàn bộ quyền sở hữu trí tuệ đối với các module tạo luồng dữ liệu giả lập (Kafka/Zookeeper Docker images, MovieLens dataset loaders) thuộc về các tác giả gốc của repo `data-streams`.
- Các mô-đun phát hiện gian lận (`spark_anomaly_detection.py`), giao diện người dùng (`dashboard.py`), và nhà cung cấp dữ liệu giả lập (`fraud_producer.py`) được tùy chỉnh và xây dựng riêng cho đồ án này.
