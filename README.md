# Real-Time Fraud Detection Dashboard 🚀

Dự án này là hệ thống xử lý dữ liệu luồng thời gian thực (Real-Time Data Streaming) sử dụng **Apache Kafka** và **Apache Spark Structured Streaming** nhằm mục đích mô phỏng và phát hiện các hành vi gian lận (Review Bombing, Rating Inflation) trên tập dữ liệu MovieLens.
Hệ thống cũng tích hợp một giao diện trực quan hóa **Streamlit** để theo dõi luồng dữ liệu thô và các cảnh báo gian lận ngay lập tức.

## 🛠️ Công nghệ sử dụng
- **Apache Kafka & Zookeeper**: Quản lý và phân phối luồng dữ liệu liên tục.
- **Apache Spark (PySpark)**: Động cơ xử lý và phân tích dữ liệu thời gian thực.
- **Streamlit**: Xây dựng giao diện Web App (Dashboard) hiển thị biểu đồ và bảng dữ liệu.
- **Docker & Docker Compose**: Ảo hóa môi trường chạy Kafka và Zookeeper.
- **Python**: Ngôn ngữ lập trình chính.

## 🚀 Cách khởi chạy hệ thống (Cực kỳ đơn giản)
Yêu cầu duy nhất: Máy tính của bạn đã cài đặt sẵn **Docker Desktop** và **Python 3**. (Hệ thống đã tự động nhúng sẵn các file sửa lỗi của Hadoop/Spark cho Windows nên bạn không cần cài thêm môi trường Java phức tạp).

**Cách 1: Khởi động bằng 1-Click (Dành cho Windows)**
1. Mở thư mục `data-streams`.
2. Click đúp chuột vào file **`start_project.bat`**. 
*(Hệ thống sẽ tự động gọi Docker, bật cửa sổ Spark AI, và tự động mở trang web Dashboard).*

**Cách 2: Kích hoạt mô phỏng Hacker tấn công (Review Bombing)**
Hệ thống AI sẽ chỉ bắt đầu lên tiếng khi có kẻ gian lận. Bạn có thể tự mình thả Bot Hacker vào hệ thống bằng cách:
1. Mở một cửa sổ Terminal/Command Prompt mới tại thư mục `data-streams`.
2. Chạy lệnh:
```powershell
python fraud_producer.py
```
Ngay lập tức, bạn sẽ thấy cột Cảnh Báo trên trang Web Dashboard "đỏ rực" vì AI và Z-Score đã bắt quả tang các đánh giá spam!

**Cách tắt hệ thống sạch sẽ:**
Click đúp vào file **`stop_project.bat`** để dọn dẹp RAM và tắt hoàn toàn Kafka chạy ngầm.

## 📄 License & Attribution (Ghi nhận bản quyền)
Dự án này sử dụng cơ sở hạ tầng phân phối luồng dữ liệu ban đầu từ kho lưu trữ mã nguồn mở **[data-streams](https://github.com/memgraph/data-streams)**.
- **License**: MIT License (Kèm theo trong file `LICENSE`).
- Toàn bộ quyền sở hữu trí tuệ đối với các module tạo luồng dữ liệu giả lập (Kafka/Zookeeper Docker images, MovieLens dataset loaders) thuộc về các tác giả gốc của repo `data-streams`.
- Các mô-đun phát hiện gian lận (`spark_anomaly_detection.py`), giao diện người dùng (`dashboard.py`), và nhà cung cấp dữ liệu giả lập (`fraud_producer.py`) được tùy chỉnh và xây dựng riêng cho đồ án này.
