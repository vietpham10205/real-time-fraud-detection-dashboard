# 🚕 NYC Taxi: Real-time Fleet Intelligence Pipeline

Đồ án môn học: Big Data & Cơ sở dữ liệu phân tán.
Dự án xây dựng một hệ thống (pipeline) phân tích dữ liệu xe Taxi NYC theo thời gian thực (Real-time Stream Processing), ứng dụng Machine Learning (Isolation Forest) để phát hiện các chuyến đi bất thường (thu phí sai luật, tốc độ phi lý...) và lưu trữ trên cơ sở dữ liệu phân tán.

## 🌟 Kiến trúc Hệ thống (Chuẩn 11 Bước)
Hệ thống tuân thủ nghiêm ngặt mô hình kiến trúc Big Data đã đề xuất:
1. **Nguồn dữ liệu**: File `Parquet` chứa hàng triệu chuyến đi (Yellow Tripdata).
2. **Đọc**: Script Python tải dữ liệu từ Parquet vào bộ nhớ.
3. **Producer (Gửi message)**: Python Kafka Producer đẩy dữ liệu dạng JSON.
4. **Topic / Partition**: Dữ liệu đi vào Kafka Topic `taxi_stream`.
5. **Kafka Cluster**: Đảm nhiệm vai trò Message Broker (Gồm Zookeeper và Kafka Broker).
6. **Consume**: Ứng dụng Spark Structured Streaming đọc dữ liệu liên tục.
7. **Apache Spark**: Lõi xử lý tính toán phân tán (Distributed Computation).
8. **Transform (Lọc/Tính toán/ML)**: 
   - Lọc các chuyến đi lỗi, tính toán vận tốc, khoảng thời gian.
   - **Huấn luyện liên tục (Continuous Learning)** model `Isolation Forest` (thông qua SynapseML) ngay trên luồng dữ liệu để phát hiện bất thường.
9. **Ghi (Sink)**: Xuất dữ liệu qua cơ chế `foreachBatch` và `writeStream`.
10. **Data Lake (Sink 1)**: Lưu trữ toàn bộ dữ liệu thô xuống đĩa dưới định dạng `Parquet` (`data_lake/taxi_raw`).
11. **Database (Sink 2)**: Ghi kết quả tổng hợp vào PostgreSQL (Mô phỏng Cơ sở dữ liệu phân tán phục vụ Dashboard).
12. **Kafka Sink (Sink 3)**: Đẩy riêng các dữ liệu bị đánh dấu "Bất thường" (Anomalies) ngược lại vào Kafka topic `taxi_anomalies`.

---

## 🛠 Yêu cầu Hệ thống (Prerequisites)
- **Hệ điều hành**: Windows 10/11, macOS, Linux
- **Phần mềm**:
  - [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Đã cài đặt và đang bật)
  - [Python 3.9+](https://www.python.org/downloads/)
  - [Java 8 hoặc 11](https://www.oracle.com/java/technologies/javase-downloads.html) (Bắt buộc để chạy lõi Apache Spark)

---

## 🚀 Hướng dẫn Cài đặt & Chạy dự án (Chỉ 1 Click)

0. **⚠️ QUAN TRỌNG: Khởi động Docker Desktop trước tiên**
   - Bạn **BẮT BUỘC** phải mở ứng dụng **Docker Desktop** trên máy tính lên trước.
   - Chờ cho đến khi Docker khởi động hoàn tất (hiện dòng chữ *Engine Running*) thì mới thực hiện các bước tiếp theo.

1. **Clone dự án về máy:**
   ```bash
   git clone <link-repo-cua-ban>
   cd "lab đồ án/data-streams"
   ```

2. **Khởi động toàn bộ hệ thống:**
   Đơn giản chỉ cần click đúp vào file `start_project.bat` hoặc chạy trên Terminal:
   ```cmd
   start_project.bat
   ```

3. **Chuyện gì sẽ xảy ra?**
   - Script tự động cài đặt tất cả các thư viện Python cần thiết (`pyspark`, `kafka-python`, `synapseml`, `streamlit`, `psycopg2`,...).
   - Tự động bật Docker (chạy Kafka, Zookeeper, PostgreSQL).
   - Tự động mở 3 cửa sổ Terminal mới song song:
     - 🧮 **Spark Processor**: Khởi động luồng xử lý dữ liệu và Machine Learning.
     - 🚕 **Kafka Producer**: Bắt đầu bơm dữ liệu Taxi vào hệ thống.
     - 📊 **Web Dashboard**: Khởi chạy giao diện giám sát thời gian thực.

4. **Truy cập Dashboard:**
   Mở trình duyệt và vào địa chỉ: [http://localhost:8501](http://localhost:8501)

---

## 🛑 Cách Dừng Hệ thống
Để dọn dẹp sạch sẽ Docker và tắt các tiến trình Python đang chạy ngầm một cách an toàn, hãy chạy file:
```cmd
stop_project.bat
```

## 🧠 Điểm nhấn Kỹ thuật (Showcase cho Báo cáo)
- **Big Data Streaming**: Xử lý luồng dữ liệu thời gian thực (Micro-batching) thay vì Batch Processing truyền thống, ứng dụng trực tiếp Apache Spark Structured Streaming.
- **Continuous Machine Learning**: Mô hình AI (Isolation Forest) không đứng im mà liên tục học lại (Retrain) trên những cụm dữ liệu (batches) mới nhất thông qua `foreachBatch` của Spark, đáp ứng sự thay đổi liên tục của giao thông đô thị.
- **Multiple Sinks Architecture**: Xử lý 1 nguồn dữ liệu từ Kafka nhưng xuất ra 3 đích đến (Sinks) đa dạng về công nghệ: 
  - `Parquet` cho Data Lake.
  - `PostgreSQL` (Relational/Distributed DB) cho truy vấn Dashboard.
  - `Kafka Topic` (Message Queue Sink) cho các hệ thống cảnh báo (Alerting) tiếp theo.
