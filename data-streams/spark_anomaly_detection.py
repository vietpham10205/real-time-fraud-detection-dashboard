import os
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr, from_unixtime, window, avg, count, hour, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType
from sklearn.ensemble import IsolationForest

# Cấu hình môi trường cho Spark kết nối Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

# =========================================================================
# PHẦN 1: HUẤN LUYỆN MÔ HÌNH MACHINE LEARNING (OFFLINE)
# =========================================================================
print("🧠 Đang huấn luyện mô hình Isolation Forest (Offline Phase)...")
np.random.seed(42)
# Giả lập dữ liệu hành vi bình thường: Điểm trung bình 3.5 - 4.5, hay đánh giá vào ban ngày (8h-23h)
normal_ratings = np.random.normal(3.8, 0.8, 2000).clip(0.5, 5.0)
normal_hours = np.random.randint(8, 24, 2000)
training_data = pd.DataFrame({'rating': normal_ratings, 'hour': normal_hours})

# Khởi tạo và huấn luyện mô hình (contamination = 0.15 nghĩa là giả định có 15% là gian lận)
iso_forest = IsolationForest(contamination=0.15, random_state=42)
iso_forest.fit(training_data[['rating', 'hour']])
print("✅ Hoàn tất huấn luyện Machine Learning!")

# Hàm UDF (User Defined Function) để nhúng ML vào Spark Streaming
@pandas_udf(StringType())
def detect_anomaly_ml(rating_series: pd.Series, hour_series: pd.Series) -> pd.Series:
    df = pd.DataFrame({'rating': rating_series, 'hour': hour_series})
    predictions = iso_forest.predict(df)
    # Isolation Forest trả về 1 (Bình thường), -1 (Bất thường)
    result = [
        "🤖 CẢNH BÁO ML: Phát hiện hành vi đa chiều bất thường (Isolation Forest)" if p == -1 else "Bình thường" 
        for p in predictions
    ]
    return pd.Series(result)

def main():
    # =========================================================================
    # BƯỚC 1: KHỞI TẠO SPARK SESSION
    # =========================================================================
    spark = SparkSession.builder \
        .appName("HybridFraudDetection") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # =========================================================================
    # BƯỚC 2: KẾT NỐI KAFKA
    # =========================================================================
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
    KAFKA_TOPIC = "ratings"

    print("📥 Đang lắng nghe luồng dữ liệu từ Kafka...")
    # Sửa startingOffsets thành earliest để quét lại toàn bộ lịch sử giúp dễ demo
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";') \
        .load()

    # =========================================================================
    # BƯỚC 3: XỬ LÝ VÀ CHUYỂN ĐỔI DỮ LIỆU
    # =========================================================================
    movie_schema = StructType([
        StructField("movieId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genres", ArrayType(StringType()), True)
    ])
    
    schema = StructType([
        StructField("userId", StringType(), True),
        StructField("movie", movie_schema, True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        
    processed_df = parsed_df \
        .withColumn("rating_val", col("rating").cast("float")) \
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp")))) \
        .withColumn("hour_of_day", hour(col("event_time"))) \
        .select("userId", "movie.title", "rating_val", "event_time", "hour_of_day")

    # =========================================================================
    # BƯỚC 4: KIẾN TRÚC LAI (HYBRID ARCHITECTURE) - PHÁT HIỆN GIAN LẬN
    # =========================================================================

    # TẦNG 1: TRÍ TUỆ NHÂN TẠO (ISOLATION FOREST ML)
    # Bắt gian lận dựa trên hành vi đa chiều (Điểm số kết hợp với Giờ đánh giá)
    ml_anomalies_df = processed_df \
        .withColumn("anomaly_type", detect_anomaly_ml(col("rating_val"), col("hour_of_day"))) \
        .filter(col("anomaly_type") != "Bình thường") \
        .selectExpr("userId", "title", "rating_val", "event_time", "anomaly_type")

    # TẦNG 2: ĐỘT BIẾN THỜI GIAN (STATISTICAL Z-SCORE / SPIKE DETECTION)
    # Nhóm dữ liệu theo cửa sổ trượt 1 giờ để tìm những Người dùng (User) spam đánh giá liên tục
    windowed_anomalies_df = processed_df \
        .withWatermark("event_time", "1 hour") \
        .groupBy(
            window(col("event_time"), "1 hour", "5 minutes"), 
            col("userId").alias("spam_user")
        ) \
        .agg(
            count("*").alias("total_reviews"),
            avg("rating_val").alias("avg_rating")
        ) \
        .filter(
            # Tiêu chí: Một user đánh giá > 5 phim trong vòng 1 giờ VÀ điểm trung bình < 2.5 hoặc > 4.5
            (col("total_reviews") >= 5) & ((col("avg_rating") <= 2.5) | (col("avg_rating") >= 4.5))
        ) \
        .selectExpr(
            "spam_user as userId",
            "'Nhiều phim' as title",
            "avg_rating as rating_val",
            "window.end as event_time",
            "concat('🔥 CẢNH BÁO ĐỘT BIẾN: Tài khoản này spam ', total_reviews, ' đánh giá trung bình ', round(avg_rating, 1), ' sao!') as anomaly_type"
        )

    # =========================================================================
    # BƯỚC 5: ĐẨY KẾT QUẢ VỀ DASHBOARD (KAFKA SINK)
    # =========================================================================
    
    # 5.1 Push Tầng 1 (ML) - Chế độ Append
    query_ml = ml_anomalies_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "movie_anomalies") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";') \
        .option("checkpointLocation", "./spark_checkpoints/kafka_sink_ml") \
        .outputMode("append") \
        .start()

    # 5.2 Push Tầng 2 (Spike) - Chế độ Update (Cập nhật liên tục khi cửa sổ thay đổi)
    query_window = windowed_anomalies_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "movie_anomalies") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";') \
        .option("checkpointLocation", "./spark_checkpoints/kafka_sink_window") \
        .outputMode("update") \
        .start()

    # 5.3 In ra console để dễ debug
    query_console = ml_anomalies_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    print("🕵️  Kiến trúc Lai (Isolation Forest ML + Spike Detection) đang chạy... (Nhấn Ctrl+C để dừng)")
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
