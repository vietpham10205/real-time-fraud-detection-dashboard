import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# Cấu hình môi trường cho Spark kết nối Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def main():
    # =========================================================================
    # BƯỚC 1: KHỞI TẠO SPARK SESSION (Động cơ phân tích)
    # =========================================================================
    spark = SparkSession.builder \
        .appName("MovieRatingAnomalyDetection") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 [Khởi động] Spark Session tạo thành công.")

    # =========================================================================
    # BƯỚC 2: KẾT NỐI DATA SOURCE (Đọc từ Kafka - Ingestion)
    # =========================================================================
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
    KAFKA_TOPIC = "ratings" # Topic mặc định của bộ dữ liệu MovieLens trong thư mục repo

    print(f"📥 Đang lắng nghe luồng dữ liệu từ Kafka Topic: '{KAFKA_TOPIC}'...")
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 500) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";') \
        .load()

    # =========================================================================
    # BƯỚC 3: XỬ LÝ VÀ CHUYỂN ĐỔI DỮ LIỆU (Data Transformation)
    # =========================================================================
    # Định nghĩa cấu trúc (Schema) của MovieLens JSON
    movie_schema = StructType([
        StructField("movieId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genres", ArrayType(StringType()), True)
    ])
    
    schema = StructType([
        StructField("userId", StringType(), True),
        StructField("movie", movie_schema, True),
        StructField("rating", StringType(), True), # Để dạng String rồi ép kiểu sau
        StructField("timestamp", StringType(), True)
    ])

    # Bóc tách JSON từ chuỗi bytes của Kafka
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        
    # Ép kiểu dữ liệu (Cast): Đổi rating sang dạng số (Float) và timestamp sang kiểu thời gian
    processed_df = parsed_df \
        .withColumn("rating_val", col("rating").cast("float")) \
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp")))) \
        .select("userId", "movie.title", "rating_val", "event_time")

    # =========================================================================
    # BƯỚC 4: PHÂN TÍCH THỜI GIAN THỰC (Real-time Analysis) - PHÁT HIỆN BẤT THƯỜNG
    # =========================================================================
    # Thuật toán phát hiện Review Bombing (Cố tình hạ thấp) và Rating Inflation (Nâng khống)
    anomalies_df = processed_df.filter(
        (col("rating_val") <= 1.0) | (col("rating_val") >= 5.0)
    ).withColumn(
        "anomaly_type", 
        expr("""
            CASE 
                WHEN rating_val <= 1.0 THEN 'CẢNH BÁO: Cố tình hạ điểm (Review Bombing)'
                WHEN rating_val >= 5.0 THEN 'CẢNH BÁO: Nâng khống điểm ảo (Rating Inflation)'
            END
        """)
    )

    # =========================================================================
    # BƯỚC 5: DATA SINK (Đẩy kết quả ra Console & Kafka cho Web App)
    # =========================================================================
    # 5.1 Ghi ra màn hình Console (để dễ theo dõi lúc code)
    query_console = anomalies_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", "./spark_checkpoints/console_sink") \
        .start()

    # 5.2 Ghi vào Kafka Topic "movie_anomalies" để Web Dashboard đọc
    query_kafka = anomalies_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "movie_anomalies") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";') \
        .option("checkpointLocation", "./spark_checkpoints/kafka_sink") \
        .start()

    print("🕵️  Hệ thống đang theo dõi và đẩy cảnh báo lên Web Dashboard... (Nhấn Ctrl+C để dừng)")
    
    # Giữ luồng để Spark chạy liên tục
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
