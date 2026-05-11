import os
import time
import numpy as np

# Set HADOOP_HOME for Windows compatibility (winutils.exe)
hadoop_home = os.path.abspath("hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, unix_timestamp, when, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from sklearn.ensemble import IsolationForest as SklearnIsolationForest
import psycopg2

# Database configuration
DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

# Kafka configuration
KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "taxi_stream"

def setup_database():
    """Create PostgreSQL tables if they don't exist."""
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS normal_trips (
                id SERIAL PRIMARY KEY,
                VendorID INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance FLOAT,
                fare_amount FLOAT,
                trip_duration_sec INT,
                avg_speed_kmh FLOAT,
                anomaly_score FLOAT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomalous_trips (
                id SERIAL PRIMARY KEY,
                VendorID INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance FLOAT,
                fare_amount FLOAT,
                trip_duration_sec INT,
                avg_speed_kmh FLOAT,
                anomaly_score FLOAT,
                reason VARCHAR(255),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id SERIAL PRIMARY KEY,
                batch_id INT,
                records_processed INT,
                processing_time_ms FLOAT,
                throughput_eps FLOAT,
                estimated_cost_usd FLOAT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("[OK] Database schema verified.")
    except Exception as e:
        print(f"[WARN] Database setup error (will keep trying later): {e}")

# Global model for continuous learning
global_model = None

def process_batch(batch_df, batch_id):
    """Process each micro-batch: Feature Engineering -> Isolation Forest -> PostgreSQL."""
    global global_model
    start_time = time.time()
    
    count = batch_df.count()
    if count < 10:
        print(f"[Batch {batch_id}] Skipped — only {count} records (need >= 10)")
        return
        
    print(f"\n{'='*60}")
    print(f"  Processing Batch {batch_id} | {count} records")
    print(f"{'='*60}")
    
    # ===== STEP 1: Feature Engineering =====
    df = batch_df \
        .withColumn("pickup_ts", unix_timestamp(to_timestamp("tpep_pickup_datetime"))) \
        .withColumn("dropoff_ts", unix_timestamp(to_timestamp("tpep_dropoff_datetime"))) \
        .withColumn("trip_duration_sec", col("dropoff_ts") - col("pickup_ts")) \
        .withColumn("pickup_hour", hour(to_timestamp("tpep_pickup_datetime")))
                 
    df = df.filter((col("trip_duration_sec") > 0) & (col("trip_distance") > 0))
    df = df.withColumn("avg_speed_kmh", col("trip_distance") * 1.60934 / (col("trip_duration_sec") / 3600.0))
    
    feature_cols = ["trip_distance", "fare_amount", "trip_duration_sec", "avg_speed_kmh", "pickup_hour"]
    df_clean = df.dropna(subset=feature_cols)
    
    clean_count = df_clean.count()
    if clean_count < 10:
        print(f"[Batch {batch_id}] Skipped after cleaning — only {clean_count} valid records")
        return
    
    # ===== STEP 2: Continuous Training — Isolation Forest (sklearn) =====
    print(f"  [ML] Training Isolation Forest on {clean_count} records...")
    
    # Collect to driver for sklearn (for real Big Data, use distributed ML)
    select_cols = list(set(feature_cols + ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count"]))
    pandas_df = df_clean.select(*select_cols).toPandas()
    
    X = pandas_df[feature_cols].values
    
    # Continuous retraining: fit a new model on every batch
    global_model = SklearnIsolationForest(
        n_estimators=100,
        contamination=0.03,  # 3% anomalies
        random_state=42
    )
    global_model.fit(X)
    
    # Predict: -1 = anomaly, 1 = normal
    predictions = global_model.predict(X)
    scores = global_model.decision_function(X)
    
    pandas_df["prediction"] = predictions
    pandas_df["anomaly_score"] = -scores  # Invert so higher = more anomalous
    
    anomalies_pdf = pandas_df[pandas_df["prediction"] == -1]
    normals_pdf = pandas_df[pandas_df["prediction"] == 1]
    
    num_anomalies = len(anomalies_pdf)
    num_normals = len(normals_pdf)
    
    print(f"  [ML] Results: {num_anomalies} anomalies | {num_normals} normal trips")
    
    # ===== STEP 3: Write to PostgreSQL =====
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        # Write anomalies
        for _, row in anomalies_pdf.iterrows():
            dist = float(row["trip_distance"])
            fare = float(row["fare_amount"])
            speed = float(row["avg_speed_kmh"])
            duration = float(row["trip_duration_sec"])
            
            if dist > 0 and (fare / dist) > 10:
                reason = "High Fare/Distance ratio"
            elif speed > 140:
                reason = "Unrealistic Speed"
            elif duration < 60 and fare > 50:
                reason = "Short trip, high fare"
            else:
                reason = "Statistical Outlier (IsolationForest)"
            
            cur.execute("""
                INSERT INTO anomalous_trips (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount, trip_duration_sec, avg_speed_kmh, anomaly_score, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (int(row["VendorID"]), str(row["tpep_pickup_datetime"]), str(row["tpep_dropoff_datetime"]),
                  int(row["passenger_count"]), dist, fare,
                  int(duration), speed, float(row["anomaly_score"]), reason))
        
        # Write a sample of normal trips
        normal_sample = normals_pdf.head(50)
        for _, row in normal_sample.iterrows():
            cur.execute("""
                INSERT INTO normal_trips (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount, trip_duration_sec, avg_speed_kmh, anomaly_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (int(row["VendorID"]), str(row["tpep_pickup_datetime"]), str(row["tpep_dropoff_datetime"]),
                  int(row["passenger_count"]), float(row["trip_distance"]), float(row["fare_amount"]),
                  int(row["trip_duration_sec"]), float(row["avg_speed_kmh"]), float(row["anomaly_score"])))
        
        # Write pipeline system metrics
        end_time = time.time()
        processing_time_ms = (end_time - start_time) * 1000
        throughput_eps = count / max(end_time - start_time, 0.001)
        estimated_cost_usd = processing_time_ms * 0.000016
        
        cur.execute("""
            INSERT INTO pipeline_metrics (batch_id, records_processed, processing_time_ms, throughput_eps, estimated_cost_usd)
            VALUES (%s, %s, %s, %s, %s)
        """, (batch_id, count, processing_time_ms, throughput_eps, estimated_cost_usd))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"  [DB] Written to PostgreSQL: {num_anomalies} anomalies + {len(normal_sample)} normals")
        print(f"  [PERF] Latency: {processing_time_ms:.0f}ms | Throughput: {throughput_eps:.1f} rows/s | Cost: ${estimated_cost_usd:.5f}")
    except Exception as e:
        print(f"  [ERROR] Failed to write to PostgreSQL: {e}")

def main():
    setup_database()
    
    spark = SparkSession.builder \
        .appName("RealTimeFleetIntelligence") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n" + "="*60)
    print("  Real-time Fleet Intelligence Pipeline")
    print("  Spark Version:", spark.version)
    print("  Kafka Broker:", KAFKA_BROKER)
    print("  Kafka Topic:", KAFKA_TOPIC)
    print("="*60 + "\n")
    
    schema = StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
    ])

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
        
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Main processing stream: foreachBatch -> ML + PostgreSQL
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    print("[STREAMING] Pipeline is running. Waiting for data from Kafka...")
    print("[STREAMING] Make sure taxi_producer.py is running to send data.\n")
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
