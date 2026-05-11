import os
import sys
import time
import json
import pandas as pd
from kafka import KafkaProducer

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "taxi_stream"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=10
    )

def stream_data():
    print(f"[PRODUCER] Connecting to Kafka broker at {KAFKA_BROKER}...")
    producer = create_producer()
    print(f"[PRODUCER] Connected! Streaming to topic: {KAFKA_TOPIC}")
    
    file_path = "datasets/taxi/yellow_tripdata_2026-03.parquet"
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return

    print(f"[PRODUCER] Reading {file_path}...")
    # Only read the columns we need to save memory
    cols_needed = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
                   "passenger_count", "trip_distance", "fare_amount"]
    df = pd.read_parquet(file_path, columns=cols_needed)
    
    # Preprocess datetimes to strings for JSON serialization
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(str)
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(str)
    df = df.fillna(0)

    total = len(df)
    print(f"[PRODUCER] Loaded {total} records. Starting stream NOW...")
    
    count = 0
    start_time = time.time()
    
    for _, row in df.iterrows():
        record = row.to_dict()
        producer.send(KAFKA_TOPIC, record)
        count += 1
        
        if count % 500 == 0:
            elapsed = time.time() - start_time
            rate = count / max(elapsed, 0.001)
            print(f"[PRODUCER] Sent {count}/{total} records | {rate:.0f} msgs/sec | Elapsed: {elapsed:.1f}s")
            producer.flush()
            
        time.sleep(0.005)  # ~200 msgs/sec

    producer.flush()
    total_time = time.time() - start_time
    print(f"\n[PRODUCER] DONE! Sent all {count} records in {total_time:.1f}s")

if __name__ == "__main__":
    stream_data()
