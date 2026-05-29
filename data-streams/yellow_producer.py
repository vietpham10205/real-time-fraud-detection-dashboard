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
TAXI_TYPE = "yellow"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=10
    )

def stream_data():
    print(f"[{TAXI_TYPE.upper()} PRODUCER] Connecting to Kafka...")
    producer = create_producer()

    file_path = "datasets/taxi/yellow_tripdata_2026-03.parquet"
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return

    print(f"[{TAXI_TYPE.upper()} PRODUCER] Pre-loading data from {file_path}...")
    cols_needed = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                   "passenger_count", "trip_distance", "fare_amount"]
    df = pd.read_parquet(file_path, columns=cols_needed)
    df = df.rename(columns={"tpep_pickup_datetime": "pickup_datetime", "tpep_dropoff_datetime": "dropoff_datetime"})
    df['pickup_datetime'] = df['pickup_datetime'].astype(str)
    df['dropoff_datetime'] = df['dropoff_datetime'].astype(str)
    df = df.fillna(0)
    df['taxi_type'] = TAXI_TYPE

    total = len(df)
    print(f"[{TAXI_TYPE.upper()} PRODUCER] Loaded {total} records. Converting to memory-optimized list...")
    records = df.to_dict(orient='records')
    del df  # Free Pandas memory
    print(f"[{TAXI_TYPE.upper()} PRODUCER] Conversion complete! Starting INFINITE stream...")

    iteration = 1
    while True:
        print(f"\n>>> Starting Iteration #{iteration} <<<")
        count = 0
        start_time = time.time()

        for record in records:
            producer.send(KAFKA_TOPIC, record)
            count += 1

            if count % 500 == 0:
                elapsed = time.time() - start_time
                rate = count / max(elapsed, 0.001)
                print(f"[{TAXI_TYPE.upper()}] Sent {count}/{total} | Rate: {rate:.0f} msg/s")
                producer.flush()

            time.sleep(0.005)

        producer.flush()
        print(f"\n[INFO] {TAXI_TYPE.upper()} reached end of file. Restarting loop...")
        iteration += 1

if __name__ == "__main__":
    stream_data()
