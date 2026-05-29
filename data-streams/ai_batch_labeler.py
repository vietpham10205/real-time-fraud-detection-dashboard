import os
import time
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

def add_advanced_features(pdf):
    # Ensure datetime format
    for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'lpep_pickup_datetime', 'lpep_dropoff_datetime']:
        if col in pdf.columns:
            pdf[col] = pd.to_datetime(pdf[col], errors='coerce')
    
    # Identify pickup/dropoff columns based on green/yellow
    pickup_col = 'tpep_pickup_datetime' if 'tpep_pickup_datetime' in pdf.columns else 'lpep_pickup_datetime'
    dropoff_col = 'tpep_dropoff_datetime' if 'tpep_dropoff_datetime' in pdf.columns else 'lpep_dropoff_datetime'
    
    # Time features
    pdf['trip_duration_sec'] = (pdf[dropoff_col] - pdf[pickup_col]).dt.total_seconds()
    pdf['pickup_hour'] = pdf[pickup_col].dt.hour
    
    # Cyclical hour features
    pdf['hour_sin'] = np.sin(2 * np.pi * pdf['pickup_hour'] / 24.0)
    pdf['hour_cos'] = np.cos(2 * np.pi * pdf['pickup_hour'] / 24.0)
    
    # Velocity and rates (handle division by zero)
    pdf['trip_distance'] = pdf['trip_distance'].astype(float)
    pdf['fare_amount'] = pdf['fare_amount'].astype(float)
    
    # Add a small epsilon to avoid div by zero
    eps = 1e-5
    pdf['avg_speed_kmh'] = (pdf['trip_distance'] * 1.60934) / ((pdf['trip_duration_sec'] + eps) / 3600.0)
    pdf['fare_per_mile'] = pdf['fare_amount'] / (pdf['trip_distance'] + eps)
    
    # Log scale features to handle long-tail distributions
    pdf['log_distance'] = np.log1p(np.maximum(0, pdf['trip_distance']))
    pdf['log_fare'] = np.log1p(np.maximum(0, pdf['fare_amount']))
    
    # Interactions
    pdf['speed_x_duration'] = pdf['avg_speed_kmh'] * (pdf['trip_duration_sec'] / 3600.0)
    pdf['fare_distance_ratio'] = pdf['fare_amount'] / (pdf['trip_distance'] + 0.001)
    
    # Contextual flags
    pdf['is_rush_hour'] = pdf['pickup_hour'].isin([7, 8, 9, 17, 18, 19]).astype(int)
    pdf['is_night'] = pdf['pickup_hour'].isin([0, 1, 2, 3, 4, 5]).astype(int)
    
    return pdf

def get_hard_rule_anomaly_reason(row):
    speed = row.get('avg_speed_kmh', 0)
    fare_per_mile = row.get('fare_per_mile', 0)
    duration = row.get('trip_duration_sec', 0)
    fare = row.get('fare_amount', 0)
    dist = row.get('trip_distance', 0)
    passengers = row.get('passenger_count', 0)
    is_night = row.get('is_night', 0)
    
    if pd.isna(duration) or duration < 0: return "Negative or missing time"
    if speed > 150: return f"Unrealistic Speed ({speed:.0f} km/h)"
    if fare_per_mile > 20 and dist > 0.5: return f"Extremely High Rate (${fare_per_mile:.1f}/mi)"
    if duration < 60 and fare > 30: return f"Short Trip, High Fare (${fare:.0f} in {duration:.0f}s)"
    if fare < 2.5 and dist > 1.0: return f"Suspiciously Low Fare (${fare:.2f} for {dist:.1f}mi)"
    if duration > 7200 and dist < 2: return f"Long Duration, Short Distance ({duration/3600:.1f}h / {dist:.1f}mi)"
    if pd.notna(passengers) and passengers == 0: return "Zero Passengers"
    if is_night and speed > 120: return f"Night-time Speeding ({speed:.0f} km/h)"
    if fare > 250: return f"Extreme Fare Amount (${fare:.0f})"
    if dist > 60 and fare_per_mile < 1.0: return "Very Long Trip, Suspiciously Low Rate"
    
    return None

def process_file(input_file, output_file, name):
    print(f"\n[{name}] Loading dataset: {input_file}")
    df = pd.read_parquet(input_file)
    print(f"[{name}] Loaded {len(df):,} rows.")
    
    # 1. Feature Engineering
    print(f"[{name}] Engineering features...")
    df = add_advanced_features(df)
    
    # Machine Learning Features
    ml_cols = [
        "trip_distance", "fare_amount", "trip_duration_sec", 
        "avg_speed_kmh", "fare_per_mile", "passenger_count",
        "hour_sin", "hour_cos", "speed_x_duration", 
        "log_distance", "log_fare", "is_rush_hour", 
        "is_night", "fare_distance_ratio"
    ]
    
    # Handle NaN for ML (fill with medians)
    print(f"[{name}] Imputing missing values for ML pipeline...")
    ml_data = df[ml_cols].copy()
    for col in ml_cols:
        if ml_data[col].isna().any():
            ml_data[col] = ml_data[col].fillna(ml_data[col].median())
    
    # 2. Scaling
    print(f"[{name}] Scaling features...")
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(ml_data.values)
    
    # 3. Anomaly Detection (Isolation Forest - highly parallelized)
    # 0.02 contamination = top 2% statistical outliers
    print(f"[{name}] Running Advanced AI Anomaly Detection (Isolation Forest)...")
    start_time = time.time()
    iso_forest = IsolationForest(
        n_estimators=300, 
        contamination=0.02, 
        max_samples=min(256000, len(df)),
        random_state=42,
        n_jobs=-1
    )
    iso_forest.fit(X_scaled)
    ml_preds = iso_forest.predict(X_scaled)
    ml_scores = iso_forest.decision_function(X_scaled) # Lower is more anomalous
    
    # Normalize score between 0 and 1 (1 being the most anomalous)
    normalized_scores = (ml_scores.max() - ml_scores) / (ml_scores.max() - ml_scores.min() + 1e-9)
    print(f"[{name}] ML detection completed in {time.time() - start_time:.2f} seconds.")
    
    # 4. Ensemble Voting (Rule-based OR Machine Learning)
    print(f"[{name}] Applying Ensemble Voting (Rules + AI)...")
    
    is_anomaly = []
    ai_reason = []
    
    # Vectorized hard rule checks to speed up processing
    records = df.to_dict(orient='records')
    for idx, row in enumerate(records):
        rule_reason = get_hard_rule_anomaly_reason(row)
        
        if rule_reason is not None:
            # Rule violation overrides ML
            is_anomaly.append(1)
            ai_reason.append(f"[RULE_VIOLATION] {rule_reason}")
        elif ml_preds[idx] == -1:
            # ML identified as statistical outlier
            is_anomaly.append(1)
            # Find the most extreme feature pushing this to be an outlier
            ai_reason.append("[AI_OUTLIER] Complex Statistical Anomaly")
        else:
            is_anomaly.append(0)
            ai_reason.append(None)
            
    df['is_anomaly'] = is_anomaly
    df['ai_anomaly_score'] = normalized_scores
    df['ai_reason'] = ai_reason
    
    # Print stats
    total = len(df)
    anomalies = sum(is_anomaly)
    print(f"[{name}] Total Anomalies Found: {anomalies:,} ({(anomalies/total)*100:.2f}%)")
    
    # 5. Save to new parquet file
    print(f"[{name}] Saving labeled dataset to {output_file}...")
    df.to_parquet(output_file, index=False)
    print(f"[{name}] DONE!")

if __name__ == "__main__":
    base_dir = r"c:\Users\admin\Downloads\Big Data\lab đồ án\data-streams\datasets\taxi"
    
    # Green Taxi
    green_in = os.path.join(base_dir, "green_tripdata_2026-03.parquet")
    green_out = os.path.join(base_dir, "green_tripdata_labeled.parquet")
    if os.path.exists(green_in):
        process_file(green_in, green_out, "GREEN")
    
    print("-" * 50)
    
    # Yellow Taxi
    yellow_in = os.path.join(base_dir, "yellow_tripdata_2026-03.parquet")
    yellow_out = os.path.join(base_dir, "yellow_tripdata_labeled.parquet")
    if os.path.exists(yellow_in):
        process_file(yellow_in, yellow_out, "YELLOW")
        
    print("\nAll datasets have been successfully labeled and saved.")
