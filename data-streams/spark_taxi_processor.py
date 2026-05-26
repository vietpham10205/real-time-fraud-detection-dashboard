import os
import time
import pickle
import numpy as np
from collections import deque
from pathlib import Path

# Set HADOOP_HOME for Windows compatibility (winutils.exe)
hadoop_home = os.path.abspath("hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, unix_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from sklearn.ensemble import IsolationForest as SklearnIsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.covariance import MinCovDet
import psycopg2
import psycopg2.extras
import pandas as pd

# ─── Configuration ────────────────────────────────────────────────────────────
DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC  = "taxi_stream"

MODEL_DIR   = Path(__file__).parent.resolve()
MODEL_PATH  = MODEL_DIR / "isolation_forest_model.pkl"
SCALER_PATH = MODEL_DIR / "robust_scaler.pkl"
MCD_PATH    = MODEL_DIR / "mcd_model.pkl"   # Robust Covariance (MinCovDet)

# MinCovDet hoạt động tốt hơn với số feature nhỏ (tránh curse of dimensionality).
# Dùng subset feature có correlation rõ ràng — đây là điểm mạnh của MCD.
MCD_FEATURE_COLS = [
    "trip_distance", "fare_amount", "trip_duration_sec",
    "avg_speed_kmh", "fare_per_mile", "speed_x_duration",
    "fare_distance_ratio",
]
# Mahalanobis distance percentile để xác định ngưỡng bất thường của MCD.
# 97.5 → top 2.5% outlier nhất mới bị flag — thận trọng hơn IF.
MCD_THRESHOLD_PERCENTILE = 97.5

# Only retrain the model every N batches (not every single batch)
# Tăng từ 5 → 25: giảm dao động contamination giữa các lần retrain
MODEL_RETRAIN_INTERVAL = 25
# Rolling window of raw feature rows kept for retraining
# Tăng từ 5000 → 10000: model học trên nhiều dữ liệu hơn → ổn định hơn
ROLLING_BUFFER_MAX = 10000

# Features computed inside Spark before toPandas()
# fare_per_minute bị loại: permutation importance = 0.000 → không đóng góp gì
SPARK_FEATURE_COLS = [
    "trip_distance", "fare_amount", "trip_duration_sec",
    "avg_speed_kmh", "pickup_hour", "fare_per_mile",
    "passenger_count",
]

# Full ML feature set:
# - hour_sin/cos thay pickup_hour thô (giữ tính liên tục 23→0)
# - speed_x_duration: proxy cho quãng đường thực tế (phát hiện odometer gian lận)
# - log_distance / log_fare: giảm skew của phân phối dài đuôi
# - is_rush_hour / is_night: context thời gian giúp phân biệt pattern bình thường
# - fare_distance_ratio: cách khác của fare_per_mile, nhạy hơn với chuyến ngắn
ML_FEATURE_COLS = [
    "trip_distance", "fare_amount", "trip_duration_sec",
    "avg_speed_kmh", "fare_per_mile", "passenger_count",
    "hour_sin", "hour_cos",
    "speed_x_duration", "log_distance", "log_fare",
    "is_rush_hour", "is_night", "fare_distance_ratio",
]

META_COLS = ["taxi_type", "VendorID", "pickup_datetime", "dropoff_datetime"]

# ─── Global State ─────────────────────────────────────────────────────────────
global_model   = None
global_scaler  = None
global_mcd     = None   # MinCovDet detector (Robust Covariance)
batch_counter  = 0
rolling_buffer = deque(maxlen=ROLLING_BUFFER_MAX)


def safe_int(value, fallback: int = 0) -> int:
    """Cast to int safely — returns fallback if value is NaN or None."""
    try:
        if value is None:
            return fallback
        import math
        if isinstance(value, float) and math.isnan(value):
            return fallback
        return int(value)
    except (ValueError, TypeError):
        return fallback

# ─── Database Helpers ─────────────────────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)


# ─── Kafka Readiness Guard ────────────────────────────────────────────────────────────
def wait_for_kafka(broker: str, topic: str, max_retries: int = 24, delay: int = 5):
    """Block until Kafka broker is up and the topic exists (auto-create if needed)."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError

    print(f"[KAFKA] Waiting for broker at {broker} (up to {max_retries * delay}s)...")
    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=[broker],
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
            existing_topics = admin.list_topics()
            if topic not in existing_topics:
                admin.create_topics([
                    NewTopic(name=topic, num_partitions=1, replication_factor=1)
                ])
                print(f"[KAFKA] Created topic '{topic}'")
            else:
                print(f"[KAFKA] Topic '{topic}' already exists.")
            admin.close()
            print(f"[KAFKA] Broker ready after {attempt} attempt(s).")
            return
        except KafkaError as e:
            print(f"[KAFKA] Attempt {attempt}/{max_retries}: broker not ready — {e}")
            time.sleep(delay)
        except Exception as e:
            print(f"[KAFKA] Attempt {attempt}/{max_retries}: unexpected error — {e}")
            time.sleep(delay)

    print("[KAFKA] WARNING: Could not confirm broker readiness. Proceeding anyway...")


def setup_database():
    """Create tables and apply safe migrations for new columns."""
    try:
        conn = get_db_conn()
        cur  = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS normal_trips (
                id SERIAL PRIMARY KEY,
                taxi_type         VARCHAR(50),
                VendorID          INT,
                pickup_datetime   TIMESTAMP,
                dropoff_datetime  TIMESTAMP,
                passenger_count   INT,
                trip_distance     FLOAT,
                fare_amount       FLOAT,
                trip_duration_sec INT,
                avg_speed_kmh     FLOAT,
                fare_per_mile     FLOAT,
                anomaly_score     FLOAT,
                inserted_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomalous_trips (
                id SERIAL PRIMARY KEY,
                taxi_type         VARCHAR(50),
                VendorID          INT,
                pickup_datetime   TIMESTAMP,
                dropoff_datetime  TIMESTAMP,
                passenger_count   INT,
                trip_distance     FLOAT,
                fare_amount       FLOAT,
                trip_duration_sec INT,
                avg_speed_kmh     FLOAT,
                fare_per_mile     FLOAT,
                anomaly_score     FLOAT,
                reason            VARCHAR(255),
                inserted_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id                  SERIAL PRIMARY KEY,
                batch_id            INT,
                records_processed   INT,
                anomalies_detected  INT,
                anomaly_rate        FLOAT,
                processing_time_ms  FLOAT,
                throughput_eps      FLOAT,
                estimated_cost_usd  FLOAT,
                model_retrained     BOOLEAN DEFAULT FALSE,
                inserted_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Non-destructive migrations for existing deployments
        migrations = [
            "ALTER TABLE normal_trips    ADD COLUMN IF NOT EXISTS fare_per_mile FLOAT",
            "ALTER TABLE anomalous_trips ADD COLUMN IF NOT EXISTS fare_per_mile FLOAT",
            "ALTER TABLE pipeline_metrics ADD COLUMN IF NOT EXISTS anomalies_detected INT",
            "ALTER TABLE pipeline_metrics ADD COLUMN IF NOT EXISTS anomaly_rate FLOAT",
            "ALTER TABLE pipeline_metrics ADD COLUMN IF NOT EXISTS model_retrained BOOLEAN DEFAULT FALSE",
        ]
        for stmt in migrations:
            cur.execute(stmt)

        conn.commit()
        cur.close()
        conn.close()
        print("[OK] Database schema verified.")
    except Exception as e:
        print(f"[WARN] Database setup error: {e}")


# ─── Model Persistence ────────────────────────────────────────────────────────
def save_model(model, scaler):
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)
    print(f"  [ML] Model persisted → {MODEL_PATH}")


def load_model():
    if MODEL_PATH.exists() and SCALER_PATH.exists():
        try:
            with open(MODEL_PATH, "rb") as f:
                model = pickle.load(f)
            with open(SCALER_PATH, "rb") as f:
                scaler = pickle.load(f)
            
            # Validate feature count to prevent shape mismatches
            expected_features = len(ML_FEATURE_COLS)
            if hasattr(scaler, "n_features_in_") and scaler.n_features_in_ != expected_features:
                print(f"[ML] Stale persisted model found ({scaler.n_features_in_} features, expected {expected_features}).")
                print("     Automatically resetting model/scaler files to retrain with the new feature schema...")
                MODEL_PATH.unlink(missing_ok=True)
                SCALER_PATH.unlink(missing_ok=True)
                return None, None
            
            print("[ML] Loaded persisted model from disk.")
            return model, scaler
        except Exception as e:
            print(f"[ML] Error loading model: {e}. Resetting files...")
            MODEL_PATH.unlink(missing_ok=True)
            SCALER_PATH.unlink(missing_ok=True)
            return None, None
    return None, None


# ─── Feature Engineering ──────────────────────────────────────────────────────
def engineer_spark_features(df):
    """Compute all derivable features while still in Spark (pre-pandas)."""
    df = (
        df.withColumn("pickup_ts",        unix_timestamp(to_timestamp("pickup_datetime")))
          .withColumn("dropoff_ts",       unix_timestamp(to_timestamp("dropoff_datetime")))
          .withColumn("trip_duration_sec", col("dropoff_ts") - col("pickup_ts"))
          .withColumn("pickup_hour",      hour(to_timestamp("pickup_datetime")))
    )
    # Remove physically impossible records before deriving ratio features
    df = df.filter(
        (col("trip_duration_sec") > 60) &
        (col("trip_distance")     > 0.1) &
        (col("fare_amount")       > 0)   &
        (col("passenger_count")   >= 1)  &
        (col("passenger_count")   <= 9)
    )
    df = df.withColumn("avg_speed_kmh", col("trip_distance") * 1.60934 / (col("trip_duration_sec") / 3600.0))
    df = df.withColumn("fare_per_mile", col("fare_amount") / col("trip_distance"))
    # fare_per_minute bị xoá: importance = 0.000 trong permutation test
    return df


def add_pandas_features(pdf):
    """
    Thêm các feature tính được trong pandas sau khi toPandas().
    Bao gồm: cyclical hour, log transforms, context flags, interaction terms.
    """
    # Cyclical encoding — giữ tính liên tục 23→0 (thay pickup_hour thô)
    pdf["hour_sin"] = np.sin(2 * np.pi * pdf["pickup_hour"] / 24.0)
    pdf["hour_cos"] = np.cos(2 * np.pi * pdf["pickup_hour"] / 24.0)

    # Log transforms — giảm skew của phân phối dài đuôi
    pdf["log_distance"] = np.log1p(pdf["trip_distance"])
    pdf["log_fare"]     = np.log1p(pdf["fare_amount"])

    # Interaction term — proxy quãng đường thực (phát hiện odometer gian lận)
    pdf["speed_x_duration"] = pdf["avg_speed_kmh"] * pdf["trip_duration_sec"] / 3600.0

    # Ratio feature — nhạy hơn fare_per_mile với chuyến ngắn
    pdf["fare_distance_ratio"] = pdf["fare_amount"] / (pdf["trip_distance"] + 0.001)

    # Context time flags
    pdf["is_rush_hour"] = pdf["pickup_hour"].isin([7, 8, 9, 17, 18, 19]).astype(int)
    pdf["is_night"]     = pdf["pickup_hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    return pdf


# ─── Anomaly Reason Labeling ──────────────────────────────────────────────────
def assign_reason(row) -> str:
    speed         = float(row["avg_speed_kmh"])
    fare_per_mile = float(row["fare_per_mile"])
    duration      = float(row["trip_duration_sec"])
    fare          = float(row["fare_amount"])
    dist          = float(row["trip_distance"])
    passengers    = safe_int(row["passenger_count"])
    score         = float(row["anomaly_score"])
    is_night      = int(row["is_night"])

    severity = "[HIGH]" if score > 0.15 else "[MEDIUM]"

    # Hard rule violations — ordered từ nghiêm trọng nhất
    if speed > 140:
        return f"{severity} Unrealistic Speed (>{speed:.0f} km/h)"
    if fare_per_mile > 15:
        return f"{severity} Extremely High Fare per Mile (${fare_per_mile:.1f}/mi)"
    if duration < 120 and fare > 50:
        return f"{severity} Short Trip, High Fare (${fare:.0f} in {duration/60:.0f}min)"
    if fare < 2.5 and dist > 0.5:
        return f"{severity} Suspiciously Low Fare (${fare:.2f} for {dist:.1f}mi)"
    if duration > 7200 and dist < 5:
        return f"{severity} Long Duration, Short Distance ({duration/3600:.1f}h / {dist:.1f}mi)"
    if passengers == 0:
        return f"{severity} Zero Passengers Reported"
    # Thêm mới: các pattern không có trong version cũ
    if is_night and speed > 100:
        return f"{severity} Night-time Speeding ({speed:.0f} km/h)"
    if fare > 150:
        return f"{severity} Extreme Fare Amount (${fare:.0f})"
    if dist > 50 and fare_per_mile < 1.0:
        return f"{severity} Very Long Trip, Suspiciously Low Rate"
    return f"{severity} Statistical Outlier (IsolationForest)"


# ─── Model Training ───────────────────────────────────────────────────────────
def train_model(X_raw, contamination: float = 0.03):
    """
    Ensemble: IsolationForest + MinCovDet (Robust Covariance).

    Tại sao kết hợp 2 model:
    - IsolationForest  : giỏi phát hiện point outlier (1 feature lạ đột ngột)
    - MinCovDet (MCD)  : giỏi phát hiện multivariate outlier (tổ hợp nhiều
                         feature lạ CÙNG LÚC, dù từng feature riêng lẻ bình thường)
    - Ensemble voting  : chỉ flag khi CẢ 2 đồng ý → giảm false positive đáng kể
    """
    scaler   = RobustScaler()
    X_scaled = scaler.fit_transform(X_raw)

    # ── Isolation Forest ──────────────────────────────────────────────────────
    # Option 3: Supervised sample weighting using business rules to increase overlap
    weights = np.ones(len(X_raw))
    
    dist_idx     = ML_FEATURE_COLS.index("trip_distance")
    fare_idx     = ML_FEATURE_COLS.index("fare_amount")
    duration_idx = ML_FEATURE_COLS.index("trip_duration_sec")
    speed_idx    = ML_FEATURE_COLS.index("avg_speed_kmh")
    rate_idx     = ML_FEATURE_COLS.index("fare_per_mile")
    pass_idx     = ML_FEATURE_COLS.index("passenger_count")
    night_idx    = ML_FEATURE_COLS.index("is_night")

    for idx, row in enumerate(X_raw):
        dist       = float(row[dist_idx])
        fare       = float(row[fare_idx])
        duration   = float(row[duration_idx])
        speed      = float(row[speed_idx])
        rate       = float(row[rate_idx])
        passengers = int(row[pass_idx])
        is_night   = int(row[night_idx])
        
        violates = (
            speed > 140 or
            rate > 15 or
            (duration < 120 and fare > 50) or
            (fare < 2.5 and dist > 0.5) or
            (duration > 7200 and dist < 5) or
            passengers == 0 or
            (is_night and speed > 100) or
            fare > 150 or
            (dist > 50 and rate < 1.0)
        )
        if violates:
            weights[idx] = 3.0  

    iforest = SklearnIsolationForest(
        n_estimators=200,
        contamination=contamination,
        max_samples="auto",
        max_features=1.0,
        bootstrap=False,
        random_state=42,
        n_jobs=-1,
    )
    iforest.fit(X_scaled, sample_weight=weights)

    # ── MinCovDet (Robust Covariance) ─────────────────────────────────────────
    # MCD chỉ dùng MCD_FEATURE_COLS (subset) — tránh curse of dimensionality
    # với 14 features. support_fraction=0.85 → bỏ qua 15% outlier nhất khi
    # ước tính covariance matrix, làm cho estimate robust hơn.
    mcd_feature_idx = [ML_FEATURE_COLS.index(f) for f in MCD_FEATURE_COLS]
    X_mcd = X_scaled[:, mcd_feature_idx]

    mcd = MinCovDet(support_fraction=0.85, random_state=42)
    try:
        mcd.fit(X_mcd)
        # Tính ngưỡng Mahalanobis distance từ training data
        mah_dist_train  = mcd.mahalanobis(X_mcd)
        mcd.threshold_  = float(np.percentile(mah_dist_train, MCD_THRESHOLD_PERCENTILE))
        print(f"  [MCD] Fitted | Mahalanobis threshold={mcd.threshold_:.2f}")
    except Exception as e:
        print(f"  [MCD] Warning: could not fit MCD ({e}). Ensemble will use IF only.")
        mcd = None

    return iforest, scaler, mcd


def save_model(model, scaler, mcd=None):
    with open(MODEL_PATH,  "wb") as f: pickle.dump(model,  f)
    with open(SCALER_PATH, "wb") as f: pickle.dump(scaler, f)
    if mcd is not None:
        with open(MCD_PATH, "wb") as f: pickle.dump(mcd, f)
    print(f"  [ML] Models persisted → {MODEL_PATH}, {MCD_PATH}")


def load_model():
    if MODEL_PATH.exists() and SCALER_PATH.exists():
        try:
            with open(MODEL_PATH,  "rb") as f: model  = pickle.load(f)
            with open(SCALER_PATH, "rb") as f: scaler = pickle.load(f)
            mcd = None
            if MCD_PATH.exists():
                with open(MCD_PATH, "rb") as f: mcd = pickle.load(f)

            # Validate feature count
            expected = len(ML_FEATURE_COLS)
            if hasattr(scaler, "n_features_in_") and scaler.n_features_in_ != expected:
                print(f"[ML] Stale model ({scaler.n_features_in_} features, expected {expected}). Resetting...")
                for p in [MODEL_PATH, SCALER_PATH, MCD_PATH]:
                    p.unlink(missing_ok=True)
                return None, None, None

            print("[ML] Loaded persisted ensemble (IF + MCD) from disk.")
            return model, scaler, mcd
        except Exception as e:
            print(f"[ML] Error loading model: {e}. Resetting...")
            for p in [MODEL_PATH, SCALER_PATH, MCD_PATH]:
                p.unlink(missing_ok=True)
            return None, None, None
    return None, None, None


# ─── Adaptive Contamination ───────────────────────────────────────────────────
FIXED_CONTAMINATION_PRIOR = 0.03  # Anchor cố định, không thay đổi theo batch

def compute_adaptive_contamination(
    prev_model, prev_scaler, X_buffer, prior: float = FIXED_CONTAMINATION_PRIOR
) -> float:
    """
    Blend prior cố định (80%) với tỉ lệ anomaly quan sát gần đây (20%).
    
    - 80% prior: giữ model ổn định, tránh dao động mạnh giữa các batch
    - 20% observed: vẫn phản ứng nhẹ với thay đổi thực sự trong dữ liệu
    - Trần 0.04 (thay vì 0.10): ngăn model "bắt" quá nhiều false positive
    - Sàn 0.01 (thay vì 0.005): đảm bảo luôn có đủ anomaly để học
    """
    if prev_model is None:
        return prior

    X_scaled     = prev_scaler.transform(X_buffer)
    recent_rate  = float((prev_model.predict(X_scaled) == -1).mean())

    blended = 0.8 * prior + 0.2 * recent_rate
    return float(np.clip(blended, 0.01, 0.04))


# ─── Batch Processor ──────────────────────────────────────────────────────────
def process_batch(batch_df, batch_id):
    global global_model, global_scaler, global_mcd, batch_counter, rolling_buffer

    start_time = time.time()
    count = batch_df.count()

    if count < 10:
        print(f"[Batch {batch_id}] Skipped — only {count} records (need >= 10)")
        return

    print(f"\n{'='*60}")
    print(f"  Processing Batch {batch_id} | {count} records")
    print(f"{'='*60}")

    # ── 1. Feature Engineering ────────────────────────────────────────────────
    df       = engineer_spark_features(batch_df)
    df_clean = df.dropna(subset=SPARK_FEATURE_COLS)

    clean_count = df_clean.count()
    if clean_count < 10:
        print(f"[Batch {batch_id}] Skipped after cleaning — only {clean_count} valid records")
        return

    select_cols = list(set(SPARK_FEATURE_COLS + META_COLS + ["pickup_hour"]))
    pdf         = df_clean.select(*select_cols).toPandas()
    pdf         = add_pandas_features(pdf)

    X = pdf[ML_FEATURE_COLS].values

    # ── 2. Model Initialisation & Warm Load ──────────────────────────────────
    if global_model is None:
        global_model, global_scaler, global_mcd = load_model()

    # Accumulate this batch into the rolling buffer for future retraining
    for row in X:
        rolling_buffer.append(row)

    # ── 3. Periodic Retraining ────────────────────────────────────────────────
    model_retrained = False
    # Train if no model exists yet, OR every N batches (skip batch 0 for modulo
    # to avoid double-training when model is also None on the very first batch)
    first_time     = global_model is None
    periodic_train = (not first_time) and (batch_counter % MODEL_RETRAIN_INTERVAL == 0) and (batch_counter > 0)
    should_retrain = first_time or periodic_train

    if should_retrain and len(rolling_buffer) >= 50:
        X_buf         = np.array(rolling_buffer)
        contamination = compute_adaptive_contamination(global_model, global_scaler, X_buf)
        print(f"  [ML] Retraining ensemble on {len(X_buf)} buffered records (contamination={contamination:.4f})...")
        global_model, global_scaler, global_mcd = train_model(X_buf, contamination)
        save_model(global_model, global_scaler, global_mcd)
        model_retrained = True

    # ── Guard: if still no model (buffer < 50 on first batch), skip scoring ──
    if global_model is None or global_scaler is None:
        print(f"  [ML] Skipping batch {batch_id} — model not ready yet (buffer={len(rolling_buffer)} rows, need 50)")
        batch_counter += 1
        return

    # ── 4. Score Current Batch — Ensemble (IF + MCD) ─────────────────────────
    X_scaled    = global_scaler.transform(X)
    if_preds    = global_model.predict(X_scaled)        # 1=normal, -1=anomaly
    if_scores   = global_model.decision_function(X_scaled)  # lower = more anomalous

    # ── MCD voting ───────────────────────────────────────────────────────────
    mcd_preds = np.ones(len(X_scaled), dtype=int)  # Default: all normal
    mah_distances = np.zeros(len(X_scaled))

    if global_mcd is not None:
        try:
            mcd_feature_idx  = [ML_FEATURE_COLS.index(f) for f in MCD_FEATURE_COLS]
            X_mcd            = X_scaled[:, mcd_feature_idx]
            mah_distances    = global_mcd.mahalanobis(X_mcd)
            threshold        = getattr(global_mcd, "threshold_", np.percentile(mah_distances, MCD_THRESHOLD_PERCENTILE))
            mcd_preds        = np.where(mah_distances > threshold, -1, 1)
            mcd_anomaly_rate = (mcd_preds == -1).mean()
            print(f"  [MCD] Flagged {(mcd_preds==-1).sum()} ({mcd_anomaly_rate:.1%}) via Mahalanobis distance")
        except Exception as e:
            print(f"  [MCD] Scoring failed ({e}). Using IF only.")

    # ── Ensemble decision: BOTH models must agree to flag as anomaly ──────────
    # Voting modes:
    #   "strict"  → anomaly only if IF AND MCD both flag  (lowest false positive)
    #   "union"   → anomaly if IF OR  MCD flags            (highest recall)
    #   "if_only" → fallback to IF alone (when MCD unavailable)
    ENSEMBLE_MODE = "strict"

    if global_mcd is not None and ENSEMBLE_MODE == "strict":
        ensemble_preds = np.where((if_preds == -1) & (mcd_preds == -1), -1, 1)
        mode_label = "IF ∩ MCD (strict)"
    elif global_mcd is not None and ENSEMBLE_MODE == "union":
        ensemble_preds = np.where((if_preds == -1) | (mcd_preds == -1), -1, 1)
        mode_label = "IF ∪ MCD (union)"
    else:
        ensemble_preds = if_preds
        mode_label = "IF only"

    # Combined anomaly score: average of normalized IF score + normalized Mahalanobis
    if_scores_norm  = (-if_scores - (-if_scores).min()) / (np.ptp(-if_scores) + 1e-9)
    mah_norm        = (mah_distances - mah_distances.min()) / (np.ptp(mah_distances) + 1e-9)
    combined_scores = 0.6 * if_scores_norm + 0.4 * mah_norm  # IF weighted slightly higher

    pdf["prediction"]    = ensemble_preds
    pdf["anomaly_score"] = combined_scores
    pdf["if_score"]      = -if_scores      # Raw IF score (for debugging)
    pdf["mah_distance"]  = mah_distances   # Raw Mahalanobis (for debugging)

    anomalies_pdf = pdf[pdf["prediction"] == -1].copy()
    normals_pdf   = pdf[pdf["prediction"] ==  1].copy()

    num_anomalies = len(anomalies_pdf)
    anomaly_rate  = num_anomalies / max(clean_count, 1)

    if_only_count = (if_preds == -1).sum()
    print(f"  [ENSEMBLE:{mode_label}] IF flagged: {if_only_count} | Final anomalies: {num_anomalies} ({anomaly_rate:.1%}) | Normal: {len(normals_pdf)}")

    if not anomalies_pdf.empty:
        anomalies_pdf["reason"] = anomalies_pdf.apply(assign_reason, axis=1)
    else:
        anomalies_pdf["reason"] = pd.Series(dtype=str)

    # ── 5. Batch Write to PostgreSQL ──────────────────────────────────────────
    try:
        conn = get_db_conn()
        cur  = conn.cursor()

        # Batch insert anomalies
        if not anomalies_pdf.empty:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO anomalous_trips
                  (taxi_type, VendorID, pickup_datetime, dropoff_datetime,
                   passenger_count, trip_distance, fare_amount,
                   trip_duration_sec, avg_speed_kmh, fare_per_mile,
                   anomaly_score, reason)
                VALUES %s
            """, [
                (
                    str(r["taxi_type"]), safe_int(r["VendorID"]),
                    str(r["pickup_datetime"]), str(r["dropoff_datetime"]),
                    safe_int(r["passenger_count"], 1), float(r["trip_distance"]),
                    float(r["fare_amount"]), safe_int(r["trip_duration_sec"]),
                    float(r["avg_speed_kmh"]), float(r["fare_per_mile"]),
                    float(r["anomaly_score"]), r["reason"],
                )
                for _, r in anomalies_pdf.iterrows()
            ])

        # Batch insert normal sample (capped at 50)
        normal_sample = normals_pdf.head(50)
        if not normal_sample.empty:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO normal_trips
                  (taxi_type, VendorID, pickup_datetime, dropoff_datetime,
                   passenger_count, trip_distance, fare_amount,
                   trip_duration_sec, avg_speed_kmh, fare_per_mile,
                   anomaly_score)
                VALUES %s
            """, [
                (
                    str(r["taxi_type"]), safe_int(r["VendorID"]),
                    str(r["pickup_datetime"]), str(r["dropoff_datetime"]),
                    safe_int(r["passenger_count"], 1), float(r["trip_distance"]),
                    float(r["fare_amount"]), safe_int(r["trip_duration_sec"]),
                    float(r["avg_speed_kmh"]), float(r["fare_per_mile"]),
                    float(r["anomaly_score"]),
                )
                for _, r in normal_sample.iterrows()
            ])

        end_time           = time.time()
        processing_time_ms = (end_time - start_time) * 1000
        throughput_eps     = count / max(end_time - start_time, 0.001)
        estimated_cost_usd = processing_time_ms * 0.000016

        cur.execute("""
            INSERT INTO pipeline_metrics
              (batch_id, records_processed, anomalies_detected, anomaly_rate,
               processing_time_ms, throughput_eps, estimated_cost_usd, model_retrained)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (batch_id, count, num_anomalies, anomaly_rate,
               processing_time_ms, throughput_eps, estimated_cost_usd, model_retrained))

        conn.commit()
        cur.close()
        conn.close()
        print(f"  [PERF] Latency: {processing_time_ms:.0f}ms | Throughput: {throughput_eps:.1f} rows/s | Cost: ${estimated_cost_usd:.4f}")

    except Exception as e:
        print(f"  [ERROR] Failed to write to PostgreSQL: {e}")

    batch_counter += 1


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main():
    wait_for_kafka(KAFKA_BROKER, KAFKA_TOPIC)   # Block until broker + topic are ready
    setup_database()

    spark = SparkSession.builder \
        .appName("RealTimeFleetIntelligence") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("  Real-time Fleet Intelligence Pipeline (Dual Source)")
    print("=" * 60 + "\n")

    schema = StructType([
        StructField("taxi_type",        StringType(), True),
        StructField("VendorID",         LongType(),   True),
        StructField("pickup_datetime",  StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count",  DoubleType(), True),
        StructField("trip_distance",    DoubleType(), True),
        StructField("fare_amount",      DoubleType(), True),
    ])

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # Tolerate missing topic/partitions at startup (topic created by producers later)
        .option("failOnDataLoss", "false")
        # Extra retry tolerance while topic is being created
        .option("kafka.consumer.auto.offset.reset", "earliest")
        .option("kafka.max.poll.records", "500")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    print("[STREAMING] Pipeline is running. Waiting for data from Kafka...")
    print("[STREAMING] Run `python yellow_producer.py` and `python green_producer.py` in parallel.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()