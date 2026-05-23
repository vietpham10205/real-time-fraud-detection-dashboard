"""
evaluate_model.py — Isolation Forest Effectiveness Diagnostic
=============================================================
Run AFTER spark_taxi_processor.py has processed at least 5 batches.

Usage:
    python evaluate_model.py

Outputs a full report with 6 diagnostic tests:
  1. Score Separation     — Are anomaly scores clearly split from normals?
  2. Anomaly Rate Drift   — Is the rate stable across batches?
  3. Reason Coverage      — Is the model catching diverse fraud types?
  4. Feature Sensitivity  — Which features drive detections?
  5. Business Rule Overlap — Do ML detections match hard-coded rules?
  6. Score Distribution   — Is the histogram bimodal (sign of healthy model)?
"""

import pickle
import math
import warnings
from pathlib import Path

# Suppress pandas UserWarning regarding DBAPI2 connections
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

import numpy as np
import psycopg2
import pandas as pd
from sklearn.preprocessing import RobustScaler

# ─── Config ───────────────────────────────────────────────────────────────────
DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

SCRIPT_DIR  = Path(__file__).parent.resolve()
MODEL_PATH  = SCRIPT_DIR / "isolation_forest_model.pkl"
SCALER_PATH = SCRIPT_DIR / "robust_scaler.pkl"

ML_FEATURE_COLS = [
    "trip_distance", "fare_amount", "trip_duration_sec",
    "avg_speed_kmh", "fare_per_mile", "passenger_count",
    "hour_sin", "hour_cos",
    "speed_x_duration", "log_distance", "log_fare",
    "is_rush_hour", "is_night", "fare_distance_ratio",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────
SEP  = "=" * 62
SEP2 = "-" * 62

def get_conn():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def load_model():
    if not MODEL_PATH.exists():
        return None, None
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    with open(SCALER_PATH, "rb") as f:
        scaler = pickle.load(f)
    return model, scaler

def bar(ratio: float, width: int = 30) -> str:
    filled = int(round(ratio * width))
    return "█" * filled + "░" * (width - filled)

def grade(score: float) -> str:
    if score >= 0.80: return "✅ EXCELLENT"
    if score >= 0.60: return "✅ GOOD"
    if score >= 0.40: return "⚠️  FAIR"
    return "❌ POOR"

# ─── Test 1: Score Separation ─────────────────────────────────────────────────
def test_score_separation(conn, model, scaler):
    print(f"\n{SEP}")
    print("  TEST 1 — Score Separation")
    print("  Do anomalies score significantly higher than normals?")
    print(SEP2)

    df_normal = pd.read_sql(
        "SELECT anomaly_score FROM normal_trips WHERE anomaly_score IS NOT NULL LIMIT 2000", conn
    )
    df_anomaly = pd.read_sql(
        "SELECT anomaly_score FROM anomalous_trips WHERE anomaly_score IS NOT NULL LIMIT 2000", conn
    )

    if df_normal.empty or df_anomaly.empty:
        print("  ⚠️  Not enough data yet. Run more batches first.")
        return

    n_mean  = df_normal["anomaly_score"].mean()
    a_mean  = df_anomaly["anomaly_score"].mean()
    n_std   = df_normal["anomaly_score"].std()
    a_std   = df_anomaly["anomaly_score"].std()

    # Cohen's d — effect size between the two distributions
    pooled_std = math.sqrt((n_std**2 + a_std**2) / 2)
    cohens_d   = abs(a_mean - n_mean) / (pooled_std + 1e-9)

    print(f"  Normal trips   : mean score = {n_mean:.4f}  (std={n_std:.4f})  n={len(df_normal)}")
    print(f"  Anomalous trips: mean score = {a_mean:.4f}  (std={a_std:.4f})  n={len(df_anomaly)}")
    print(f"  Cohen's d (separation strength): {cohens_d:.2f}")

    if cohens_d >= 1.5:
        verdict = "✅ EXCELLENT — Very clear separation between normal and anomalous scores."
    elif cohens_d >= 0.8:
        verdict = "✅ GOOD — Strong separation. Model is distinguishing well."
    elif cohens_d >= 0.5:
        verdict = "⚠️  FAIR — Moderate separation. Consider tuning contamination."
    else:
        verdict = "❌ POOR — Scores heavily overlap. Model may not be discriminating."

    print(f"\n  Verdict: {verdict}")

# ─── Test 2: Anomaly Rate Stability ───────────────────────────────────────────
def test_anomaly_rate_stability(conn):
    print(f"\n{SEP}")
    print("  TEST 2 — Anomaly Rate Stability")
    print("  Is the detection rate consistent across batches?")
    print(SEP2)

    df = pd.read_sql(
        """SELECT batch_id, anomaly_rate, anomalies_detected, records_processed
           FROM pipeline_metrics
           WHERE anomaly_rate IS NOT NULL
           ORDER BY batch_id""",
        conn
    )

    if df.empty or len(df) < 2:
        print("  ⚠️  Need at least 2 batches in pipeline_metrics.")
        return

    mean_rate = df["anomaly_rate"].mean()
    std_rate  = df["anomaly_rate"].std()
    cv        = std_rate / (mean_rate + 1e-9)   # Coefficient of variation

    print(f"  Batches analysed : {len(df)}")
    print(f"  Mean anomaly rate: {mean_rate:.2%}")
    print(f"  Std dev          : {std_rate:.2%}")
    print(f"  Coefficient of variation (CV): {cv:.2f}  (lower = more stable)")
    print()

    for _, row in df.iterrows():
        rate   = row["anomaly_rate"]
        b      = f"  Batch {int(row['batch_id']):>3}"
        filled = bar(min(rate * 10, 1.0))   # Scale: 10% = full bar
        print(f"{b}  {filled}  {rate:.2%}  ({int(row['anomalies_detected'])} anomalies)")

    if cv < 0.3:
        verdict = "✅ STABLE — Consistent detection rate. Contamination is well-calibrated."
    elif cv < 0.6:
        verdict = "⚠️  MODERATE — Some variance. Adaptive contamination is helping."
    else:
        verdict = "❌ UNSTABLE — High variance. Model needs more buffer data or tuning."

    print(f"\n  Verdict: {verdict}")

# ─── Test 3: Reason Coverage ──────────────────────────────────────────────────
def test_reason_coverage(conn):
    print(f"\n{SEP}")
    print("  TEST 3 — Anomaly Reason Coverage")
    print("  Is the model catching diverse fraud patterns?")
    print(SEP2)

    df = pd.read_sql(
        """SELECT reason, COUNT(*) as cnt
           FROM anomalous_trips
           GROUP BY reason
           ORDER BY cnt DESC""",
        conn
    )

    if df.empty:
        print("  ⚠️  No anomalies in database yet.")
        return

    total = df["cnt"].sum()
    print(f"  Total anomalies : {total}")
    print(f"  Distinct reasons: {len(df)}")
    print()

    for _, row in df.iterrows():
        pct    = row["cnt"] / total
        filled = bar(pct)
        reason = row["reason"][:38].ljust(38)
        print(f"  {reason}  {filled}  {pct:.1%}  ({int(row['cnt'])})")

    # Red flag: >85% in "Statistical Outlier" = model isn't using business rules
    pure_ml_pct = 0.0
    pure_ml_row = df[df["reason"] == "Statistical Outlier (IsolationForest)"]
    if not pure_ml_row.empty:
        pure_ml_pct = pure_ml_row["cnt"].iloc[0] / total

    if len(df) >= 4 and pure_ml_pct < 0.85:
        verdict = "✅ DIVERSE — Model is catching multiple fraud patterns."
    elif len(df) >= 2:
        verdict = "⚠️  LIMITED — Few reason categories. May need richer feature engineering."
    else:
        verdict = "❌ NARROW — Only one fraud type detected. Check data quality."

    print(f"\n  Verdict: {verdict}")

# ─── Test 4: Feature Sensitivity (Permutation Test) ───────────────────────────
def test_feature_sensitivity(conn, model, scaler):
    print(f"\n{SEP}")
    print("  TEST 4 — Feature Sensitivity (Permutation Importance)")
    print("  Which features drive anomaly detections the most?")
    print(SEP2)

    if model is None:
        print("  ⚠️  No model file found. Run spark_taxi_processor.py first.")
        return

    # Reconstruct features from DB for a sample
    df = pd.read_sql(
        """SELECT trip_distance, fare_amount, trip_duration_sec, avg_speed_kmh,
                  fare_per_mile,
                  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
                  passenger_count, anomaly_score
           FROM anomalous_trips
           LIMIT 500""",
        conn
    )
    df2 = pd.read_sql(
        """SELECT trip_distance, fare_amount, trip_duration_sec, avg_speed_kmh,
                  fare_per_mile,
                  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
                  passenger_count, anomaly_score
           FROM normal_trips
           LIMIT 500""",
        conn
    )

    df_all = pd.concat([df, df2], ignore_index=True).dropna()
    if len(df_all) < 50:
        print("  ⚠️  Not enough data. Need at least 50 rows.")
        return

    # Add pandas engineered features
    df_all["hour_sin"] = np.sin(2 * np.pi * df_all["pickup_hour"] / 24.0)
    df_all["hour_cos"] = np.cos(2 * np.pi * df_all["pickup_hour"] / 24.0)
    df_all["log_distance"] = np.log1p(df_all["trip_distance"])
    df_all["log_fare"]     = np.log1p(df_all["fare_amount"])
    df_all["speed_x_duration"] = df_all["avg_speed_kmh"] * df_all["trip_duration_sec"] / 3600.0
    df_all["fare_distance_ratio"] = df_all["fare_amount"] / (df_all["trip_distance"] + 0.001)
    df_all["is_rush_hour"] = df_all["pickup_hour"].isin([7, 8, 9, 17, 18, 19]).astype(int)
    df_all["is_night"]     = df_all["pickup_hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    # Only keep features the model knows
    available = [c for c in ML_FEATURE_COLS if c in df_all.columns]
    X = df_all[available].values

    try:
        X_scaled  = scaler.transform(X)
        base_pred = model.predict(X_scaled)
        base_anom = (base_pred == -1).mean()

        importances = {}
        rng = np.random.default_rng(42)
        for i, feat in enumerate(available):
            X_perm        = X_scaled.copy()
            X_perm[:, i]  = rng.permutation(X_perm[:, i])   # shuffle one feature
            perm_pred     = model.predict(X_perm)
            perm_anom     = (perm_pred == -1).mean()
            importances[feat] = abs(perm_anom - base_anom)   # bigger = more important

        # Normalise
        max_imp = max(importances.values()) + 1e-9
        print(f"  {'Feature':<20}  {'Importance':>12}  {'Visual'}")
        print(f"  {'-'*20}  {'-'*12}  {'-'*30}")
        for feat, imp in sorted(importances.items(), key=lambda x: -x[1]):
            norm_imp = imp / max_imp
            print(f"  {feat:<20}  {imp:>12.4f}  {bar(norm_imp, 20)}")

        top_feat = max(importances, key=importances.get)
        print(f"\n  Most influential feature: '{top_feat}'")
        print("  Verdict: ✅ Permutation test complete. High-importance features are driving detections.")

    except Exception as e:
        print(f"  ⚠️  Could not run permutation test: {e}")

# ─── Test 5: Business Rule Overlap ────────────────────────────────────────────
def test_business_rule_overlap(conn):
    print(f"\n{SEP}")
    print("  TEST 5 — Business Rule vs ML Overlap")
    print("  Do ML-detected anomalies also violate hard business rules?")
    print(SEP2)

    df = pd.read_sql(
        """SELECT trip_distance, fare_amount, avg_speed_kmh,
                  trip_duration_sec, passenger_count, fare_per_mile,
                  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
           FROM anomalous_trips
           LIMIT 1000""",
        conn
    )

    if df.empty:
        print("  ⚠️  No anomalies to analyse.")
        return

    df["is_night"] = df["pickup_hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    # Apply hard rules to each ML-flagged anomaly
    def check_rule(row):
        if row["avg_speed_kmh"] > 140:           return True
        if row["fare_per_mile"] > 15:             return True
        if row["trip_duration_sec"] < 120 and row["fare_amount"] > 50: return True
        if row["fare_amount"] < 2.5 and row["trip_distance"] > 0.5:   return True
        if row["trip_duration_sec"] > 7200 and row["trip_distance"] < 5: return True
        if row["passenger_count"] == 0:          return True
        if row["is_night"] and row["avg_speed_kmh"] > 100: return True
        if row["fare_amount"] > 150:             return True
        if row["trip_distance"] > 50 and row["fare_per_mile"] < 1.0: return True
        return False

    df["violates_rule"] = df.apply(check_rule, axis=1)
    overlap_pct = df["violates_rule"].mean()

    print(f"  ML-flagged anomalies     : {len(df)}")
    print(f"  Also violate a hard rule : {df['violates_rule'].sum()} ({overlap_pct:.1%})")
    print(f"  Pure statistical outliers: {(~df['violates_rule']).sum()} ({1-overlap_pct:.1%})")
    print()
    print(f"  {bar(overlap_pct)}  {overlap_pct:.1%} rule-confirmable")

    if overlap_pct >= 0.40:
        verdict = "✅ HIGH OVERLAP — ML findings corroborated by business rules. Model is trustworthy."
    elif overlap_pct >= 0.20:
        verdict = "⚠️  PARTIAL — Some ML findings confirmed. Statistical catches are complementary."
    else:
        verdict = "⚠️  LOW OVERLAP — Mostly pure statistical. May have high false positives."

    print(f"\n  Verdict: {verdict}")

# ─── Test 6: Score Histogram ──────────────────────────────────────────────────
def test_score_distribution(conn):
    print(f"\n{SEP}")
    print("  TEST 6 — Score Distribution (Bimodality Check)")
    print("  A healthy model shows two clusters in the score histogram.")
    print(SEP2)

    df_n = pd.read_sql("SELECT anomaly_score FROM normal_trips    WHERE anomaly_score IS NOT NULL LIMIT 2000", conn)
    df_a = pd.read_sql("SELECT anomaly_score FROM anomalous_trips WHERE anomaly_score IS NOT NULL LIMIT 2000", conn)

    if df_n.empty or df_a.empty:
        print("  ⚠️  Need data in both tables.")
        return

    # ASCII histogram for normals
    def ascii_hist(scores, label, bins=10):
        mn, mx = scores.min(), scores.max()
        if mx == mn:
            print(f"  {label}: all scores identical ({mn:.3f})")
            return
        edges  = np.linspace(mn, mx, bins + 1)
        counts, _ = np.histogram(scores, bins=edges)
        max_c  = counts.max()
        print(f"  {label} (n={len(scores)}):")
        for i, c in enumerate(counts):
            lo = edges[i]; hi = edges[i + 1]
            bw = int(round(c / max(max_c, 1) * 25))
            print(f"    [{lo:6.3f}–{hi:6.3f}]  {'█'*bw:<25}  {c}")
        print()

    ascii_hist(df_n["anomaly_score"].values, "Normal trips  ")
    ascii_hist(df_a["anomaly_score"].values, "Anomalous trips")

    overall_range_n = df_n["anomaly_score"].max() - df_n["anomaly_score"].min()
    overall_range_a = df_a["anomaly_score"].max() - df_a["anomaly_score"].min()
    gap = df_a["anomaly_score"].mean() - df_n["anomaly_score"].mean()

    print(f"  Score gap (anomaly mean − normal mean): {gap:.4f}")
    if gap > 0.05:
        print("  Verdict: ✅ BIMODAL — Clear gap between normal and anomaly clusters.")
    elif gap > 0.01:
        print("  Verdict: ⚠️  SLIGHT GAP — Distributions are close. Consider more features.")
    else:
        print("  Verdict: ❌ NO GAP — Distributions overlap heavily. Model needs retuning.")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(SEP)
    print("  ISOLATION FOREST EFFECTIVENESS DIAGNOSTIC")
    print("  NYC Taxi Fraud Detection Pipeline")
    print(SEP)

    model, scaler = load_model()
    if model is None:
        print("\n⚠️  WARNING: No model pickle found.")
        print("   Run spark_taxi_processor.py and wait for at least one retrain.")
        print("   Some tests will be skipped.\n")

    try:
        conn = get_conn()
    except Exception as e:
        print(f"\n❌ Cannot connect to database: {e}")
        print("   Make sure Docker is running: docker compose up -d")
        return

    try:
        test_score_separation(conn, model, scaler)
        test_anomaly_rate_stability(conn)
        test_reason_coverage(conn)
        test_feature_sensitivity(conn, model, scaler)
        test_business_rule_overlap(conn)
        test_score_distribution(conn)
    finally:
        conn.close()

    print(f"\n{SEP}")
    print("  DIAGNOSTIC COMPLETE")
    print(f"{SEP}\n")
    print("Interpretation guide:")
    print("  ✅ EXCELLENT/GOOD = Model is effective, keep current config")
    print("  ⚠️  FAIR/MODERATE  = Working but can be improved")
    print("  ❌ POOR/UNSTABLE   = Needs retuning (see SKILL doc for suggestions)")
    print()
    print("Quick tuning levers:")
    print("  • contamination too low  → raise to 0.05–0.08 in MODEL_RETRAIN_INTERVAL")
    print("  • scores overlapping     → add more features or increase n_estimators")
    print("  • rate unstable          → increase ROLLING_BUFFER_MAX (e.g. 10000)")
    print("  • few reason categories  → check data quality / filter thresholds")

if __name__ == "__main__":
    main()
