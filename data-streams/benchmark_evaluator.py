"""
benchmark_evaluator.py
======================
So sánh hiệu suất phát hiện bất thường của hệ thống Spark Streaming
với bộ nhãn chuẩn (Ground Truth) do AI tạo ra.

Matching key: (pickup_datetime rounded to minute) + trip_distance + fare_amount
Run: streamlit run benchmark_evaluator.py --server.port 8502
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import numpy as np
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────
DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

BASE_DIR = Path(__file__).parent / "datasets" / "taxi"
LABELED_FILES = {
    "yellow": BASE_DIR / "yellow_tripdata_labeled.parquet",
    "green":  BASE_DIR / "green_tripdata_labeled.parquet",
}

st.set_page_config(
    page_title="Benchmark Evaluator — Streaming vs Ground Truth",
    page_icon=None,
    layout="wide",
)

# ── Design System (shared with dashboard) ──────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
}

.stApp { background-color: #08090d; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2rem 2.5rem 3rem; max-width: 100%; }

/* ── Page Header ─────────────────────────────────────────────────────────── */
.page-header {
    padding: 28px 0 20px;
    border-bottom: 1px solid #1c2030;
    margin-bottom: 28px;
}
.page-header h1 {
    font-size: 22px;
    font-weight: 600;
    color: #e8ecf5;
    letter-spacing: -0.3px;
    margin: 0 0 4px 0;
}
.page-header p {
    font-size: 13px;
    color: #505870;
    margin: 0;
}
.refresh-badge {
    display: inline-block;
    background: #0d1a2e;
    color: #38bdf8;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    padding: 3px 10px;
    border-radius: 20px;
    border: 1px solid #1a3050;
    margin-left: 12px;
    vertical-align: middle;
}

/* ── Section Labels ─────────────────────────────────────────────────────── */
.section-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #3d4560;
    margin: 32px 0 14px 0;
}
.section-label.taxi-yellow { color: #6b5a1e; }
.section-label.taxi-green  { color: #1a4a2a; }

.divider { border: none; border-top: 1px solid #141824; margin: 28px 0; }

/* ── KPI Cards ──────────────────────────────────────────────────────────── */
.kpi-card {
    background: #0d1017;
    border: 1px solid #181e2c;
    border-radius: 10px;
    padding: 16px 18px 14px;
    position: relative;
    overflow: hidden;
    margin-bottom: 4px;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}
.kpi-blue::before   { background: linear-gradient(90deg, #1d4ed8, #38bdf8); }
.kpi-green::before  { background: linear-gradient(90deg, #059669, #34d974); }
.kpi-amber::before  { background: linear-gradient(90deg, #b45309, #fbbf24); }
.kpi-red::before    { background: linear-gradient(90deg, #b91c1c, #f87171); }
.kpi-slate::before  { background: linear-gradient(90deg, #475569, #94a3b8); }

.kpi-label {
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: #3d4560;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 28px;
    font-weight: 700;
    color: #d8dff0;
    line-height: 1;
    letter-spacing: -0.5px;
}
.kpi-sub { font-size: 11px; color: #2e3448; margin-top: 5px; }

/* ── Performance Badge ──────────────────────────────────────────────────── */
.badge {
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 2px 8px;
    border-radius: 4px;
    margin-left: 8px;
    vertical-align: middle;
}
.badge-good  { background: #0f2a1a; color: #34d974; border: 1px solid #1a4a2a; }
.badge-warn  { background: #1e1600; color: #fbbf24; border: 1px solid #3a2a00; }
.badge-poor  { background: #1a0a0a; color: #f87171; border: 1px solid #3a1a1a; }

/* ── Info Box ───────────────────────────────────────────────────────────── */
.info-box {
    background: #0a0c12;
    border: 1px solid #181e2c;
    border-radius: 8px;
    padding: 16px 20px;
    color: #3d4560;
    font-size: 12px;
    line-height: 1.8;
    margin-top: 8px;
}
.info-box strong { color: #505870; }

/* ── Alert ──────────────────────────────────────────────────────────────── */
.alert {
    background: #0f0b00;
    border: 1px solid #2a1f00;
    border-left: 3px solid #d97706;
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 12px;
    color: #926b20;
    margin-bottom: 12px;
}
</style>
""", unsafe_allow_html=True)


# ── Data Helpers ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def load_ground_truth() -> dict:
    result = {}
    for taxi_type, path in LABELED_FILES.items():
        if not path.exists():
            continue
        pickup_col = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
        df = pd.read_parquet(path, columns=[
            "trip_distance", "fare_amount", "is_anomaly",
            "ai_anomaly_score", "ai_reason", pickup_col,
        ])
        df = df.rename(columns={pickup_col: "pickup_datetime"})
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        df["_key"] = (
            df["pickup_datetime"].dt.floor("min").astype(str)
            + "|" + df["trip_distance"].round(2).astype(str)
            + "|" + df["fare_amount"].round(2).astype(str)
        )
        df = df.drop_duplicates(subset=["_key"])
        result[taxi_type] = df.set_index("_key")
    return result


def get_db_conn():
    try:
        return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    except Exception:
        return None


@st.cache_data(ttl=30)
def load_streaming_results() -> pd.DataFrame:
    conn = get_db_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        anom = pd.read_sql(
            "SELECT taxi_type, pickup_datetime, trip_distance, fare_amount, "
            "anomaly_score, reason, inserted_at, 1 AS y_pred FROM anomalous_trips", conn
        )
        norm = pd.read_sql(
            "SELECT taxi_type, pickup_datetime, trip_distance, fare_amount, "
            "anomaly_score, NULL AS reason, inserted_at, 0 AS y_pred FROM normal_trips", conn
        )
        df = pd.concat([anom, norm], ignore_index=True)
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        df["_key"] = (
            df["pickup_datetime"].dt.floor("min").astype(str)
            + "|" + df["trip_distance"].round(2).astype(str)
            + "|" + df["fare_amount"].round(2).astype(str)
        )
        return df
    except Exception as e:
        return pd.DataFrame()
    finally:
        conn.close()


def evaluate(streaming_df: pd.DataFrame, ground_truth: dict) -> dict:
    results = {}
    for taxi_type, gt in ground_truth.items():
        sub = streaming_df[streaming_df["taxi_type"] == taxi_type].copy()
        if sub.empty:
            continue
        merged = sub.join(gt[["is_anomaly", "ai_anomaly_score", "ai_reason"]], on="_key", how="inner")
        if len(merged) < 5:
            results[taxi_type] = {"matched": 0}
            continue

        y_true = merged["is_anomaly"].astype(int)
        y_pred = merged["y_pred"].astype(int)

        tp = int(((y_pred == 1) & (y_true == 1)).sum())
        tn = int(((y_pred == 0) & (y_true == 0)).sum())
        fp = int(((y_pred == 1) & (y_true == 0)).sum())
        fn = int(((y_pred == 0) & (y_true == 1)).sum())
        total = tp + tn + fp + fn

        precision   = tp / (tp + fp)   if (tp + fp) > 0   else 0.0
        recall      = tp / (tp + fn)   if (tp + fn) > 0   else 0.0
        f1          = 2*precision*recall / (precision+recall) if (precision+recall) > 0 else 0.0
        accuracy    = (tp + tn) / total if total > 0        else 0.0
        specificity = tn / (tn + fp)   if (tn + fp) > 0   else 0.0

        results[taxi_type] = {
            "matched": len(merged), "total": len(sub),
            "tp": tp, "tn": tn, "fp": fp, "fn": fn,
            "precision": precision, "recall": recall,
            "f1": f1, "accuracy": accuracy, "specificity": specificity,
            "merged": merged,
        }
    return results


# ── Chart helpers ──────────────────────────────────────────────────────────────
BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#404660", size=11, family="Inter"),
    margin=dict(l=4, r=4, t=36, b=4),
    height=260,
    title_font=dict(size=11, color="#404660"),
)


def chart_confusion(tp, tn, fp, fn, title):
    z    = [[tn, fp], [fn, tp]]
    text = [[f"TN  {tn:,}", f"FP  {fp:,}"], [f"FN  {fn:,}", f"TP  {tp:,}"]]
    fig = go.Figure(go.Heatmap(
        z=z,
        x=["Predicted Normal", "Predicted Anomaly"],
        y=["Actual Normal", "Actual Anomaly"],
        text=text, texttemplate="%{text}",
        colorscale=[[0, "#0a1520"], [0.5, "#0f2a3a"], [1, "#0e4d6b"]],
        showscale=False,
    ))
    fig.update_layout(**BASE_LAYOUT, title=dict(text=title, font=dict(size=11, color="#404660")))
    return fig


def chart_score_dist(merged, taxi_type):
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=merged[merged["is_anomaly"] == 0]["anomaly_score"],
        name="Normal (Ground Truth)", nbinsx=40,
        marker_color="#1e4a2e", opacity=0.8,
    ))
    fig.add_trace(go.Histogram(
        x=merged[merged["is_anomaly"] == 1]["anomaly_score"],
        name="Anomaly (Ground Truth)", nbinsx=40,
        marker_color="#6b1a1a", opacity=0.8,
    ))
    fig.update_layout(
        **BASE_LAYOUT,
        barmode="overlay",
        title=dict(text=f"Score Distribution — {taxi_type.capitalize()}", font=dict(size=11, color="#404660")),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
        xaxis=dict(gridcolor="#11151f", linecolor="#1c2030", title="Anomaly Score"),
        yaxis=dict(gridcolor="#11151f", linecolor="#1c2030", title="Count"),
    )
    return fig


def chart_radar(m, taxi_type):
    cats = ["Precision", "Recall", "F1-Score", "Accuracy", "Specificity"]
    vals = [m["precision"], m["recall"], m["f1"], m["accuracy"], m["specificity"]]
    fig = go.Figure(go.Scatterpolar(
        r=[v * 100 for v in vals], theta=cats,
        fill="toself",
        fillcolor="rgba(56, 189, 248, 0.08)",
        line=dict(color="#38bdf8", width=1.5),
        marker=dict(size=5, color="#38bdf8"),
    ))
    fig.update_layout(
        **BASE_LAYOUT,
        title=dict(text=f"Performance Profile — {taxi_type.capitalize()}", font=dict(size=11, color="#404660")),
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(visible=True, range=[0, 100], ticksuffix="%",
                            gridcolor="#141824", tickfont=dict(size=9, color="#2e3448")),
            angularaxis=dict(gridcolor="#141824", tickfont=dict(color="#404660")),
        ),
        showlegend=False,
    )
    return fig


def performance_badge(f1: float) -> str:
    if f1 >= 0.8: return '<span class="badge badge-good">Strong</span>'
    if f1 >= 0.6: return '<span class="badge badge-warn">Moderate</span>'
    return '<span class="badge badge-poor">Weak</span>'


def kpi(label, value, sub="", variant="kpi-blue"):
    return f"""<div class="kpi-card {variant}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>"""


# ── Page Header ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
  <h1>Benchmark Evaluator<span class="refresh-badge">Auto 30s</span></h1>
  <p>Streaming system accuracy measured against AI-generated ground truth labels (Isolation Forest offline batch)</p>
</div>
""", unsafe_allow_html=True)

# Filter sits outside the fragment so user interaction persists across refreshes
taxi_filter = st.selectbox("Filter by source", ["All", "Yellow", "Green"])


@st.fragment(run_every=30)
def render_benchmark():
    """
    Auto-refreshes every 30 seconds.
    Uses `return` instead of `st.stop()` so the fragment keeps rescheduling
    itself even when the database is not yet available.
    """
    # Force fresh DB query on every run (bypass the 30s cache TTL)
    load_streaming_results.clear()

    ground_truth = load_ground_truth()
    streaming_df = load_streaming_results()

    if streaming_df.empty:
        st.markdown(
            '<div class="alert">No database connection or no streaming data yet. '
            'Retrying automatically every 30 s...</div>',
            unsafe_allow_html=True,
        )
        return

    if not ground_truth:
        st.markdown(
            '<div class="alert">Ground truth Parquet files not found. '
            'Run ai_batch_labeler.py first.</div>',
            unsafe_allow_html=True,
        )
        return

    # ── Evaluate ───────────────────────────────────────────────────────────────
    eval_results = evaluate(streaming_df, ground_truth)

    types_to_show = list(eval_results.keys())
    if taxi_filter == "Yellow": types_to_show = ["yellow"]
    if taxi_filter == "Green":  types_to_show = ["green"]
    valid = {
        k: v for k, v in eval_results.items()
        if k in types_to_show and v.get("matched", 0) >= 5
    }

    if not valid:
        st.markdown(
            '<div class="alert">Not enough matched records yet. '
            'Retrying automatically every 30 s...</div>',
            unsafe_allow_html=True,
        )
        return

    # ── Overview KPIs ──────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Overall Performance</div>', unsafe_allow_html=True)

    avg_f1    = np.mean([v["f1"]        for v in valid.values()])
    avg_prec  = np.mean([v["precision"] for v in valid.values()])
    avg_rec   = np.mean([v["recall"]    for v in valid.values()])
    avg_acc   = np.mean([v["accuracy"]  for v in valid.values()])
    tot_match = sum(v["matched"] for v in valid.values())
    tot_tp    = sum(v["tp"]      for v in valid.values())
    tot_fp    = sum(v["fp"]      for v in valid.values())
    tot_fn    = sum(v["fn"]      for v in valid.values())

    o1, o2, o3, o4, o5 = st.columns(5)
    with o1: st.markdown(kpi("F1-Score",        f"{avg_f1:.1%}",    f"Harmonic mean {performance_badge(avg_f1)}", "kpi-blue"),  unsafe_allow_html=True)
    with o2: st.markdown(kpi("Precision",        f"{avg_prec:.1%}", "True alerts / Total alerts",                  "kpi-green"), unsafe_allow_html=True)
    with o3: st.markdown(kpi("Recall",           f"{avg_rec:.1%}",  "Caught / Total real anomalies",               "kpi-amber"), unsafe_allow_html=True)
    with o4: st.markdown(kpi("Accuracy",         f"{avg_acc:.1%}",  "Correct classifications",                     "kpi-slate"), unsafe_allow_html=True)
    with o5: st.markdown(kpi("Matched Records",  f"{tot_match:,}",  f"TP {tot_tp} · FP {tot_fp} · FN {tot_fn}",   "kpi-red"),   unsafe_allow_html=True)

    # ── Per Taxi Type ──────────────────────────────────────────────────────────
    for taxi_type, m in valid.items():
        label_class = "taxi-yellow" if taxi_type == "yellow" else "taxi-green"
        st.markdown(
            f'<hr class="divider">'
            f'<div class="section-label {label_class}">'
            f'{taxi_type.upper()} TAXI — DETAILED RESULTS</div>',
            unsafe_allow_html=True,
        )

        r1, r2, r3, r4, r5 = st.columns(5)
        with r1: st.markdown(kpi("F1-Score",    f"{m['f1']:.1%}",          performance_badge(m['f1']),          "kpi-blue"),  unsafe_allow_html=True)
        with r2: st.markdown(kpi("Precision",   f"{m['precision']:.1%}",   f"TP {m['tp']} · FP {m['fp']}",     "kpi-green"), unsafe_allow_html=True)
        with r3: st.markdown(kpi("Recall",      f"{m['recall']:.1%}",      f"TP {m['tp']} · FN {m['fn']}",     "kpi-amber"), unsafe_allow_html=True)
        with r4: st.markdown(kpi("Accuracy",    f"{m['accuracy']:.1%}",    f"{m['matched']:,} records matched", "kpi-slate"), unsafe_allow_html=True)
        with r5: st.markdown(kpi("Specificity", f"{m['specificity']:.1%}", f"TN {m['tn']}",                    "kpi-red"),   unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1: st.plotly_chart(chart_confusion(m["tp"], m["tn"], m["fp"], m["fn"], "Confusion Matrix"), use_container_width=True)
        with c2: st.plotly_chart(chart_score_dist(m["merged"], taxi_type), use_container_width=True)
        with c3: st.plotly_chart(chart_radar(m, taxi_type), use_container_width=True)

        with st.expander(f"Record-level detail — {taxi_type.capitalize()} (first 100 matched)"):
            merged = m["merged"].copy()
            merged["Streaming Prediction"] = merged["y_pred"].map({1: "Anomaly", 0: "Normal"})
            merged["Ground Truth"]         = merged["is_anomaly"].map({1: "Anomaly", 0: "Normal"})
            merged["Correct"]              = np.where(merged["y_pred"] == merged["is_anomaly"], "Yes", "No")
            show_cols = ["pickup_datetime", "trip_distance", "fare_amount",
                         "anomaly_score", "ai_anomaly_score",
                         "Streaming Prediction", "Ground Truth", "Correct", "ai_reason"]
            cols = [c for c in show_cols if c in merged.columns]
            st.dataframe(merged[cols].head(100), use_container_width=True, hide_index=True)

    # ── Footer Glossary ────────────────────────────────────────────────────────
    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="info-box">
    <strong>Precision</strong> — Of all trips the streaming system flagged as anomalous, what fraction were truly anomalous per the AI ground truth.<br>
    <strong>Recall</strong> — Of all trips the AI ground truth marks as anomalous, what fraction did the streaming system catch.<br>
    <strong>F1-Score</strong> — Harmonic mean of Precision and Recall. The primary metric for imbalanced anomaly detection tasks.<br>
    <strong>Specificity</strong> — The system's ability to correctly leave normal trips unlabelled (True Negative Rate).<br>
    <strong>Confusion Matrix</strong> — TP: correct anomaly · TN: correct normal · FP: false alarm · FN: missed anomaly.
    </div>
    """, unsafe_allow_html=True)


render_benchmark()



