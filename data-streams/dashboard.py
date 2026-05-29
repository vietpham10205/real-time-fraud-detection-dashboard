import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Fleet Intelligence — Real-Time Pipeline",
    page_icon=None,
    layout="wide",
)

DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

# ── Design System ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
}

/* Background */
.stApp { background-color: #08090d; }
section[data-testid="stSidebar"] { background-color: #0d0f16; }

/* Hide Streamlit chrome */
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
    font-weight: 400;
}
.live-badge {
    display: inline-block;
    background: #0f2a1a;
    color: #34d974;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    padding: 3px 10px;
    border-radius: 20px;
    border: 1px solid #1a4a2a;
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
.divider {
    border: none;
    border-top: 1px solid #141824;
    margin: 28px 0;
}

/* ── KPI Cards ──────────────────────────────────────────────────────────── */
.kpi-card {
    background: #0d1017;
    border: 1px solid #181e2c;
    border-radius: 10px;
    padding: 16px 18px 14px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
}
.kpi-card:hover { border-color: #252d42; }
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}
.kpi-blue::before   { background: linear-gradient(90deg, #2563eb, #38bdf8); }
.kpi-green::before  { background: linear-gradient(90deg, #059669, #34d974); }
.kpi-amber::before  { background: linear-gradient(90deg, #d97706, #fbbf24); }
.kpi-red::before    { background: linear-gradient(90deg, #dc2626, #f87171); }

.kpi-label {
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: #404660;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 26px;
    font-weight: 700;
    color: #d8dff0;
    line-height: 1;
    letter-spacing: -0.5px;
}
.kpi-sub {
    font-size: 11px;
    color: #343850;
    margin-top: 5px;
    font-weight: 400;
}

/* ── Sys Cards (Throughput, Latency etc.) ───────────────────────────────── */
.sys-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 4px;
}
.sys-card {
    background: #0d1017;
    border: 1px solid #181e2c;
    border-radius: 10px;
    padding: 14px 18px;
}
.sys-label {
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: #343850;
    margin-bottom: 5px;
}
.sys-value {
    font-size: 22px;
    font-weight: 600;
    color: #38bdf8;
    letter-spacing: -0.3px;
}
.sys-value.green { color: #34d974; }
.sys-value.amber { color: #fbbf24; }
.sys-value.red   { color: #f87171; }

/* ── Alert Strip ────────────────────────────────────────────────────────── */
.alert-strip {
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


def get_db_connection():
    try:
        return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    except Exception:
        return None


def kpi(label, value, sub="", variant="kpi-blue"):
    return f"""
    <div class="kpi-card {variant}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>"""


def sys_card(label, value, variant=""):
    return f"""
    <div class="sys-card">
        <div class="sys-label">{label}</div>
        <div class="sys-value {variant}">{value}</div>
    </div>"""


CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#505870", size=11, family="Inter"),
    margin=dict(l=0, r=0, t=36, b=0),
    height=260,
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    xaxis=dict(gridcolor="#11151f", linecolor="#1c2030"),
    yaxis=dict(gridcolor="#11151f", linecolor="#1c2030"),
    title_font=dict(size=12, color="#505870"),
)


# ── Page Header ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
  <h1>NYC Fleet Intelligence Pipeline<span class="live-badge">Live</span></h1>
  <p>Kafka — Spark Structured Streaming — PostgreSQL — Streamlit</p>
</div>
""", unsafe_allow_html=True)


@st.fragment(run_every=3)
def update_dashboard():
    conn = get_db_connection()
    if not conn:
        st.markdown('<div class="alert-strip">Database unavailable. Waiting for pipeline to initialise...</div>', unsafe_allow_html=True)
        return

    try:
        df_sys = pd.read_sql(
            "SELECT batch_id, inserted_at, processing_time_ms, throughput_eps, "
            "estimated_cost_usd, records_processed, anomalies_detected, anomaly_rate "
            "FROM pipeline_metrics ORDER BY inserted_at DESC LIMIT 50", conn
        )
        df_counts = pd.read_sql(
            "SELECT taxi_type, COUNT(*) AS total_normal, AVG(fare_amount) AS avg_fare "
            "FROM normal_trips GROUP BY taxi_type", conn
        )
        df_anomaly_counts = pd.read_sql(
            "SELECT taxi_type, COUNT(*) AS total_anomalies FROM anomalous_trips GROUP BY taxi_type", conn
        )
        df_normal    = pd.read_sql("SELECT * FROM normal_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_anomalies = pd.read_sql("SELECT * FROM anomalous_trips ORDER BY inserted_at DESC LIMIT 500", conn)
        df_live      = pd.read_sql(
            "SELECT taxi_type, VendorID, pickup_datetime, trip_distance, "
            "fare_amount, avg_speed_kmh, anomaly_score FROM normal_trips "
            "ORDER BY inserted_at DESC LIMIT 8", conn
        )
    except Exception:
        st.markdown('<div class="alert-strip">Pipeline tables not ready yet. Waiting for first batch...</div>', unsafe_allow_html=True)
        return
    finally:
        conn.close()

    # ── Derived values ─────────────────────────────────────────────────────────
    tps        = f"{df_sys['throughput_eps'].iloc[0]:,.0f}" if not df_sys.empty else "—"
    latency    = f"{df_sys['processing_time_ms'].iloc[0]:,.0f} ms" if not df_sys.empty else "—"
    total_recs = f"{df_sys['records_processed'].sum():,}" if not df_sys.empty else "—"
    total_cost = f"${df_sys['estimated_cost_usd'].sum():.4f}" if not df_sys.empty else "—"

    def safe_get(df, taxi, col, default=0):
        row = df[df["taxi_type"] == taxi]
        return row[col].iloc[0] if not row.empty else default

    y_normal   = int(safe_get(df_counts, "yellow", "total_normal"))
    g_normal   = int(safe_get(df_counts, "green",  "total_normal"))
    y_fare     = round(safe_get(df_counts, "yellow", "avg_fare"), 2)
    g_fare     = round(safe_get(df_counts, "green",  "avg_fare"), 2)
    y_anom     = int(safe_get(df_anomaly_counts, "yellow", "total_anomalies"))
    g_anom     = int(safe_get(df_anomaly_counts, "green",  "total_anomalies"))

    # ── Section: System Performance ────────────────────────────────────────────
    st.markdown('<div class="section-label">System Performance</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(sys_card("Throughput", f"{tps} rows/s"), unsafe_allow_html=True)
    with c2: st.markdown(sys_card("Processing Latency", latency, "amber"), unsafe_allow_html=True)
    with c3: st.markdown(sys_card("Records (last 50 batches)", total_recs, "green"), unsafe_allow_html=True)
    with c4: st.markdown(sys_card("Estimated Cloud Cost", total_cost, "red"), unsafe_allow_html=True)

    if not df_sys.empty:
        df_plot = df_sys.sort_values("inserted_at")
        ch1, ch2 = st.columns(2)
        with ch1:
            fig = px.line(df_plot, x="inserted_at", y="throughput_eps",
                          title="Throughput — rows per second")
            fig.update_traces(line=dict(color="#38bdf8", width=1.5))
            fig.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
        with ch2:
            fig2 = px.line(df_plot, x="inserted_at", y="processing_time_ms",
                           title="Batch Latency — milliseconds")
            fig2.update_traces(line=dict(color="#f87171", width=1.5))
            fig2.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    # ── Section: Live Data Stream ──────────────────────────────────────────────
    st.markdown('<div class="section-label">Live Data Stream</div>', unsafe_allow_html=True)
    if not df_live.empty:
        st.dataframe(
            df_live.style.apply(
                lambda col: [
                    "color: #f5c542; font-weight:500" if v == "yellow" else "color: #34d974; font-weight:500"
                    for v in col
                ], subset=["taxi_type"]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.markdown('<div class="alert-strip">Waiting for producer data...</div>', unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    # ── Section: Business Analytics ────────────────────────────────────────────
    st.markdown('<div class="section-label">Business Analytics — Yellow vs Green</div>', unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    with k1: st.markdown(kpi("Yellow Trips (Normal)", f"{y_normal:,}", f"Avg fare ${y_fare}", "kpi-amber"), unsafe_allow_html=True)
    with k2: st.markdown(kpi("Green Trips (Normal)",  f"{g_normal:,}", f"Avg fare ${g_fare}", "kpi-green"), unsafe_allow_html=True)
    with k3: st.markdown(kpi("Yellow Anomalies",      f"{y_anom:,}",  f"{y_anom/(y_normal+y_anom)*100:.1f}% of total" if (y_normal+y_anom)>0 else "", "kpi-red"), unsafe_allow_html=True)
    with k4: st.markdown(kpi("Green Anomalies",       f"{g_anom:,}",  f"{g_anom/(g_normal+g_anom)*100:.1f}% of total" if (g_normal+g_anom)>0 else "", "kpi-red"), unsafe_allow_html=True)

    if not df_normal.empty:
        p1, p2 = st.columns(2)
        with p1:
            fig3 = px.scatter(
                df_normal, x="trip_distance", y="avg_speed_kmh", color="taxi_type",
                color_discrete_map={"yellow": "#f5c542", "green": "#34d974"},
                hover_data=["fare_amount"],
                title="Speed vs Distance",
                opacity=0.6,
            )
            fig3.update_traces(marker=dict(size=4))
            fig3.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig3, use_container_width=True)
        with p2:
            fig4 = px.scatter(
                df_normal, x="trip_duration_sec", y="fare_amount", color="taxi_type",
                color_discrete_map={"yellow": "#f5c542", "green": "#34d974"},
                title="Fare vs Trip Duration",
                opacity=0.6,
            )
            fig4.update_traces(marker=dict(size=4))
            fig4.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig4, use_container_width=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    # ── Section: Anomaly Log ───────────────────────────────────────────────────
    st.markdown('<div class="section-label">Detected Anomalies — Latest 500</div>', unsafe_allow_html=True)
    if not df_anomalies.empty:
        show_cols = ["taxi_type", "pickup_datetime", "trip_distance",
                     "fare_amount", "avg_speed_kmh", "anomaly_score", "reason"]
        available = [c for c in show_cols if c in df_anomalies.columns]
        st.dataframe(
            df_anomalies[available].style.apply(
                lambda col: ["color: #f87171" for _ in col], subset=["reason"]
            ) if "reason" in available else df_anomalies[available],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.markdown('<div class="alert-strip">No anomalies detected yet.</div>', unsafe_allow_html=True)


update_dashboard()
