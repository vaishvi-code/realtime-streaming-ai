"""
Sentiment Pipeline Dashboard — Clean Professional UI
Run: streamlit run dashboard/app.py
"""

import os
import time
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH     = os.path.join(DATA_DIR, "gold", "sentiment_warehouse.duckdb")
SILVER_PATH = os.path.join(DATA_DIR, "silver", "posts")
REFRESH_SEC = 30

SENTIMENT_COLORS = {
    "POSITIVE": "#10b981",
    "NEGATIVE": "#f43f5e",
    "NEUTRAL":  "#6b7280",
    "MIXED":    "#f59e0b",
}

CHART_THEME = dict(
    plot_bgcolor  = "#0d1117",
    paper_bgcolor = "#0d1117",
    font_color    = "#c9d1d9",
    margin        = dict(l=0, r=0, t=24, b=0),
)

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Sentiment Pipeline",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0d1117;
    color: #c9d1d9;
}

h1, h2, h3, .mono { font-family: 'IBM Plex Mono', monospace; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #161b22;
    border-right: 1px solid #21262d;
}

/* Hide Streamlit branding */
#MainMenu, footer, header { visibility: hidden; }

/* KPI cards */
.kpi-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1px; margin-bottom: 2rem; background: #21262d; border: 1px solid #21262d; border-radius: 8px; overflow: hidden; }
.kpi { background: #161b22; padding: 1.25rem 1.5rem; }
.kpi-label { font-size: 0.7rem; font-weight: 600; letter-spacing: 0.12em; text-transform: uppercase; color: #6b7280; margin-bottom: 0.5rem; }
.kpi-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.75rem; font-weight: 600; color: #e6edf3; line-height: 1; }
.kpi-value.positive { color: #10b981; }
.kpi-value.negative { color: #f43f5e; }
.kpi-value.accent   { color: #58a6ff; font-size: 1rem; }

/* Section headers */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #6b7280;
    border-bottom: 1px solid #21262d;
    padding-bottom: 0.5rem;
    margin: 1.5rem 0 1rem 0;
}

/* Status indicator */
.status-bar {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.75rem;
    color: #6b7280;
    font-family: 'IBM Plex Mono', monospace;
}
.status-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: #10b981;
    animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

/* Dividers */
hr { border: none; border-top: 1px solid #21262d; margin: 1.5rem 0; }

/* Data table */
.stDataFrame { border-radius: 6px; border: 1px solid #21262d !important; }
</style>
""", unsafe_allow_html=True)


# ── Data Loaders ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    os.makedirs(os.path.join(DATA_DIR, "gold"), exist_ok=True)
    return duckdb.connect(DB_PATH, read_only=True)


def load_trends(con, hours=6):
    try:
        return con.execute(f"""
            SELECT * FROM main_gold.sentiment_trends
            WHERE five_min_window >= NOW() - INTERVAL '{hours} hours'
            ORDER BY five_min_window
        """).df()
    except Exception:
        return pd.DataFrame()


def load_keywords(con, limit=15):
    try:
        return con.execute(f"""
            SELECT * FROM main_gold.topic_leaderboard
            ORDER BY mention_count DESC LIMIT {limit}
        """).df()
    except Exception:
        return pd.DataFrame()


def load_recent(limit=50):
    try:
        con = duckdb.connect()
        return con.execute(f"""
            SELECT source, title, sentiment_label,
                   ROUND(sentiment_compound, 3) AS score,
                   num_comments, ingested_at
            FROM read_parquet('{SILVER_PATH.replace(chr(92), "/")}/**/*.parquet')
            ORDER BY ingested_at DESC LIMIT {limit}
        """).df()
    except Exception:
        return pd.DataFrame()


def load_summary():
    try:
        silver = SILVER_PATH.replace("\\", "/")
        con = duckdb.connect()
        row = con.execute(f"""
            SELECT COUNT(*) AS total,
                   AVG(sentiment_compound) AS avg_sent,
                   SUM(CASE WHEN sentiment_label='POSITIVE' THEN 1 ELSE 0 END) AS pos,
                   SUM(CASE WHEN sentiment_label='NEGATIVE' THEN 1 ELSE 0 END) AS neg,
                   MAX(ingested_at) AS latest
            FROM read_parquet('{silver}/**/*.parquet')
        """).fetchone()
        total = row[0] or 0
        return {
            "total":      total,
            "avg_sent":   round(row[1] or 0, 3),
            "pos_pct":    round(100 * (row[2] or 0) / total, 1) if total else 0,
            "neg_pct":    round(100 * (row[3] or 0) / total, 1) if total else 0,
            "latest":     str(row[4])[:19] if row[4] else "—",
        }
    except Exception:
        return {"total": 0, "avg_sent": 0, "pos_pct": 0, "neg_pct": 0, "latest": "—"}


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown('<div class="section-header">Configuration</div>', unsafe_allow_html=True)
    time_window = st.slider("Time window (hours)", 1, 24, 6)
    top_n       = st.slider("Keywords to display", 5, 30, 15)
    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="status-bar">
        <div class="status-dot"></div>
        Live &nbsp;·&nbsp; refresh {REFRESH_SEC}s
    </div>
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;color:#6b7280;margin-top:0.5rem;">
        {datetime.now().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)
    if st.button("Refresh now", use_container_width=True):
        st.rerun()
    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.7rem;color:#6b7280;font-family:'IBM Plex Mono',monospace;line-height:1.8;">
    Stack<br>
    <span style="color:#c9d1d9;">Kafka · Python · dbt<br>DuckDB · Parquet<br>VADER · TextBlob<br>Streamlit · Docker</span>
    </div>
    """, unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<div style="padding: 1rem 0 0.5rem 0;">
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;letter-spacing:0.2em;text-transform:uppercase;color:#6b7280;margin-bottom:0.4rem;">
        Real-Time Data Pipeline
    </div>
    <div style="font-family:'IBM Plex Mono',monospace;font-size:1.6rem;font-weight:600;color:#e6edf3;letter-spacing:-0.02em;">
        Sentiment Intelligence
    </div>
    <div style="font-size:0.8rem;color:#6b7280;margin-top:0.3rem;">
        Hacker News &rarr; Kafka &rarr; Python Consumer &rarr; Parquet Lakehouse &rarr; dbt &rarr; DuckDB
    </div>
</div>
<hr>
""", unsafe_allow_html=True)


# ── KPIs ──────────────────────────────────────────────────────────────────────

stats = load_summary()
avg_sign = "+" if stats["avg_sent"] >= 0 else ""

st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi">
        <div class="kpi-label">Total Posts</div>
        <div class="kpi-value">{stats['total']:,}</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Avg Sentiment</div>
        <div class="kpi-value {'positive' if stats['avg_sent'] >= 0.05 else 'negative' if stats['avg_sent'] <= -0.05 else ''}">{avg_sign}{stats['avg_sent']:.3f}</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Positive</div>
        <div class="kpi-value positive">{stats['pos_pct']}%</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Negative</div>
        <div class="kpi-value negative">{stats['neg_pct']}%</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Last Ingested</div>
        <div class="kpi-value accent">{stats['latest']}</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Charts row ────────────────────────────────────────────────────────────────

con = get_connection()
trends_df  = load_trends(con, time_window)
keywords_df = load_keywords(con, top_n)

col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<div class="section-header">Sentiment Trend — 5-min windows</div>', unsafe_allow_html=True)
    if not trends_df.empty:
        fig = px.line(
            trends_df,
            x="five_min_window",
            y="avg_sentiment_score",
            color="sentiment_label",
            color_discrete_map=SENTIMENT_COLORS,
            line_shape="spline",
        )
        fig.update_traces(line_width=1.5)
        fig.update_layout(
            **CHART_THEME,
            height=280,
            legend=dict(title="", orientation="v", x=1.01, font_size=11),
            xaxis=dict(title="", gridcolor="#21262d", showgrid=True),
            yaxis=dict(title="Compound Score", range=[-1, 1],
                       gridcolor="#21262d", zeroline=True, zerolinecolor="#30363d"),
        )
        fig.add_hline(y=0, line_dash="dot", line_color="#30363d", opacity=0.8)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for Gold layer data. Run: dbt run --profiles-dir .")

with col_right:
    st.markdown('<div class="section-header">Distribution</div>', unsafe_allow_html=True)
    if not trends_df.empty:
        dist = trends_df.groupby("sentiment_label")["post_count"].sum().reset_index()
        fig2 = go.Figure(go.Pie(
            labels=dist["sentiment_label"],
            values=dist["post_count"],
            hole=0.6,
            marker_colors=[SENTIMENT_COLORS.get(l, "#6b7280") for l in dist["sentiment_label"]],
            textinfo="percent",
            textfont_size=12,
        ))
        fig2.update_layout(**CHART_THEME, height=280,
                           legend=dict(orientation="v", x=1.01, font_size=11))
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data yet.")


# ── Keywords ──────────────────────────────────────────────────────────────────

st.markdown('<div class="section-header">Top Keywords by Mentions</div>', unsafe_allow_html=True)

if not keywords_df.empty:
    fig3 = px.bar(
        keywords_df,
        x="mention_count",
        y="keyword",
        orientation="h",
        color="avg_sentiment_score",
        color_continuous_scale=[[0, "#f43f5e"], [0.5, "#374151"], [1, "#10b981"]],
        color_continuous_midpoint=0,
        labels={"mention_count": "Mentions", "keyword": ""},
    )
    fig3.update_layout(
        **CHART_THEME,
        height=420,
        yaxis={"categoryorder": "total ascending"},
        coloraxis_colorbar=dict(title="Sentiment", tickfont_size=10, len=0.6),
        xaxis=dict(gridcolor="#21262d"),
    )
    fig3.update_traces(marker_line_width=0)
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Run dbt to populate the Gold layer.")


# ── Recent Posts ──────────────────────────────────────────────────────────────

st.markdown('<div class="section-header">Recent Posts</div>', unsafe_allow_html=True)

recent_df = load_recent()
if not recent_df.empty:
    def color_label(val):
        m = {"POSITIVE": "color:#10b981", "NEGATIVE": "color:#f43f5e",
             "NEUTRAL": "color:#6b7280", "MIXED": "color:#f59e0b"}
        return m.get(val, "")
    styled = recent_df.style.map(color_label, subset=["sentiment_label"])
    st.dataframe(styled, use_container_width=True, height=280)
else:
    st.info("No Silver layer data found.")


# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("""
<hr>
<div style="font-family:'IBM Plex Mono',monospace;font-size:0.65rem;color:#6b7280;text-align:center;">
    Kafka &nbsp;·&nbsp; Python Consumer &nbsp;·&nbsp; Parquet Medallion Lakehouse (Bronze / Silver / Gold)
    &nbsp;·&nbsp; dbt + DuckDB &nbsp;·&nbsp; VADER + TextBlob NLP &nbsp;·&nbsp; Docker &nbsp;·&nbsp; Streamlit
    &nbsp;&nbsp;|&nbsp;&nbsp; Zero paid APIs
</div>
""", unsafe_allow_html=True)

# Auto-refresh
time.sleep(REFRESH_SEC)
st.rerun()
