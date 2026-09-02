"""
LaplapTech Analytics Dashboard - Premium Version
===============================================
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup & Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

st.set_page_config(
    page_title="LaplapTech | Executive Dashboard",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium CSS Injection (Glassmorphism & Gradients)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
    
    * { font-family: 'Outfit', sans-serif; }
    
    /* Main Background */
    .stApp {
        background: #0f172a;
        color: #f8fafc;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: rgba(15, 23, 42, 0.95);
        border-right: 1px solid rgba(255,255,255,0.05);
    }
    
    /* Gradient Text Header */
    .gradient-text {
        background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    
    /* Metric Cards - Glassmorphism */
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        padding: 20px;
        border-radius: 16px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3), 0 4px 6px -2px rgba(0, 0, 0, 0.15);
        border: 1px solid rgba(56, 189, 248, 0.4);
    }
    
    /* Metric Value */
    div[data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700 !important;
        color: #f8fafc !important;
    }
    
    /* Metric Label */
    div[data-testid="stMetricLabel"] {
        font-size: 1.1rem !important;
        color: #94a3b8 !important;
        font-weight: 500 !important;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent;
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1.1rem;
        font-weight: 500;
        color: #94a3b8;
        background-color: transparent;
        border: none;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        color: #38bdf8 !important;
        border-bottom: 3px solid #38bdf8 !important;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: rgba(30, 41, 59, 0.5) !important;
        border-radius: 8px !important;
        color: #f8fafc !important;
    }
    
    /* Dataframes */
    [data-testid="stDataFrame"] {
        background: rgba(30, 41, 59, 0.5);
        border-radius: 12px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "123456")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DATABASE", "laplaptech_pipeline")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)

@st.cache_data(ttl=300)
def query_df(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

def safe_query(sql: str, fallback_msg: str = "Chưa có dữ liệu") -> pd.DataFrame:
    try:
        return query_df(sql)
    except Exception as e:
        st.warning(f"{fallback_msg}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# Custom Plotly Template
# ---------------------------------------------------------------------------
pio_template = go.layout.Template()
pio_template.layout.plot_bgcolor = 'rgba(0,0,0,0)'
pio_template.layout.paper_bgcolor = 'rgba(0,0,0,0)'
pio_template.layout.font.color = '#f8fafc'
pio_template.layout.font.family = 'Outfit, sans-serif'
pio_template.layout.xaxis.gridcolor = 'rgba(255,255,255,0.05)'
pio_template.layout.yaxis.gridcolor = 'rgba(255,255,255,0.05)'
pio_template.layout.colorway = ['#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb923c']

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("<h1 style='text-align: center; color: #38bdf8;'>LaplapTech 🚀</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #94a3b8; font-weight: 300;'>Premium Analytics</p>", unsafe_allow_html=True)
    st.divider()
    
    st.markdown("### 🧭 Navigation")
    page = st.radio(
        "",
        ["🏠 Overview", "🔥 Brand & Hardware", "⚡ Performance Matrix"],
        index=0,
        label_visibility="collapsed"
    )
    
    st.divider()
    st.markdown("### 📅 Global Filter")
    date_filter = st.date_input("Select Date Range", value=None)
    st.caption("Note: Date filtering applies to time-series charts.")

# ===========================================================================
# PAGE 1: Overview
# ===========================================================================
if page == "🏠 Overview":
    st.markdown("<div class='gradient-text'>Site Overview & Conversion Funnel</div>", unsafe_allow_html=True)
    st.markdown("<p style='color: #94a3b8;'>Monitor high-level KPIs, user traffic, and behavioral conversion rates.</p>", unsafe_allow_html=True)
    st.write("")

    # ── KPI Cards ──
    kpi_df = safe_query("""
        SELECT 
            SUM(total_sessions) as total_sessions, 
            SUM(total_events) as total_events,
            SUM(sessions_with_detail_view) as detail_views,
            SUM(sessions_with_comparison) as comparisons,
            ROUND(AVG(comparison_rate_pct), 1) as avg_comparison_rate
        FROM public_gold.mart_daily_site_kpis
    """)
    
    if not kpi_df.empty:
        row = kpi_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🌍 Total Sessions", f"{int(row.get('total_sessions', 0)):,}", delta="Live")
        c2.metric("🖱️ Total Events", f"{int(row.get('total_events', 0)):,}")
        c3.metric("📱 Detail Views", f"{int(row.get('detail_views', 0)):,}", delta="Key Event", delta_color="normal")
        c4.metric("⚖️ Comparison Rate", f"{row.get('avg_comparison_rate', 0)}%")
        
    st.write("")
    st.write("")

    # ── Daily trend & OS distribution ──
    col_left, col_right = st.columns([2, 1], gap="large")

    with col_left:
        st.markdown("### 📈 Traffic & Engagement Trend")
        daily_df = safe_query("""
            SELECT event_date, total_sessions, sessions_with_detail_view, sessions_with_comparison 
            FROM public_gold.mart_daily_site_kpis 
            ORDER BY event_date
        """)
        if not daily_df.empty:
            fig = px.area(
                daily_df, x="event_date", 
                y=["total_sessions", "sessions_with_detail_view", "sessions_with_comparison"],
                labels={"value": "Sessions", "event_date": "Date", "variable": "Action"},
            )
            fig.update_layout(template=pio_template, legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0))
            fig.update_traces(mode='lines+markers', line=dict(width=3))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("### 🖥️ OS Distribution")
        os_df = safe_query("SELECT user_os, sessions FROM public_gold.mart_user_os ORDER BY sessions DESC")
        if not os_df.empty:
            fig = px.pie(
                os_df.head(5), values="sessions", names="user_os", 
                hole=0.6, 
            )
            fig.update_layout(
                template=pio_template, 
                margin=dict(t=40, b=40, l=0, r=0),
                annotations=[dict(text='OS', x=0.5, y=0.5, font_size=20, showarrow=False, font_color='#f8fafc')]
            )
            fig.update_traces(hoverinfo='label+percent', textinfo='percent', textfont_size=14,
                              marker=dict(line=dict(color='#0f172a', width=2)))
            st.plotly_chart(fig, use_container_width=True)

    # ── Funnel ──
    st.write("")
    st.markdown("### 🔄 E-Commerce Funnel Conversion")
    funnel_df = safe_query("""
        SELECT 
            SUM(step_1_pageview) as "Pageview",
            SUM(step_2_product_detail) as "Product Detail",
            SUM(step_3_select_compare) as "Select Compare",
            SUM(step_4_comparison_page) as "Comparison Page",
            SUM(step_5_sort_action) as "Sort Action"
        FROM public_gold.mart_behavior_funnel_daily
    """)
    if not funnel_df.empty:
        funnel_data = funnel_df.iloc[0].reset_index()
        funnel_data.columns = ["step", "count"]
        funnel_data = funnel_data[funnel_data["count"] > 0]
        
        fig = go.Figure(go.Funnel(
            y=funnel_data["step"], x=funnel_data["count"],
            textinfo="value+percent initial",
            marker=dict(
                color=['#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb923c'],
                line=dict(width=0)
            ),
            connector=dict(line=dict(color='rgba(255,255,255,0.1)', width=2))
        ))
        fig.update_layout(template=pio_template, margin=dict(t=20, b=20, l=150))
        st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# PAGE 2: Brand & Trend
# ===========================================================================
elif page == "🔥 Brand & Hardware":
    st.markdown("<div class='gradient-text'>Brand Insights & Hardware Trends</div>", unsafe_allow_html=True)
    st.markdown("<p style='color: #94a3b8;'>Discover which brands dominate user mindshare and track the rising popularity of CPU/GPU models.</p>", unsafe_allow_html=True)
    
    st.write("")
    t1, t2, t3 = st.tabs(["🏷️ Brand Power", "🧠 CPU Intelligence", "🎮 GPU Dominance"])
    
    with t1:
        st.markdown("### Most Viewed Brands")
        brand_df = safe_query("""
            SELECT brand_name, total_views, comparison_rate_pct, views_per_product, active_products
            FROM public_gold.mart_brand_interest
            ORDER BY total_views DESC LIMIT 10
        """)
        if not brand_df.empty:
            c1, c2 = st.columns([3, 2], gap="large")
            with c1:
                fig = px.bar(
                    brand_df, x="total_views", y="brand_name", orientation="h",
                    color="comparison_rate_pct", color_continuous_scale="PuBu",
                    text_auto=".2s"
                )
                fig.update_layout(template=pio_template, yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
                fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                st.markdown("### Top Brand Efficiency")
                st.dataframe(
                    brand_df[["brand_name", "views_per_product", "active_products"]].head(5),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "brand_name": "Brand",
                        "views_per_product": st.column_config.ProgressColumn("Avg Views / Product", format="%d", min_value=0, max_value=int(brand_df["views_per_product"].max())),
                        "active_products": "Active Models"
                    }
                )

    with t2:
        st.markdown("### CPU Interest Trend (Top 5)")
        cpu_df = safe_query("""
            SELECT month, cpu_name, total_views 
            FROM public_gold.mart_cpu_trend 
            ORDER BY month
        """)
        if not cpu_df.empty:
            top_cpus = cpu_df.groupby("cpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_cpu = cpu_df[cpu_df["cpu_name"].isin(top_cpus)]
            fig = px.line(filtered_cpu, x="month", y="total_views", color="cpu_name", markers=True)
            fig.update_layout(template=pio_template, legend=dict(title="", orientation="h", yanchor="bottom", y=1.02, x=0))
            fig.update_traces(line=dict(width=4), marker=dict(size=8))
            st.plotly_chart(fig, use_container_width=True)

    with t3:
        st.markdown("### GPU Interest Trend (Top 5)")
        gpu_df = safe_query("""
            SELECT month, gpu_name, total_views 
            FROM public_gold.mart_gpu_trend 
            ORDER BY month
        """)
        if not gpu_df.empty:
            top_gpus = gpu_df.groupby("gpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_gpu = gpu_df[gpu_df["gpu_name"].isin(top_gpus)]
            fig = px.area(filtered_gpu, x="month", y="total_views", color="gpu_name")
            fig.update_layout(template=pio_template, legend=dict(title="", orientation="h", yanchor="bottom", y=1.02, x=0))
            st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# PAGE 3: Performance
# ===========================================================================
elif page == "⚡ Performance Matrix":
    st.markdown("<div class='gradient-text'>Performance Matrix & Benchmarks</div>", unsafe_allow_html=True)
    st.markdown("<p style='color: #94a3b8;'>Analyze benchmark scores across price segments and discover the correlation between battery life and user interest.</p>", unsafe_allow_html=True)
    
    st.write("")
    
    perf_df = safe_query("""
        SELECT laptop_name, brand_name, performance_tier,
               geekbench6_multi, office_battery_hours
        FROM public_gold.mart_performance_ranking
        WHERE geekbench6_multi IS NOT NULL
    """)
    
    if not perf_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 🏆 Top 10 by CPU Power (Multi-core)")
            top_cpu = perf_df.nlargest(10, "geekbench6_multi").sort_values("geekbench6_multi", ascending=True)
            fig = px.bar(top_cpu, x="geekbench6_multi", y="laptop_name", color="performance_tier", orientation="h")
            fig.update_layout(template=pio_template, showlegend=False, margin=dict(l=200))
            st.plotly_chart(fig, use_container_width=True)
            
        with c2:
            st.markdown("### 🔋 Top 10 by Battery Life (Office Use)")
            top_batt = perf_df.dropna(subset=["office_battery_hours"]).nlargest(10, "office_battery_hours").sort_values("office_battery_hours", ascending=True)
            fig = px.bar(top_batt, x="office_battery_hours", y="laptop_name", orientation="h", color_discrete_sequence=['#4ade80'])
            fig.update_layout(template=pio_template, showlegend=False, margin=dict(l=200))
            st.plotly_chart(fig, use_container_width=True)
            
    st.divider()
    
    st.markdown("### 🎯 Does Battery Life Drive User Interest?")
    bvi_df = safe_query("""
        SELECT laptop_name, brand_name, office_battery_hours, 
               total_views, geekbench6_multi
        FROM public_gold.mart_battery_vs_interest
        WHERE office_battery_hours IS NOT NULL AND total_views > 0
    """)
    
    if not bvi_df.empty:
        fig = px.scatter(
            bvi_df, x="office_battery_hours", y="total_views", 
            color="brand_name", size="geekbench6_multi",
            hover_name="laptop_name", size_max=40,
            labels={"office_battery_hours": "Battery (Hours)", "total_views": "User Views"}
        )
        fig.update_layout(template=pio_template, height=500)
        # Add glassmorphism box behind plot
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(255,255,255,0.02)")
        st.plotly_chart(fig, use_container_width=True)
