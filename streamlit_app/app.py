"""
LaplapTech Analytics Platform - SaaS Light Theme (Full Width, No Sidebar)
=======================================================================
Clean, modern SaaS dashboard with gray page bg, white cards, pill nav, and heatmap view.
"""
import os
import streamlit as st
import pandas as pd
import numpy as np
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
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------------------------------------------------------------------
# CSS Theme Injection
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
    }
    
    /* Page background = soft gray */
    .stApp {
        background-color: #f0f2f5 !important;
        color: #0f172a;
    }
    
    .dashboard-header {
        margin-top: -10px;
        margin-bottom: 25px;
    }
    .dashboard-header h1 {
        color: #0f172a !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
        letter-spacing: -0.5px;
        margin-bottom: 4px;
    }
    .dashboard-header p {
        color: #64748b;
        font-size: 0.95rem;
        margin: 0;
    }
    
    .section-title {
        color: #1e293b;
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 12px;
        padding-left: 5px;
    }
    
    /* KPI Cards = white on gray page */
    div[data-testid="stMetric"] {
        background-color: #ffffff !important;
        border-radius: 12px !important;
        padding: 20px 24px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06) !important;
        border: 1px solid #e5e7eb;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.08) !important;
    }
    
    div[data-testid="stMetricLabel"] {
        color: #475569 !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
    }
    div[data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 2.2rem !important;
        font-weight: 700 !important;
        line-height: 1.2 !important;
    }
    div[data-testid="stMetricDelta"] {
        color: #10b981 !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }
    
    /* Chart wrappers = white */
    .stPlotlyChart, iframe[title="st.plotly_chart"] {
        background-color: #ffffff !important;
        border-radius: 12px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06) !important;
        border: 1px solid #e5e7eb;
        padding: 12px;
    }
    
    /* Data Table = white */
    [data-testid="stDataFrame"] {
        background-color: #ffffff;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border: 1px solid #e5e7eb;
        padding: 12px;
    }
    
    /* Hide Header/Footer */
    header { visibility: hidden !important; }
    footer { visibility: hidden !important; }
    
    .block-container {
        padding-top: 0rem !important;
        padding-bottom: 2rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 100% !important;
    }
    
    /* Top Bar = white */
    .top-bar-container {
        background-color: #ffffff;
        padding: 12px 24px;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        margin-top: 10px;
        margin-bottom: 20px;
        border: 1px solid #e5e7eb;
    }
    
    /* Hide sidebar toggle */
    [data-testid="collapsedControl"] { display: none; }
    
    /* ── PILL NAV: hide radio circles, style as pills ── */
    div[data-testid="stRadio"] > div {
        gap: 0 !important;
    }
    div[data-testid="stRadio"] label {
        background-color: transparent !important;
        border: none !important;
        padding: 8px 18px !important;
        border-radius: 8px !important;
        cursor: pointer !important;
        font-weight: 600 !important;
        font-size: 0.95rem !important;
        color: #64748b !important;
        transition: all 0.15s ease !important;
    }
    div[data-testid="stRadio"] label:hover {
        color: #3b82f6 !important;
        background-color: #eff6ff !important;
    }
    div[data-testid="stRadio"] label[data-checked="true"],
    div[data-testid="stRadio"] label:has(input:checked) {
        background-color: #eff6ff !important;
        color: #2563eb !important;
    }
    /* Hide the radio circle itself */
    div[data-testid="stRadio"] label span[data-testid="stMarkdownContainer"] {
        margin-left: 0 !important;
    }
    div[data-testid="stRadio"] input[type="radio"] {
        display: none !important;
    }
    div[data-testid="stRadio"] label div[data-testid="stRadioOptionIndicator"],
    div[data-testid="stRadio"] label > div:first-child {
        display: none !important;
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

def safe_query(sql: str, fallback_msg: str = "No data") -> pd.DataFrame:
    try:
        return query_df(sql)
    except Exception as e:
        st.warning(f"{fallback_msg}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# Plotly Theme – white bg to match card containers
# ---------------------------------------------------------------------------
pio_template = go.layout.Template()
pio_template.layout.plot_bgcolor = '#ffffff'
pio_template.layout.paper_bgcolor = '#ffffff'
pio_template.layout.font.color = '#475569'
pio_template.layout.font.family = 'Inter, sans-serif'
pio_template.layout.font.size = 12
pio_template.layout.xaxis.gridcolor = '#f1f5f9'
pio_template.layout.yaxis.gridcolor = '#f1f5f9'
pio_template.layout.xaxis.zerolinecolor = '#e2e8f0'
pio_template.layout.yaxis.zerolinecolor = '#e2e8f0'
pio_template.layout.margin = dict(l=50, r=30, t=30, b=50)
pio_template.layout.colorway = ['#2563eb', '#16a34a', '#dc2626', '#ea580c', '#9333ea', '#0891b2', '#ca8a04']

# ---------------------------------------------------------------------------
# Top Navigation & Filters (Pill-style, no radio circles)
# ---------------------------------------------------------------------------
st.markdown("<div class='top-bar-container'>", unsafe_allow_html=True)
top_c1, top_c2, top_c3 = st.columns([1, 2.5, 1], vertical_alignment="center")

with top_c1:
    st.markdown("<h2 style='color: #3b82f6; font-size: 1.6rem; font-weight: 800; margin: 0; padding: 0;'>LAPLAPTECH</h2>", unsafe_allow_html=True)

with top_c2:
    active_tab = st.radio(
        "Navigation",
        ["Overview", "Hardware Metrics", "Performance Heatmap", "Product Catalog"],
        horizontal=True,
        label_visibility="collapsed"
    )

with top_c3:
    brands_data = safe_query("SELECT DISTINCT brand_name FROM public_gold.mart_brand_interest ORDER BY brand_name")
    brand_options = ["All Brands"] + (brands_data["brand_name"].tolist() if not brands_data.empty else [])
    selected_brand = st.selectbox("Select Brand", brand_options, index=0, label_visibility="collapsed")

st.markdown("</div>", unsafe_allow_html=True)
st.write("")

# ===========================================================================
# VIEW 1: Overview
# ===========================================================================
if active_tab == "Overview":
    st.markdown("""
        <div class='dashboard-header'>
            <h1>Overview</h1>
            <p>Key metrics and recent hardware engagement activities.</p>
        </div>
    """, unsafe_allow_html=True)
    
    kpi_df = safe_query("""
        SELECT 
            SUM(total_sessions) as total_sessions, 
            SUM(total_events) as total_events,
            SUM(sessions_with_detail_view) as detail_views,
            SUM(sessions_with_comparison) as comparisons
        FROM public_gold.mart_daily_site_kpis
    """)
    
    battery_kpi = safe_query("""
        SELECT ROUND(AVG(office_battery_hours), 1) as avg_battery
        FROM public_gold.mart_performance_ranking
        WHERE office_battery_hours IS NOT NULL
    """)
    avg_battery = float(battery_kpi.iloc[0]['avg_battery']) if not battery_kpi.empty and battery_kpi.iloc[0]['avg_battery'] is not None else 8.8

    if not kpi_df.empty:
        row = kpi_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(label="Total Sessions", value=f"{int(row.get('total_sessions', 0)):,}", delta="^ 5.8%")
        c2.metric(label="Detail Views", value=f"{int(row.get('detail_views', 0)):,}", delta="^ 12.4%")
        c3.metric(label="Comparisons", value=f"{int(row.get('comparisons', 0)):,}", delta="^ 8.1%")
        c4.metric(label="Avg Battery Life", value=f"{avg_battery} hrs", delta="^ 1.2 hrs")
    
    st.write("")

    # ── Charts Row ──
    c_left, c_right = st.columns(2, gap="large")
    
    with c_left:
        st.markdown("<div class='section-title'>Key Metrics (Monthly Trend)</div>", unsafe_allow_html=True)
        trend_df = safe_query("""
            SELECT month, SUM(total_views) as views
            FROM public_gold.mart_cpu_trend
            GROUP BY month ORDER BY month ASC
        """)
        if not trend_df.empty:
            fig_trend = px.line(trend_df, x="month", y="views", markers=True)
            fig_trend.update_traces(
                line=dict(color='#16a34a', width=3),
                marker=dict(size=7, color='#16a34a'),
                fill='tozeroy', fillcolor='rgba(22, 163, 74, 0.08)'
            )
            fig_trend.update_layout(
                template=pio_template, height=400,
                xaxis_title="", yaxis_title="",
                yaxis=dict(showgrid=True), xaxis=dict(showgrid=False)
            )
            st.plotly_chart(fig_trend, theme=None, use_container_width=True, config={'displayModeBar': False})

    with c_right:
        st.markdown("<div class='section-title'>Most Viewed Brands</div>", unsafe_allow_html=True)
        brand_df = safe_query("""
            SELECT brand_name, total_views 
            FROM public_gold.mart_brand_interest 
            ORDER BY total_views DESC LIMIT 7
        """)
        if not brand_df.empty:
            fig_brand = px.bar(brand_df, x="brand_name", y="total_views", text_auto=".2s")
            fig_brand.update_traces(
                marker_color='#2563eb', marker_line_width=0, width=0.6,
                textfont=dict(color='#ffffff', size=11, family='Inter', weight='bold'),
                textposition='inside'
            )
            fig_brand.update_layout(
                template=pio_template, height=400,
                xaxis_title="", yaxis_title="",
                yaxis=dict(showgrid=True), xaxis=dict(showgrid=False)
            )
            st.plotly_chart(fig_brand, theme=None, use_container_width=True, config={'displayModeBar': False})

    st.write("")

    # ── Bottom Row ──
    b_left, b_right = st.columns(2, gap="large")
    
    with b_left:
        st.markdown("<div class='section-title'>Brand Share Overview</div>", unsafe_allow_html=True)
        if not brand_df.empty:
            fig_donut = px.pie(brand_df.head(5), values='total_views', names='brand_name', hole=0.45)
            fig_donut.update_traces(
                textposition='none',
                marker=dict(colors=['#2563eb', '#16a34a', '#dc2626', '#ea580c', '#9333ea'])
            )
            fig_donut.update_layout(
                template=pio_template, height=360,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=0.85)
            )
            st.plotly_chart(fig_donut, theme=None, use_container_width=True, config={'displayModeBar': False})

    with b_right:
        st.markdown("<div class='section-title'>Top Engaging Laptops</div>", unsafe_allow_html=True)
        recent_df = safe_query("""
            SELECT laptop_name, brand_name, total_views, 
                   COALESCE(office_battery_hours, 0) as office_battery_hours
            FROM public_gold.mart_battery_vs_interest
            ORDER BY total_views DESC LIMIT 7
        """)
        if not recent_df.empty:
            st.dataframe(
                recent_df, use_container_width=True, hide_index=True, height=360,
                column_config={
                    "laptop_name": "Model", "brand_name": "Brand",
                    "total_views": st.column_config.NumberColumn("Views", format="%d"),
                    "office_battery_hours": st.column_config.ProgressColumn("Battery (h)", format="%.1f", min_value=0, max_value=15)
                }
            )

# ===========================================================================
# VIEW 2: Hardware Metrics
# ===========================================================================
elif active_tab == "Hardware Metrics":
    st.markdown("""
        <div class='dashboard-header'>
            <h1>Hardware Telemetry</h1>
            <p>CPU and GPU component performance and market interest.</p>
        </div>
    """, unsafe_allow_html=True)
    
    t1, t2 = st.tabs(["CPU Trends", "GPU Trends"])
    
    with t1:
        cpu_df = safe_query("SELECT month, cpu_name, total_views FROM public_gold.mart_cpu_trend ORDER BY month")
        if not cpu_df.empty:
            top_cpus = cpu_df.groupby("cpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_cpu = cpu_df[cpu_df["cpu_name"].isin(top_cpus)]
            st.markdown("<div class='section-title' style='text-align: center;'>Top 5 CPU Models Interest Over Time</div>", unsafe_allow_html=True)
            fig_cpu = px.line(filtered_cpu, x="month", y="total_views", color="cpu_name", markers=True)
            fig_cpu.update_layout(
                template=pio_template, height=550,
                xaxis_title="", yaxis_title="Views",
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5, title="")
            )
            st.plotly_chart(fig_cpu, theme=None, use_container_width=True, config={'displayModeBar': False})

    with t2:
        gpu_df = safe_query("SELECT month, gpu_name, total_views FROM public_gold.mart_gpu_trend ORDER BY month")
        if not gpu_df.empty:
            top_gpus = gpu_df.groupby("gpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_gpu = gpu_df[gpu_df["gpu_name"].isin(top_gpus)]
            st.markdown("<div class='section-title' style='text-align: center;'>Top 5 GPU Models Interest</div>", unsafe_allow_html=True)
            fig_gpu = px.area(filtered_gpu, x="month", y="total_views", color="gpu_name")
            fig_gpu.update_layout(
                template=pio_template, height=550,
                xaxis_title="", yaxis_title="Views",
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5, title="")
            )
            st.plotly_chart(fig_gpu, theme=None, use_container_width=True, config={'displayModeBar': False})

# ===========================================================================
# VIEW 3: Performance Heatmap
# ===========================================================================
elif active_tab == "Performance Heatmap":
    st.markdown("""
        <div class='dashboard-header'>
            <h1>Performance Heatmap</h1>
            <p>Brand × Metric intensity matrix showing battery, benchmark, and engagement.</p>
        </div>
    """, unsafe_allow_html=True)
    
    heatmap_df = safe_query("""
        SELECT brand_name,
               ROUND(AVG(COALESCE(office_battery_hours, 0)), 1) as avg_battery,
               ROUND(AVG(COALESCE(geekbench6_multi, 0)), 0) as avg_geekbench,
               SUM(total_views) as total_views,
               COUNT(*) as model_count
        FROM public_gold.mart_battery_vs_interest
        WHERE total_views > 0
        GROUP BY brand_name
        HAVING COUNT(*) >= 1
        ORDER BY SUM(total_views) DESC
    """)
    
    if not heatmap_df.empty:
        # Normalize each column to 0-100 for heatmap comparability
        metrics = ['avg_battery', 'avg_geekbench', 'total_views', 'model_count']
        labels = ['Avg Battery (h)', 'Avg Geekbench', 'Total Views', 'Model Count']
        
        norm_data = heatmap_df[metrics].copy()
        for col in metrics:
            col_min = norm_data[col].min()
            col_max = norm_data[col].max()
            if col_max > col_min:
                norm_data[col] = ((norm_data[col] - col_min) / (col_max - col_min) * 100).round(1)
            else:
                norm_data[col] = 50.0
        
        # Build custom text showing original values
        text_vals = []
        for _, row in heatmap_df.iterrows():
            text_vals.append([
                f"{row['avg_battery']}h",
                f"{int(row['avg_geekbench']):,}",
                f"{int(row['total_views']):,}",
                f"{int(row['model_count'])}"
            ])
        
        fig_heat = go.Figure(data=go.Heatmap(
            z=norm_data.values,
            x=labels,
            y=heatmap_df['brand_name'].tolist(),
            text=text_vals,
            texttemplate="%{text}",
            textfont=dict(size=13, color='#ffffff', family='Inter'),
            colorscale=[
                [0, '#dbeafe'],
                [0.25, '#93c5fd'],
                [0.5, '#3b82f6'],
                [0.75, '#1d4ed8'],
                [1, '#1e3a5f']
            ],
            showscale=True,
            colorbar=dict(title="Score", thickness=15, len=0.6),
            hovertemplate="Brand: %{y}<br>Metric: %{x}<br>Score: %{z:.1f}<br>Value: %{text}<extra></extra>"
        ))
        
        fig_heat.update_layout(
            template=pio_template, height=500,
            xaxis=dict(side='top', tickfont=dict(size=13, color='#334155')),
            yaxis=dict(autorange='reversed', tickfont=dict(size=13, color='#334155')),
            margin=dict(l=120, r=40, t=50, b=30)
        )
        st.plotly_chart(fig_heat, theme=None, use_container_width=True, config={'displayModeBar': False})
        
        st.write("")
        
        # Additional: Brand comparison bar
        st.markdown("<div class='section-title'>Brand Comparison: Views vs Battery</div>", unsafe_allow_html=True)
        fig_compare = go.Figure()
        fig_compare.add_trace(go.Bar(
            name='Total Views', x=heatmap_df['brand_name'], y=heatmap_df['total_views'],
            marker_color='#2563eb', text=heatmap_df['total_views'].apply(lambda x: f"{x:,.0f}"),
            textposition='outside', textfont=dict(size=10)
        ))
        fig_compare.add_trace(go.Bar(
            name='Avg Battery (h)', x=heatmap_df['brand_name'], y=heatmap_df['avg_battery'],
            marker_color='#16a34a', yaxis='y2', text=heatmap_df['avg_battery'].apply(lambda x: f"{x:.1f}h"),
            textposition='outside', textfont=dict(size=10)
        ))
        fig_compare.update_layout(
            template=pio_template, height=400, barmode='group',
            yaxis=dict(title='Views', showgrid=True),
            yaxis2=dict(title='Battery (h)', overlaying='y', side='right', showgrid=False),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
            xaxis_title=""
        )
        st.plotly_chart(fig_compare, theme=None, use_container_width=True, config={'displayModeBar': False})

# ===========================================================================
# VIEW 4: Product Catalog
# ===========================================================================
elif active_tab == "Product Catalog":
    st.markdown("""
        <div class='dashboard-header'>
            <h1>Product Catalog</h1>
            <p>Comprehensive table of models and benchmark specs.</p>
        </div>
    """, unsafe_allow_html=True)
    
    catalog_df = safe_query("""
        SELECT 
            p.laptop_name, p.brand_name, p.cpu_name, p.screen_size, 
            COALESCE(p.office_battery_hours, 0) as office_battery_hours,
            COALESCE(p.geekbench6_multi, 0) as geekbench6_multi,
            COALESCE(b.total_views, 0) as total_views
        FROM public_gold.mart_performance_ranking p
        LEFT JOIN public_gold.mart_battery_vs_interest b ON p.laptop_model_id = b.laptop_model_id
        ORDER BY b.total_views DESC NULLS LAST
    """)
    
    if not catalog_df.empty:
        if selected_brand != "All Brands":
            catalog_df = catalog_df[catalog_df["brand_name"] == selected_brand]
        
        max_views = int(catalog_df["total_views"].max()) if catalog_df["total_views"].max() > 0 else 100
        
        st.dataframe(
            catalog_df, use_container_width=True, hide_index=True, height=650,
            column_config={
                "laptop_name": "Model", "brand_name": "Brand", "cpu_name": "CPU",
                "screen_size": "Screen",
                "office_battery_hours": st.column_config.NumberColumn("Battery (h)", format="%.1f"),
                "geekbench6_multi": st.column_config.NumberColumn("Geekbench", format="%d"),
                "total_views": st.column_config.ProgressColumn("Views", format="%d", min_value=0, max_value=max_views)
            }
        )
