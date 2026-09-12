"""
LaplapTech Analytics Platform - Modern Executive Dashboard
===========================================================
Enterprise-grade dark analytics web app featuring:
- Seamless dark theme (Graphite/Slate #0f1117 palette)
- Ant Design segmented navigation (streamlit-antd-components)
- High-contrast custom HTML/CSS KPI cards with pill badges
- Polished, dark-themed charts with spline curves and soft gradients
- Interactive AgGrid data tables with search, sorting, and pagination
"""
import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import streamlit_antd_components as sac
from st_aggrid import AgGrid, GridOptionsBuilder

# ---------------------------------------------------------------------------
# Setup & Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

st.set_page_config(
    page_title="LaplapTech | Executive Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------------------------------------------------------------------
# Modern CSS Dark Theme Injection
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    /* Global font & smooth transitions */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* Page background: Deep Graphite */
    .stApp {
        background-color: #0f1117 !important;
        color: #f1f5f9 !important;
    }
    
    /* Hide default Streamlit header and footer */
    header { visibility: hidden !important; }
    footer { visibility: hidden !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    
    /* Layout padding */
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 2.5rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 100% !important;
    }
    
    /* View Header */
    .dashboard-header {
        margin-top: -5px;
        margin-bottom: 22px;
    }
    .dashboard-header h1 {
        color: #f8fafc !important;
        font-size: 1.85rem !important;
        font-weight: 700 !important;
        letter-spacing: -0.5px;
        margin-bottom: 4px;
    }
    .dashboard-header p {
        color: #94a3b8;
        font-size: 0.92rem;
        margin: 0;
    }
    
    /* Section Title */
    .section-title {
        color: #f1f5f9;
        font-size: 1.05rem;
        font-weight: 600;
        margin-bottom: 12px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    /* KPI Card Component (Ultra high contrast) */
    .kpi-card {
        background-color: #181b20;
        border: 1px solid #262a33;
        border-radius: 10px;
        padding: 18px 20px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
        transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: #3b82f6;
        box-shadow: 0 8px 24px rgba(59, 130, 246, 0.12);
    }
    .kpi-card .kpi-label {
        color: #94a3b8;
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 8px;
    }
    .kpi-card .kpi-value {
        color: #f8fafc;
        font-size: 2.1rem;
        font-weight: 700;
        line-height: 1.1;
        letter-spacing: -0.5px;
        margin-bottom: 10px;
    }
    .kpi-badge {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 3px 9px;
        border-radius: 6px;
        font-size: 0.78rem;
        font-weight: 600;
        width: fit-content;
    }
    .kpi-badge.positive {
        background-color: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .kpi-badge.negative {
        background-color: rgba(239, 68, 68, 0.15);
        color: #f87171;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .kpi-badge.neutral {
        background-color: rgba(148, 163, 184, 0.12);
        color: #94a3b8;
        border: 1px solid rgba(148, 163, 184, 0.25);
    }
    
    /* Chart Container Wrapper (No scrollbars, seamless card) */
    .stPlotlyChart {
        background-color: #181b20 !important;
        border-radius: 10px !important;
        border: 1px solid #262a33 !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35) !important;
        padding: 4px !important;
        overflow: hidden !important;
    }
    .stPlotlyChart > div,
    .stPlotlyChart .plot-container,
    .stPlotlyChart .svg-container {
        overflow: hidden !important;
    }
    iframe[title="st.plotly_chart"] {
        overflow: hidden !important;
        border: none !important;
    }
    
    /* AgGrid Container */
    .ag-theme-alpine-dark, .ag-theme-balham-dark, .ag-root-wrapper {
        border-radius: 10px !important;
        border: 1px solid #262a33 !important;
        background-color: #181b20 !important;
    }
    
    /* BaseWeb Selectbox: 100% Dark Mode fix */
    div[data-baseweb="select"] {
        background-color: #181b20 !important;
    }
    div[data-baseweb="select"] > div {
        background-color: #181b20 !important;
        border: 1px solid #262a33 !important;
        border-radius: 8px !important;
        color: #f1f5f9 !important;
    }
    div[data-baseweb="select"] > div:hover {
        border-color: #3b82f6 !important;
    }
    div[data-baseweb="select"] span,
    div[data-baseweb="select"] div {
        color: #f1f5f9 !important;
    }
    div[data-baseweb="select"] svg {
        fill: #94a3b8 !important;
    }
    
    /* Selectbox dropdown popup */
    div[data-baseweb="popover"],
    div[data-baseweb="menu"],
    ul[data-testid="stSelectboxVirtualDropdown"],
    ul[role="listbox"] {
        background-color: #181b20 !important;
        border: 1px solid #262a33 !important;
        border-radius: 8px !important;
        box-shadow: 0 10px 25px rgba(0, 0, 0, 0.6) !important;
    }
    li[role="option"] {
        background-color: #181b20 !important;
        color: #cbd5e1 !important;
        padding: 8px 14px !important;
        font-size: 0.9rem !important;
    }
    li[role="option"]:hover,
    li[role="option"][aria-selected="true"] {
        background-color: #222630 !important;
        color: #3b82f6 !important;
    }
    
    /* Scrollbars */
    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }
    ::-webkit-scrollbar-track {
        background: #0f1117;
    }
    ::-webkit-scrollbar-thumb {
        background: #262a33;
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #3b82f6;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Database Connection & Query Cache
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
# Dark Plotly Helper (Zero white background leaks)
# ---------------------------------------------------------------------------
def apply_dark_theme(fig, height=340):
    fig.update_layout(
        paper_bgcolor='#181b20',
        plot_bgcolor='#181b20',
        font=dict(family='Inter, sans-serif', color='#94a3b8', size=12),
        margin=dict(l=40, r=20, t=25, b=35),
        height=height,
        autosize=True,
        hoverlabel=dict(
            bgcolor='#111217',
            font_size=12,
            font_family='Inter, sans-serif',
            font_color='#f8fafc',
            bordercolor='#262a33'
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor='#222630',
            gridwidth=1,
            zeroline=False,
            tickfont=dict(color='#94a3b8', size=11)
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#222630',
            gridwidth=1,
            zeroline=False,
            tickfont=dict(color='#94a3b8', size=11)
        )
    )
    return fig

# ---------------------------------------------------------------------------
# Custom KPI Card Helper
# ---------------------------------------------------------------------------
def render_kpi_card(title: str, value: str, delta_text: str, delta_type: str = "neutral"):
    arrow = "↑" if delta_type == "positive" else ("↓" if delta_type == "negative" else "•")
    badge_html = f"""
        <div class='kpi-badge {delta_type}'>
            <span>{arrow}</span> <span>{delta_text}</span>
        </div>
    """
    card_html = f"""
        <div class='kpi-card'>
            <div class='kpi-label'>{title}</div>
            <div class='kpi-value'>{value}</div>
            {badge_html}
        </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# AgGrid Dark Theme Custom CSS
# ---------------------------------------------------------------------------
custom_grid_css = {
    ".ag-root-wrapper": {
        "background-color": "#181b20 !important",
        "border": "1px solid #262a33 !important",
        "border-radius": "10px !important",
        "color": "#f1f5f9 !important"
    },
    ".ag-header": {
        "background-color": "#14161b !important",
        "border-bottom": "1px solid #262a33 !important"
    },
    ".ag-header-cell-label": {
        "color": "#94a3b8 !important",
        "font-weight": "600 !important"
    },
    ".ag-row": {
        "background-color": "#181b20 !important",
        "color": "#f1f5f9 !important",
        "border-bottom": "1px solid #222630 !important"
    },
    ".ag-row-hover": {
        "background-color": "#222630 !important"
    },
    ".ag-row-selected": {
        "background-color": "#1e3a5f !important"
    },
    ".ag-paging-panel": {
        "background-color": "#14161b !important",
        "color": "#94a3b8 !important",
        "border-top": "1px solid #262a33 !important"
    },
    ".ag-paging-button": {
        "color": "#94a3b8 !important"
    },
    ".ag-cell": {
        "color": "#f1f5f9 !important"
    },
    ".ag-body-vertical-scroll": {
        "background-color": "#181b20 !important"
    },
    ".ag-body-horizontal-scroll": {
        "background-color": "#181b20 !important"
    },
    ".ag-body-vertical-scroll-viewport": {
        "background-color": "#181b20 !important"
    }
}

# ---------------------------------------------------------------------------
# Top Navigation & Header
# ---------------------------------------------------------------------------
top_c1, top_c2 = st.columns([1.1, 2.9], vertical_alignment="center")

with top_c1:
    st.markdown("""
        <div style='display: flex; align-items: center; gap: 10px; padding: 4px 0;'>
            <div style='width: 9px; height: 9px; border-radius: 50%; background-color: #10b981; box-shadow: 0 0 10px #10b981;'></div>
            <span style='color: #3b82f6; font-size: 1.5rem; font-weight: 800; letter-spacing: -0.5px;'>LAPLAPTECH</span>
            <span style='font-size: 0.7rem; background: #262a33; color: #94a3b8; padding: 2px 7px; border-radius: 5px; font-weight: 700;'>LIVE</span>
        </div>
    """, unsafe_allow_html=True)

with top_c2:
    active_tab = sac.segmented(
        items=[
            sac.SegmentedItem(label="Overview", icon="grid-fill"),
            sac.SegmentedItem(label="Hardware Metrics", icon="cpu-fill"),
            sac.SegmentedItem(label="Performance Heatmap", icon="fire"),
            sac.SegmentedItem(label="Product Catalog", icon="laptop-fill"),
        ],
        align="end",
        size="md",
        radius="lg",
        color="#2b5c8f",
        bg_color="#181b20",
        divider=False,
        use_container_width=True,
        key="main_nav_tab"
    )

st.markdown("<div style='margin-bottom: 18px;'></div>", unsafe_allow_html=True)

# ===========================================================================
# VIEW 1: Overview
# ===========================================================================
if active_tab == "Overview":
    head_col1, head_col2 = st.columns([3, 1.3], vertical_alignment="center")
    with head_col1:
        st.markdown("""
            <div class='dashboard-header' style='margin-bottom: 0;'>
                <h1>Overview</h1>
                <p>Key performance metrics and hardware engagement telemetry.</p>
            </div>
        """, unsafe_allow_html=True)
    with head_col2:
        time_scope = sac.segmented(
            items=[
                sac.SegmentedItem(label="Today", icon="clock-history"),
                sac.SegmentedItem(label="All-Time Total", icon="infinity"),
            ],
            align="end",
            size="sm",
            radius="md",
            color="#2b5c8f",
            bg_color="#181b20",
            key="kpi_time_scope"
        )
    
    st.write("")
    
    battery_kpi = safe_query("""
        SELECT ROUND(AVG(office_battery_hours), 1) as avg_battery
        FROM public_gold.mart_performance_ranking
        WHERE office_battery_hours IS NOT NULL
    """)
    avg_battery = float(battery_kpi.iloc[0]['avg_battery']) if not battery_kpi.empty and battery_kpi.iloc[0]['avg_battery'] is not None else 8.8

    if time_scope == "All-Time Total":
        total_kpi_df = safe_query("""
            SELECT 
                COALESCE(SUM(total_sessions), 0) as total_sessions,
                COALESCE(SUM(sessions_with_detail_view), 0) as total_details,
                COALESCE(SUM(sessions_with_comparison), 0) as total_comps,
                (SELECT COUNT(*) FROM public_gold.mart_performance_ranking) as total_models
            FROM public_gold.mart_daily_site_kpis
        """)
        if not total_kpi_df.empty:
            row = total_kpi_df.iloc[0]
            tot_sess = int(row.get('total_sessions') or 0)
            tot_det = int(row.get('total_details') or 0)
            tot_comp = int(row.get('total_comps') or 0)
            tot_models = int(row.get('total_models') or 0)
            det_rate = f"{(tot_det / tot_sess * 100):.1f}% view rate" if tot_sess > 0 else "0% view rate"

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi_card("Total Sessions", f"{tot_sess:,}", "All-time cumulative", "positive")
            with c2:
                render_kpi_card("Total Detail Views", f"{tot_det:,}", det_rate, "positive")
            with c3:
                render_kpi_card("Total Comparisons", f"{tot_comp:,}", "Feature telemetry", "neutral")
            with c4:
                render_kpi_card("Catalog Inventory", f"{tot_models} Models", f"{avg_battery}h avg battery", "neutral")
    else:
        # Today's metrics (default)
        kpi_df = safe_query("""
            WITH recent_dates AS (
                SELECT MAX(event_date) as max_date FROM public_gold.mart_daily_site_kpis
            )
            SELECT 
                (SELECT total_sessions FROM public_gold.mart_daily_site_kpis WHERE event_date = (SELECT max_date FROM recent_dates)) as today_sessions,
                (SELECT SUM(total_sessions) FROM public_gold.mart_daily_site_kpis WHERE event_date > (SELECT max_date - interval '7 days' FROM recent_dates) AND event_date <= (SELECT max_date FROM recent_dates)) as week_sessions,
                
                (SELECT sessions_with_detail_view FROM public_gold.mart_daily_site_kpis WHERE event_date = (SELECT max_date FROM recent_dates)) as today_details,
                (SELECT SUM(sessions_with_detail_view) FROM public_gold.mart_daily_site_kpis WHERE event_date > (SELECT max_date - interval '7 days' FROM recent_dates) AND event_date <= (SELECT max_date FROM recent_dates)) as week_details,
                
                (SELECT sessions_with_comparison FROM public_gold.mart_daily_site_kpis WHERE event_date = (SELECT max_date FROM recent_dates)) as today_comps,
                (SELECT SUM(sessions_with_comparison) FROM public_gold.mart_daily_site_kpis WHERE event_date > (SELECT max_date - interval '7 days' FROM recent_dates) AND event_date <= (SELECT max_date FROM recent_dates)) as week_comps
        """)
        if not kpi_df.empty:
            row = kpi_df.iloc[0]
            t_sess = int(row.get('today_sessions') or 0)
            w_sess = int(row.get('week_sessions') or 0) / 7.0
            d_sess = t_sess - w_sess
            p_sess = f"{d_sess/w_sess * 100:+.1f}% vs 7-day avg" if w_sess > 0 else "+0%"
            sess_type = "positive" if d_sess > 0 else ("negative" if d_sess < 0 else "neutral")

            t_det = int(row.get('today_details') or 0)
            w_det = int(row.get('week_details') or 0) / 7.0
            d_det = t_det - w_det
            p_det = f"{d_det/w_det * 100:+.1f}% vs 7-day avg" if w_det > 0 else "+0%"
            det_type = "positive" if d_det > 0 else ("negative" if d_det < 0 else "neutral")

            t_comp = int(row.get('today_comps') or 0)
            w_comp = int(row.get('week_comps') or 0) / 7.0
            d_comp = t_comp - w_comp
            p_comp = f"{d_comp/w_comp * 100:+.1f}% vs 7-day avg" if w_comp > 0 else "+0%"
            comp_type = "positive" if d_comp > 0 else ("negative" if d_comp < 0 else "neutral")

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi_card("Today's Sessions", f"{t_sess:,}", p_sess, sess_type)
            with c2:
                render_kpi_card("Today's Detail Views", f"{t_det:,}", p_det, det_type)
            with c3:
                render_kpi_card("Today's Comparisons", f"{t_comp:,}", p_comp, comp_type)
            with c4:
                render_kpi_card("Avg Battery Life (All)", f"{avg_battery} hrs", "Benchmark fleet avg", "neutral")

    
    st.write("")

    # ── Charts Row 1 ──
    c_left, c_right = st.columns(2, gap="medium")
    
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
                line=dict(color='#3b82f6', width=3, shape='spline'),
                marker=dict(size=7, color='#60a5fa', line=dict(color='#181b20', width=2)),
                fill='tozeroy',
                fillcolor='rgba(59, 130, 246, 0.12)'
            )
            apply_dark_theme(fig_trend, height=380)
            fig_trend.update_layout(
                xaxis_title="", yaxis_title="",
                yaxis=dict(showgrid=True, gridcolor='#222630'),
                xaxis=dict(showgrid=False)
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
                marker=dict(color='#3b82f6', line=dict(width=0)),
                width=0.55,
                textfont=dict(color='#ffffff', size=11, family='Inter', weight='bold'),
                textposition='inside'
            )
            apply_dark_theme(fig_brand, height=380)
            fig_brand.update_layout(
                xaxis_title="", yaxis_title="",
                yaxis=dict(showgrid=True, gridcolor='#222630'),
                xaxis=dict(showgrid=False)
            )
            st.plotly_chart(fig_brand, theme=None, use_container_width=True, config={'displayModeBar': False})

    st.write("")

    # ── Bottom Row 2 ──
    b_left, b_right = st.columns([1, 1], gap="medium")
    
    with b_left:
        st.markdown("<div class='section-title'>Brand Share Overview</div>", unsafe_allow_html=True)
        if not brand_df.empty:
            fig_donut = px.pie(brand_df.head(6), values='total_views', names='brand_name', hole=0.55)
            fig_donut.update_traces(
                textposition='none',
                marker=dict(
                    colors=['#3b82f6', '#10b981', '#f59e0b', '#ec4899', '#8b5cf6', '#06b6d4', '#64748b'],
                    line=dict(color='#181b20', width=2)
                )
            )
            apply_dark_theme(fig_donut, height=360)
            fig_donut.update_layout(
                showlegend=True,
                legend=dict(
                    orientation="v",
                    yanchor="middle",
                    y=0.5,
                    xanchor="left",
                    x=0.82,
                    font=dict(color='#94a3b8', size=11)
                ),
                annotations=[
                    dict(
                        text="Brand<br>Share",
                        x=0.41, y=0.5,
                        font_size=13,
                        font_color='#f1f5f9',
                        font_family='Inter',
                        showarrow=False,
                        font_weight='bold'
                    )
                ]
            )
            st.plotly_chart(fig_donut, theme=None, use_container_width=True, config={'displayModeBar': False})

    with b_right:
        st.markdown("<div class='section-title'>Top Engaging Laptops</div>", unsafe_allow_html=True)
        recent_df = safe_query("""
            SELECT laptop_name, brand_name, total_views, 
                   COALESCE(office_battery_hours, 0) as office_battery_hours
            FROM public_gold.mart_battery_vs_interest
            ORDER BY total_views DESC LIMIT 15
        """)
        if not recent_df.empty:
            gob = GridOptionsBuilder.from_dataframe(recent_df)
            gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=7)
            gob.configure_default_column(sortable=True, filterable=True, resizable=True)
            gob.configure_column("laptop_name", header_name="Model", minWidth=170)
            gob.configure_column("brand_name", header_name="Brand", minWidth=100)
            gob.configure_column("total_views", header_name="Views", type=["numericColumn"], minWidth=90)
            gob.configure_column("office_battery_hours", header_name="Battery (h)", type=["numericColumn"], minWidth=105)
            grid_options = gob.build()

            AgGrid(
                recent_df,
                gridOptions=grid_options,
                theme="alpine",
                custom_css=custom_grid_css,
                height=360,
                key="top_laptops_grid"
            )

# ===========================================================================
# VIEW 2: Hardware Metrics
# ===========================================================================
elif active_tab == "Hardware Metrics":
    st.markdown("""
        <div class='dashboard-header'>
            <h1>Hardware Telemetry</h1>
            <p>CPU and GPU component performance and market interest trends.</p>
        </div>
    """, unsafe_allow_html=True)
    
    hw_tab = sac.segmented(
        items=[
            sac.SegmentedItem(label="CPU Trends", icon="cpu"),
            sac.SegmentedItem(label="GPU Trends", icon="gpu-card"),
        ],
        align="center",
        size="sm",
        radius="md",
        color="#2b5c8f",
        bg_color="#181b20",
        key="hw_segmented_selector"
    )
    
    if hw_tab == "CPU Trends":
        cpu_df = safe_query("""
            SELECT month, cpu_name, SUM(total_views) as total_views 
            FROM public_gold.mart_cpu_trend 
            GROUP BY month, cpu_name 
            ORDER BY month ASC
        """)
        if not cpu_df.empty:
            top_cpus = cpu_df.groupby("cpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_cpu = cpu_df[cpu_df["cpu_name"].isin(top_cpus)]
            st.markdown("<div class='section-title' style='justify-content: center;'>Top 5 CPU Models Interest Over Time</div>", unsafe_allow_html=True)
            
            fig_cpu = px.line(
                filtered_cpu, x="month", y="total_views", color="cpu_name", markers=True,
                color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b', '#ec4899', '#8b5cf6']
            )
            fig_cpu.update_traces(line=dict(width=2.5), marker=dict(size=5))
            apply_dark_theme(fig_cpu, height=420)
            fig_cpu.update_layout(
                xaxis_title="", yaxis_title="Views",
                legend=dict(orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5, title="")
            )
            st.plotly_chart(fig_cpu, theme=None, use_container_width=True, config={'displayModeBar': False})

    elif hw_tab == "GPU Trends":
        gpu_df = safe_query("""
            SELECT month, gpu_name, SUM(total_views) as total_views 
            FROM public_gold.mart_gpu_trend 
            GROUP BY month, gpu_name 
            ORDER BY month ASC
        """)
        if not gpu_df.empty:
            top_gpus = gpu_df.groupby("gpu_name")["total_views"].sum().nlargest(5).index.tolist()
            filtered_gpu = gpu_df[gpu_df["gpu_name"].isin(top_gpus)]
            st.markdown("<div class='section-title' style='justify-content: center;'>Top 5 GPU Models Interest</div>", unsafe_allow_html=True)
            
            fig_gpu = px.area(
                filtered_gpu, x="month", y="total_views", color="gpu_name",
                color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b', '#ec4899', '#8b5cf6']
            )
            apply_dark_theme(fig_gpu, height=420)
            fig_gpu.update_layout(
                xaxis_title="", yaxis_title="Views",
                legend=dict(orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5, title="")
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
               ROUND(AVG(office_battery_hours)::numeric, 1) as avg_battery,
               ROUND(AVG(geekbench6_multi)::numeric, 0) as avg_geekbench,
               SUM(total_views) as total_views,
               COUNT(*) as model_count
        FROM public_gold.mart_battery_vs_interest
        WHERE total_views > 0
        GROUP BY brand_name
        HAVING COUNT(*) >= 1
        ORDER BY SUM(total_views) DESC
    """)
    
    if not heatmap_df.empty:
        metrics = ['avg_battery', 'avg_geekbench', 'total_views', 'model_count']
        labels = ['Avg Battery (h)', 'Avg Geekbench', 'Total Views', 'Model Count']
        
        norm_data = heatmap_df[metrics].copy()
        for col in metrics:
            col_min = norm_data[col].min()
            col_max = norm_data[col].max()
            if pd.notnull(col_min) and pd.notnull(col_max) and col_max > col_min:
                norm_data[col] = ((norm_data[col] - col_min) / (col_max - col_min) * 100).round(1)
            else:
                norm_data[col] = 50.0
        norm_data = norm_data.fillna(0)
        
        text_vals = []
        for _, row in heatmap_df.iterrows():
            b_val = row['avg_battery']
            g_val = row['avg_geekbench']
            text_vals.append([
                f"{b_val:.1f}h" if pd.notnull(b_val) and b_val > 0 else "—",
                f"{int(g_val):,}" if pd.notnull(g_val) and g_val > 0 else "—",
                f"{int(row['total_views']):,}",
                f"{int(row['model_count'])}"
            ])
        
        fig_heat = go.Figure(data=go.Heatmap(
            z=norm_data.values,
            x=labels,
            y=heatmap_df['brand_name'].tolist(),
            text=text_vals,
            texttemplate="%{text}",
            textfont=dict(size=12, color='#ffffff', family='Inter'),
            colorscale=[
                [0, '#111217'],
                [0.25, '#1e293b'],
                [0.5, '#1d4ed8'],
                [0.75, '#3b82f6'],
                [1, '#60a5fa']
            ],
            showscale=True,
            colorbar=dict(
                title=dict(text="Index", font=dict(color='#94a3b8', size=11)),
                thickness=14,
                len=0.7,
                tickfont=dict(color='#94a3b8', size=10)
            ),
            hovertemplate="Brand: %{y}<br>Metric: %{x}<br>Score: %{z:.1f}<br>Value: %{text}<extra></extra>"
        ))
        
        apply_dark_theme(fig_heat, height=400)
        fig_heat.update_layout(
            xaxis=dict(side='top', tickfont=dict(size=12, color='#f1f5f9')),
            yaxis=dict(autorange='reversed', tickfont=dict(size=12, color='#f1f5f9')),
            margin=dict(l=120, r=40, t=50, b=30)
        )
        st.plotly_chart(fig_heat, theme=None, use_container_width=True, config={'displayModeBar': False})
        
        st.write("")
        
        comp_head1, comp_head2 = st.columns([3, 1.2], vertical_alignment="center")
        with comp_head1:
            st.markdown("<div class='section-title' style='margin-bottom: 0;'>Brand Comparison: Views vs Battery</div>", unsafe_allow_html=True)
        with comp_head2:
            compare_mode = sac.segmented(
                items=[
                    sac.SegmentedItem(label="Combo Chart", icon="bar-chart-line-fill"),
                    sac.SegmentedItem(label="Side-by-Side", icon="columns"),
                ],
                size="xs",
                radius="md",
                color="#2b5c8f",
                bg_color="#181b20",
                key="compare_chart_mode"
            )
        
        if compare_mode == "Side-by-Side":
            side_col1, side_col2 = st.columns(2)
            with side_col1:
                st.markdown("<p style='color: #94a3b8; font-size: 0.82rem; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 6px;'>Total Views by Brand</p>", unsafe_allow_html=True)
                fig_views = go.Figure()
                max_v = heatmap_df['total_views'].max()
                fig_views.add_trace(go.Bar(
                    x=heatmap_df['brand_name'],
                    y=heatmap_df['total_views'],
                    marker_color='#3b82f6',
                    text=heatmap_df['total_views'].apply(lambda x: f"{x/1000:.1f}k" if x >= 1000 else f"{x:,}"),
                    textposition='outside',
                    textfont=dict(size=10, color='#94a3b8'),
                    hovertemplate="<b>%{x}</b><br>Total Views: %{y:,.0f}<extra></extra>"
                ))
                apply_dark_theme(fig_views, height=360)
                fig_views.update_layout(
                    yaxis=dict(title='Total Views', range=[0, max_v * 1.15], showgrid=True, gridcolor='#222630'),
                    margin=dict(l=50, r=20, t=30, b=40),
                    xaxis_title=""
                )
                st.plotly_chart(fig_views, theme=None, use_container_width=True, config={'displayModeBar': False})
            
            with side_col2:
                st.markdown("<p style='color: #94a3b8; font-size: 0.82rem; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 6px;'>Average Battery Life (Hours)</p>", unsafe_allow_html=True)
                battery_df = heatmap_df[pd.notnull(heatmap_df['avg_battery']) & (heatmap_df['avg_battery'] > 0)]
                fig_batt = go.Figure()
                if not battery_df.empty:
                    fig_batt.add_trace(go.Bar(
                        x=battery_df['brand_name'],
                        y=battery_df['avg_battery'],
                        marker_color='#10b981',
                        text=battery_df['avg_battery'].apply(lambda x: f"{x:.1f} hrs"),
                        textposition='outside',
                        textfont=dict(size=11, color='#34d399'),
                        width=0.35,
                        hovertemplate="<b>%{x}</b><br>Avg Battery: %{y:.1f} hrs<extra></extra>"
                    ))
                    apply_dark_theme(fig_batt, height=360)
                    fig_batt.update_layout(
                        yaxis=dict(title='Battery (hours)', range=[0, 16], showgrid=True, gridcolor='#222630'),
                        margin=dict(l=50, r=20, t=30, b=40),
                        xaxis_title=""
                    )
                else:
                    fig_batt.add_annotation(text="No battery benchmark recorded", showarrow=False, font=dict(color="#94a3b8", size=13))
                    apply_dark_theme(fig_batt, height=360)
                st.plotly_chart(fig_batt, theme=None, use_container_width=True, config={'displayModeBar': False})
        else:
            # Combo Chart (Default) - Clean dual-axis with zero text collision
            fig_compare = go.Figure()
            max_views = heatmap_df['total_views'].max()
            
            # 1. Total Views Bar (Blue)
            fig_compare.add_trace(go.Bar(
                name='Total Views',
                x=heatmap_df['brand_name'],
                y=heatmap_df['total_views'],
                marker=dict(color='#3b82f6', opacity=0.9),
                text=heatmap_df['total_views'].apply(lambda x: f"{x/1000:.1f}k" if x >= 1000 else f"{x:,}"),
                textposition='outside',
                textfont=dict(size=10, color='#94a3b8'),
                hovertemplate="<b>%{x}</b><br>Total Views: %{y:,.0f}<extra></extra>"
            ))
            
            # 2. Avg Battery Line + Scatter (Emerald) - only plot points > 0 to eliminate 0.0h clutter
            battery_vals = [v if pd.notnull(v) and v > 0 else None for v in heatmap_df['avg_battery']]
            battery_labels = [f"{v:.1f}h" if pd.notnull(v) and v > 0 else "" for v in heatmap_df['avg_battery']]
            
            fig_compare.add_trace(go.Scatter(
                name='Avg Battery (h)',
                x=heatmap_df['brand_name'],
                y=battery_vals,
                yaxis='y2',
                mode='lines+markers+text',
                line=dict(color='#10b981', width=3, dash='dot'),
                marker=dict(color='#10b981', size=11, symbol='diamond', line=dict(color='#ffffff', width=1.5)),
                text=battery_labels,
                textposition='top center',
                textfont=dict(size=12, color='#34d399'),
                hovertemplate="<b>%{x}</b><br>Avg Battery: %{y:.1f} hrs<extra></extra>"
            ))
            
            apply_dark_theme(fig_compare, height=390)
            fig_compare.update_layout(
                yaxis=dict(
                    title=dict(text='Total Views', font=dict(color='#94a3b8', size=11)),
                    range=[0, max_views * 1.18],
                    showgrid=True,
                    gridcolor='#222630'
                ),
                yaxis2=dict(
                    title=dict(text='Battery Life (Hours)', font=dict(color='#10b981', size=11)),
                    overlaying='y',
                    side='right',
                    range=[0, 16],
                    showgrid=False,
                    tickfont=dict(color='#10b981', size=10)
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
                margin=dict(l=50, r=65, t=40, b=40),
                xaxis_title=""
            )
            st.plotly_chart(fig_compare, theme=None, use_container_width=True, config={'displayModeBar': False})

# ===========================================================================
# VIEW 4: Product Catalog
# ===========================================================================
elif active_tab == "Product Catalog":
    cat_h1, cat_h2 = st.columns([3, 1.2], vertical_alignment="center")
    with cat_h1:
        st.markdown("""
            <div class='dashboard-header' style='margin-bottom: 0;'>
                <h1>Product Catalog</h1>
                <p>Interactive table of models and benchmark specs with multi-column sorting and filtering.</p>
            </div>
        """, unsafe_allow_html=True)
    with cat_h2:
        brands_data = safe_query("SELECT DISTINCT brand_name FROM public_gold.mart_brand_interest ORDER BY brand_name")
        brand_options = ["All Brands"] + (brands_data["brand_name"].tolist() if not brands_data.empty else [])
        selected_brand = st.selectbox("Filter Brand", brand_options, index=0, label_visibility="collapsed")
    
    st.write("")
    
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
        
        gob = GridOptionsBuilder.from_dataframe(catalog_df)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=15)
        gob.configure_default_column(sortable=True, filterable=True, resizable=True)
        gob.configure_column("laptop_name", header_name="Model", minWidth=200)
        gob.configure_column("brand_name", header_name="Brand", minWidth=120)
        gob.configure_column("cpu_name", header_name="Processor", minWidth=180)
        gob.configure_column("screen_size", header_name="Screen", minWidth=90)
        gob.configure_column("office_battery_hours", header_name="Battery (h)", type=["numericColumn"], minWidth=110)
        gob.configure_column("geekbench6_multi", header_name="Geekbench 6", type=["numericColumn"], minWidth=120)
        gob.configure_column("total_views", header_name="Views", type=["numericColumn"], minWidth=100)
        grid_options = gob.build()

        AgGrid(
            catalog_df,
            gridOptions=grid_options,
            theme="alpine",
            custom_css=custom_grid_css,
            height=620,
            key="product_catalog_grid"
        )
