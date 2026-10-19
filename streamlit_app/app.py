"""LaplapTech research dashboard. All analytics are served from gold_* models."""
from datetime import datetime, timedelta
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import streamlit as st

from streamlit_app.data import (
    GOLD_NAMES, brand_share, filter_days, literal_search, load_gold,
    numeric_label, summarize_daily,
)

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
st.set_page_config(page_title='LaplapTech | Analytics', page_icon='◈', layout='wide')
VN = ZoneInfo('Asia/Ho_Chi_Minh')
COLORS = ['#68d8ee', '#a78bfa', '#34d399', '#fbbf24', '#fb7185', '#60a5fa', '#94a3b8']
st.markdown("""
<style>
.stApp { background:#0c111b; }
.block-container { padding-top:4rem; max-width:1500px; }
h1,h2,h3 { letter-spacing:-.035em; }
h1 { font-size:2.5rem!important; }
[data-testid="stMetric"] { background:#141d2c; border:1px solid #243248;
 padding:18px 20px; border-radius:14px; min-height:130px; }
[data-testid="stMetricValue"], [data-testid="stMetricValue"] div {
 color:#f1f5f9; font-size:clamp(1.35rem,3vw,2rem)!important; }
[data-testid="stMetricLabel"] p { white-space:normal!important; overflow:visible!important; font-size:.8rem; }
@media(max-width:900px) { [data-testid="stMetric"] { padding:14px 12px; } }
[data-testid="stMetricLabel"] { color:#a9bad0; }
[data-testid="stTabs"] button { font-size:.95rem; }
[data-testid="stDataFrame"] { border:1px solid #243248; border-radius:10px; }
.eyebrow { color:#68d8ee; font-size:.75rem; font-weight:700; letter-spacing:.18em; }
.intro { color:#9aafc9; font-size:1.04rem; margin-bottom:1.4rem; }
</style>
""", unsafe_allow_html=True)

head, action = st.columns([5, 1])
with head:
    st.markdown('<div class="eyebrow">LAPLAPTECH / RESEARCH ANALYTICS</div>', unsafe_allow_html=True)
    st.title('Laptop interest & performance')
    st.markdown('<div class="intro">Explore traffic, product interest and measured hardware performance.</div>', unsafe_allow_html=True)
with action:
    if st.button('Refresh data', width='stretch'):
        load_gold.clear()
        st.rerun()

try:
    gold = {name: load_gold(name) for name in GOLD_NAMES}
except Exception:
    logging.getLogger(__name__).exception('Dashboard warehouse query failed')
    st.error('Data is unavailable. Check the warehouse connection and build the Gold models before opening the dashboard.')
    st.stop()

daily = gold['gold_daily_site_kpis'].sort_values('event_date')
today = datetime.now(VN).date()
latest = pd.to_datetime(daily['latest_event_at'], utc=True).max() if not daily.empty else pd.NaT
if pd.isna(latest):
    st.warning('No event data is available. Catalog and benchmark data may still be available below.')
else:
    latest_vn = latest.tz_convert(VN)
    hours = (datetime.now(VN) - latest_vn.to_pydatetime()).total_seconds() / 3600
    message = f"Latest received event: {latest_vn:%d %b %Y, %H:%M} VN · batch schedule 08:15 / 16:45 · query cache 5 min"
    if hours > 48:
        st.error(message + ' · source events older than 48 hours')
    elif hours > 24:
        st.warning(message + ' · source events older than 24 hours')
    else:
        st.caption(message)
st.caption('Event recency describes the dataset, not the completion time of the latest pipeline run.')


def chart(fig, height=350):
    fig.update_layout(
        template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)', height=height,
        font=dict(color='#a9bad0'), margin=dict(l=15, r=15, t=30, b=25),
        legend=dict(orientation='h', y=-.18), colorway=COLORS,
    )
    st.plotly_chart(fig, width='stretch', config={'displayModeBar': False})


def table(frame):
    st.dataframe(frame, hide_index=True, width='stretch', column_config={c: c.replace('_', ' ').capitalize() for c in frame.columns})


overview, market, behavior, performance, catalog = st.tabs([
    'Overview', 'Brands & hardware', 'Behavior', 'Performance', 'Catalog'
])

with overview:
    st.subheader('Traffic overview')
    scope = st.radio('Reporting period', ['Today', 'Last 7 days', 'All available', 'Custom'], index=1, horizontal=True)
    start, end = today, today
    if scope == 'Last 7 days':
        start = today - timedelta(days=6)
    elif scope == 'All available' and not daily.empty:
        start = pd.to_datetime(daily.event_date).min().date()
        end = max(today, pd.to_datetime(daily.event_date).max().date())
    elif scope == 'Custom':
        selected = st.date_input('Date range', value=(today-timedelta(days=6), today), max_value=today)
        if isinstance(selected, (tuple, list)) and len(selected) == 2:
            start, end = selected
        else:
            st.info('Choose both the start and end date.')
    window = filter_days(daily, start, end)
    metrics = summarize_daily(window)
    st.caption(f'{start:%d %b %Y} — {end:%d %b %Y} · calendar dates in Vietnam time · today is a partial day')
    cols = st.columns(4)
    cols[0].metric('Events', f"{metrics['total_events']:,}")
    cols[1].metric('Active sessions' if start == end else 'Active session-days', f"{metrics['total_sessions']:,}",
                   help='Distinct sessions within each calendar day. Across multiple days, a returning session can count again.')
    cols[2].metric('Detail pageviews', f"{metrics['detail_pageviews']:,}", help='Pageview events on DeviceDetail, not the number of sessions.')
    cols[3].metric('Comparison pageviews', f"{metrics['comparison_pageviews']:,}")
    st.write('')
    if window.empty:
        st.info('No events in this reporting period. A successful query with no events is shown as zero; a connection failure is shown as an error.')
    else:
        chart(px.line(window.rename(columns={'total_events': 'Events', 'total_pageviews': 'Pageviews'}), x='event_date', y=['Events', 'Pageviews'], markers=True,
                      labels={'event_date': 'Vietnam calendar date', 'value': 'Events', 'variable': 'Metric'}))
    left, right = st.columns(2)
    left.metric('Detail reach', numeric_label(metrics['detail_reach_pct'], '%'),
                help='Session-days with pageview and detail / session-days with pageview.')
    right.metric('Comparison co-reach', numeric_label(metrics['comparison_reach_pct'], '%'),
                 help='Session-days with both detail and comparison / session-days with detail. No ordering requirement.')
    with st.expander('Metric definitions and daily data'):
        st.write('These rates measure reach within a session-day. They are not sequential conversion rates. Session-days cannot be added to obtain all-time unique sessions.')
        table(window)

with market:
    st.subheader('Brand interest')
    st.caption('All available event history · detail pageviews mapped to the current product catalog')
    brands = gold['gold_brand_interest'].sort_values(['total_views', 'brand_name'], ascending=[False, True])
    left, right = st.columns([1.2, 1])
    with left:
        if not brands.empty:
            chart(px.bar(brands.head(10), x='brand_name', y='total_views', color_discrete_sequence=COLORS,
                         labels={'brand_name': 'Brand', 'total_views': 'Detail pageviews'}))
    with right:
        share = brand_share(brands)
        if not share.empty and share.total_views.sum() > 0:
            chart(px.pie(share, values='total_views', names='brand_name', hole=.65, color_discrete_sequence=COLORS))
        else:
            st.info('No product detail views are available.')
    table(brands[['brand_name', 'total_views', 'view_sessions', 'comparison_sessions', 'comparison_rate_pct']])
    st.caption('Brand comparison rate uses sessions with both detail views and attributed comparison interactions, divided by detail-view sessions. These are all-time sessions, not daily session-days.')
    st.subheader('Hardware interest over calendar months')
    hardware = st.radio('Component', ['CPU', 'GPU'], horizontal=True)
    frame = gold[f'gold_{hardware.lower()}_trend']
    component = hardware.lower() + '_name'
    if not frame.empty:
        grouped = frame.groupby(['month', component], as_index=False)['total_views'].sum()
        top = grouped.groupby(component).total_views.sum().nlargest(5).index
        chart(px.line(grouped[grouped[component].isin(top)].sort_values('month'),
                      x='month', y='total_views', color=component, markers=True, labels={'month': 'Vietnam month', 'total_views': 'Detail pageviews', component: hardware + ' model'}))
    st.caption('Monthly detail pageviews in Vietnam time; no minimum-view threshold is applied.')
    with st.expander('Brand pair co-occurrence and popular specifications'):
        st.write('Pairs indicate products involved in comparison interactions in the same session-day, including selected devices and device arrays; not necessarily one simultaneous comparison action.')
        table(gold['gold_brand_comparison'].sort_values('comparison_session_days', ascending=False))
        table(gold['gold_spec_popularity'].sort_values('total_views', ascending=False))

with behavior:
    st.subheader('Daily step reach')
    st.caption('All available dates · each step is independently counted per session-day; no ordering requirement')
    funnel = gold['gold_behavior_funnel_daily']
    steps = ['step_1_pageview', 'step_2_product_detail', 'step_3_select_compare', 'step_4_comparison_page', 'step_5_sort_action']
    reached = pd.DataFrame({
        'Step': ['Pageview', 'Product detail', 'Select for comparison', 'Comparison page', 'Sort action'],
        'Session-days': [int(funnel[s].sum()) if not funnel.empty else 0 for s in steps],
    })
    chart(px.bar(reached, x='Session-days', y='Step', orientation='h', color_discrete_sequence=COLORS))
    left, right = st.columns([1.5, 1])
    with left:
        st.subheader('Top search queries')
        searches = gold['gold_search_analytics'].sort_values(['search_volume', 'search_keyword'], ascending=[False, True]).head(25)
        if not searches.empty:
            chart(px.bar(searches, x='search_keyword', y='search_volume', color_discrete_sequence=COLORS))
        else:
            st.info('No search events are available.')
        st.caption('Search volume alone does not establish unmet demand or zero-result searches.')
    with right:
        st.subheader('Visitor platforms')
        platforms = gold['gold_user_os']
        if not platforms.empty:
            chart(px.pie(platforms, values='sessions', names='user_os', hole=.6, color_discrete_sequence=COLORS))
        st.caption('One OS per all-time session: first non-empty OS ordered by event time and ID. Missing OS is Unknown.')

with performance:
    st.subheader('Measured performance')
    ranking = gold['gold_performance_ranking']
    measured = ranking.dropna(subset=['office_battery_hours', 'geekbench6_multi'])
    cols = st.columns(3)
    cols[0].metric('Active catalog models', f'{len(ranking):,}')
    cols[1].metric('Battery measurements', f"{ranking.office_battery_hours.notna().sum():,}")
    cols[2].metric('Average office battery', numeric_label(ranking.office_battery_hours.mean(), ' h'))
    st.caption('One benchmark per laptop: highest benchmark ID. Missing measurements remain N/A, not zero.')
    if not measured.empty:
        chart(px.scatter(measured, x='office_battery_hours', y='geekbench6_multi',
                         color='brand_name', hover_name='laptop_name',
                         labels={'office_battery_hours': 'Office battery (hours)', 'geekbench6_multi': 'Geekbench 6 multi-core'}))
    else:
        st.info('No models have both battery and CPU measurements.')
    left, right = st.columns(2)
    efficiency = gold['gold_performance_efficiency']
    with left:
        st.subheader('Performance per kilogram')
        valid = efficiency.dropna(subset=['performance_per_kg']).nlargest(10, 'performance_per_kg')
        if not valid.empty:
            chart(px.bar(valid, x='performance_per_kg', y='laptop_name', orientation='h', color='brand_name'))
        else:
            st.info('Weight or benchmark measurements are unavailable.')
    with right:
        st.subheader('Battery life and product interest')
        interest = gold['gold_battery_vs_interest'].dropna(subset=['office_battery_hours'])
        if not interest.empty:
            chart(px.scatter(interest, x='office_battery_hours', y='total_views', color='brand_name', hover_name='laptop_name',
                             labels={'total_views': 'Detail pageviews', 'office_battery_hours': 'Office battery (hours)'}))
        else:
            st.info('Battery measurements are unavailable.')
    with st.expander('Benchmark and efficiency details'):
        table(efficiency)

with catalog:
    st.subheader('Product catalog')
    st.caption('Active catalog models · benchmark fields without measurements are left empty')
    frame = gold['gold_performance_ranking'].merge(
        gold['gold_battery_vs_interest'][['laptop_model_id', 'total_views']],
        on='laptop_model_id', how='left', validate='one_to_one',
    )
    frame['total_views'] = frame['total_views'].fillna(0)
    left, right = st.columns([3, 1])
    query = left.text_input('Search models', placeholder='Try a model name; punctuation is treated literally')
    options = ['All brands'] + sorted(frame.brand_name.dropna().unique().tolist())
    selected = right.selectbox('Brand', options)
    if selected != 'All brands':
        frame = frame[frame.brand_name == selected]
    if query:
        frame = literal_search(frame, query)
    st.caption(f'{len(frame):,} matching models')
    table(frame.sort_values(['total_views', 'laptop_name'], ascending=[False, True])[
        ['laptop_name', 'brand_name', 'cpu_name', 'gpu_name', 'screen_size',
         'office_battery_hours', 'geekbench6_multi', 'total_views']
    ])
