"""Gold-only data access and UI-independent metric helpers."""
from datetime import date, timedelta
import os
import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import streamlit as st

from streamlit_app.constants import GOLD_NAMES


@st.cache_resource
def get_engine():
    url = os.getenv('DASHBOARD_DATABASE_URL')
    if not url:
        url = URL.create(
            'postgresql+psycopg2', username=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD'), host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT') or '5432'),
            database=os.getenv('POSTGRES_DATABASE', 'laplaptech_pipeline'),
            query={'sslmode': os.getenv('POSTGRES_SSLMODE') or 'prefer'},
        )
    engine = create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=0,
                           connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as conn:
            database, role, admin = conn.execute(text(
                'SELECT current_database(), current_user, rolsuper FROM pg_roles WHERE rolname=current_user'
            )).one()
        if database == 'laplaptech_pipeline_dev' and (role != 'laplaptech_dashboard' or admin):
            raise RuntimeError('Dashboard requires its dedicated Gold reader login')
        return engine
    except Exception:
        engine.dispose()
        raise


@st.cache_data(ttl=300)
def load_gold(name: str) -> pd.DataFrame:
    if name not in GOLD_NAMES:
        raise ValueError('Unknown Gold model')
    schema = os.getenv('GOLD_SCHEMA', 'public_gold')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', schema):
        raise ValueError('Invalid Gold schema')
    with get_engine().connect() as conn:
        return pd.read_sql(text(f'SELECT * FROM "{schema}"."{name}"'), conn)


def filter_days(frame, start, end):
    if frame.empty:
        return frame.copy()
    days = pd.to_datetime(frame['event_date']).dt.date
    return frame.loc[(days >= start) & (days <= end)].copy()


def summarize_daily(frame):
    cols = ['total_events', 'total_sessions', 'total_pageviews', 'detail_pageviews',
            'comparison_pageviews', 'sessions_with_pageview', 'sessions_with_detail_view',
            'pageview_and_detail', 'detail_and_comparison']
    values = {c: int(frame[c].sum()) if not frame.empty else 0 for c in cols}
    values['detail_reach_pct'] = (100 * values['pageview_and_detail'] / values['sessions_with_pageview']
                                  if values['sessions_with_pageview'] else None)
    values['comparison_reach_pct'] = (100 * values['detail_and_comparison'] / values['sessions_with_detail_view']
                                      if values['sessions_with_detail_view'] else None)
    return values


def literal_search(frame, query):
    return frame.loc[frame['laptop_name'].str.contains(query, case=False, na=False, regex=False)].copy()


def brand_share(frame, limit=6):
    data = frame[['brand_name', 'total_views']].sort_values(['total_views', 'brand_name'], ascending=[False, True]).copy()
    if len(data) > limit:
        tail = pd.DataFrame([{'brand_name': 'Other brands (remaining)', 'total_views': data.iloc[limit:]['total_views'].sum()}])
        data = pd.concat([data.head(limit), tail], ignore_index=True)
    return data


def numeric_label(value, suffix='', decimals=1):
    return 'N/A' if value is None or pd.isna(value) else f'{float(value):,.{decimals}f}{suffix}'
