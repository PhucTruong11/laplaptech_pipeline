"""Small deterministic source contract with midnight, duplicate and fanout cases."""
from datetime import datetime, time, timedelta
import json
import re
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd


def frames():
    now = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
    midnight = datetime.combine(now.date(), time(), now.tzinfo).timestamp()
    catalog = [
        dict(id=i, name=name, brand_id=brand, cpu_model_id=cpu, gpu_model_id=gpu,
             screen_size=14, laptop_weight=1.5, battery_capacity_whr=60,
             is_visible=True, is_active=True)
        for i, name, brand, cpu, gpu in [(1, 'Aster [Pro]', 1, 1, 1), (2, 'Boreal Air', 2, 2, 2), (3, 'Aster Basic', 1, 1, 1)]
    ]
    benchmarks = [
        dict(id=i, laptop_model_id=laptop, office_battery_result_minutes=battery,
             gaming_battery_result_minutes=90, geekbench_6_cpu_single_core_plugged_in=1500,
             geekbench_6_cpu_multi_core_plugged_in=score,
             geekbench_6_cpu_multi_core_battery=7000)
        for i, laptop, battery, score in [(101, 1, 360, 8000), (102, 1, 480, 10000), (201, 2, 600, 12000), (301, 3, None, None)]
    ]
    records = []

    def event(session, name, page=None, device=None, ts=None, **extra):
        payload = {**extra}
        if page is not None: payload['page_name'] = page
        if device is not None: payload['device_id'] = str(device)
        stamp = ts if ts is not None else midnight + 60 * (len(records) + 1)
        records.append(dict(id=len(records) + 1, session_id=session, event_name=name,
                            event_local_timestamp=stamp - 10, event_received_on_server_timestamp=stamp,
                            event_data=json.dumps(payload),
                            device=json.dumps({'os_name': 'Windows', 'device_type': 'Desktop', 'browser': 'Chrome'})))

    event('cross', 'pageview', 'Home', ts=midnight - 60)
    event('cross', 'pageview', 'DeviceDetail', 1, ts=midnight + 60)
    event('cross', 'pageview', 'DeviceComparison', 1)
    event('cross', 'pageview', 'CompareDevice', 2)
    event('cross', 'comparison_chart_sort_selection', device_ids=['1', '2', '2'], sort_by='performance')
    event('cross', 'select_device_for_comparison', device=1)
    event('second', 'pageview', 'Home')
    event('second', 'pageview', 'DeviceDetail', 2)
    event('second', 'search_for_device', keyword='[')
    event('compare-only', 'pageview', 'CompareDevice', 1)
    event('oversized', 'pageview', 'DeviceDetail', 99999999999)
    event(None, 'pageview', 'Home')  # retained in raw, intentionally excluded in Silver
    month_start = datetime(now.year, now.month, 1, tzinfo=now.tzinfo).timestamp()
    event('month-boundary', 'pageview', 'DeviceDetail', 1, ts=month_start - 60)
    event('month-boundary', 'pageview', 'DeviceDetail', 1, ts=month_start + 60)
    event('recent', 'pageview', 'DeviceDetail', 1, ts=now.timestamp())
    records.append(records[-1].copy())  # same event replayed by the source
    return {
        'brand': pd.DataFrame([{'id': 1, 'name': 'Aster'}, {'id': 2, 'name': 'Boreal'}]),
        'cpu_model': pd.DataFrame([{'id': 1, 'name': 'CPU A'}, {'id': 2, 'name': 'CPU B'}]),
        'gpu_model': pd.DataFrame([{'id': 1, 'name': 'GPU A'}, {'id': 2, 'name': 'GPU B'}]),
        'laptop_model': pd.DataFrame(catalog),
        'laptop_benchmark_result': pd.DataFrame(benchmarks),
        'user_event_tracking': pd.DataFrame(records),
    }


class FixtureClickHouse:
    def __init__(self): self.tables = frames()

    def query(self, query, parameters=None):
        name = re.search(r'FROM\s+(\w+)', query, re.I).group(1)
        frame = self.tables[name].copy()
        params = parameters or {}
        if 'watermark' in params:
            frame = frame[frame.event_received_on_server_timestamp >= params['watermark']]
        if 'start' in params:
            frame = frame[(frame.event_received_on_server_timestamp >= params['start']) &
                          (frame.event_received_on_server_timestamp < params['end'])]
        return SimpleNamespace(column_names=frame.columns.tolist(), result_rows=list(frame.itertuples(index=False, name=None)))

    def close(self): pass
