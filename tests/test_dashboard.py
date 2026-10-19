from datetime import date
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

from streamlit_app.data import brand_share, filter_days, literal_search, numeric_label, summarize_daily


class DashboardMetricTests(unittest.TestCase):
    def test_literal_search_accepts_regex_punctuation(self):
        frame = pd.DataFrame({'laptop_name': ['Aster [Pro]', 'Boreal', None]})
        self.assertEqual(literal_search(frame, '[').laptop_name.tolist(), ['Aster [Pro]'])

    def test_missing_benchmark_is_not_fabricated(self):
        self.assertEqual(numeric_label(float('nan'), ' h'), 'N/A')
        self.assertEqual(numeric_label(None), 'N/A')
        self.assertEqual(numeric_label(0), '0.0')

    def test_brand_share_retains_all_views(self):
        frame = pd.DataFrame({'brand_name': list('ABCDEFGH'), 'total_views': range(1, 9)})
        self.assertEqual(brand_share(frame).total_views.sum(), frame.total_views.sum())
        self.assertIn('Other brands (remaining)', brand_share(frame).brand_name.tolist())

    def test_today_does_not_fall_back_to_latest_dataset_day(self):
        frame = pd.DataFrame({'event_date': [date(2026, 10, 18)]})
        window = filter_days(frame, date(2026, 10, 19), date(2026, 10, 19))
        self.assertTrue(window.empty)
        self.assertEqual(summarize_daily(window)['total_events'], 0)

    def test_rates_use_aggregate_numerators_not_mean_percent(self):
        cols = ['total_events', 'total_sessions', 'total_pageviews', 'detail_pageviews', 'comparison_pageviews',
                'sessions_with_pageview', 'sessions_with_detail_view', 'pageview_and_detail', 'detail_and_comparison']
        frame = pd.DataFrame([[10, 10, 10, 1, 1, 10, 1, 1, 1], [100, 100, 100, 50, 0, 100, 50, 50, 0]], columns=cols)
        result = summarize_daily(frame)
        self.assertAlmostEqual(result['comparison_reach_pct'], 100/51)
        self.assertEqual(result['total_sessions'], 110)  # session-days, not all-time distinct sessions


@unittest.skipUnless(os.getenv('DASHBOARD_TEST_DATABASE_URL'), 'Requires built LOCAL fixture Gold database')
class DashboardAppTests(unittest.TestCase):
    def test_all_tabs_and_literal_catalog_search(self):
        from sqlalchemy.engine import make_url
        from streamlit.testing.v1 import AppTest
        from streamlit_app.data import get_engine, load_gold
        url = os.environ['DASHBOARD_TEST_DATABASE_URL']
        parsed = make_url(url)
        self.assertIn(parsed.host, {'localhost', '127.0.0.1'})
        self.assertIn(parsed.database, {'laplaptech_pipeline_dev', 'laplaptech_pipeline_test'})
        get_engine.clear()
        load_gold.clear()
        with patch.dict(os.environ, {'DASHBOARD_DATABASE_URL': url}):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'streamlit_app/app.py')).run(timeout=60)
            self.assertEqual(len(app.exception), 0, [str(e.value) for e in app.exception])
            self.assertEqual(len(app.tabs), 5)
            app.radio[0].set_value('Today').run(timeout=60)
            self.assertEqual(len(app.exception), 0)
            app.text_input[0].set_value('[').run(timeout=60)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any('1 matching models' in c.value for c in app.caption))
        get_engine.clear()
        load_gold.clear()

    def test_query_failure_is_error_not_zero_kpis(self):
        from streamlit.testing.v1 import AppTest
        with patch('streamlit_app.data.load_gold', side_effect=RuntimeError('simulated unavailable database')):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'streamlit_app/app.py')).run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(app.error)
            self.assertEqual(len(app.metric), 0)


if __name__ == '__main__':
    unittest.main()
