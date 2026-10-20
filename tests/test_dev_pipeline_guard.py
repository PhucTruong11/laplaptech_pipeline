"""Keep fixture/reset and ad-hoc validation away from the active Airflow DB."""
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts import run_dev_pipeline as runner


def local_url(database='laplaptech_pipeline_test'):
    return f'postgresql+psycopg2://tester:unit-test-only@localhost:55434/{database}'


class DevPipelineTargetTests(unittest.TestCase):
    def assert_blocked_before_side_effects(self, url, args, message):
        with patch.dict(os.environ, {'DEV_DATABASE_URL': url}), patch(
            'sys.argv', ['run_dev_pipeline', *args]
        ), patch.object(runner, 'create_engine') as engine, patch.object(
            runner.ingestion, 'run_pipeline'
        ) as ingest, patch.object(Path, 'mkdir') as mkdir:
            with self.assertRaisesRegex(RuntimeError, message):
                runner.main()
            engine.assert_not_called()
            ingest.assert_not_called()
            mkdir.assert_not_called()

    def test_active_warehouse_is_blocked_for_default_fixture_reset_and_live_validation(self):
        for args in ([], ['--reset-fixture'], ['--source', 'clickhouse']):
            with self.subTest(args=args):
                self.assert_blocked_before_side_effects(
                    local_url('laplaptech_pipeline_dev'), args, 'active Airflow warehouse'
                )

    def test_fixture_cannot_replace_the_dev_live_dataset(self):
        self.assert_blocked_before_side_effects(
            local_url('laplaptech_pipeline_dev_live'), [], 'Fixture validation requires'
        )

    def test_reset_is_never_available_for_clickhouse_validation(self):
        for database in ('laplaptech_pipeline_test', 'laplaptech_pipeline_dev_live'):
            with self.subTest(database=database):
                self.assert_blocked_before_side_effects(
                    local_url(database), ['--source', 'clickhouse', '--reset-fixture'],
                    '--reset-fixture requires',
                )

    def test_remote_host_and_libpq_query_overrides_are_blocked(self):
        targets = [
            local_url().replace('@localhost:', '@remote.invalid:'),
            local_url() + '?host=remote.invalid',
            local_url() + '?service=other-server',
            local_url() + '?dbname=laplaptech_pipeline_dev',
        ]
        for target in targets:
            with self.subTest(target=target):
                self.assert_blocked_before_side_effects(target, [], 'host must|query overrides')

    def test_invalid_or_incomplete_url_does_not_echo_credentials(self):
        for value, message in (
            (None, 'Set DEV_DATABASE_URL'),
            ('not-a-url:never-echo-this', 'valid explicit PostgreSQL URL'),
            ('sqlite:///laplaptech_pipeline_test', 'must use PostgreSQL'),
            ('postgresql://localhost/laplaptech_pipeline_test', 'explicit username and password'),
            ('postgresql://tester@localhost/laplaptech_pipeline_test', 'explicit username and password'),
            (local_url().replace(':55434/', ':0/'), 'port must'),
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, message) as raised:
                    runner.validated_dev_url(value, source='fixture')
                self.assertNotIn('never-echo-this', str(raised.exception))

    def test_only_isolated_database_source_combinations_are_allowed(self):
        cases = [
            ('laplaptech_pipeline_test', 'fixture', False),
            ('laplaptech_pipeline_test', 'fixture', True),
            ('laplaptech_pipeline_test', 'clickhouse', False),
            ('laplaptech_pipeline_dev_live', 'clickhouse', False),
        ]
        for database, source, reset in cases:
            with self.subTest(database=database, source=source, reset=reset):
                url = runner.validated_dev_url(local_url(database), source=source, reset_fixture=reset)
                self.assertEqual(url.database, database)
                self.assertEqual(url.host, 'localhost')

    def test_unknown_local_database_is_not_a_validation_target(self):
        self.assert_blocked_before_side_effects(local_url('postgres'), [], 'Fixture validation requires')
        self.assert_blocked_before_side_effects(
            local_url('postgres'), ['--source', 'clickhouse'], 'ClickHouse validation requires'
        )


if __name__ == '__main__':
    unittest.main()
