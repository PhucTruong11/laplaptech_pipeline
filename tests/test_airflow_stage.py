"""Protect the local-target boundary and reject unsupported rebuild requests."""
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts import run_airflow_stage as stage
from scripts.verify_local_stack import validated_timetable


class LocalRuntimeTests(unittest.TestCase):
    def test_legacy_admin_config_is_rejected_before_connection(self):
        with patch.object(Path, 'is_file', return_value=True), patch.object(stage, 'dotenv_values', return_value={
            'POSTGRES_HOST':'warehouse-laplaptech','POSTGRES_PORT':'5432','POSTGRES_USER':'laplaptech',
        }), patch.object(stage, 'create_engine') as engine:
            with self.assertRaises(RuntimeError):
                stage.lab_environment()
            engine.assert_not_called()
    def test_remote_warehouse_is_rejected_before_any_connection(self):
        with patch.object(Path, 'is_file', return_value=True), patch.object(
            stage, 'dotenv_values', return_value={'POSTGRES_HOST': 'example.neon.tech'}
        ), patch.object(stage, 'create_engine') as engine:
            with self.assertRaisesRegex(RuntimeError, 'POSTGRES_HOST'):
                stage.lab_environment()
            engine.assert_not_called()

    def test_rebuild_is_explicit_and_does_not_change_ingestion(self):
        self.assertNotIn('--full-refresh', stage.stage_command('build', Path('/tmp/test')))
        self.assertIn('--full-refresh', stage.stage_command('build', Path('/tmp/test'), full_refresh=True))
        with patch('sys.argv', ['runtime', 'ingest', '--full-refresh']), patch.object(stage, 'lab_environment') as config:
            with self.assertRaises(SystemExit) as result:
                stage.main()
            self.assertEqual(result.exception.code, 2)
            config.assert_not_called()

    def test_local_target_overrides_shell_and_unrelated_file_keys_are_ignored(self):
        configured = {
            'POSTGRES_HOST': 'warehouse-laplaptech', 'POSTGRES_PORT': '5432',
            'POSTGRES_USER': 'laplaptech_pipeline', 'POSTGRES_PASSWORD': 'test-only',
            'POSTGRES_DATABASE': 'laplaptech_pipeline_dev', 'POSTGRES_SSLMODE': 'prefer',
            'POSTGRES_CREATE_DATABASE': 'false', 'CLICKHOUSE_USER': 'public',
            'UNRELATED_SECRET': 'must-not-copy',
        }
        with patch.object(Path, 'is_file', return_value=True), patch.object(stage, 'dotenv_values', return_value=configured), patch.dict(os.environ, {'POSTGRES_HOST': 'remote.invalid'}, clear=True):
            env = stage.lab_environment()
        self.assertEqual(env['POSTGRES_HOST'], 'warehouse-laplaptech')
        self.assertNotIn('UNRELATED_SECRET', env)


class DeployedScheduleTests(unittest.TestCase):
    def serialized(self, expressions=None, timezone='Asia/Ho_Chi_Minh'):
        return {'timetable': {'__type': 'airflow.timetables.trigger.MultipleCronTriggerTimetable',
                '__var': {'expressions': expressions or ['0 7 * * *', '0 19 * * *'],
                          'timezone': timezone, 'run_immediately': False}}}

    def test_expected_live_timetable_is_accepted(self):
        self.assertEqual(validated_timetable(self.serialized())['cron'], ['0 7 * * *', '0 19 * * *'])

    def test_old_live_timetable_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'Deployed timetable'):
            validated_timetable(self.serialized(['15 8 * * *', '45 16 * * *']))

    def test_wrong_timezone_or_missing_timetable_is_rejected(self):
        for value in (self.serialized(timezone='UTC'), {}):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                validated_timetable(value)


if __name__ == '__main__':
    unittest.main()
