"""Protect the local-target boundary and reject unsupported rebuild requests."""
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts import run_airflow_stage as stage


class LocalRuntimeTests(unittest.TestCase):
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
            'POSTGRES_USER': 'laplaptech', 'POSTGRES_PASSWORD': 'test-only',
            'POSTGRES_DATABASE': 'laplaptech_pipeline_dev', 'POSTGRES_SSLMODE': 'prefer',
            'POSTGRES_CREATE_DATABASE': 'false', 'CLICKHOUSE_USER': 'public',
            'UNRELATED_SECRET': 'must-not-copy',
        }
        with patch.object(Path, 'is_file', return_value=True), patch.object(stage, 'dotenv_values', return_value=configured), patch.dict(os.environ, {'POSTGRES_HOST': 'remote.invalid'}, clear=True):
            env = stage.lab_environment()
        self.assertEqual(env['POSTGRES_HOST'], 'warehouse-laplaptech')
        self.assertNotIn('UNRELATED_SECRET', env)


if __name__ == '__main__':
    unittest.main()
