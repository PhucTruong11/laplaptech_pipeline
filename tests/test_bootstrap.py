"""Exercise bootstrap coordination without a Docker daemon or real credentials."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


POWERSHELL = shutil.which('pwsh') or shutil.which('powershell')
BOOTSTRAP = Path(__file__).resolve().parents[1] / 'scripts' / 'Start-LocalStack.ps1'

HARNESS = r'''
$ErrorActionPreference = 'Stop'
$global:FakePause = $env:BOOTSTRAP_INITIAL_PAUSE
function global:Start-Sleep { param([int]$Seconds) }
function global:docker {
    $arguments = @($args)
    $line = ConvertTo-Json -InputObject ([object[]]$arguments) -Compress -Depth 4
    [IO.File]::AppendAllText($env:BOOTSTRAP_TRACE, $line + [Environment]::NewLine)
    $global:LASTEXITCODE = 0
    $joined = $arguments -join ' '
    if ($arguments[0] -eq 'info') { return 'linux' }
    if ($arguments[0] -eq 'ps') {
        if ($joined.Contains('name=^airflow-platform-metadata-1$')) {
            return 'airflow-platform-metadata-1'
        }
        if ($env:BOOTSTRAP_ORPHAN_TASK -eq '1') { return 'orphan-laplaptech-task' }
        return
    }
    if ($joined.Contains('SELECT coalesce(json_agg')) {
        $paused = $global:FakePause -eq 't'
        return (ConvertTo-Json -InputObject @(@{dag_id='laplaptech_pipeline';is_paused=$paused}, @{dag_id='future_project';is_paused=($env:BOOTSTRAP_OTHER_UNPAUSED -ne '1')}) -Compress)
    }
    if ($joined.Contains('SELECT count(*) FROM dag_run')) { return $env:BOOTSTRAP_ACTIVE_RUNS }
    if ($arguments -contains 'pause') { $global:FakePause = 't'; return }
    if ($arguments -contains 'unpause') { $global:FakePause = 'f'; return }
    if ($arguments[0] -eq 'build' -and $env:BOOTSTRAP_FAIL_BUILD -eq '1') {
        $global:LASTEXITCODE = 27
        return
    }
}
try {
    & $env:BOOTSTRAP_SCRIPT -PlatformDir $env:BOOTSTRAP_PLATFORM -SourceEnvFile $env:BOOTSTRAP_SOURCE
}
catch {
    Write-Output $_.Exception.Message
    exit 20
}
'''


@unittest.skipUnless(POWERSHELL, 'PowerShell is required for bootstrap control-flow tests')
class BootstrapCoordinationTests(unittest.TestCase):
    def invoke_bootstrap(self, *, active_runs=0, initially_paused=False, fail_build=False, orphan_task=False, other_unpaused=False, existing_policy=None):
        temporary = tempfile.TemporaryDirectory(prefix='laplaptech-bootstrap-test-')
        root = Path(temporary.name)

        def cleanup():
            resolved = root.resolve()
            if resolved.parent != Path(tempfile.gettempdir()).resolve() or not resolved.name.startswith('laplaptech-bootstrap-test-'):
                raise RuntimeError('Refusing cleanup outside this test temporary directory')
            temporary.cleanup()

        self.addCleanup(cleanup)
        platform = root / 'platform'
        platform.mkdir()
        original_env = 'AIRFLOW_ADMIN_PASSWORD=test-password-preserved\nAIRFLOW_FERNET_KEY=test-key-preserved\n'
        (platform / '.env').write_text(original_env, encoding='utf-8')
        (platform / 'compose.yaml').write_text('# original config\n', encoding='utf-8')
        (platform / 'README.md').write_text('# Owner platform architecture and onboarding\n', encoding='utf-8')
        if existing_policy is not None:
            (platform / 'resources.json').write_text(json.dumps(existing_policy), encoding='utf-8')
        source = root / 'source.env'
        source.write_text('CLICKHOUSE_PASSWORD=fake-source-only\n', encoding='utf-8')
        harness = root / 'harness.ps1'
        harness.write_text(HARNESS, encoding='utf-8')
        trace = root / 'trace.jsonl'
        env = os.environ.copy()
        env.update(
            BOOTSTRAP_SCRIPT=str(BOOTSTRAP), BOOTSTRAP_PLATFORM=str(platform),
            BOOTSTRAP_SOURCE=str(source), BOOTSTRAP_TRACE=str(trace),
            BOOTSTRAP_INITIAL_PAUSE='t' if initially_paused else 'f',
            BOOTSTRAP_ACTIVE_RUNS=str(active_runs),
            BOOTSTRAP_FAIL_BUILD='1' if fail_build else '0',
            BOOTSTRAP_ORPHAN_TASK='1' if orphan_task else '0',
            BOOTSTRAP_OTHER_UNPAUSED='1' if other_unpaused else '0',
        )
        result = subprocess.run(
            [POWERSHELL, '-NoLogo', '-NoProfile', '-NonInteractive',
             '-ExecutionPolicy', 'Bypass', '-File', str(harness)],
            env=env, capture_output=True, text=True, timeout=45,
        )
        if not trace.exists():
            raise RuntimeError('Bootstrap harness exited before invoking Docker: '
                               f'{result.stdout[-1200:]} {result.stderr[-1200:]}')
        calls = [json.loads(line) for line in trace.read_text(encoding='utf-8').splitlines()]
        return result, calls, platform, original_env

    def test_active_run_blocks_mutations_and_restores_original_schedule(self):
        result, calls, platform, original = self.invoke_bootstrap(active_runs=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Wait for queued/running', result.stdout)
        self.assertTrue(any('pause' in call for call in calls))
        self.assertEqual(calls[-1][-2:], ['unpause', 'laplaptech_pipeline'])
        self.assertFalse(any(call[0] in {'build', 'run', 'volume', 'compose'} for call in calls))
        self.assertEqual((platform / '.env').read_text(encoding='utf-8'), original)
        self.assertEqual((platform / 'compose.yaml').read_text(encoding='utf-8'), '# original config\n')

    def test_update_pauses_before_build_and_resumes_after_pool_without_replacing_keys(self):
        result, calls, platform, original = self.invoke_bootstrap()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        pause = next(i for i, call in enumerate(calls) if 'pause' in call)
        build = next(i for i, call in enumerate(calls) if call[0] == 'build')
        imported_config = next(i for i, call in enumerate(calls) if call[0] == 'run')
        pool = next(i for i, call in enumerate(calls) if 'pools' in call)
        resumed = next(i for i, call in enumerate(calls) if 'unpause' in call)
        self.assertLess(pause, build)
        self.assertLess(build, imported_config)
        self.assertLess(imported_config, pool)
        self.assertLess(pool, resumed)
        for line in original.splitlines():
            self.assertIn(line, (platform / '.env').read_text(encoding='utf-8').splitlines())
        backups = list((platform / 'backups').glob('bootstrap-*/.env'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(encoding='utf-8'), original)
        self.assertEqual((platform / 'README.md').read_text(encoding='utf-8'),
                         '# Owner platform architecture and onboarding\n')

    def test_partial_update_failure_keeps_project_paused(self):
        result, calls, _, _ = self.invoke_bootstrap(fail_build=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Platform DAGs remain paused', result.stdout)
        self.assertTrue(any('pause' in call for call in calls))
        self.assertFalse(any('unpause' in call for call in calls))
        self.assertFalse(any(call[0] == 'run' for call in calls))

    def test_orphan_runtime_blocks_update_without_touching_config(self):
        result, calls, platform, original = self.invoke_bootstrap(orphan_task=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('task container is still running', result.stdout)
        self.assertEqual(calls[-1][-2:], ['unpause', 'laplaptech_pipeline'])
        self.assertFalse(any(call[0] in {'build', 'run', 'volume', 'compose'} for call in calls))
        self.assertEqual((platform / '.env').read_text(encoding='utf-8'), original)

    def test_initially_paused_project_stays_paused_after_success(self):
        result, calls, _, _ = self.invoke_bootstrap(initially_paused=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any('pause' in call or 'unpause' in call for call in calls))

    def test_shared_update_drains_and_restores_other_project_schedule(self):
        result, calls, _, _ = self.invoke_bootstrap(other_unpaused=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        build = next(i for i, call in enumerate(calls) if call[0] == 'build')
        pauses = [(i, call[-1]) for i, call in enumerate(calls) if 'pause' in call]
        self.assertEqual({name for _, name in pauses}, {'laplaptech_pipeline', 'future_project'})
        self.assertTrue(all(i < build for i, _ in pauses))
        self.assertEqual({call[-1] for call in calls if 'unpause' in call}, {'laplaptech_pipeline', 'future_project'})

    def test_existing_future_project_budget_is_preserved_and_overbudget_blocks_build(self):
        template = BOOTSTRAP.parents[1] / 'infra/airflow-platform/resources.json'
        policy = json.loads(template.read_text())
        policy['projects']['future'] = {'services': {}, 'dbt_threads': 1,
                                        'tasks': {'transform': {'memory_mib': 512, 'cpus': 0.5}}}
        result, calls, platform, original = self.invoke_bootstrap(existing_policy=policy)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(json.loads((platform / 'resources.json').read_text()), policy)
        self.assertIn('warehouse-laplaptech:5432', (platform / 'AGENTS.md').read_text())
        for line in original.splitlines():
            self.assertIn(line, (platform / '.env').read_text().splitlines())
        policy['headroom_mib'] = 4096
        result, calls, _, _ = self.invoke_bootstrap(existing_policy=policy)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('exceeds WSL', result.stdout)
        self.assertFalse(any(call[0] in {'build', 'run', 'volume', 'compose'} for call in calls))


if __name__ == '__main__':
    unittest.main()
