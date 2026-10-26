"""Exercise shared-platform preservation with fake Docker, never the live daemon."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

POWERSHELL = shutil.which('pwsh') or shutil.which('powershell')
PROJECT = Path(__file__).resolve().parents[1]
BOOTSTRAP = PROJECT/'scripts/Start-LocalStack.ps1'
HARNESS = r'''
$ErrorActionPreference = 'Stop'
function global:docker {
    $line = ConvertTo-Json -InputObject ([object[]]$args) -Compress -Depth 4
    [IO.File]::AppendAllText($env:BOOTSTRAP_TRACE, $line+[Environment]::NewLine)
    $global:LASTEXITCODE = 0
    $joined = $args -join ' '
    if ($args[0] -eq 'info') { return 'linux' }
    if ($args[0] -eq 'ps') {
        if ($env:BOOTSTRAP_NO_METADATA -eq '1') { return }
        if ($joined.Contains('name=^airflow-platform-metadata-1$')) { return 'airflow-platform-metadata-1' }
        if ($env:BOOTSTRAP_ORPHAN_TASK -eq '1') { return 'orphan-task' }
        return
    }
    if ($joined.Contains('SELECT coalesce(json_agg')) {
        return (ConvertTo-Json -InputObject @(@{dag_id='laplaptech_pipeline';is_paused=($env:BOOTSTRAP_INITIAL_PAUSE -eq '1')}, @{dag_id='future_project';is_paused=($env:BOOTSTRAP_OTHER_UNPAUSED -ne '1')}) -Compress)
    }
    if ($joined.Contains('SELECT count(*) FROM dag_run')) { return $env:BOOTSTRAP_ACTIVE_RUNS }
    if ($args[0] -eq 'build' -and $env:BOOTSTRAP_FAIL_BUILD -eq '1') { $global:LASTEXITCODE = 27 }
}
try {
    $parameters = @{PlatformDir=$env:BOOTSTRAP_PLATFORM;SourceEnvFile=$env:BOOTSTRAP_SOURCE}
    if ($env:BOOTSTRAP_INITIALIZE -eq '1') { $parameters.InitializePlatform=$true }
    & $env:BOOTSTRAP_SCRIPT @parameters
} catch { Write-Output $_.Exception.Message; exit 20 }
'''


@unittest.skipUnless(POWERSHELL, 'PowerShell is required')
class BootstrapCoordinationTests(unittest.TestCase):
    def invoke_bootstrap(self, *, active_runs=0, initially_paused=False, fail_build=False,
                         orphan_task=False, other_unpaused=False, overbudget=False,
                         missing=False, initialize=False):
        temporary = tempfile.TemporaryDirectory(prefix='laplaptech-bootstrap-test-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        platform = root/'platform'
        platform.mkdir()
        template = PROJECT/'infra/airflow-platform'
        policy = json.loads((template/'resources.json').read_text())
        policy['projects']['future'] = {'services': {}, 'dbt_threads': 1,
                                        'tasks': {'transform': {'memory_mib': 512, 'cpus': .5}}}
        if overbudget:
            policy['headroom_mib'] = 4096
        for name in ('Configure-Resources.ps1',):
            shutil.copyfile(template/name, platform/name)
        (platform/'resources.json').write_text(json.dumps(policy))
        (platform/'dags').mkdir()
        for name in ('budget_docker.py','platform_resource_policy.py'):
            (platform/'dags'/name).write_text('# platform-owned custom implementation\n')
        original = ('AIRFLOW_ADMIN_PASSWORD=test-key-preserved\nWAREHOUSE_PASSWORD=fake-admin-preserved\n'
                    f'LAPLAPTECH_PROJECT_DIR={PROJECT.as_posix()}\n'
                    'WAREHOUSE_HOST_PORT=55434\nDASHBOARD_PORT=8501\n')
        (platform/'.env').write_text(original)
        sentinels = {}
        for name in ('compose.yaml','compose.laplaptech.yaml','.gitignore','.env.example',
                     'Initialize-LocalEnv.ps1','README.md','AGENTS.md','ARCHITECTURE.md','ONBOARDING.md'):
            text = '# platform-owned '+name+'\n'
            (platform/name).write_text(text)
            sentinels[name] = text
        if missing:
            (platform/'compose.yaml').unlink()
        source = root/'source.env'
        source.write_text('CLICKHOUSE_PASSWORD=fake-source\n')
        harness = root/'harness.ps1'
        harness.write_text(HARNESS)
        trace = root/'trace.jsonl'
        env = {**os.environ, 'BOOTSTRAP_SCRIPT':str(BOOTSTRAP), 'BOOTSTRAP_PLATFORM':str(platform),
               'BOOTSTRAP_SOURCE':str(source),'BOOTSTRAP_TRACE':str(trace),
               'BOOTSTRAP_ACTIVE_RUNS':str(active_runs), 'BOOTSTRAP_INITIAL_PAUSE':str(int(initially_paused)),
               'BOOTSTRAP_FAIL_BUILD':str(int(fail_build)), 'BOOTSTRAP_ORPHAN_TASK':str(int(orphan_task)),
               'BOOTSTRAP_OTHER_UNPAUSED':str(int(other_unpaused)), 'BOOTSTRAP_INITIALIZE':str(int(initialize))}
        result = subprocess.run([POWERSHELL,'-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(harness)], env=env,capture_output=True,text=True,timeout=45)
        calls = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
        return result, calls, platform, original, sentinels

    def test_shared_files_and_registrations_survive_existing_install(self):
        result,calls,platform,original,sentinels = self.invoke_bootstrap(other_unpaused=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        for name,value in sentinels.items():
            self.assertEqual((platform/name).read_text(),value,name)
        self.assertEqual((platform/'dags/budget_docker.py').read_text(),'# platform-owned custom implementation\n')
        self.assertIn('future',json.loads((platform/'resources.json').read_text())['projects'])
        for line in original.splitlines():
            self.assertIn(line,(platform/'.env').read_text().splitlines())
        self.assertEqual({c[-1] for c in calls if 'pause' in c},{'laplaptech_pipeline','future_project'})
        self.assertEqual({c[-1] for c in calls if 'unpause' in c},{'laplaptech_pipeline','future_project'})
        self.assertFalse(any('build' in c and 'airflow-scheduler' in c for c in calls))
        self.assertNotIn('fake-admin-preserved',json.dumps(calls))
        self.assertNotIn('fake-source',json.dumps(calls))

    def test_active_run_blocks_updates_and_restores_schedules(self):
        result,calls,platform,original,_ = self.invoke_bootstrap(active_runs=1)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('Wait for queued/running',result.stdout)
        self.assertEqual((platform/'.env').read_text(),original)
        self.assertFalse(any(c[0] in ('build','volume','run') for c in calls))
        self.assertEqual(calls[-1][-2:],['unpause','laplaptech_pipeline'])

    def test_orphan_task_blocks_updates(self):
        result,calls,platform,original,_ = self.invoke_bootstrap(orphan_task=True)
        self.assertNotEqual(result.returncode,0)
        self.assertEqual((platform/'.env').read_text(),original)
        self.assertFalse(any(c[0] in ('build','volume','run') for c in calls))

    def test_failure_keeps_dags_paused(self):
        result,calls,*_ = self.invoke_bootstrap(fail_build=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('DAGs remain paused',result.stdout)
        self.assertFalse(any('unpause' in c for c in calls))
        self.assertFalse(any(c[0]=='run' for c in calls))

    def test_paused_project_stays_paused(self):
        result,calls,*_ = self.invoke_bootstrap(initially_paused=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertFalse(any('pause' in c or 'unpause' in c for c in calls))

    def test_overbudget_fails_before_writes(self):
        result,calls,platform,original,_ = self.invoke_bootstrap(overbudget=True)
        self.assertNotEqual(result.returncode,0)
        self.assertEqual((platform/'.env').read_text(),original)
        self.assertFalse(any(c[0] in ('build','volume','run') for c in calls))

    def test_missing_platform_requires_explicit_initialization(self):
        result,calls,platform,original,_ = self.invoke_bootstrap(missing=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('-InitializePlatform',result.stdout+result.stderr)
        self.assertFalse(calls)
        self.assertEqual((platform/'.env').read_text(),original)

    def test_initialize_refuses_nonempty_directory(self):
        result,calls,platform,original,_ = self.invoke_bootstrap(initialize=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('never replaced',result.stdout+result.stderr)
        self.assertFalse(calls)
        self.assertEqual((platform/'.env').read_text(),original)

    def test_explicit_initialization_builds_complete_standalone_config(self):
        with tempfile.TemporaryDirectory(prefix='laplaptech-bootstrap-test-') as directory:
            root=Path(directory)
            platform=root/'new-platform'
            source=root/'source.env'
            source.write_text('CLICKHOUSE_PASSWORD=fake-source\n')
            harness=root/'harness.ps1'
            harness.write_text(HARNESS)
            trace=root/'trace.jsonl'
            env={**os.environ,'BOOTSTRAP_SCRIPT':str(BOOTSTRAP),'BOOTSTRAP_PLATFORM':str(platform),
                 'BOOTSTRAP_SOURCE':str(source),'BOOTSTRAP_TRACE':str(trace),
                 'BOOTSTRAP_INITIALIZE':'1','BOOTSTRAP_NO_METADATA':'1','BOOTSTRAP_ACTIVE_RUNS':'0',
                 'BOOTSTRAP_FAIL_BUILD':'0','BOOTSTRAP_ORPHAN_TASK':'0','BOOTSTRAP_INITIAL_PAUSE':'1'}
            result=subprocess.run([POWERSHELL,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(harness)],env=env,capture_output=True,text=True,timeout=45)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            config=(platform/'.env').read_text()
            for key in ('WAREHOUSE_PASSWORD','LAPLAPTECH_PIPELINE_PASSWORD','LAPLAPTECH_DASHBOARD_PASSWORD','LAPLAPTECH_PROJECT_DIR','RESOURCE_METADATA_MEMORY_MIB'):
                self.assertIn(key+'=',config)
            self.assertNotIn('GENERATE_WITH_SCRIPT',config)
            self.assertTrue((platform/'README.md').is_file())
            self.assertTrue((platform/'AGENTS.md').is_file())
            self.assertTrue((platform/'diagnostics/platform_smoke_test.py').is_file())
            self.assertFalse((platform/'dags/platform_smoke_test.py').exists())


if __name__=='__main__':
    unittest.main()
