import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.run_cosmos import collect, release_nodes


class CosmosArtifactTests(unittest.TestCase):
    def test_release_hash_is_checked_before_using_the_inventory(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'manifest.json'
            path.write_text(json.dumps({'nodes': {
                'model.project.a': {'resource_type': 'model'},
                'test.project.a': {'resource_type': 'test'},
                'exposure.project.dashboard': {'resource_type': 'exposure'},
            }}))
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            models, tests = release_nodes(path, sha)
            self.assertEqual(set(models), {'model.project.a'})
            self.assertEqual(tests, {'test.project.a'})
            path.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'differs'):
                release_nodes(path, sha)

    def test_collector_rejects_new_failure_then_accepts_targeted_retry(self):
        def save(root, key, attempt, outcome, results, task_id):
            target = root / 'cosmos' / key / f'attempt-{attempt}'
            target.mkdir(parents=True)
            (target / 'report.json').write_text(json.dumps({
                'started_at': f'2026-11-22T00:00:0{attempt}+00:00',
                'status': outcome, 'results': results, 'task_id': task_id,
                'release': 'this_release', 'run_id': 'this_run', 'attempt': attempt,
                'full_refresh': False,
            }))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            first, second, test = 'model.project.a', 'model.project.b', 'test.project.a'
            save(root, first, 1, 'success', {first: 'success'}, 'models.a')
            save(root, second, 1, 'success', {second: 'success'}, 'models.b')
            save(root, second, 2, 'failed', {}, 'models.b')
            save(root, 'tests', 1, 'success', {test: 'pass'}, 'dbt_tests')
            with self.assertRaisesRegex(ValueError, 'Latest Cosmos attempt'):
                collect(root, 'this_release', 'this_run', dict.fromkeys((first, second)), {test}, 'collect')
            save(root, second, 3, 'success', {second: 'success'}, 'models.b')
            result = collect(root, 'this_release', 'this_run', dict.fromkeys((first, second)), {test}, 'collect')
            self.assertEqual(result['model_count'], 2)
            self.assertEqual(result['test_count'], 1)
            self.assertEqual(result['models'][first]['attempt'], 1)
            self.assertEqual(result['models'][second]['attempt'], 3)
