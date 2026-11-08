"""Hermetic contract controls for later, separately admitted verification.

Run directly with the prepared Python -I, not pytest (repo conftest imports LSMS).
All gh/git/srun/task/interpreter probes are temporary local executable mocks.
No provider, real Git operation, DVC, survey data or LSMS import is used.
Author phase: drafted and AST parsed only; execution is intentionally deferred.
"""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


HELPERS = Path(__file__).resolve().parents[1] / '.claude/skills/backlog-workflow/sweep'
HEAD = 'a' * 40
BASE = 'b' * 40
OTHER = 'c' * 40

# The mock has fixed fixture paths compiled into it so env -i cannot discard its
# control channel. It never delegates to a real executable/provider.
MOCK = r'''
import json, os
from pathlib import Path
import sys
root = Path(FIXTURE)
config = json.loads((root / 'control.json').read_text())
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / 'calls.jsonl').open('a') as f:
    f.write(json.dumps({'name': name, 'args': args, 'cwd': os.getcwd(), 'env': dict(os.environ)}) + '\n')
if name == 'git':
    if args[:1] == ['-c']:
        args = args[2:]
    op = args[0]
    if op == 'clone':
        Path(args[-1]).mkdir()
    elif op == 'checkout':
        pass
    elif op == 'rev-parse':
        ref = args[-1].removesuffix('^{commit}')
        print(config['head'] if ref == 'HEAD' else ref)
    elif op == 'symbolic-ref':
        if '-q' in args:
            sys.exit(0 if config.get('attached') else 1)
        print('fix/example')
    elif op == 'merge-base':
        print(config['base'])
    elif op == 'ls-remote':
        branch = args[-1]
        if branch == 'refs/heads/development':
            prior = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
            count = sum(x['name'] == 'git' and 'ls-remote' in x['args'] and
                        x['args'][-1] == branch for x in prior)
            sha = config.get('remote_base_after', config['base']) if count > 1 else config['base']
        else:
            sha = config.get('remote_head', config['head'])
        print(sha + '\t' + branch)
        if config.get('duplicate_remote'):
            print(sha + '\t' + branch)
    elif op == 'push':
        sys.exit(config.get('push_exit', 0))
    elif op == 'worktree':
        Path(args[-4]).mkdir(parents=True)
    elif op == 'cherry-pick':
        sys.exit(config.get('cherry_exit', 0))
    elif op in ('check-ref-format', 'log'):
        pass
    else:
        raise RuntimeError('unexpected Git operation: ' + repr(args))
elif name == 'gh':
    if args[:2] == ['pr', 'view']:
        print(json.dumps(config['observation']))
    elif args[:2] == ['pr', 'create']:
        print('mock://created')
    elif args[:1] == ['api']:
        endpoint = args[args.index('GET') + 1]
        if '/comments?' in endpoint:
            pages = config['pages']
            if config.get('comment_fault') == 'missing':
                pages[-1] = pages[-1][:-1]
            elif config.get('comment_fault') == 'duplicate':
                pages[-1][-1] = pages[0][0]
            elif config.get('comment_fault') == 'unsupported':
                pages = {'nodes': pages}
            print(json.dumps(pages))
        else:
            issue = dict(config['issue'])
            prior = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
            count = sum(x['name'] == 'gh' and x['args'][:1] == ['api'] and
                        '/comments?' not in ' '.join(x['args']) for x in prior)
            if config.get('issue_drift') and count > 1:
                issue['body'] += '\nNEW RULING'
            print(json.dumps(issue))
    else:
        raise RuntimeError('unexpected gh operation: ' + repr(args))
elif name == 'srun':
    sys.exit(config.get('srun_exit', 0))
elif name in ('prepared-python', 'task-probe'):
    # Observes environment before a would-be import; never executes Python -c.
    sys.exit(config.get('task_exit', 0) if name == 'task-probe' else 0)
else:
    raise RuntimeError('unexpected mocked tool')
'''


class Contracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='backlog-contract-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.config = {'head': HEAD, 'base': BASE}
        for tool in ('git', 'gh', 'srun', 'prepared-python', 'task-probe'):
            path = self.bin / tool
            path.write_text(f'#!{sys.executable}\nFIXTURE = {str(self.root)!r}\n' + MOCK)
            path.chmod(0o700)
        # No real credentials or native homes. These fixture homes are empty.
        self.home = self.root / 'home'
        self.home.mkdir()
        self.codex = self.root / 'adapter-home'
        self.codex.mkdir()
        self.env = {
            'PATH': str(self.bin) + ':/usr/bin:/bin', 'HOME': str(self.home),
            'CODEX_HOME': str(self.codex), 'PROJECT_PYTHON': sys.executable,
            'SWEEP_PR_DIR': str(self.root / 'prs'),
        }
        (self.root / 'prs/fix_example').mkdir(parents=True)
        self.source = self.root / 'admitted-source.git'
        self.source.mkdir()

    def invoke(self, name, *args, env=None):
        (self.root / 'control.json').write_text(json.dumps(self.config))
        command = (['bash', str(HELPERS / name)] if name.endswith('.sh') else
                   [sys.executable, '-I', str(HELPERS / name)])
        return subprocess.run([*command, *map(str, args)], cwd=self.root,
                              env=self.env if env is None else env,
                              capture_output=True, text=True, timeout=15)

    def calls(self, name=None):
        file = self.root / 'calls.jsonl'
        records = [json.loads(line) for line in file.read_text().splitlines()] if file.exists() else []
        return [r for r in records if name is None or r['name'] == name]

    def clear_calls(self):
        (self.root / 'calls.jsonl').unlink(missing_ok=True)

    def approval(self):
        return {'schema_version': 1, 'repository': 'example/library', 'approvals': [{
            'pr': 12, 'head_sha': HEAD, 'base_sha': BASE, 'base_branch': 'development',
            'required_success_checks': ['test'], 'permitted_skips': [],
        }]}

    def observation(self):
        return {'number': 12, 'state': 'OPEN', 'isDraft': False,
                'headRefOid': HEAD, 'baseRefOid': BASE, 'baseRefName': 'development',
                'mergeable': 'MERGEABLE', 'mergeStateStatus': 'CLEAN', 'reviewDecision': 'APPROVED',
                'statusCheckRollup': [{
                    '__typename': 'CheckRun', 'name': 'test',
                    'status': 'COMPLETED', 'conclusion': 'SUCCESS',
                }]}

    def merge(self, manifest=None, observation=None):
        self.config['observation'] = self.observation() if observation is None else observation
        file = self.root / 'approval.json'
        file.write_text(json.dumps(self.approval() if manifest is None else manifest))
        result = self.invoke('merge_on_green.sh', file)
        self.assertFalse(any(r['args'][:2] == ['pr', 'merge'] for r in self.calls('gh')))
        return result

    def test_green_without_complete_thread_proof_requires_coordinator(self):
        result = self.merge()
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn('review/thread proof unavailable', result.stdout)
        self.assertIn('"--match-head-commit", "' + HEAD + '"', result.stdout)
        self.assertNotIn(OTHER, result.stdout)

    def test_pr_drift_closed_draft_reviews_and_unknown_mergeability_cannot_merge(self):
        changes = [('headRefOid', OTHER), ('baseRefOid', OTHER), ('baseRefName', 'master'),
                   ('state', 'CLOSED'), ('state', 'MERGED'), ('isDraft', True),
                   ('reviewDecision', 'CHANGES_REQUESTED'), ('reviewDecision', None),
                   ('mergeable', 'UNKNOWN'), ('mergeable', 'CONFLICTING'),
                   ('mergeStateStatus', 'UNKNOWN'), ('mergeStateStatus', 'BLOCKED')]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                self.clear_calls()
                observed = self.observation()
                observed[key] = value
                result = self.merge(observation=observed)
                self.assertEqual(result.returncode, 3)
                self.assertIn('observation rejected', result.stderr)
                self.assertNotIn('proposed argv', result.stdout)

    def test_missing_duplicate_and_unsupported_checks_cannot_merge(self):
        check = self.observation()['statusCheckRollup'][0]
        other = dict(check, name='unrelated')
        for checks in (None, [], [other], [check, check],
                       [{'__typename': 'StatusContext', 'context': 'test', 'state': 'SUCCESS'}],
                       [dict(check, name='')], [dict(check, status='QUEUED')]):
            with self.subTest(checks=checks):
                self.clear_calls()
                observed = self.observation()
                observed['statusCheckRollup'] = checks
                result = self.merge(observation=observed)
                self.assertEqual(result.returncode, 3)
                self.assertIn('observation rejected', result.stderr)
                self.assertNotIn('proposed argv', result.stdout)

    def test_every_non_success_and_unknown_conclusion_fails_closed(self):
        for conclusion in ('FAILURE', 'CANCELLED', 'TIMED_OUT', 'ACTION_REQUIRED',
                           'NEUTRAL', 'STALE', 'STARTUP_FAILURE', 'SKIPPED',
                           'FUTURE_STATE', '', None):
            with self.subTest(conclusion=conclusion):
                self.clear_calls()
                observed = self.observation()
                observed['statusCheckRollup'][0]['conclusion'] = conclusion
                result = self.merge(observation=observed)
                self.assertEqual(result.returncode, 3)
                self.assertIn('observation rejected', result.stderr)
                self.assertNotIn('proposed argv', result.stdout)

    def test_skip_is_explicit_and_does_not_replace_required_success(self):
        manifest = self.approval()
        manifest['approvals'][0]['permitted_skips'] = ['optional']
        observed = self.observation()
        observed['statusCheckRollup'].append({
            '__typename': 'CheckRun', 'name': 'optional', 'status': 'COMPLETED', 'conclusion': 'SKIPPED'})
        result = self.merge(manifest, observed)
        self.assertIn('review/thread proof unavailable', result.stdout)
        manifest['approvals'][0]['permitted_skips'] = []
        result = self.merge(manifest, observed)
        self.assertNotIn('proposed argv', result.stdout)

    def test_invalid_or_empty_approval_rejected_before_provider_call(self):
        changes = [('head_sha', HEAD[:12]), ('base_sha', 'development'),
                   ('base_branch', 'main'), ('required_success_checks', []),
                   ('required_success_checks', ['test', 'test']), ('permitted_skips', ['test'])]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                self.clear_calls()
                manifest = self.approval()
                manifest['approvals'][0][key] = value
                result = self.merge(manifest)
                self.assertEqual(result.returncode, 3)
                self.assertEqual(self.calls('gh'), [])
        for kind in ('duplicate-key', 'missing-skip-list', 'duplicate-pr'):
            with self.subTest(kind=kind):
                self.clear_calls()
                manifest = self.approval()
                if kind == 'duplicate-pr':
                    manifest['approvals'].append(copy.deepcopy(manifest['approvals'][0]))
                elif kind == 'missing-skip-list':
                    del manifest['approvals'][0]['permitted_skips']
                file = self.root / 'approval.json'
                payload = json.dumps(manifest)
                if kind == 'duplicate-key':
                    payload = payload.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1')
                file.write_text(payload)
                result = self.invoke('merge_on_green.sh', file)
                self.assertEqual(result.returncode, 3)
                self.assertEqual(self.calls('gh'), [])

    def open_pr(self):
        body = self.root / 'PR.md'
        body.write_text('Reviewed task report; exact head/base recorded.')
        return self.invoke('open_pr.sh', 'fix/example', HEAD, BASE, 'Example change', body)

    def test_failed_push_never_creates_pr(self):
        self.config['push_exit'] = 27
        result = self.open_pr()
        self.assertEqual(result.returncode, 27, result.stderr)
        self.assertEqual(self.calls('gh'), [])
        self.assertEqual(sum('push' in r['args'] for r in self.calls('git')), 1)

    def test_stale_remote_head_and_base_drift_never_create_pr(self):
        for setting in ({'remote_head': OTHER}, {'remote_base_after': OTHER}, {'duplicate_remote': True}):
            with self.subTest(setting=setting):
                self.clear_calls()
                self.config = {'head': HEAD, 'base': BASE, **setting}
                result = self.open_pr()
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertEqual(self.calls('gh'), [])

    def test_local_head_and_initial_base_drift_stop_before_push(self):
        for head, base in ((OTHER, BASE), (HEAD, OTHER)):
            with self.subTest(head=head, base=base):
                self.clear_calls()
                self.config.update(head=head, base=base)
                result = self.open_pr()
                self.assertEqual(result.returncode, 3)
                self.assertFalse(any('push' in r['args'] for r in self.calls('git')))
                self.assertEqual(self.calls('gh'), [])

    def test_exact_remote_head_allows_only_expected_pr_creation(self):
        result = self.open_pr()
        self.assertEqual(result.returncode, 0, result.stderr)
        create = self.calls('gh')
        self.assertEqual(len(create), 1)
        self.assertEqual(create[0]['args'][:2], ['pr', 'create'])
        self.assertIn('development', create[0]['args'])
        self.assertIn('fix/example', create[0]['args'])
        push = [r for r in self.calls('git') if 'push' in r['args']][0]
        self.assertIn(HEAD + ':refs/heads/fix/example', push['args'])

    def burst(self, sha=HEAD, coord='77', import_check=True):
        env = dict(self.env, SLURM_JOB_ID='88', SLURM_CPUS_PER_TASK='2', SLURM_CPUS_ON_NODE='128',
                   LSMS_DATA_DIR='/not-admitted/shared', LSMS_CONFIG_DIR='/not-admitted/config',
                   LSMS_ALLOW_INTREE_SOURCES='1', LSMS_BUILD_WORKERS='64',
                   AWS_ACCESS_KEY_ID='SYNTHETIC_ONLY', MICRODATA_API_KEY='SYNTHETIC_ONLY',
                   LSMS_SKIP_AUTH='0', DVC_CACHE_DIR='/not-admitted/l1',
                   XDG_CACHE_HOME='/not-admitted/cache', PYTHONHOME='/not-admitted/python')
        args = ['--source', self.source, '--expected-source-sha', sha,
                '--python', self.bin / 'prepared-python', '--coordinator-job', coord,
                '--work-root', self.root / 'private']
        if import_check:
            args.append('--lsms-import-check')
        return self.invoke('burst.sh', *args, '--', self.bin / 'task-probe', env=env)

    def test_private_isolation_precedes_first_import_and_task(self):
        result = self.burst()
        self.assertEqual(result.returncode, 0, result.stderr)
        records = self.calls()
        probes = [r for r in records if r['name'] in ('prepared-python', 'task-probe')]
        self.assertEqual([r['name'] for r in probes], ['prepared-python', 'task-probe'])
        for r in records:  # Even clone sees private roots, never inherited credentials.
            env = r['env']
            for key in ('AWS_ACCESS_KEY_ID', 'MICRODATA_API_KEY', 'LSMS_ALLOW_INTREE_SOURCES',
                        'DVC_CACHE_DIR', 'PYTHONHOME', 'SLURM_CPUS_ON_NODE'):
                self.assertNotIn(key, env)
            for key in ('LSMS_BUILD_WORKERS', 'OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS'):
                self.assertEqual(env[key], '1')
            self.assertEqual(env['SLURM_CPUS_PER_TASK'], '2')
            self.assertEqual(env['LSMS_SKIP_AUTH'], '1')
            self.assertEqual(env['HOME'], str(self.home))
            self.assertEqual(env['CODEX_HOME'], str(self.codex))
            for key in ('LSMS_CONFIG_DIR', 'LSMS_DATA_DIR', 'XDG_STATE_HOME', 'XDG_CACHE_HOME',
                        'DVC_SITE_CACHE_DIR', 'TMPDIR', 'MPLCONFIGDIR'):
                path = Path(env[key])
                self.assertTrue(path.is_relative_to(self.root / 'private'), (key, path))
                self.assertTrue(path.is_dir(), (key, path))
                self.assertFalse(path.is_symlink(), (key, path))
            l1 = Path(env['LSMS_DATA_DIR']) / 'dvc-cache'
            self.assertTrue(l1.is_dir())
            self.assertFalse(l1.is_symlink())
            self.assertEqual(list(l1.iterdir()), [])
        checkout = [r for r in records if 'checkout' in r['args']][0]
        self.assertEqual(checkout['args'][-2:], ['--detach', HEAD])
        self.assertIn('source', Path(probes[0]['cwd']).parts)

    def test_source_free_mode_omits_ls_ms_import(self):
        result = self.burst(import_check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls('prepared-python'), [])
        self.assertEqual(len(self.calls('task-probe')), 1)

    def test_source_sha_drift_or_attached_head_cannot_start_task(self):
        for config in ({'head': OTHER}, {'attached': True}):
            with self.subTest(config=config):
                self.clear_calls()
                self.config = {'head': HEAD, 'base': BASE, **config}
                result = self.burst()
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertEqual(self.calls('prepared-python'), [])
                self.assertEqual(self.calls('task-probe'), [])

    def test_short_sha_and_coordinator_job_refused_before_clone(self):
        for sha, coord in ((HEAD[:12], '77'), (HEAD, '88')):
            with self.subTest(sha=sha, coord=coord):
                self.clear_calls()
                result = self.burst(sha=sha, coord=coord)
                self.assertEqual(result.returncode, 3)
                self.assertEqual(self.calls(), [])

    def test_burst_cannot_override_coordinator_or_skip_task_cpu_binding(self):
        args = ['--source', self.source, '--expected-source-sha', HEAD,
                '--python', self.bin / 'prepared-python', '--coordinator-job', '77']
        env = dict(self.env, SLURM_JOB_ID='88', SLURM_CPUS_PER_TASK='2')
        result = self.invoke('burst.sh', *args, '--coordinator-job', '66',
                             '--', self.bin / 'task-probe', env=env)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(self.calls(), [])
        del env['SLURM_CPUS_PER_TASK']
        result = self.invoke('burst.sh', *args, '--', self.bin / 'task-probe', env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_burst_task_exit_propagates(self):
        self.config['task_exit'] = 42
        result = self.burst()
        self.assertEqual(result.returncode, 42, result.stderr)

    def submit(self, log=False, extra=()):
        args = ['-c', '3', '--coordinator-job', '77', '--exclude-nodes', 'coord-node']
        if log:
            args.extend(['-l', self.root / 'log.txt'])
        args.extend(extra)
        args.extend(['--', '--source', self.source, '--expected-source-sha', HEAD,
                     '--python', self.bin / 'prepared-python', '--', self.bin / 'task-probe'])
        env = dict(self.env, SLURM_JOB_ID='77', SLURM_JOB_NODELIST='coord-node',
                   LSMS_DATA_DIR='/not-admitted/data', AWS_ACCESS_KEY_ID='SYNTHETIC_ONLY',
                   TMPDIR='/not-admitted/tmp', SUCODER_LOCAL_ROOT='/not-admitted/native')
        return self.invoke('submit.sh', *args, env=env)

    def test_submit_isolates_srun_environment_and_resources(self):
        result = self.submit()
        self.assertEqual(result.returncode, 0, result.stderr)
        call = self.calls('srun')[0]
        for key in ('SLURM_JOB_ID', 'SLURM_JOB_NODELIST', 'LSMS_DATA_DIR', 'AWS_ACCESS_KEY_ID',
                    'TMPDIR', 'SUCODER_LOCAL_ROOT'):
            self.assertNotIn(key, call['env'])
        self.assertEqual(call['env']['HOME'], str(self.home))
        self.assertEqual(call['env']['CODEX_HOME'], str(self.codex))
        self.assertEqual(call['args'][call['args'].index('-c') + 1], '3')
        self.assertIn('--exclude=coord-node', call['args'])
        self.assertIn('co_carleton', call['args'])
        self.assertIn(HEAD, call['args'])

    def test_submit_preserves_failure_with_and_without_log(self):
        self.config['srun_exit'] = 42
        for log in (False, True):
            with self.subTest(log=log):
                result = self.submit(log=log)
                self.assertEqual(result.returncode, 42, result.stderr)
        result = self.submit(extra=('-a', 'fc_jevons'))
        self.assertEqual(result.returncode, 3)

    def test_submit_refuses_missing_exclusion(self):
        result = self.invoke('submit.sh', '--coordinator-job', '77', '--', '--source', self.source)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(self.calls('srun'), [])

    def test_unknown_submission_is_returned_once_without_retry(self):
        self.config['srun_exit'] = 3
        result = self.submit(log=True)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(len(self.calls('srun')), 1)

    def test_submit_cannot_forward_an_exclusion_override(self):
        result = self.invoke('submit.sh', '--coordinator-job', '77', '--exclude-nodes', 'coord-node',
                             '--', '--coordinator-job', '66', '--', self.bin / 'task-probe')
        self.assertEqual(result.returncode, 3)
        self.assertEqual(self.calls('srun'), [])

    def briefs(self, total=205):
        body = 'START-BODY\n' + 'b' * 8000 + '\nEND-BODY {BASE_SHA}'
        comments = [{'id': i + 1, 'body': ('EARLY BINDING RULING\n' + 'r' * 4000 if i == 0 else f'comment {i}'),
                     'updated_at': '2026-11-08T00:00:00Z', 'user': {'login': 'maintainer'}}
                    for i in range(total)]
        self.config.update(issue={'number': 12, 'body': body, 'title': 'Synthetic issue',
                                  'updated_at': '2026-11-08T00:00:00Z', 'comments': total},
                           pages=[comments[i:i + 100] for i in range(0, total, 100)] or [[]])
        queue = self.root / 'queue.json'
        queue.write_text(json.dumps({'Synthetic': [12]}))
        plan = self.root / 'task.txt'
        plan.write_text('Source-free task; no imports or survey access. Required own ledger and evidence.')
        args = ['--queue', queue, '--out', self.root / 'briefs', '--base-sha', BASE,
                '--candidate-sha', HEAD, '--sweep-dir', '/approved/sweep', '--python', sys.executable,
                '--coord-clone', '/approved/coordinator', '--worktree-root', '/approved/worktrees',
                '--task-plan', plan, '--repo', 'example/library']
        return self.invoke('make_briefs.py', *args), body, comments

    def test_full_body_and_every_page_retains_early_ruling(self):
        result, body, comments = self.briefs()
        self.assertEqual(result.returncode, 0, result.stderr)
        text = (self.root / 'briefs/Synthetic-r1.md').read_text()
        self.assertIn(body, text)  # Includes literal template-looking braces.
        for c in comments:
            self.assertIn(c['body'], text)
        self.assertIn(BASE, text)
        self.assertIn(HEAD, text)
        self.assertIn('Source-free task; no imports or survey access.', text)
        evidence = json.loads((self.root / 'briefs/Synthetic-r1.issues.json').read_text())
        record = evidence['records'][0]
        self.assertEqual(record['pagination']['page_sizes'], [100, 100, 5])
        self.assertEqual(record['pagination']['total_comments'], 205)
        self.assertEqual(record['comments'], comments)
        self.assertEqual(record['issue']['body'], body)
        self.assertIn('--paginate', record['pagination']['comments_argv'])
        self.assertIn('--slurp', record['pagination']['comments_argv'])
        self.assertIn('complete current body', text)

    def test_changed_history_stops_before_brief_action(self):
        self.config['issue_drift'] = True
        result, _, _ = self.briefs()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('changed during retrieval', result.stderr)
        self.assertFalse((self.root / 'briefs/Synthetic-r1.md').exists())

    def test_missing_duplicate_or_unsupported_comment_pages_refuse_brief(self):
        for fault in ('missing', 'duplicate', 'unsupported'):
            with self.subTest(fault=fault):
                self.config['comment_fault'] = fault
                result, _, _ = self.briefs()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / 'briefs/Synthetic-r1.md').exists())

    def test_empty_comment_history_has_explicit_count_receipt(self):
        result, _, _ = self.briefs(total=0)
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = json.loads((self.root / 'briefs/Synthetic-r1.issues.json').read_text())
        self.assertEqual(evidence['records'][0]['pagination']['page_sizes'], [0])
        self.assertEqual(evidence['records'][0]['pagination']['total_comments'], 0)

    def test_prep_pins_base_and_stops_on_semantic_conflict(self):
        self.config.update(head=BASE, cherry_exit=1)
        result = self.invoke('prep_branch.sh', 'new/example', BASE, HEAD, OTHER)
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn('preserve all prior authority', result.stderr)
        picks = [r for r in self.calls('git') if 'cherry-pick' in r['args']]
        self.assertEqual(len(picks), 1)
        self.assertIn(HEAD, picks[0]['args'])
        self.assertFalse(any('fetch' in r['args'] or '--continue' in r['args'] for r in self.calls('git')))

    def test_prep_base_drift_refuses_worktree_mutation(self):
        self.config['base'] = OTHER
        result = self.invoke('prep_branch.sh', 'new/example', BASE, HEAD)
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertFalse(any('worktree' in r['args'] for r in self.calls('git')))

    def test_report_copy_is_exact_explicit_text_and_dispatch_report_only(self):
        report = self.root / 'final.txt'
        payload = b'Final report\r\nCommands run: none.\nUnverified: functional checks.\n'
        report.write_bytes(payload)
        out = self.root / 'reports/REPORT.md'
        result = self.invoke('save_report.py', '--report-artifact', report, '--out', out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(out.read_bytes(), payload)
        self.assertEqual(out.stat().st_mode & 0o777, 0o600)
        evidence = self.root / 'evidence'
        evidence.mkdir()
        (evidence / 'report.md').write_bytes(payload)
        (evidence / 'unrelated.jsonl').write_text('must not parse')
        result = self.invoke('save_report.py', '--dispatch-evidence-dir', evidence, '--out', out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(out.read_bytes(), payload)
        self.assertEqual(self.calls(), [])

    def test_native_transcript_empty_and_symlink_reports_rejected(self):
        out = self.root / 'REPORT.md'
        transcript = self.root / 'transcript.jsonl'
        transcript.write_text('{"message":{"role":"assistant","content":"do not extract"}}')
        native = self.codex / 'native.txt'
        native.write_text('synthetic native-state sentinel')
        empty = self.root / 'empty.md'
        empty.write_text(' \n')
        link = self.root / 'link.md'
        link.symlink_to(native)
        for src in (transcript, native, empty, link):
            with self.subTest(src=src):
                result = self.invoke('save_report.py', '--report-artifact', src, '--out', out)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(out.exists())
        out.symlink_to(native)
        final = self.root / 'final.md'
        final.write_text('actual task report')
        result = self.invoke('save_report.py', '--report-artifact', final, '--out', out)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(native.read_text(), 'synthetic native-state sentinel')


if __name__ == '__main__':
    unittest.main(verbosity=2)
