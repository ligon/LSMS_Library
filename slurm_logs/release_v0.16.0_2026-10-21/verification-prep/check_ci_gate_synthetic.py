"""Auth-free controller controls: fake metadata/commands/time, no real CI or tests."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import ci_gate

SHA = 'a' * 40


def record(status='completed', conclusion='success', **changes):
    return dict({'id': 123, 'head_sha': SHA, 'event': 'push', 'head_branch': 'master',
                 'path': '.github/workflows/ci.yml', 'head_repository': {'full_name': 'ligon/LSMS_Library'},
                 'status': status, 'conclusion': conclusion, 'html_url': 'https://github.com/ligon/LSMS_Library/actions/runs/123'}, **changes)


class Controls(unittest.TestCase):
    def test_success(self):
        self.assertEqual(ci_gate.validate_run(record(), 123, SHA), 'success')

    def test_all_non_success_conclusions_refused(self):
        for conclusion in ['failure', 'cancelled', 'timed_out', 'neutral', 'skipped', 'action_required', 'stale', None]:
            with self.subTest(conclusion=conclusion):
                self.assertEqual(ci_gate.validate_run(record(conclusion=conclusion), 123, SHA), 'failure')

    def test_identity_mismatches_fail_closed(self):
        for fields in [{'id': 124}, {'head_sha': 'b' * 40}, {'event': 'workflow_dispatch'},
                       {'head_branch': 'development'}, {'path': '.github/workflows/publish.yml'},
                       {'head_repository': {'full_name': 'someone/else'}}, {'status': 'unknown'}]:
            with self.subTest(fields=fields), self.assertRaises(AssertionError):
                ci_gate.validate_run(record(**fields), 123, SHA)

    def test_exact_get_only_command(self):
        with patch.object(ci_gate.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(record()), '')) as call:
            self.assertEqual(ci_gate.read_run(123)['head_sha'], SHA)
        args, kwargs = call.call_args
        self.assertEqual(args[0], ['gh', 'api', 'repos/ligon/LSMS_Library/actions/runs/123'])
        self.assertNotIn('--method', args[0])
        self.assertTrue(kwargs['check'])

    def test_wait_records_local_success_then_exact_ci_success(self):
        readings = iter([record('queued', None), record('in_progress', None), record()])
        elapsed = [0]
        def sleep(seconds):
            self.assertEqual(seconds, 45)
            elapsed[0] += seconds
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory, contextlib.redirect_stdout(io.StringIO()) as printed:
            result = ci_gate.wait_for_success(123, SHA, directory, fetch=lambda _: next(readings), sleep=sleep, clock=lambda: elapsed[0])
            self.assertEqual(result['conclusion'], 'success')
            self.assertEqual(elapsed[0], 90)
            self.assertEqual(len(printed.getvalue().splitlines()), 3)

    def test_repeated_status_prints_once(self):
        readings = iter([record('in_progress', None), record('in_progress', None), record()])
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory, contextlib.redirect_stdout(io.StringIO()) as printed:
            ci_gate.wait_for_success(123, SHA, directory, fetch=lambda _: next(readings), sleep=lambda _: None, clock=lambda: 0)
            self.assertEqual(len(printed.getvalue().splitlines()), 2)

    def test_timeout_refused(self):
        elapsed = [0]
        def sleep(seconds):
            elapsed[0] += seconds
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory, contextlib.redirect_stdout(io.StringIO()), self.assertRaises(TimeoutError):
            ci_gate.wait_for_success(123, SHA, directory, timeout=45, fetch=lambda _: record('in_progress', None), sleep=sleep, clock=lambda: elapsed[0])

    def test_workflow_failure_refused(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory, self.assertRaises(RuntimeError):
            ci_gate.wait_for_success(123, SHA, directory, fetch=lambda _: record(conclusion='failure'))

    def test_five_read_errors_refused(self):
        calls = []
        def error(_):
            calls.append(1)
            raise subprocess.CalledProcessError(1, 'gh')
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory, contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
            ci_gate.wait_for_success(123, SHA, directory, fetch=error, sleep=lambda _: None, clock=lambda: 0)
        self.assertEqual(len(calls), 5)


if __name__ == '__main__':
    unittest.main()
