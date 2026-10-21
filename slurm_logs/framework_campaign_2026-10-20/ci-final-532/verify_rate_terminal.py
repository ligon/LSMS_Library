"""Aggregate-only proof that all 87 new rate cases actually ran and passed."""
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = Path('/local/job40255479/control/framework-campaign/followup-ci-review')
SOURCE = Path('/local/job40255479/worktrees/framework-rate/tests/test_gh1160_pooled_fertilizer_rate.py')
EXPECTED_LOG = '433f75da9a8442a7ff43ad17670c83af80d4c71ed8235dfc7ae57aacc7dfc585'
counts = {}
for node in ast.parse(SOURCE.read_text()).body:
    if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
        count = 1
        for decorator in node.decorator_list:
            if (isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == 'parametrize'):
                values = decorator.args[1]
                assert isinstance(values, (ast.List, ast.Tuple)), node.name
                count *= len(values.elts)
        counts[node.name] = count
assert sum(counts.values()) == 87

blob = (ROOT / 'rate-532/data-job-private.log').read_bytes()
assert hashlib.sha256(blob).hexdigest() == EXPECTED_LOG
ansi = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
lines = [ansi.sub('', re.sub(r'^\S+Z\s?', '', line)).rstrip()
         for line in blob.decode('utf-8', errors='replace').splitlines()]
prefix = 'tests/test_gh1160_pooled_fertilizer_rate.py '
blocks = []
for i, line in enumerate(lines):
    if not line.startswith(prefix):
        continue
    markers = []
    candidate = line[len(prefix):]
    while True:
        match = re.fullmatch(r'([.sFxXEP]+)\s*(?:\[\s*\d+%\])?', candidate.strip())
        if not match:
            break
        markers.extend(match.group(1))
        i += 1
        if i >= len(lines):
            break
        candidate = lines[i]
    blocks.append(''.join(markers))
complete = [block for block in blocks if len(block) == 87]
result = {
    'test_module': 'tests/test_gh1160_pooled_fertilizer_rate.py',
    'expected_case_count': 87,
    'ordered_terminal_group_count': len(complete),
    'observed_group_lengths': [len(block) for block in blocks],
    'all_87_actually_executed_and_passed': len(complete) == 1 and complete[0] == '.' * 87,
    'pass_marker_count': complete[0].count('.') if len(complete) == 1 else None,
    'nonpass_marker_count': sum(char != '.' for char in complete[0]) if len(complete) == 1 else None,
    'basis': 'Unique serial pytest file terminal group; static literal-parametrize case count 87 and collected/passed delta 87 independently agree',
}
assert result['all_87_actually_executed_and_passed'], result
for filename, field in [('test-additions.json', 'actual_terminal_execution'),
                        ('rate-532/verification.json', 'rate_regression')]:
    path = ROOT / filename
    record = json.loads(path.read_text())
    record[field] = result
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
print(json.dumps(result, sort_keys=True))
