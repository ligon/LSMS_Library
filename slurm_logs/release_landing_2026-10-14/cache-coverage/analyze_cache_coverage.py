"""Extract aggregate outcomes; keep failure payloads in private scratch."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

root = Path('/local/job39919092/release-work/logs/cache-coverage')
control = Path(__file__).parent
groups = []
failures = []
for path in sorted(root.glob('*.xml')):
    tree = ET.parse(path)
    counts = Counter()
    skips = Counter()
    failed_nodes = []
    for case in tree.findall('.//testcase'):
        node = case.attrib.get('classname', '') + '::' + case.attrib['name']
        problem = case.find('failure')
        if problem is None:
            problem = case.find('error')
        skip = case.find('skipped')
        if problem is not None:
            counts[problem.tag] += 1
            failed_nodes.append(node)
            failures.append({'group': path.stem, 'node': node,
                             'kind': problem.tag, 'message': problem.attrib.get('message'),
                             'text': problem.text})
        elif skip is not None:
            counts['xfail' if skip.attrib.get('type') == 'pytest.xfail' else 'skipped'] += 1
            skips[skip.attrib.get('message', '')] += 1
        else:
            counts['passed_or_non_strict_xpass'] += 1
    log = path.with_suffix('.log')
    summaries = [line for line in log.read_text(errors='replace').splitlines()
                 if re.search(r'\b\d+ (?:passed|failed|skipped).* in \d', line)]
    groups.append({'group': path.stem, 'counts': dict(counts),
                   'pytest_summaries': summaries[-2:], 'skip_reasons': dict(skips),
                   'failed_nodes': failed_nodes,
                   'evidence': [{'path': str(p), 'bytes': p.stat().st_size,
                                 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
                                for p in [path, log]]})
(control/'cache-coverage-summary.json').write_text(json.dumps(groups, indent=2)+'\n')
(control/'cache-coverage-failures-private.json').write_text(json.dumps(failures, indent=2)+'\n')
print(json.dumps([{k: x[k] for k in ['group', 'counts', 'pytest_summaries', 'failed_nodes']} for x in groups], indent=2))
