"""Summarise a pytest -v -rfE log without copying data: outcome counts, the per-node
outcomes of chosen files, failed node ids, and only numeric assertion lines.
Usage: python pysum.py <log> <out.json> [file-substring ...]"""
import collections
import json
import re
import sys

log, out = sys.argv[1], sys.argv[2]
watch = sys.argv[3:]
text = open(log, errors='replace').read().splitlines()
res = {'log_lines': len(text)}
node = re.compile(r'^(?:\[gw\d+\] \[\s*\d+%\] )?(PASSED|FAILED|SKIPPED|ERROR|XFAIL|XPASS)\s+(\S+::\S+)|^(\S+::\S+) (PASSED|FAILED|SKIPPED|ERROR|XFAIL|XPASS)')
outcomes = {}
for ln in text:
    m = node.search(ln)
    if m:
        st, nid = (m.group(1), m.group(2)) if m.group(1) else (m.group(4), m.group(3))
        outcomes[nid] = st
res['counts'] = dict(collections.Counter(outcomes.values()))
res['summary_line'] = [ln for ln in text if re.match(r'^=+ .*(passed|failed|error).* =+$', ln)][-1:]
res['failed'] = sorted(n for n, s in outcomes.items() if s in ('FAILED', 'ERROR'))
res['watched'] = {n: s for n, s in sorted(outcomes.items()) if any(w in n for w in watch)} if watch else {}
num_assert = re.compile(r'^E\s+(assert [-+\d.e_, ]+ (==|<=|>=|<|>|!=) [-+\d.e_, ]+)$')
exc = re.compile(r'^E\s+([A-Z][A-Za-z]+(Error|Exception|Warning))\b')
res['numeric_asserts'] = [m.group(1) for ln in text if (m := num_assert.match(ln))][:60]
res['exception_classes'] = dict(collections.Counter(m.group(1) for ln in text if (m := exc.match(ln))))
json.dump(res, open(out, 'w'), indent=1)
print(json.dumps({k: res[k] for k in ('counts', 'summary_line', 'failed')}, indent=1)[:5000])
