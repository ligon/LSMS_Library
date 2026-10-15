"""Explicit aggregate-only archive; raw warning/pytest/JSONL files stay private."""
from pathlib import Path
import collections
import csv
import hashlib
import json
import shutil

ROOT = Path(__file__).resolve().parent
OUT = ROOT.parent / 'final-promotion-publishable'
assert not OUT.exists()
OUT.mkdir()
FILES = ['REPORT.md', 'PLAN.md', 'LEDGER.md', 'copy-summary.json',
         'final-validation-summary.json', 'promotion-manifest.candidate.json',
         'served-exceptions.json', 'targeted-summary.json', 'preservation.json',
         'report-amendments.json', 'uganda-sibling-write.json',
         'run/final-hash-impact.json', 'run/served-comparison.json',
         'run/raw-cache-comparison.json', 'run/promotion-exclusions.json',
         'run/completion.json', 'run/cache-comparison.json',
         'promotion-synthetic-checks.json', 'PROMOTION_COMMANDS.md',
         'promote_cache.py', 'build_final_manifest.py', 'freeze_served_exceptions.py',
         'prepare_targeted.py', 'verify_final_preservation.py', 'publish_aggregates.py',
         'check_final_hashes.py', 'run_final.py', 'trailing_uganda.py', 'summarize_final.py',
         'final_env.sh', 'runner-adaptation.diff', 'runner-provenance.json',
         'summary-adaptation.diff', 'check_promotion_transaction.py', 'prepare_copy.py']
for name in ('mali-date-review', 'tanzania-date-review'):
    FILES += [f'{name}/{file}' for file in ('REPORT.md', 'LEDGER.md', 'replay.py', 'replay.json')]
FILES += [f'date-workaround/{file}' for file in ('REPORT.md', 'run.py', 'result.json', 'copy-manifest.json')]
for relative in FILES:
    source, dest = ROOT / relative, OUT / relative
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
records = sorted([r for p in (ROOT / 'run').glob('*/results.jsonl')
                  for r in map(json.loads, p.read_text().splitlines())], key=lambda r: (r['country'], r['table']))
assert len(records) == 549
with (OUT / 'tables.tsv').open('w') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['country', 'table', 'status', 'seconds', 'rows', 'columns', 'wave_rows', 'served_hash', 'error_type'])
    for r in records:
        s = r.get('served', {})
        writer.writerow([r['country'], r['table'], r['status'], r['elapsed_seconds'],
            s.get('shape', ['', ''])[0], s.get('shape', ['', ''])[1],
            json.dumps(s.get('wave_rows', {}), sort_keys=True),
            s.get('content_hash', r.get('content_hash', '')), r.get('error', '').split(':', 1)[0]])
with (OUT / 'warning-counts.tsv').open('w') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['country', 'table', 'warning_category', 'count'])
    for r in records:
        for category, count in sorted(r.get('warning_counts', {}).items()):
            writer.writerow([r['country'], r['table'], category, count])
trailing = json.loads((ROOT / 'run/trailing-uganda.json').read_text())
for call in trailing['calls']:
    warnings = call.pop('warnings')
    call['warning_counts'] = dict(collections.Counter(w['category'] for w in warnings))
(OUT / 'trailing-uganda-aggregate.json').write_text(json.dumps(trailing, indent=2) + '\n')
events = [json.loads(line) for line in (ROOT / 'run/processes.jsonl').read_text().splitlines()]
stopped = json.loads((ROOT / 'run/stopped-processes.json').read_text())
resumed = json.loads((ROOT / 'run/resumed-processes.json').read_text())
timing = {'country_process_wall_seconds': max(r['time'] for r in events) - min(r['time'] for r in events),
          'recorded_pause_seconds': resumed['resumed_at'] - stopped['stopped_at'],
          'per_table_elapsed_sum_seconds': sum(r['elapsed_seconds'] for r in records),
          'scope': 'Elapsed table/wall measurements include the diagnostic SIGSTOP pause when applicable; wall times overlap across at most two countries.'}
(OUT / 'timing.json').write_text(json.dumps(timing, indent=2) + '\n')
private = sorted(set((ROOT / 'run').rglob('*-warnings.json')) |
                 set((ROOT / 'run').glob('*/results.jsonl')) |
                 set(ROOT.rglob('*.log')) | {ROOT / 'run/trailing-uganda.json', ROOT / 'targeted-copy-manifest.json', ROOT / 'copy-manifest.json'})
private_meta = {str(p.relative_to(ROOT)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': p.stat().st_size}
                for p in private if p.is_file()}
(OUT / 'private-artifacts.json').write_text(json.dumps({'files': private_meta,
    'privacy': 'Raw warnings/logs may include survey identifiers or example rows and remain private. Only hashes, sizes and sanitized aggregate summaries are published.'}, indent=2) + '\n')
(OUT / 'REPRODUCTION.md').write_text('These generators were executed from /local/job39919092/release-work/control/runtime/final-promotion, against the explicitly fixed private source/cache roots in their code. They are archived here as exact generator provenance, not as portable scripts runnable in place. Reproduction needs equivalent private artifacts and a separately reviewed path configuration; never substitute the original or shared cache as a writable root. Raw JSONL/warning/test logs remain private and are identified in private-artifacts.json. No survey rows are included in this package. Independent dtype and country reviews are hash-pinned in served-exceptions.json and archived separately by the release coordinator.\n')
manifest = {str(p.relative_to(OUT)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': p.stat().st_size}
            for p in sorted(OUT.rglob('*')) if p.is_file()}
(OUT / 'MANIFEST.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'directory': str(OUT), 'files': len(manifest) + 1,
                  'bytes': sum(r['bytes'] for r in manifest.values()),
                  'manifest_sha256': hashlib.sha256((OUT / 'MANIFEST.json').read_bytes()).hexdigest()}, indent=2))
