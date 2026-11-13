"""Export only safe platform aggregates after all owned jobs finish."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
summary = json.loads((root / 'verification.json').read_text())
assert summary['sha'] == '097283459f80016b0f00a51b814a0cb964a42789'
assert summary['run'] == 39604418333
assert summary['state'] == 'platform jobs passed; Linux data suite reported separately'
jobs = summary['jobs']
for name, record in jobs.items():
    if record['conclusion'] == 'success':
        assert record['actual_checkout_shas'] == [summary['sha']]
    else:
        assert name == 'refresh-dvc-locks' and record['conclusion'] == 'skipped'
# The shell source text echoes both branches; it cannot establish which branch ran.
jobs['dvc-lock-check'].pop('dirty_output_true_witness', None)
jobs['dvc-lock-check']['lock_check_limit'] = 'The check uses continue-on-error. Individual step outcomes are preserved; refresh-dvc-locks was skipped, so no automatic refresh was scheduled in this run.'
summary['credential_count_limit'] = 'Zero literal error-marker witnesses is not individual adjudication of every skipped test. Unit and Windows smoke are intentionally data-free; native Windows builds and the separate data gate have their own credential evidence.'
(root / 'verification.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
unit = jobs['unit-tests']['pytest_summaries']
smoke = jobs['windows-smoke']['pytest_summaries']
native = jobs['windows-script-build']['native_results']
report = f'''# Master release CI: platform jobs

PASS for the owned platform scope of [run 39604418333](https://github.com/ligon/LSMS_Library/actions/runs/39604418333), automatic master push at `{summary['sha']}`.
All six executed platform jobs independently expose that exact checkout in their completed logs.
Linux data-suite completion and the whole workflow verdict are reported separately; neither is inferred from this platform PASS.

| Job | Result |
| --- | --- |
| unit-tests | success; {unit[0] if unit else 'see verification.json'} |
| windows-smoke | success; {smoke[0] if smoke else 'see verification.json'} |
| windows-script-build | success; installed code/config checks and real cold tables below |
| dvc-remote-reachability | success; credentials configured and remote sweep step passed |
| dvc-lock-check | success; per-step outcomes retained in verification.json |
| docs | success; documentation build/upload/deployment steps passed |
| refresh-dvc-locks | skipped; no automatic refresh was scheduled |

The actual native Windows installed-library run produced:

| Table | Rows | Columns | Waves | Digest |
| --- | ---: | ---: | ---: | --- |
'''
for row in native:
    report += f"| {row['table']} | {row['rows']} | {row['columns']} | {row['waves']} | `{row['digest']}` |\n"
report += '''
Both match the approved expectations. The native job's installed-code/package-config assertions pass;
its Make variable expansion has a nonempty parquet list and zero find errors. S3 credential configuration
and the actual cold-build step both succeed. This is actual 097 master evidence, not reuse of historical
native results. It exercises two installed data paths, not all installed microdata or scientific policies.

The exact run's Linux `requires_s3` gate is already successful in job metadata; the data monitor owns
its raw log, complete suite and credential reconciliation. Platform raw logs contain zero literal
`NoCredentialsError`, `PartialCredentialsError`, `UnmarkedS3TestWarning` or runtime credential-skip
marker witnesses. Unit/smoke authentication is intentionally disabled and their skips remain skips;
this report does not individually adjudicate the entire skip population.

`verification.json` retains each job/step result, actual checkout SHA, log byte count/SHA256, native
dimensions/digests and literal credential-indicator counts. Full logs and raw API metadata stay private.
Logs were fetched only after their jobs completed. Polling used read-only GETs every 45 seconds;
no workflow dispatch/rerun, Git/GitHub mutation, package import, local test or cache operation occurred.

All owned monitoring/log-fetch processes are stopped. Release owner still owns the remaining Linux
gate, local Makefile gate, tag and publication.

Sue
'''
(root / 'REPORT.md').write_text(report)
copy = ['REPORT.md', 'verification.json', 'monitor.py', 'finalize.py', 'COPYLIST.txt']
(root / 'COPYLIST.txt').write_text('\n'.join(copy + ['MANIFEST.json']) + '\n')
manifest = {name: {'sha256': hashlib.sha256((root / name).read_bytes()).hexdigest(), 'bytes': (root / name).stat().st_size} for name in copy}
(root / 'MANIFEST.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
print('manifest_sha256', hashlib.sha256((root / 'MANIFEST.json').read_bytes()).hexdigest())
