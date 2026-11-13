"""Root read-only acceptance of the separately frozen GH1206 critical supplement."""
from pathlib import Path
import ast
import collections
import datetime
import hashlib
import json
import os
import stat
import subprocess

CONTROL = Path('/local/job40255479/control/isa-backlog-20261021')
WORK = Path('/local/job40255479/worktrees')
source = CONTROL / 'verify_849_218_root.py'
binding = json.loads((WORK / 'isa-campaign/slurm_logs/isa_backlog_2026-10-21/root-checkers/849-218-checker-binding.json').read_text())
assert hashlib.sha256(source.read_bytes()).hexdigest() == binding['sha256']
tree = ast.parse(source.read_text())
# Reuse only the already-reviewed stdlib receipt/manifest/consumer functions;
# none of the earlier executable acceptance body or mkdir calls runs.
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
exec(compile(ast.Module(body=functions, type_ignores=[]), str(source) + '[functions-only]', 'exec'))

P = CONTROL / 'comparator-values-reassessment'
F = P / 'critical-release'
A = P / 'critical-release-attestation'
specs = [
    (F, '63419e4e8f4559e34f411d346eefce85906877057641696351a92fc75affa91a',
     'ff982bd1561f6f79151f18a0b048cfb694990514a8ef5562fceb4f32795bd309', 449),
    (A, '408ae7ced1fdbf4b3ce86ac223f633dc264ca68bea3d2229101ac9f849acd82c',
     '598f1016f1dca1c0454176ac8735e34af82906caffe5a527817a8d865b910bb3', 3),
]
for root, ready_sha, mf_sha, count in specs:
    assert digest(root / 'READY.json') == ready_sha
    ready = load(root / 'READY.json')
    assert ready['manifest_entries'] == count and ready['manifest_sha256'] == mf_sha
    entries = verify_manifest(root, 'FROZEN-MANIFEST.json', mf_sha, count)
    paths = {str(q.relative_to(root)) for q in root.rglob('*') if q.is_file() or q.is_symlink()}
    assert paths == set(entries) | {'FROZEN-MANIFEST.json', 'READY.json'}
    for rel, receipt in ready['safe_export_allowlist'].items():
        verify_receipt(root / rel, receipt)
proof = load(F / 'safe-exports/proof.json')
assert proof['total_executed_pair_groups'] == 433
assert proof['full_strict_groups'] == 407 and proof['qualified_stage_groups'] == 26
assert proof['mismatches'] == proof['source_API_build_test_replays'] == 0
assert proof['remaining_implementation_review_cohorts'] == 22
assert proof['remaining_integration_cohorts'] == 3 and not proof['all_campaign_result']
assert proof['original162main_and152source_bound_cache_families_reassessed']
assert proof['original76direct_cache_cold_warm_family_reassessed']
main = load(F / 'critical-pair-results-private.json')
direct = load(F / 'critical-direct-cache-pairs-private.json')
extra = load(F / '1196-extra-pair-results-private.json')
assert len(main) == 341 and len(direct) == 76 and len(extra) == 16
assert all(r['status'] == 'PASS' for r in main + direct + extra)
assert collections.Counter(r['cohort'] for r in main) == {'twenty': 329, '1196': 12}
assert sum(bool(r['qualification']) for r in main) == 25
assert sum(bool(r['qualification']) for r in extra) == 1
assert all(r['left'] != r['right'] for r in main + direct)
calibration = load(F / 'safe-exports/calibration.json')
assert len(calibration['negative_controls']) == 12
assert set(calibration['old_pandas_frame_escape_demonstrated']) == {'tiny', 'nextafter'}
checks = load(F / 'safe-exports/checker-sources.json')
for rel, receipt in checks.items():
    assert digest(F / rel) == receipt['sha256'] and (F / rel).stat().st_size == receipt['bytes']
attest = load(A / 'POSTEXIT-safe.json')
assert attest['critical_manifest_entries'] == 449 and attest['zero_active_analysis_consumers_at_check']
assert attest['write_denials'] == attest['network_denials'] == 0
assert attest['full_old_manifest_entries_after_analysis'] == {'twenty': 2629, '1196': 281}
assert digest(A / 'package-critical.py') == attest['actual_packaging_source_sha256']
assert digest(A / 'attest-critical.py') == attest['actual_postexit_source_sha256']
verify_manifest(CONTROL / 'integration/tanzania-twenty-review', 'private-final-manifest.json',
                '74b8958b3914c9012401555c1fbe293cdd5672c09cf548130c82dd6e6a3d1b9f', 2629)
verify_manifest(CONTROL / 'shared1196-diagnosis', 'final-private-manifest-v4.json',
                '368e4997fd99d0982c6db54992fa19e8f6bea82271b28a1e2ec91e2d1afcd24c', 281)
R = WORK / 'isa-review-comparator-values'
assert git(R, 'rev-parse', 'HEAD').decode().strip() == 'f8d236694c26fe9e709c5548f02c70f74ea72c33'
assert git(R, 'status', '--porcelain') == b''
prepared = load(F / 'critical-preparation-safe.json')
for cohort in ('twenty', '1196'):
    for module in ('population', 'recall', 'sampling', 'derivations'):
        row = prepared['class_module_blobs'][cohort + '/' + module]
        rel = 'lsms_library/' + module + '.py'
        assert git(R, 'rev-parse', 'HEAD:' + rel).decode().strip() == row['prepared_f8_blob']
        assert row['cohort_blobs_exact']
        actual = (R / rel).read_bytes()
        assert hashlib.sha1(b'blob ' + str(len(actual)).encode() + b'\0' + actual).hexdigest() == row['prepared_f8_blob']
live_consumers([F, A])
output = CONTROL / '1206-critical-ROOT-ACCEPTANCE.json'
result = {
    'status': 'ROOT_ACCEPT_BOUNDED_CRITICAL_NUMERIC_SUPPLEMENT',
    'recorded_at_UTC': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'critical_READY_sha256': specs[0][1], 'critical_manifest_sha256': specs[0][2],
    'attestation_READY_sha256': specs[1][1], 'attestation_manifest_sha256': specs[1][2],
    'all_new_manifest_entries_verified': 452, 'old_manifest_entries_unchanged': 2910,
    'safe_exports_verified': 8, 'checker_sources_verified': len(checks) + 2,
    'executed_groups': 433, 'full_strict': 407, 'qualified_stages': 26,
    'groups_overlap_unique_pair_count_not_claimed': True, 'mismatches': 0,
    'negative_controls_rejected': 12, 'original_162_main_and_152_source_bound_and_76_direct_cache_families_covered': True,
    'saved_CLI_byte_controls': 16, 'all_campaign_result': False,
    'remaining_implementation_review_cohorts': 22, 'remaining_integration_cohorts': 3,
    'root_read_actual_comparator_pair_selection_qualification_and_postexit_sources': True,
    'root_verification_stdlib_JSON_receipts_and_static_sources_only': True,
    'no_payload_decode_API_source_or_test_replay': True,
    'preserved_limits': [
        'NA-policy, JSON typed attrs, unused writer index levels, empty Description dtype and id_converted qualifications remain explicit.',
        'Fourteen additional retained-native/raw parquet pairs are new artifact-stage reads, not recovered original prewrite or map_index memory.',
        'Combined20 overwritten populated-child-log history remains missing.',
        'No all-campaign numeric verdict, full suite, production fix, merge or issue closure.'
    ], 'signed': 'Sue'
}
with output.open('x') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
output.chmod(0o600)
print(json.dumps({k: result[k] for k in ['status', 'executed_groups', 'full_strict', 'qualified_stages',
                                       'mismatches', 'remaining_implementation_review_cohorts']}))
