"""Pin only the four independently diagnosed cold/warm differences.

This writes audit evidence only; it does not modify caches or source. Exact
recorded fingerprints and evidence hashes prevent a broad dtype/loss waiver.
"""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parent / 'corpus'
SHA = '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f'

CASES = [
    ('Liberia', 'interview_date', 'serialization_timestamp_resolution',
     '12468dab89868f7c9790ab1a0fdc67d87a66d8ac2602b6e5071c17818179d7b3',
     '5998082be93cb80d5bbe5e298f6cec70ef43fc5452704bc129a078ccd194707e',
     'Seconds to milliseconds; exact timestamps at ns, missingness, index and wave counts preserved.'),
    ('Mali', 'household_roster', 'serialization_numeric_scalars_to_text',
     'd7ff366add23225bbdd3fbec487ac91afb19128a692ac464ca675bef3f183da9',
     '6c400824a4bd8599e7921b1049a55e914c99661b0925571db2d5b348317607de',
     'MonthsSpent numeric scalars become text through existing writer cast. Exact numeric values, null masks, keys, waves and other six columns preserved; arithmetic interface differs.'),
    ('Mali', 'interview_date', 'existing_798_warm_date_row_loss',
     '2d4fefbce6238f216a09e3042abaec82c8ed3aaeab330a630564f471ffeaf299',
     '9285bfbf9190053270d4f4d20b4f8ec4b92e52610c4e5637559003c63196aa94',
     'Existing mixed whole-column datetime parsing drops 12,745 valid warm rows. Complete raw cache preserved; no production repair and no API parity claim.'),
    ('Tanzania', 'interview_date', 'existing_798_warm_date_row_loss',
     '53085328789e3e2a72ae3b4f0150abe99e571e46fc76e680897f2e8f6832fd3c',
     'd593293bb3d884ed191bfb83f73cc63d0d9954c3b87204bd0bde010cc21e9215',
     'Existing mixed whole-column datetime parsing drops 5,893 valid warm rows, separate from six original null dates. Complete raw cache preserved; no production repair and no API parity claim.'),
]


def get(root, country, table):
    return next(r for r in map(json.loads, (root / country / 'results.jsonl').read_text().splitlines()) if r['table'] == table)


records = []
for country, table, classification, before_hash, after_hash, scope in CASES:
    before, after = get(BASE, country, table), get(ROOT / 'run', country, table)
    assert before['served']['content_hash'] == before_hash
    assert after['served']['content_hash'] == after_hash and after['source_sha'] == SHA
    assert before['status'] == after['status'] == 'success'
    records.append({'country': country, 'table': table, 'field': 'served',
        'before': before['served'], 'after': after['served'],
        'classification': classification, 'scope': scope})
evidence = [ROOT / 'mali-date-review/REPORT.md', ROOT / 'mali-date-review/replay.json',
            ROOT / 'tanzania-date-review/REPORT.md', ROOT / 'tanzania-date-review/replay.json',
            ROOT / 'date-workaround/REPORT.md', ROOT / 'date-workaround/result.json',
            ROOT.parents[1] / 'framework/dtype-roundtrip-publishable/REPORT.md',
            ROOT.parents[1] / 'framework/dtype-roundtrip-publishable/strict-semantics.json',
            ROOT.parents[1] / 'countries/mali-date-review/REVIEW.md',
            ROOT.parents[1] / 'countries/mali-date-review/validation.json',
            ROOT.parents[1] / 'countries/tanzania-date-review/REVIEW.md',
            ROOT.parents[1] / 'countries/tanzania-date-review/validation.json']
result = {'source_sha': SHA, 'baseline_sha': '7fa05179c8519f44ea329ba6387f5d6d6441a8ea',
          'universal_cold_warm_api_equality': False, 'exceptions': records,
          'evidence_sha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in evidence}}
target = ROOT / 'served-exceptions.json'
assert not target.exists(), 'refusing to overwrite a frozen allowlist'
target.write_text(json.dumps(result, indent=2) + '\n')
print(hashlib.sha256(target.read_bytes()).hexdigest())
