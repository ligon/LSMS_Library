"""Exact pooled-frame comparison plus aggregate scanner reconciliation."""
from pathlib import Path
import argparse
import json
import os
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root
checkout = Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve() == checkout / 'lsms_library/__init__.py'
assert countries_root().resolve() == checkout / 'lsms_library/countries'
ap = argparse.ArgumentParser()
ap.add_argument('--baseline-data', required=True, type=Path)
ap.add_argument('--fixed-data', required=True, type=Path)
ap.add_argument('--baseline-scan', required=True, type=Path)
ap.add_argument('--fixed-scan', required=True, type=Path)
ap.add_argument('--out', required=True, type=Path)
args = ap.parse_args()
a = pd.read_pickle(args.baseline_data / 'warm-pooled.pkl')
b = pd.read_pickle(args.fixed_data / 'warm-pooled.pkl')
def organic_malawi(frame):
    return ((frame.index.get_level_values('country') == 'Malawi') &
            (frame.index.get_level_values('input') == 'Organic Fertilizer'))
oa, ob = organic_malawi(a), organic_malawi(b)
pd.testing.assert_frame_equal(a.loc[~oa], b.loc[~ob], check_exact=True)
pd.testing.assert_frame_equal(
    a.loc[oa].drop(columns='Quantity').reset_index(level='u', drop=True),
    b.loc[ob].drop(columns='Quantity').reset_index(level='u', drop=True), check_exact=True)
assert len(a) == len(b) == 264400
before = [json.loads(s) for s in args.baseline_scan.read_text().splitlines()]
after = [json.loads(s) for s in args.fixed_scan.read_text().splitlines()]
def identity(record):
    return [record[k] for k in ('phase', 'country', 'check', 'status', 'severity')]
assert list(map(identity, before)) == list(map(identity, after))
assert not any(r['status'] in ('fail', 'error') for r in after)
changes = []
for i, (old, new) in enumerate(zip(before, after)):
    if (old['detail'], old['metrics']) != (new['detail'], new['metrics']):
        changes.append({'record': i, 'identity': identity(new),
                        'before_detail': old['detail'], 'after_detail': new['detail'],
                        'before_metrics': old['metrics'], 'after_metrics': new['metrics']})
result = {'pooled_rows': len(b), 'Uganda_rows': int((b.index.get_level_values('country') == 'Uganda').sum()),
          'Malawi_rows': int((b.index.get_level_values('country') == 'Malawi').sum()),
          'all_nonorganic_Malawi_and_all_Uganda_rows_exact': True,
          'organic_other_index_and_measure_values_order_dtypes_exact': True,
          'warnings_before': sum(r['status'] == 'warn' for r in before),
          'warnings_after': sum(r['status'] == 'warn' for r in after),
          'failures_after': 0, 'errors_after': 0,
          'scanner_check_identities_and_statuses_exact': True,
          'scanner_changed_details_or_metrics': changes}
args.out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'scanner_changed_details_or_metrics'}))
