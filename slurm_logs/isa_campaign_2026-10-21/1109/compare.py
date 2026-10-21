"""Compare source-backed amendment with a private cold c08 (or f8) build."""
from pathlib import Path
import json
import os
import argparse
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root
checkout = Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve() == checkout / 'lsms_library/__init__.py'
assert countries_root().resolve() == checkout / 'lsms_library/countries'
ap = argparse.ArgumentParser()
ap.add_argument('--baseline-data', required=True, type=Path)
ap.add_argument('--fixed-data', required=True, type=Path)
ap.add_argument('--out', required=True, type=Path)
args = ap.parse_args()
a = pd.read_pickle(args.baseline_data / 'cold-served.pkl')
b = pd.read_pickle(args.fixed_data / 'cold-served.pkl')
org_a = a.index.get_level_values('input') == 'Organic Fertilizer'
org_b = b.index.get_level_values('input') == 'Organic Fertilizer'
original_columns = [c for c in a.columns if c != 'Applied']
pd.testing.assert_frame_equal(a.loc[~org_a, original_columns], b.loc[~org_b, original_columns], check_exact=True)
# Organic unit is deliberately restored; all other original index values,
# column values, ordering and dtypes must be exactly preserved.
other_columns = [c for c in original_columns if c != 'Quantity']
aa = a.loc[org_a, other_columns].reset_index(level='u', drop=True)
bb = b.loc[org_b, other_columns].reset_index(level='u', drop=True)
pd.testing.assert_frame_equal(aa, bb, check_exact=True)
assert a.Quantity.dtype == b.Quantity.dtype
assert b.loc[org_b, 'Applied'].eq(True).all()
assert b.loc[~org_b, 'Applied'].isna().all()
assert b.Applied.dtype == pd.BooleanDtype()
assert len(a) == len(b) == 180184
na = pd.read_pickle(args.baseline_data / 'cold-nitrogen.pkl')
nb = pd.read_pickle(args.fixed_data / 'cold-nitrogen.pkl')
pd.testing.assert_frame_equal(na, nb, check_exact=True)
assert na.attrs['nitrogen_input_match'] == nb.attrs['nitrogen_input_match']
report = {'rows_before': len(a), 'rows_after': len(b),
          'nonorganic_rows': int((~org_b).sum()),
          'nonorganic_index_order_values_dtypes_exact': True,
          'organic_index_except_unit_and_other_measures_exact': True,
          'organic_rows': int(org_b.sum()), 'organic_applied_true': int(b.loc[org_b, 'Applied'].sum()),
          'before_organic_missing_amounts': int(a.loc[org_a, 'Quantity'].isna().sum()),
          'after_organic_missing_amounts': int(b.loc[org_b, 'Quantity'].isna().sum()),
          'after_organic_positive_amounts': int(b.loc[org_b, 'Quantity'].gt(0).sum()),
          'nitrogen_output_exact': True, 'nitrogen_rows': len(nb),
          'nitrogen_input_match_tally_exact': True,
          'nitrogen_total': float(nb.Nitrogen_kg.sum()),
          'private_builds_offline': True, 'no_shared_cache_promotion': True}
args.out.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
