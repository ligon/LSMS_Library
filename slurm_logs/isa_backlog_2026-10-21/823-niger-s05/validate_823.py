"""GH #823 phase 3: real-data validation of the Niger 2018-19 community_prices
wiring.  Run ONCE PER CHECKOUT, cold, each with its own private data root,
then once more with `compare`:

    LSMS_DATA_DIR=<root A> PYTHONPATH=<development checkout> \
        python validate_823.py build base   <out_dir>
    LSMS_DATA_DIR=<root B> PYTHONPATH=<branch checkout> \
        python validate_823.py build branch <out_dir>
    PYTHONPATH=<branch checkout> python validate_823.py compare <out_dir>

`build` asserts the imported package is the checkout on PYTHONPATH, builds
Country('Niger').community_prices() cold and saves it (row-level, so <out_dir>
must be a PRIVATE directory, never the repo).  On the branch it also runs
Feature('community_prices')(['Niger']), derivation_inputs(<key>) and Niger
2018-19 food_acquired (for the shared-label check).  `compare` writes
validate_823.json next to this script: aggregates only.
"""
import json
import os
import sys
import time
import warnings
from pathlib import Path

import pandas as pd

KEY = 'Niger::community_prices::one-item-per-price'
ECVMA = ['2011-12', '2014-15']


def _assert_identity():
    import lsms_library
    top = os.path.realpath(os.environ['PYTHONPATH'].split(os.pathsep)[0])
    assert os.path.realpath(lsms_library.__file__).startswith(top + os.sep), \
        (lsms_library.__file__, top)
    return lsms_library


def build(label, out):
    ll = _assert_identity()
    out.mkdir(parents=True, exist_ok=True)
    rec = {'label': label, 'lsms_library': ll.__file__,
           'data_dir': os.environ.get('LSMS_DATA_DIR')}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        cp = ll.Country('Niger').community_prices()
    rec['community_prices_seconds'] = round(time.time() - t0, 1)
    # Full messages can embed household/cluster ids: they stay in the PRIVATE
    # build json; only class counts reach the committed compare output.
    rec['warnings'] = sorted({f'{x.category.__name__}: {str(x.message)[:200]}' for x in w})
    rec['warning_classes'] = pd.Series([x.category.__name__ for x in w],
                                       dtype=object).value_counts().to_dict()
    flat = cp.reset_index()
    flat.attrs = {}
    flat.to_parquet(out / f'community_prices_{label}.parquet')
    rec['rows_by_t'] = flat['t'].value_counts().sort_index().to_dict()
    rec['index_names'] = list(cp.index.names)
    rec['columns'] = list(cp.columns)
    rec['dtypes'] = {k: str(v) for k, v in cp.reset_index().dtypes.items()}
    rec['attrs_keys'] = sorted(cp.attrs)
    rec['attrs_derivations'] = json.loads(json.dumps(cp.attrs.get('derivations'), default=str))
    if label == 'branch':
        f = ll.Feature('community_prices')(['Niger'])
        rec['feature'] = {'rows': int(len(f)), 'index_names': list(f.index.names),
                          'rows_by_t': f.reset_index()['t'].value_counts().sort_index().to_dict(),
                          'derivation_rows': int(f['Derivation'].notna().sum())
                          if 'Derivation' in f else None,
                          'attrs_derivations': json.loads(json.dumps(f.attrs.get('derivations'),
                                                                     default=str))}
        c = ll.Country('Niger')
        raw = c.derivation_inputs(KEY, wave='2018-19')
        flat_raw = raw.reset_index()
        flat_raw.attrs = {}
        flat_raw.to_parquet(out / 'derivation_inputs_branch.parquet')
        fa = c.food_acquired(waves=['2018-19'])
        fa_idx = fa.reset_index()[['t', 'j', 'u']].drop_duplicates()
        fa_idx.attrs = {}
        fa_idx.to_parquet(out / 'food_acquired_2018_ju_branch.parquet')
    (out / f'build_{label}.json').write_text(json.dumps(rec, indent=1, default=str,
                                                        ensure_ascii=False))
    print(json.dumps(rec, indent=1, default=str, ensure_ascii=False)[:3000])


def compare(out):
    _assert_identity()
    base = pd.read_parquet(out / 'community_prices_base.parquet')
    br = pd.read_parquet(out / 'community_prices_branch.parquet')
    idx = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    res = {'base_rows_by_t': base['t'].value_counts().sort_index().to_dict(),
           'branch_rows_by_t': br['t'].value_counts().sort_index().to_dict()}
    # 1. ECVMA rows exactly unchanged (every column development serves).
    b = base[base['t'].isin(ECVMA)].sort_values(idx).reset_index(drop=True)
    n = br[br['t'].isin(ECVMA)].sort_values(idx).reset_index(drop=True)
    common = list(base.columns)
    pd.testing.assert_frame_equal(b[common], n[common], check_exact=True)
    res['ecvma_identical_columns'] = common
    res['ecvma_rows_compared'] = int(len(b))
    res['branch_only_columns'] = sorted(set(br.columns) - set(base.columns))
    res['ecvma_derivation_all_na'] = bool(n['Derivation'].isna().all()) \
        if 'Derivation' in n else None
    # 2. 2018-19 on the branch.
    w = br[br['t'] == '2018-19']
    res['w1819'] = {
        'rows': int(len(w)), 'grappes': int(w['v'].nunique()),
        'products': int(w['j'].nunique()), 'units': int(w['u'].nunique()),
        'obs': w['obs'].value_counts().sort_index().to_dict(),
        'visit': w['visit'].value_counts().sort_index().to_dict(),
        'variety': w['variety'].value_counts().to_dict(),
        'quantity_values': w['Quantity'].value_counts(dropna=False).to_dict(),
        'derivation_values': w['Derivation'].value_counts(dropna=False).to_dict(),
        'price_quantiles': w['Price'].quantile([0, .5, 1]).to_dict(),
        'top_units': w['u'].value_counts().head(12).to_dict(),
        'served_units': sorted(w['u'].unique()),
        'sized_units': sorted(u for u in w['u'].unique() if u.endswith(')')
                              and u.rsplit(' (', 1)[-1].rstrip(')') in
                              {'Petit', 'Moyen', 'Grand', 'Quart', 'Demi', 'Entier'}),
    }
    # sample() grappe set is the same keyspace (via the inputs' v, checked below)
    # 3. Shared labels with Niger 2018-19 food_acquired.
    fa = pd.read_parquet(out / 'food_acquired_2018_ju_branch.parquet')
    fa_u, fa_j = set(fa['u']), set(fa['j'])
    sized = set(res['w1819']['sized_units'])
    bare = sorted(set(w['u']) - sized)
    res['bare_units'] = bare
    res['bare_units_in_food_acquired_u'] = sorted(u for u in bare if u in fa_u)
    res['bare_units_not_in_food_acquired_u'] = sorted(u for u in bare if u not in fa_u)
    res['bare_unit_rows_sharing_food_u'] = int(w['u'].isin(fa_u).sum())
    res['sized_unit_rows'] = int(w['u'].isin(sized).sum())
    res['sized_units_in_food_acquired_u'] = sorted(u for u in sized if u in fa_u)
    res['j_in_food_acquired_j'] = int(pd.Series(sorted(set(w['j']))).isin(fa_j).sum())
    res['j_not_in_food_acquired_j'] = sorted(set(w['j']) - fa_j)
    res['rows_whose_j_in_food_acquired'] = int(w['j'].isin(fa_j).sum())
    # 4. derivation_inputs joins the served rows.
    raw = pd.read_parquet(out / 'derivation_inputs_branch.parquet')
    keys = ['t', 'v', 'j', 'u', 'visit']
    s = pd.MultiIndex.from_frame(w[keys].drop_duplicates())
    r = pd.MultiIndex.from_frame(raw[keys].drop_duplicates())
    res['inputs'] = {
        'rows': int(len(raw)), 'columns': list(raw.columns),
        'served_keys': int(len(s)), 'input_keys': int(len(r)),
        'served_keys_found_in_inputs': int(s.isin(r).sum()),
        'input_keys_found_in_served': int(r.isin(s).sum()),
    }
    m = w.merge(raw, on=keys, how='left', indicator=True)
    res['inputs']['served_rows_matched'] = int((m['_merge'] == 'both').sum())
    res['inputs']['served_rows_unmatched'] = int((m['_merge'] == 'left_only').sum())
    # a served obs-1/2 price equals its record's s05q03/s05q04 (obs<=2 rows,
    # keys with a single record), as a value check of the join.
    single = raw.groupby(keys).size()
    single = single[single == 1].index
    ww = w.set_index(keys)
    ww = ww[ww.index.isin(single) & ww['obs'].isin([1, 2])].reset_index()
    mm = ww.merge(raw, on=keys)
    got = mm['Price'].astype(float)
    want = mm['s05q03'].where(mm['obs'] == 1, mm['s05q04']).astype(float)
    res['inputs']['single_record_rows_checked'] = int(len(mm))
    res['inputs']['single_record_price_equals_source'] = int((got == want).sum())
    bfe = json.loads((out / 'build_branch.json').read_text())
    res['feature'] = bfe.get('feature')
    res['branch_attrs_derivations'] = bfe.get('attrs_derivations')
    res['base_seconds'] = json.loads((out / 'build_base.json').read_text())['community_prices_seconds']
    res['branch_seconds'] = bfe['community_prices_seconds']
    res['branch_warning_classes'] = bfe['warning_classes']
    res['base_warning_classes'] = json.loads(
        (out / 'build_base.json').read_text()).get('warning_classes')
    (Path(__file__).parent / 'validate_823.json').write_text(
        json.dumps(res, indent=1, default=str, ensure_ascii=False))
    print(json.dumps(res, indent=1, default=str, ensure_ascii=False)[:6000])


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'build':
        build(sys.argv[2], Path(sys.argv[3]))
    else:
        compare(Path(sys.argv[2]))
