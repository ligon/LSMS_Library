"""GH #637 raw-source oracle for Tanzania plot_inputs (2019-20, 2020-21).

Independent field bindings from the held AG_SEC_3A / AG_SEC_4A (adapted from
the #1118 oracle), keyed on (t, i, plot_id, input, crop, u) with a missing
unit as U_UNKNOWN BEFORE grouping.  Asserts the helper equals the oracle
row-for-row, every recorded (key, unit) quantity is conserved, and each
incommensurable group of the development build is now one row per unit.

Usage (cwd = checkout under test; PYTHONPATH = same):
    python source_oracle.py <private_dir> <aggregate_json>
Aggregates only in <aggregate_json>; nothing row-level is written in-tree.
"""
import contextlib
import importlib.util
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

import lsms_library as ll
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN

priv, out = Path(sys.argv[1]), Path(sys.argv[2])
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
TZ = countries_root() / 'Tanzania'
assert TZ.resolve() == Path(top, 'lsms_library/countries/Tanzania').resolve()
spec = importlib.util.spec_from_file_location('tz_oracle', TZ / '_/tanzania.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@contextlib.contextmanager
def cwd(p):
    old = Path.cwd()
    os.chdir(p)
    try:
        yield
    finally:
        os.chdir(old)


with cwd(TZ / '_'):
    cropmap = mod._crop_labels()
    fertmap = mod._plot_harmonized_codes('harmonize_input')


def val(r, k):
    return pd.NA if pd.isna(r[k]) else float(r[k])


def positive(x):
    return pd.notna(x) and x > 0


def yn(x):
    return {1: True, 2: False}.get(x, pd.NA) if pd.notna(x) else pd.NA


# Independent copies of the instrument's code lists (Stata dictionaries).
seed_units = {1: 'kg', 2: '20 liter bucket', 3: 'small cup', 4: 'large cup', 5: pd.NA}
liquid_units = {1: 'kg', 2: 'litre', 3: 'millilitre', 4: pd.NA}
KEY5 = ['t', 'i', 'plot_id', 'input', 'crop']
KEY6 = KEY5 + ['u']
summary = {}
for wave, n3, n4, hh, plot in [('2019-20', 'AG_SEC_3A', 'AG_SEC_4A', 'sdd_hhid', 'plotnum'),
                               ('2020-21', 'ag_sec_3a', 'ag_sec_4a', 'y5_hhid', 'plot_id')]:
    d3 = get_dataframe(TZ / wave / 'Data' / f'{n3}.dta', convert_categoricals=False)
    d4 = get_dataframe(TZ / wave / 'Data' / f'{n4}.dta', convert_categoricals=False)
    recs = []

    def add(r, name, crop, q, u, p, pq=pd.NA, improved=pd.NA, channel=''):
        recs.append(dict(t=wave, i=format_id(r[hh]), plot_id=format_id(r[plot]), input=name, crop=crop,
                         Quantity=q, u=u, Purchased=p, Quantity_purchased=pq, Improved=improved,
                         channel=channel))

    for _, r in d4.iterrows():
        code = r['cropid']
        crop = cropmap.get(int(code), pd.NA) if pd.notna(code) else pd.NA
        q = val(r, 'ag4a_10_1')
        if pd.isna(q) and pd.isna(crop):
            continue
        if pd.isna(crop) and pd.notna(code):
            crop = f'crop_{int(code)}'
        pq = val(r, 'ag4a_10c_1')
        paid = val(r, 'ag4a_12')
        uc = r['ag4a_10_2']
        st = r['ag4a_08']
        add(r, 'Seed', crop, q, seed_units.get(uc, pd.NA) if pd.notna(uc) else pd.NA,
            positive(pq) or positive(paid), pq,
            {1: True, 2: False, 3: True}.get(st, pd.NA) if pd.notna(st) else pd.NA, 'seed')
    for channel, used, qc, uc, pc, pqc, tc in [
            ('organic', 'ag3a_41', 'ag3a_42', None, 'ag3a_43', 'ag3a_44', None),
            ('inorganic1', 'ag3a_47', 'ag3a_49', None, 'ag3a_51', None, 'ag3a_48'),
            ('inorganic2', 'ag3a_54', 'ag3a_56', None, 'ag3a_58', None, 'ag3a_55'),
            ('herbicide', 'ag3a_60', 'ag3a_62_1', 'ag3a_62_2', 'ag3a_63', None, None),
            ('pesticide', 'ag3a_65a', 'ag3a_65b_1', 'ag3a_65b_2', 'ag3a_65c', None, None)]:
        for _, r in d3.loc[d3[used].eq(1)].iterrows():
            if channel == 'organic':
                name = 'Organic Fertilizer'
            elif tc is not None:
                typ = r[tc]
                name = fertmap.get(int(typ), pd.NA) if pd.notna(typ) else pd.NA
                if pd.isna(name):
                    name = 'Other Fertilizer'
            else:
                name = channel.capitalize()
            code = r[uc] if uc else None
            unit = 'kg' if uc is None else (liquid_units.get(code, pd.NA) if pd.notna(code) else pd.NA)
            paid = val(r, pc)
            p = yn(paid) if channel == 'organic' else positive(paid)
            add(r, name, pd.NA, val(r, qc), unit, p, val(r, pqc) if pqc else pd.NA, channel=channel)
    raw = pd.DataFrame(recs)
    raw['u'] = raw['u'].astype('string')
    blank = raw['u'].isna() | raw['u'].str.strip().eq('').fillna(False)
    raw.loc[blank, 'u'] = U_UNKNOWN

    def sum1(s):
        return s.sum(min_count=1)

    g6 = raw.groupby(KEY6, dropna=False, sort=True)
    oracle = g6.agg(Quantity=('Quantity', sum1), Purchased=('Purchased', 'max'),
                    Quantity_purchased=('Quantity_purchased', sum1), Improved=('Improved', 'max'))
    oracle = oracle.astype({'Quantity': 'Float64', 'Purchased': 'boolean',
                            'Quantity_purchased': 'Float64', 'Improved': 'boolean'})
    with cwd(TZ / wave / '_'):
        actual = mod.plot_inputs_for_wave(wave, d3, d4, dict(hhid=hh, plot=plot, crop='cropid'))
    assert list(actual.index.names) == KEY6, actual.index.names
    a = actual.reset_index().astype({k: 'string' for k in KEY6}).sort_values(KEY6).reset_index(drop=True)
    o = oracle.reset_index().astype({k: 'string' for k in KEY6}).sort_values(KEY6).reset_index(drop=True)
    assert_frame_equal(a, o, check_dtype=False)
    # Conservation: every (key, unit) quantity total equals the source's.
    src_tot = raw.groupby(KEY6, dropna=False)['Quantity'].sum(min_count=1)
    got_tot = actual['Quantity']
    assert (src_tot.reindex(got_tot.index).fillna(-1) == got_tot.fillna(-1)).all()
    # Groups that mix units at the 5-key: each unit is its own served row.
    mixed = Counter()
    split_rows = 0
    for kv, g in raw.groupby(KEY5, dropna=False):
        us = sorted(set(g['u']))
        if len(g) > 1 and len(us) > 1:
            kind = 'two recorded units' if U_UNKNOWN not in us else 'Unknown + recorded unit'
            mixed[kind] += 1
            served = actual.xs(kv, level=KEY5, drop_level=False)
            assert sorted(served.index.get_level_values('u')) == us, (wave, kind)
            for uu in us:
                exp = g.loc[g['u'] == uu, 'Quantity'].sum(min_count=1)
                got = served.xs(uu, level='u')['Quantity'].iloc[0]
                assert (pd.isna(exp) and pd.isna(got)) or exp == got
            split_rows += len(us) - 1
    sz6 = g6.size()
    dup6 = sz6[sz6 > 1]
    dups6 = len(dup6)
    dups6_unknown = int(pd.Series(dup6.index.get_level_values('u')).eq(U_UNKNOWN).sum())
    summary[wave] = {'source_long_rows': len(raw), 'served_rows': len(actual),
                     'groups_mixing_units_at_5key': dict(mixed),
                     'extra_rows_from_unit_split': split_rows,
                     'remaining_dup_groups_6key': dups6,
                     'remaining_dup_groups_6key_recorded_unit': dups6 - dups6_unknown,
                     'remaining_dup_groups_6key_both_Unknown_residual': dups6_unknown,
                     'unknown_u_rows_served': int(pd.Series(actual.index.get_level_values('u')).eq(U_UNKNOWN).sum()),
                     'helper_equals_oracle': True, 'quantity_conserved_per_key_and_unit': True}
    raw.to_pickle(priv / f'oracle-source-long-{wave}.pkl')
out.write_text(json.dumps(summary, indent=1) + '\n')
print(json.dumps(summary, indent=1))
