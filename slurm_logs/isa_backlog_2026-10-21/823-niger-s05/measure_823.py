"""GH #823 phase 2: native measurement of Niger 2018-19 s05_co_ner2018.dta.

READ-ONLY.  Measures the EHCVM 2018-19 community price module (section 5 of
the community questionnaire) against the household food module (s07b) and the
2018-19 cluster set, to decide HOW community_prices could be wired.  It wires
nothing, converts no unit and fabricates no Quantity / variety / visit / obs.

Run from the repo top with the lane env (see the GH #823 ledger, phase 2):

    PYTHONPATH=<checkout> LSMS_DATA_DIR=<private root> \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    LSMS_BUILD_WORKERS=1 python slurm_logs/isa_backlog_2026-10-21/823-niger-s05/measure_823.py

Optional instrument check (the held community questionnaire .xls): set
LSMS_823_XLRD_PATH to a directory holding the pure-python `xlrd` package; the
check is skipped (and recorded as skipped) when it is not importable.  The core
.dta measurements need only the library.

Outputs, next to this script (aggregates and label metadata only):
    measure_823.json            summary of every number quoted in the ledger
    s05_unit_crosswalk.csv      releveID code -> household (unit, size) pair
    s05_item_crosswalk.csv      produitID code -> household label / j label
and, because it holds ROW-LEVEL microdata (grappe, product, prices) that must
not be redistributed in the repo, PRIVATELY under the data root:
    $LSMS_DATA_DIR/823-niger-s05/s05_duplicate_keys.csv
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.io.stata import StataReader

import lsms_library

TOP = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'],
                                   text=True, cwd=Path(__file__).parent).strip())
assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(TOP) + os.sep), lsms_library.__file__

from lsms_library.paths import countries_root  # noqa: E402
from lsms_library.local_tools import get_dataframe, format_id  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
import lsms_library.local_tools as tools  # noqa: E402

OUT = Path(__file__).parent
WAVE = '2018-19'
S05 = f'Niger/{WAVE}/Data/s05_co_ner2018.dta'
S07B = f'Niger/{WAVE}/Data/s07b_me_ner2018.dta'
S00 = f'Niger/{WAVE}/Data/s00_me_ner2018.dta'

summary = {'checkout': str(TOP), 'lsms_library': lsms_library.__file__}


def jsonable(o):
    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [jsonable(v) for v in o]
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if o is pd.NA:
        return None
    return o


def stata_meta(rel):
    """Variable labels, value-label sets and the label set each variable
    uses -- read from the held file's metadata (get_dataframe drops them)."""
    path = get_data_file(rel)
    with StataReader(path) as r:
        varlabels = r.variable_labels()
        value_labels = r.value_labels()
        lbllist = list(r._lbllist)  # label-set name per variable, '' = none
        names = list(r._varlist)
    return path, varlabels, value_labels, dict(zip(names, lbllist))


def read(path, **kw):
    return get_dataframe(str(path), **kw)


def is_int(v):
    """Integer-valued within float32 storage noise (e.g. 479.999977)."""
    v = v.astype(float)
    return (v - v.round()).abs() <= np.maximum(1e-3, 1e-6 * v.abs())


def denominator(v, maxq=40):
    """Smallest q <= maxq with v*q integer-valued: the denominator a ratio
    Price/Quantity would leave.  NA if none."""
    out = pd.Series(pd.NA, index=v.index, dtype='Int64')
    for q in range(1, maxq + 1):
        m = out.isna() & is_int(v * q)
        out[m] = q
    return out


# ---------------------------------------------------------------- A. metadata
s05_path, s05_vl, s05_lbl, s05_lblmap = stata_meta(S05)
summary['s05'] = {
    'path_sidecar_md5': (countries_root() / f'Niger/{WAVE}/Data/s05_co_ner2018.dta.dvc')
    .read_text().split('md5:')[1].split()[0],
    'read_file_md5': __import__('hashlib').md5(Path(s05_path).read_bytes()).hexdigest(),
    'variable_labels': s05_vl,
    'label_set_of_variable': s05_lblmap,
    'value_label_sets': {k: len(v) for k, v in s05_lbl.items()},
}

# ---------------------------------------------------------------- B. s05 data
lab = read(s05_path)                                   # labels
raw = read(s05_path, convert_categoricals=False)      # codes
assert len(lab) == len(raw)
B = summary['s05']
B['rows'], B['cols'] = map(int, raw.shape)
B['columns'] = list(raw.columns)
B['dtypes'] = {k: str(v) for k, v in raw.dtypes.items()}
B['Existe_values'] = raw['Existe'].value_counts(dropna=False).to_dict()
B['vague_rows'] = raw['vague'].value_counts(dropna=False).sort_index().to_dict()
B['grappes'] = int(raw['grappe'].nunique())
B['grappes_with_two_vagues'] = int((raw.groupby('grappe')['vague'].nunique() > 1).sum())
B['grappes_by_vague'] = raw.groupby('vague')['grappe'].nunique().to_dict()
B['product_codes_declared'] = len(s05_lbl['produitID'])
B['product_codes_used'] = int(raw['s05q01'].nunique())
B['unit_codes_declared'] = len(s05_lbl['releveID'])
B['unit_codes_used'] = int(raw['s05q02'].nunique())
B['products_per_grappe_quantiles'] = raw.groupby('grappe')['s05q01'].nunique() \
    .quantile([0, .25, .5, .75, 1]).to_dict()
B['units_per_grappe_product_quantiles'] = raw.groupby(['grappe', 's05q01'])['s05q02'] \
    .nunique().quantile([0, .25, .5, .75, .9, 1]).to_dict()
B['distinct_units_per_product'] = raw.groupby('s05q01')['s05q02'].nunique() \
    .describe().round(2).to_dict()

# identifier structure
keys = {}
for k in (['grappe', 's05q01', 's05q02'], ['grappe', 'vague', 's05q01', 's05q02'],
          ['grappe', 's05q01'], ['grappe', 's05q02']):
    d = raw.duplicated(k, keep=False)
    keys['+'.join(k)] = {'rows_in_dup_keys': int(d.sum()),
                         'dup_keys': int(raw[d].drop_duplicates(k).shape[0])}
B['candidate_keys'] = keys
B['exact_duplicate_rows'] = int(raw.duplicated(keep=False).sum())
B['exact_duplicate_pairs'] = int(raw.duplicated(keep='first').sum())

K = ['grappe', 's05q01', 's05q02']
raw = raw.assign(row=np.arange(len(raw)))
dupk = raw[raw.duplicated(K, keep=False)].copy()
dupk['item_label'] = lab.loc[dupk.index, 's05q01'].astype(str)
dupk['unit_label'] = lab.loc[dupk.index, 's05q02'].astype(str)
grp = dupk.groupby(K)
dk = grp.agg(rows=('row', 'size'), rows_list=('row', lambda s: ' '.join(map(str, s))),
             row_gap=('row', lambda s: int(s.max() - s.min())),
             item=('item_label', 'first'), unit=('unit_label', 'first'),
             q3=('s05q03', lambda s: ' '.join(f'{x:g}' for x in s)),
             q4=('s05q04', lambda s: ' '.join(f'{x:g}' for x in s)),
             n_distinct_rows=('s05q03', 'size'))
dk['identical'] = grp.apply(lambda g: not g.drop(columns='row').duplicated(keep=False).eq(False).any())
dk = dk.drop(columns='n_distinct_rows').reset_index()
from lsms_library.paths import data_root  # noqa: E402
PRIVATE = Path(data_root()) / '823-niger-s05'
PRIVATE.mkdir(parents=True, exist_ok=True)
dk.to_csv(PRIVATE / 's05_duplicate_keys.csv', index=False)  # row-level: private
B['dup_keys'] = {
    'keys': int(len(dk)), 'rows': int(dk['rows'].sum()),
    'rows_per_key': dk['rows'].value_counts().to_dict(),
    'identical_keys': int(dk['identical'].sum()),
    'conflicting_keys': int((~dk['identical']).sum()),
    'grappes': int(dk['grappe'].nunique()),
    'row_gap_quantiles': dk['row_gap'].quantile([0, .25, .5, .75, 1]).to_dict(),
    'top_items': dk.groupby('item')['rows'].sum().sort_values(ascending=False).head(8).to_dict(),
}
# If the two readings were melted to obs 1/2 and a second record enumerated as
# obs 3/4 (the CS07 slot + slots*rank convention), how many of the conflicting
# keys' second-record slots repeat the first record's SAME slot (the #840
# melted-repeat drop) and how many are new prices?
conf = dupk.merge(dk.loc[~dk['identical'], K], on=K).sort_values(K + ['row'])
conf['rank'] = conf.groupby(K).cumcount()
first = conf[conf['rank'] == 0].set_index(K)
second = conf[conf['rank'] == 1].set_index(K)
rep = {}
for slot, col in ((1, 's05q03'), (2, 's05q04')):
    same = (first[col] - second[col]).abs() < 1e-9
    rep[f'slot{slot}_second_repeats_first'] = int(same.sum())
    rep[f'slot{slot}_second_new_price'] = int((~same).sum())
B['dup_keys']['conflicting_second_record_slots'] = rep
# Arithmetic projection (no build): rows a wiring would serve if it kept both
# readings as obs 1/2, dropped byte-identical rows and the slot-wise repeats
# of a second record, and dropped nothing else (0 NA/zero/sentinel prices).
records = len(raw) - B['exact_duplicate_pairs']
B['projection_obs12_policy'] = {
    'records_after_exact_drop': int(records), 'melted_slots': int(2 * records),
    'slot_repeats_dropped': int(rep['slot1_second_repeats_first'] + rep['slot2_second_repeats_first']),
    'served_rows': int(2 * records - rep['slot1_second_repeats_first'] - rep['slot2_second_repeats_first']),
    'second_record_rows_served_as_obs_3_4': int(rep['slot1_second_new_price'] + rep['slot2_second_new_price']),
}

# price fields
q3, q4 = raw['s05q03'].astype(float), raw['s05q04'].astype(float)
P = {}
for name, v in (('s05q03', q3), ('s05q04', q4)):
    d = denominator(v)
    P[name] = {
        'na': int(v.isna().sum()), 'zero': int((v == 0).sum()), 'negative': int((v < 0).sum()),
        'sentinels_999_9999_99999_9999.9': int(v.isin([999, 9999, 99999, 999999, 9999.9]).sum()),
        'quantiles': v.quantile([0, .01, .1, .25, .5, .75, .9, .99, 1]).round(4).to_dict(),
        'integer_valued_share': round(float(is_int(v).mean()), 4),
        'non_integer_rows': int((~is_int(v)).sum()),
        'denominator_counts_noninteger': d[~is_int(v)].value_counts(dropna=False).head(12).to_dict(),
    }
r = q4 / q3
cat = pd.Series(np.select([(r - 1).abs() < 1e-9, (r - 0.85).abs() < 1e-6,
                           (r - 1 / 0.85).abs() < 1e-6], ['equal', 'q4=0.85*q3', 'q3=0.85*q4'],
                          'other'), index=raw.index)
P['ratio_q4_over_q3_quantiles'] = r.quantile([0, .01, .05, .25, .5, .75, .95, .99, 1]).round(4).to_dict()
P['ratio_category_rows'] = cat.value_counts().to_dict()
P['top_ratios'] = r.round(4).value_counts().head(12).to_dict()
P['extreme_ratio_rows'] = {'>2': int((r > 2).sum()), '<0.5': int((r < 0.5).sum()),
                           '>5': int((r > 5).sum()), '<0.2': int((r < 0.2).sum())}
# the 0.85 rows: which side is the round number
for c_ in ('q4=0.85*q3', 'q3=0.85*q4'):
    m = cat == c_
    P[f'{c_}_integer_share'] = {'s05q03': round(float(is_int(q3[m]).mean()), 3),
                               's05q04': round(float(is_int(q4[m]).mean()), 3)}
# file-order structure: contiguous runs of each ratio category
runs = (cat != cat.shift()).cumsum()
rl = pd.DataFrame({'cat': cat, 'run': runs, 'row': raw['row']}).groupby('run') \
    .agg(cat=('cat', 'first'), start=('row', 'min'), end=('row', 'max'), n=('row', 'size'))
P['file_order_runs_total'] = int(len(rl))
P['file_order_runs_over_200_rows'] = rl[rl['n'] > 200].to_dict('records')
P['grappe_monotonic_in_file'] = bool(raw['grappe'].is_monotonic_increasing)
# Is the block structure a SORT on the two readings' ratio?  Rank correlation
# of q4/q3 with file position inside the rows before / after the equal block.
eq_block = rl[(rl['cat'] == 'equal')].sort_values('n').iloc[-1]
pos = raw['row']
before, after = pos < eq_block['start'], pos > eq_block['end']
P['file_order_sort_on_ratio'] = {
    'equal_block': [int(eq_block['start']), int(eq_block['end'])],
    'before_rows': int(before.sum()), 'before_share_q4_lt_q3': round(float((r[before] < 1).mean()), 4),
    'after_rows': int(after.sum()), 'after_share_q4_gt_q3': round(float((r[after] > 1).mean()), 4),
    'spearman_ratio_vs_position_before': round(float(r[before].rank().corr(pos[before].rank())), 4),
    'spearman_ratio_vs_position_after': round(float(r[after].rank().corr(pos[after].rank())), 4),
}
# The exact 0.85 ratio: concentrated in a few grappes / products, or spread?
for c_ in ('q4=0.85*q3', 'q3=0.85*q4'):
    m = cat == c_
    g = raw[m].groupby('grappe').size()
    P[f'{c_}_spread'] = {'rows': int(m.sum()), 'grappes': int(g.size),
                         'top10_grappe_share': round(float(g.nlargest(10).sum() / m.sum()), 4),
                         'products': int(raw.loc[m, 's05q01'].nunique()),
                         'by_vague': raw.loc[m, 'vague'].value_counts().sort_index().to_dict()}
P['ratio_0.8_0.85_0.9_rows'] = {str(x): int(((r - x).abs() < 1e-6).sum()) for x in (0.8, 0.85, 0.9)}
# per-grappe mix of categories (is "equal" a per-grappe property?)
mix = pd.crosstab(raw['grappe'], cat, normalize='index')
P['per_grappe_equal_share_quantiles'] = mix.get('equal', pd.Series(0, index=mix.index)) \
    .quantile([0, .1, .25, .5, .75, .9, 1]).round(3).to_dict()
P['grappes_all_equal'] = int((mix.get('equal', 0) == 1).sum())
P['grappes_no_equal'] = int((mix.get('equal', 0) == 0).sum())
P['equal_share_by_vague'] = (cat == 'equal').groupby(raw['vague']).mean().round(4).to_dict()
# does the size suffix predict a non-integer price's denominator?
size = raw['s05q02'] % 10
d3 = denominator(q3)
tab = pd.crosstab(size, np.where(is_int(q3), 'int', d3.astype('string').fillna('NA')))
P['size_digit_by_q3_denominator'] = tab.to_dict('index')
P['noninteger_share_by_size_digit'] = (~is_int(q3)).groupby(size).mean().round(4).to_dict()
B['price'] = P

# ---------------------------------------------------------------- C. coverage
s00_path = get_data_file(S00)
s00 = read(s00_path, convert_categoricals=False)
s05_v = set(raw['grappe'].map(format_id))
s00_v = set(s00['grappe'].map(format_id))
C = {'s00_me_grappes': len(s00_v), 's05_grappes': len(s05_v),
     's05_not_in_s00': sorted(s05_v - s00_v), 's00_not_in_s05': sorted(s00_v - s05_v)}
try:
    samp = lsms_library.Country('Niger').sample(waves=[WAVE])
    sv = samp.index.get_level_values('v') if 'v' in samp.index.names else samp['v']
    sv = set(pd.Series(sv).dropna().astype(str))
    C.update({'sample_v': len(sv), 's05_not_in_sample': sorted(s05_v - sv),
              'sample_not_in_s05': sorted(sv - s05_v)})
except Exception as e:  # recorded, not fatal: the raw cover is the fallback
    C['sample_error'] = f'{type(e).__name__}: {e}'
# s05 vague vs the cover's vague for the same grappe
cover_vague = s00.groupby(s00['grappe'].map(format_id))['vague'].agg(lambda s: sorted(set(s)))
s05_vague = raw.groupby(raw['grappe'].map(format_id))['vague'].agg(lambda s: sorted(set(s)))
both = s05_vague.index.intersection(cover_vague.index)
C['s05_vague_equals_cover_vague'] = int((s05_vague[both] == cover_vague[both]).sum())
C['grappes_compared'] = int(len(both))
summary['coverage'] = C

# ---------------------------------------------------------------- D. items
s07_path, s07_vl, s07_lbl, s07_lblmap = stata_meta(S07B)
summary['s07b'] = {'variable_labels': s07_vl, 'label_set_of_variable': s07_lblmap,
                   'value_label_sets': {k: len(v) for k, v in s07_lbl.items()}}
hh_lab = read(s07_path)
hh_raw = read(s07_path, convert_categoricals=False)
consumed = hh_raw['s07bq03a'].notna() & (hh_raw['s07bq03a'] > 0)

sys.path.insert(0, str(countries_root() / 'Niger' / '_'))
sys.path.insert(0, str(countries_root() / 'Niger' / WAVE / '_'))
import niger  # noqa: E402
import mapping as m1819  # noqa: E402  (2018-19/_/mapping.py)
# The production label maps, resolved exactly as a wave script resolves them:
# cwd = the wave's _/ dir, default dirs ('./', '../../_/').  Read-only.
_cwd = os.getcwd()
os.chdir(countries_root() / 'Niger' / WAVE / '_')
try:
    item_map, unit_map = niger._community_prices_maps()
finally:
    os.chdir(_cwd)

# household served j: harmonize_food on the raw label, then the wave's
# mojibake decode (data_info.yml mappings run before mapping.py).
hh_item_label = hh_lab['s07bq01'].astype(str).str.strip()
hh_j = hh_item_label.map(lambda x: item_map.get(x, x)).map(m1819._decode_mojibake)
hh_j_set = set(hh_j[consumed])
hh_code_label = {int(k): v for k, v in s07_lbl[s07_lblmap['s07bq01']].items()}

s05_item_label = lab['s05q01'].astype(str).str.strip()
s05_code_label = {int(k): v for k, v in s05_lbl['produitID'].items()}
rows_by_code = raw['s05q01'].value_counts()
items = []
for code, n in rows_by_code.sort_index().items():
    code = int(code)
    lbl_read = s05_item_label[raw['s05q01'] == code].iloc[0]
    hh_read = hh_item_label[hh_raw['s07bq01'] == code]
    hh_read = hh_read.iloc[0] if len(hh_read) else None
    mapped = item_map.get(lbl_read)
    decoded = m1819._decode_mojibake(lbl_read)
    mapped_dec = item_map.get(decoded)
    j = mapped if mapped is not None else (mapped_dec if mapped_dec is not None else decoded)
    # Code bridge: the SAME produitID code's household label, through the
    # household pipeline (harmonize_food on the raw label, then decode).
    j_code = (m1819._decode_mojibake(item_map.get(hh_read, hh_read))
              if hh_read is not None else None)
    items.append({
        'code': code, 'rows': int(n), 's05_label_meta': s05_code_label.get(code),
        's05_label_read': lbl_read, 'hh_label_meta_same_code': hh_code_label.get(code),
        'hh_label_read_same_code': hh_read,
        'meta_label_equal': s05_code_label.get(code) == hh_code_label.get(code),
        'read_label_equal': lbl_read == hh_read,
        'harmonize_food_exact': mapped is not None,
        'harmonize_food_after_decode': mapped is None and mapped_dec is not None,
        'j': j, 'j_in_household_served_j': j in hh_j_set,
        'j_by_code': j_code, 'j_by_code_in_household_served_j': j_code in hh_j_set,
        'label_j_equals_code_j': j == j_code,
        'household_rows_consumed_same_code': int((consumed & (hh_raw['s07bq01'] == code)).sum()),
    })
items = pd.DataFrame(items)
items.to_csv(OUT / 's05_item_crosswalk.csv', index=False)
summary['items'] = {
    'codes_used': int(len(items)),
    'household_produitID_codes_declared': len(hh_code_label),
    's05_codes_absent_from_household_set': items.loc[items['hh_label_meta_same_code'].isna(), 'code'].tolist(),
    'meta_label_equal_codes': int(items['meta_label_equal'].sum()),
    'read_label_equal_codes': int(items['read_label_equal'].sum()),
    'read_label_equal_rows': int(items.loc[items['read_label_equal'], 'rows'].sum()),
    'harmonize_food_exact_codes': int(items['harmonize_food_exact'].sum()),
    'harmonize_food_exact_rows': int(items.loc[items['harmonize_food_exact'], 'rows'].sum()),
    'harmonize_food_after_decode_codes': int(items['harmonize_food_after_decode'].sum()),
    'j_in_household_served_codes': int(items['j_in_household_served_j'].sum()),
    'j_in_household_served_rows': int(items.loc[items['j_in_household_served_j'], 'rows'].sum()),
    'unmatched': items.loc[~items['j_in_household_served_j'],
                           ['code', 's05_label_read', 'hh_label_read_same_code', 'j', 'rows']]
    .to_dict('records'),
    'j_by_code_in_household_served_codes': int(items['j_by_code_in_household_served_j'].sum()),
    'j_by_code_in_household_served_rows': int(items.loc[items['j_by_code_in_household_served_j'], 'rows'].sum()),
    'label_j_equals_code_j_codes': int(items['label_j_equals_code_j'].sum()),
    'label_j_differs_from_code_j': items.loc[~items['label_j_equals_code_j'],
                                             ['code', 's05_label_read', 'j', 'j_by_code', 'rows']]
    .to_dict('records'),
    'declared_codes_with_zero_rows': sorted(set(s05_code_label) - set(items['code'])),
}

# ---------------------------------------------------------------- E. units
hh_unit_lbl = {int(k): v for k, v in s07_lbl[s07_lblmap['s07bq03b']].items()}
hh_size_lbl = {int(k): v for k, v in s07_lbl[s07_lblmap['s07bq03c']].items()}
hh_pairs = hh_raw.loc[consumed, ['s07bq03b', 's07bq03c']].dropna()
hh_pair_rows = hh_pairs.astype(int).value_counts()
hh_u_served = set(hh_lab.loc[consumed, 's07bq03b'].astype(str).str.strip()
                  .map(m1819._decode_mojibake).map(lambda x: unit_map.get(x, x)))
s05_unit_lbl = {int(k): v for k, v in s05_lbl['releveID'].items()}
size_word = {0: '', 1: 'petit', 2: 'moyen', 3: 'grand', 4: 'quart', 5: 'demi', 6: 'entier'}


def fold(s):
    """Decode, strip accents/case/spacing: separates a SPELLING difference
    from a different unit."""
    import unicodedata
    s = m1819._decode_mojibake(str(s).strip())
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    return ' '.join(s.lower().split())
units = []
for code, n in raw['s05q02'].value_counts().sort_index().items():
    code = int(code)
    ucode, scode = divmod(code, 10)
    lbl = s05_unit_lbl.get(code)
    lbl_read = lab.loc[raw['s05q02'] == code, 's05q02'].astype(str).iloc[0].strip()
    head, _, tail = lbl.partition(',')
    hh_u = hh_unit_lbl.get(ucode)
    tail_norm = tail.strip().lower()
    size_lbl_ok = (scode == 0 and tail_norm in ('', 'n/a')) or tail_norm == size_word.get(scode)
    u_mapped = unit_map.get(lbl_read)
    u_after = u_mapped if u_mapped is not None else m1819._decode_mojibake(lbl_read)
    sized = scode != 0
    # Code bridge: the household unit code's label through the household
    # pipeline (decode, then the u table) -- the food_acquired.u it would join.
    hh_u_dec = m1819._decode_mojibake(hh_u.strip()) if hh_u is not None else None
    u_code = unit_map.get(hh_u_dec, hh_u_dec) if hh_u_dec is not None else None
    units.append({
        'code': code, 'label': lbl, 'rows': int(n), 'unit_code': ucode, 'size_code': scode,
        'hh_unit_label': hh_u, 'hh_size_label': hh_size_lbl.get(scode),
        'unit_label_matches_hh': (hh_u is not None and
                                  m1819._decode_mojibake(head.strip()).lower()
                                  == m1819._decode_mojibake(hh_u.strip()).lower()),
        'unit_label_matches_hh_unaccented': (hh_u is not None and
                                             fold(head) == fold(hh_u)),
        'hh_unit_via_code': u_code,
        'hh_unit_via_code_in_household_served_u': u_code in hh_u_served,
        'size_suffix_matches_code': size_lbl_ok,
        'hh_pair_rows': int(hh_pair_rows.get((ucode, scode), 0)),
        'class': ('no-household-unit' if hh_u is None else
                  'size-qualified' if sized else 'bare'),
        'u_table_hit': u_mapped is not None, 'u_if_wired': u_after,
        'u_if_wired_in_household_served_u': u_after in hh_u_served,
    })
units = pd.DataFrame(units)
units.to_csv(OUT / 's05_unit_crosswalk.csv', index=False)
U = {'codes_used': int(len(units))}
for cl, g in units.groupby('class'):
    U[f'{cl}_codes'] = int(len(g))
    U[f'{cl}_rows'] = int(g['rows'].sum())
U['unit_part_label_matches_hh_codes'] = int(units['unit_label_matches_hh'].sum())
U['unit_part_label_mismatch'] = units.loc[~units['unit_label_matches_hh'] & units['hh_unit_label'].notna(),
                                          ['code', 'label', 'hh_unit_label']].to_dict('records')
U['unit_part_matches_hh_unaccented_codes'] = int(units['unit_label_matches_hh_unaccented'].sum())
U['unit_part_mismatch_unaccented'] = units.loc[~units['unit_label_matches_hh_unaccented'],
                                               ['code', 'label', 'hh_unit_label', 'rows']].to_dict('records')
U['bare_via_code_in_household_served_u_codes'] = int((bare_ := units['class'] == 'bare')[
    units['hh_unit_via_code_in_household_served_u']].sum())
U['bare_via_code_in_household_served_u_rows'] = int(units.loc[bare_ & units['hh_unit_via_code_in_household_served_u'], 'rows'].sum())
U['bare_via_code_not_in_household_served_u'] = units.loc[bare_ & ~units['hh_unit_via_code_in_household_served_u'],
                                                         ['code', 'label', 'hh_unit_via_code', 'rows']].to_dict('records')
U['size_suffix_matches_code_codes'] = int(units['size_suffix_matches_code'].sum())
U['size_suffix_mismatch'] = units.loc[~units['size_suffix_matches_code'],
                                      ['code', 'label']].to_dict('records')
U['pair_observed_in_household_codes'] = int((units['hh_pair_rows'] > 0).sum())
U['pair_observed_in_household_rows'] = int(units.loc[units['hh_pair_rows'] > 0, 'rows'].sum())
U['u_table_hit_codes'] = int(units['u_table_hit'].sum())
bare = units['class'] == 'bare'
U['bare_and_in_household_served_u_codes'] = int((bare & units['u_if_wired_in_household_served_u']).sum())
U['bare_and_in_household_served_u_rows'] = int(units.loc[bare & units['u_if_wired_in_household_served_u'], 'rows'].sum())
U['bare_not_in_household_served_u'] = units.loc[bare & ~units['u_if_wired_in_household_served_u'],
                                                ['code', 'label', 'u_if_wired', 'rows']].to_dict('records')
U['sized_label_in_household_served_u_codes'] = int((~bare & units['u_if_wired_in_household_served_u']).sum())
U['household_size_code_rows_consumed'] = hh_raw.loc[consumed, 's07bq03c'].value_counts(dropna=False) \
    .sort_index().to_dict()
summary['units'] = U

# ---------------------------------------------------------------- F. 2021-22
d21 = countries_root() / 'Niger' / '2021-22' / 'Data'
summary['wave_2021_22'] = {
    'community_sidecars': sorted(p.name for p in d21.glob('*_co_*.dvc')),
    's05_co_sidecars': sorted(p.name for p in d21.glob('s05_co*')),
    'price_sidecars': sorted(p.name for p in d21.glob('*prix*.dvc')) + sorted(p.name for p in d21.glob('*nsu*.dvc')),
}
summary['wave_2018_19_community_sidecars'] = sorted(
    p.name for p in (countries_root() / 'Niger' / WAVE / 'Data').glob('*_co_*.dvc'))

# ---------------------------------------------------------------- G. instrument
G = {}
xp = os.environ.get('LSMS_823_XLRD_PATH')
if xp:
    sys.path.insert(0, xp)
try:
    import xlrd
    doc = countries_root() / 'Niger' / WAVE / 'Documentation'
    for v in (1, 2):
        name = f'ner_ehcvm1_qnr_community_excel_vague{v}.xls'
        md5 = (doc / f'{name}.dvc').read_text().split('md5:')[1].split()[0]
        wb = xlrd.open_workbook(str(get_data_file(f'Niger/{WAVE}/Documentation/{name}')))
        cover = wb.sheet_by_name('Couvert')
        cover_text = [str(cover.cell_value(r_, c_)).strip() for r_ in range(cover.nrows)
                      for c_ in range(cover.ncols) if str(cover.cell_value(r_, c_)).strip()]
        sec5 = wb.sheet_by_name('SEC5_PRIX')
        hdr = [[str(sec5.cell_value(r_, c_)).strip() for c_ in range(sec5.ncols)] for r_ in range(2, 5)]
        first_products = sorted({str(sec5.cell_value(r_, 0)).strip() for r_ in range(6, 40)} - {''})
        G[name] = {'sidecar_md5': md5, 'sheets': wb.sheet_names(), 'cover_text': cover_text[:6],
                   'sec5_header_rows': hdr, 'sec5_first_products': first_products}
except ImportError:
    G['skipped'] = 'xlrd not importable; set LSMS_823_XLRD_PATH'
summary['instrument'] = G

(OUT / 'measure_823.json').write_text(json.dumps(jsonable(summary), indent=1, ensure_ascii=False,
                                                 default=str))
print(json.dumps(jsonable({k: summary[k] for k in ('coverage', 'items', 'units', 'wave_2021_22')}),
                 indent=1, ensure_ascii=False, default=str)[:6000])
print('wrote', OUT / 'measure_823.json')
