"""GH #856 step 1 -- Malawi harvest-side "other (specify)" residue, measured
from the RAW Module G (seasonal) / Module P (perennial) files.

READ-ONLY.  Reads exactly the 12 harvest files the four crop_production wave
scripts pass to ``_harvest_block`` (Malawi/{2010-11,2013-14,2016-17,2019-20}/
_/crop_production.py), with the same crop / quantity / unit columns, and
measures, per (wave, half, module):

* rows in the file, and rows ``_harvest_block`` keeps (its only row filter is
  a non-null crop code: ``crop.notna() | _crop_code.notna()``);
* rows whose harvest UNIT code is 13 = OTHER (SPECIFY) (confirmed against each
  file's own value labels at run time);
* rows whose CROP code is the generic OTHER (SPECIFY): seasonal 48; perennial
  18 (IHS3/IHPS) or 21 (IHS4/IHS5, recoded to 1018 by _PERENNIAL_RECODE) --
  also confirmed against the file's own labels at run time;
* the other-specify TEXT columns (names found from the files' own variable
  labels; there are none in the held 2013-14 extract), with distinct
  non-empty strings, top 30, and how many strings cover 80% / 90% of rows;
* a code x text cross-tab, so a code-13 row with no text and a non-13 row
  with text are both visible;
* INDICATIVE first-match buckets over the code-13 unit text (does it restate a
  unit the code list already has?) -- a measurement, not a mapping;
* feasibility only: for IHS4/IHS5 Module G, which has NO crop-name specify
  column, how many code-48 rows Module D's plot-level "Other Crop" text
  (``ag_d20_oth`` / ``ag_d20b_oth``) could name.

No kg conversion; no Quantity is summed across units except one labelled
within-unit figure (share of KILOGRAM-reported quantity on generic-other crop
rows).

Run from the worktree root with the env the lane uses, e.g.::

    PYTHONPATH=$PWD LSMS_DATA_DIR=... OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
    .venv/bin/python slurm_logs/isa_backlog_2026-10-21/856-harvest-residue/measure_856.py

Writes ``measure_856.json`` (the committed record) and
``measure_856_summary.csv`` (one row per cell; ``*.csv`` is gitignored
repo-wide, so it is a local convenience only) next to itself.
"""
import csv
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

import lsms_library

TOP = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'],
                              text=True, cwd=Path(__file__).parent).strip()
assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(TOP) + os.sep), lsms_library.__file__

from pandas.io.stata import StataReader  # metadata only

from lsms_library.data_access import get_data_file
from lsms_library.local_tools import get_dataframe

OUT_DIR = Path(__file__).resolve().parent
TOPN = 30

# (wave, half, module, countries-relative file, crop col, qty col, unit col,
#  crop O/S text col or None, unit O/S text col or None,
#  extra text cols worth checking on generic-other crop rows)
CELLS = [
    ('2010-11', 'Full_Sample', 'G',
     'Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta',
     'ag_g0d', 'ag_g13a', 'ag_g13b', 'ag_g0d_os', 'ag_g13b_os', []),
    ('2010-11', 'Full_Sample', 'P',
     'Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_p.dta',
     'ag_p0d', 'ag_p09a', 'ag_p09b', 'ag_p0d_os', 'ag_p09b_os', []),
    ('2013-14', 'Panel', 'G', 'Malawi/2013-14/Data/AG_MOD_G_13.dta',
     'ag_g0b', 'ag_g13a', 'ag_g13b', None, None, []),
    ('2013-14', 'Panel', 'P', 'Malawi/2013-14/Data/AG_MOD_P_13.dta',
     'ag_p0c', 'ag_p09a', 'ag_p09b', None, None, []),
    ('2016-17', 'Cross_Sectional', 'G',
     'Malawi/2016-17/Data/Cross_Sectional/ag_mod_g.dta',
     'crop_code', 'ag_g13a', 'ag_g13b', None, 'ag_g13b_oth',
     ['ag_g0e_1_oth', 'primary_variety']),
    ('2016-17', 'Cross_Sectional', 'P',
     'Malawi/2016-17/Data/Cross_Sectional/ag_mod_p.dta',
     'ag_p0c', 'ag_p09a', 'ag_p09b', 'ag_p0c_oth', 'ag_p09b_oth',
     ['ag_p0_crops']),
    ('2016-17', 'Panel', 'G', 'Malawi/2016-17/Data/Panel/ag_mod_g_16.dta',
     'crop_code', 'ag_g13a', 'ag_g13b', None, 'ag_g13b_oth',
     ['ag_g0e_oth', 'primary_variety']),
    ('2016-17', 'Panel', 'P', 'Malawi/2016-17/Data/Panel/ag_mod_p_16.dta',
     'crop_code', 'ag_p09a', 'ag_p09b', 'ag_p0d_oth', 'ag_p09b_oth',
     ['ag_p0_crops']),
    ('2019-20', 'Cross_Sectional', 'G',
     'Malawi/2019-20/Data/Cross_Sectional/ag_mod_g.dta',
     'crop_code', 'ag_g13a', 'ag_g13b', None, 'ag_g13b_oth',
     ['ag_g0e_1_oth']),
    ('2019-20', 'Cross_Sectional', 'P',
     'Malawi/2019-20/Data/Cross_Sectional/ag_mod_p.dta',
     'ag_p0c', 'ag_p09a', 'ag_p09b', 'ag_p0c_oth', 'ag_p09b_oth',
     ['ag_p0_crops']),
    ('2019-20', 'Panel', 'G', 'Malawi/2019-20/Data/Panel/ag_mod_g_19.dta',
     'crop_code', 'ag_g13a', 'ag_g13b', None, 'ag_g13b_oth',
     ['ag_g0e_1_oth']),
    ('2019-20', 'Panel', 'P', 'Malawi/2019-20/Data/Panel/ag_mod_p_19.dta',
     'crop_code', 'ag_p09a', 'ag_p09b', 'ag_p0d_oth', 'ag_p09b_oth',
     ['ag_p0_crops']),
]

UNIT_OTHER = 13
SEASONAL_OTHER = {48}
SEASONAL_VARIETY_OTHER = {10, 16, 26}   # OTHER TOBACCO / GROUNDNUT / RICE (SPECIFY)
PERENNIAL_OTHER_CANDIDATES = {18, 21}


def _norm_text(s):
    """strip, collapse whitespace, upper-case; '' -> NA."""
    t = s.astype('string').str.strip().str.replace(r'\s+', ' ', regex=True)
    t = t.str.upper()
    return t.where(t.notna() & (t != ''), pd.NA)


def _string_profile(text):
    """Distinct / top-N / coverage for a Series of normalised strings (NA
    already dropped)."""
    vc = text.value_counts()
    n = int(vc.sum())
    cum = vc.cumsum() / n if n else vc
    def k_for(q):
        if not n:
            return 0
        return int((cum < q).sum() + 1)
    return {
        'rows_with_text': n,
        'n_distinct': int(len(vc)),
        'n_singletons': int((vc == 1).sum()),
        'strings_to_cover_80pct': k_for(0.80),
        'strings_to_cover_90pct': k_for(0.90),
        'top': [[str(k), int(v)] for k, v in vc.head(TOPN).items()],
    }


#: INDICATIVE buckets for code-13 unit text -- a measurement of how much of
#: the residue restates a unit the instrument already has a code for, NOT a
#: mapping.  Only self-describing strings are bucketed; nothing is guessed.
UNIT_TEXT_BUCKETS = [
    ('restates_90kg_bag (code 3)', r'(?:^|\D)90\s*-?\s*KG'),
    ('restates_dengu_basket (code 11)', r'DENGU|BASKET'),
    ('other_explicit_kg_size', r'\d+\s*-?\s*(?:KGS?|KILO)'),
    ('container_word (basin/pail/bucket/tin/litre)',
     r'BASIN|PAIL|BUCKET|\bTINA?\b|LITRE|LITER|\d\s*L\b'),
]


def _bucket_counts(text):
    """First-match bucket counts over a Series of normalised strings."""
    left = text.copy()
    out = {}
    for name, rx in UNIT_TEXT_BUCKETS:
        hit = left.str.contains(rx, regex=True, na=False)
        out[name] = int(hit.sum())
        left = left[~hit]
    out['unclassified'] = int(len(left))
    return out


#: Literal-spelling counter only: strings that SAY cowpea.  Local-language
#: names are not translated here (that is a recovery-list decision).
COWPEA_RX = r'COW\s*-?\s*PEA'


def _codes(series):
    return pd.to_numeric(series, errors='coerce')


def _quad(flag, has_text):
    return {
        'code_and_text': int((flag & has_text).sum()),
        'code_no_text': int((flag & ~has_text).sum()),
        'text_other_code': int((~flag & has_text).sum()),
    }


def measure_cell(wave, half, module, rel, crop_col, qty_col, unit_col,
                 crop_os, unit_os, extra):
    path = get_data_file(rel)
    assert path is not None, rel
    with StataReader(path) as r:
        varlabels = r.variable_labels()
        vallabels = r.value_labels()
        lblmap = dict(zip(r._varlist, r._lbllist))

    def labels_of(col):
        return {int(k): str(v) for k, v in vallabels.get(lblmap.get(col), {}).items()}

    unit_labels = labels_of(unit_col)
    crop_labels = labels_of(crop_col)
    # Confirm code semantics against the file's OWN labels.
    assert 'OTHER' in unit_labels.get(UNIT_OTHER, '').upper(), (rel, unit_labels)
    if module == 'G':
        other_codes = {c for c in SEASONAL_OTHER
                       if 'OTHER' in crop_labels.get(c, '').upper()}
        assert other_codes == SEASONAL_OTHER, (rel, crop_labels.get(48))
        variety_other = {c for c in SEASONAL_VARIETY_OTHER
                         if 'OTHER' in crop_labels.get(c, '').upper()}
    else:
        other_codes = {c for c in PERENNIAL_OTHER_CANDIDATES
                       if 'OTHER' in crop_labels.get(c, '').upper()}
        assert len(other_codes) == 1, (rel, other_codes)
        variety_other = set()

    df = get_dataframe(str(path), convert_categoricals=False)
    n_raw = len(df)
    crop = _codes(df[crop_col])
    keep = crop.notna()                      # _harvest_block's only row filter
    d = df[keep]
    crop = crop[keep]
    unit = _codes(d[unit_col])
    qty = _codes(d[qty_col])
    n = len(d)
    qty_notna = qty.notna()
    qty_pos = qty > 0

    unit13 = unit == UNIT_OTHER
    crop_other = crop.isin(other_codes)
    crop_var_other = crop.isin(variety_other)
    either = unit13 | crop_other

    def block(flag):
        return {
            'rows': int(flag.sum()),
            'share_of_builder_rows': round(float(flag.sum()) / n, 6) if n else None,
            'rows_qty_notna': int((flag & qty_notna).sum()),
            'share_of_qty_notna_rows': (round(float((flag & qty_notna).sum())
                                              / qty_notna.sum(), 6)
                                        if qty_notna.sum() else None),
            'rows_qty_pos': int((flag & qty_pos).sum()),
            'share_of_qty_pos_rows': (round(float((flag & qty_pos).sum())
                                            / qty_pos.sum(), 6)
                                      if qty_pos.sum() else None),
        }

    out = {
        'wave': wave, 'half': half, 'module': module, 'file': rel,
        'columns': {'crop': crop_col, 'quantity': qty_col, 'unit': unit_col,
                    'crop_specify': crop_os, 'unit_specify': unit_os},
        'column_labels': {c: varlabels.get(c) for c in
                          [crop_col, qty_col, unit_col, crop_os, unit_os] + extra
                          if c is not None},
        'confirmed_codes': {
            'unit_other': {UNIT_OTHER: unit_labels.get(UNIT_OTHER)},
            'crop_other': {c: crop_labels.get(c) for c in sorted(other_codes)},
            'crop_variety_other': {c: crop_labels.get(c) for c in sorted(variety_other)},
        },
        'n_raw_rows': n_raw,
        'n_builder_rows': n,
        'n_qty_notna': int(qty_notna.sum()),
        'n_qty_pos': int(qty_pos.sum()),
        'quantity_max': (float(qty.max()) if qty_notna.any() else None),
        'unit13': block(unit13),
        'crop_other': block(crop_other),
        'crop_variety_other': block(crop_var_other),
        'unit13_or_crop_other': block(either),
        'unit13_and_crop_other': int((unit13 & crop_other).sum()),
    }

    # Unit codes in data vs in the file's own label set.
    present = sorted(int(x) for x in unit.dropna().unique())
    out['unit_codes'] = {
        'value_counts': {int(k): int(v) for k, v in unit.value_counts().sort_index().items()},
        'n_unit_missing': int(unit.isna().sum()),
        'in_data_not_labelled': [c for c in present if c not in unit_labels],
        'labelled_not_in_data': sorted(c for c in unit_labels if c not in present),
    }

    # Within-unit (KILOGRAM, code 1) quantity weight of generic-other crop rows.
    kg = (unit == 1) & qty_notna
    kg_tot = float(qty[kg].sum())
    out['crop_other_share_of_kilogram_quantity'] = (
        round(float(qty[kg & crop_other].sum()) / kg_tot, 6) if kg_tot else None)

    # --- unit specify text ---
    if unit_os is not None:
        ut = _norm_text(d[unit_os])
        has = ut.notna()
        out['unit_specify'] = {
            'quadrant': _quad(unit13, has),
            'profile_on_code13': _string_profile(ut[unit13 & has]),
            'indicative_buckets_on_code13': _bucket_counts(ut[unit13 & has]),
            'text_on_other_codes_by_unit_code': {
                int(k): int(v) for k, v in unit[~unit13 & has].value_counts().items()},
            'qty_on_code13_with_text': {
                'rows_qty_pos': int((unit13 & has & qty_pos).sum())},
        }
    else:
        out['unit_specify'] = None

    # --- crop specify text ---
    if crop_os is not None:
        ct = _norm_text(d[crop_os])
        has = ct.notna()
        out['crop_specify'] = {
            'quadrant_generic_other': _quad(crop_other, has),
            'text_rows_by_crop_code': {int(k): int(v) for k, v in
                                       crop[has].value_counts().sort_index().items()},
            'profile_on_generic_other': _string_profile(ct[crop_other & has]),
            'generic_other_rows_text_says_cowpea': int(
                ct[crop_other & has].str.contains(COWPEA_RX, regex=True).sum()),
            'profile_on_variety_other': (_string_profile(ct[crop_var_other & has])
                                         if variety_other else None),
        }
    else:
        out['crop_specify'] = None

    # --- other text columns, looked at on generic-other crop rows only ---
    out['extra_text_on_generic_other'] = {}
    for c in extra:
        if c not in d.columns:
            out['extra_text_on_generic_other'][c] = 'absent'
            continue
        et = _norm_text(d[c])
        prof = _string_profile(et[crop_other & et.notna()])
        prof['top'] = prof['top'][:15]
        out['extra_text_on_generic_other'][c] = prof

    # --- asides the brief asked to quantify ---
    if 'visit' in d.columns:
        out['visit_value_counts'] = {str(k): int(v) for k, v in
                                     d['visit'].value_counts(dropna=False).items()}
    if 'ag_p09_1' in d.columns:
        lab = labels_of('ag_p09_1')
        vc = _codes(d['ag_p09_1']).value_counts(dropna=False)
        out['ag_p09_1_condition'] = {
            'label': varlabels.get('ag_p09_1'),
            'value_labels': lab,
            'value_counts': {('NA' if pd.isna(k) else int(k)): int(v)
                             for k, v in vc.items()},
        }
    return out


# Seasonal Module G in IHS4/IHS5 carries NO crop-name specify column.  The
# only held text that could name a G code-48 crop is the plot-level crop list
# of Module D (ag_d20a..e codes + ag_d20_oth / ag_d20b_oth "Other Crop").
# (half label, G file, D file, household id column)
MODULE_D = [
    ('2016-17', 'Cross_Sectional', 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_g.dta',
     'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta', 'case_id'),
    ('2016-17', 'Panel', 'Malawi/2016-17/Data/Panel/ag_mod_g_16.dta',
     'Malawi/2016-17/Data/Panel/ag_mod_d_16.dta', 'y3_hhid'),
    ('2019-20', 'Cross_Sectional', 'Malawi/2019-20/Data/Cross_Sectional/ag_mod_g.dta',
     'Malawi/2019-20/Data/Cross_Sectional/ag_mod_d.dta', 'case_id'),
    ('2019-20', 'Panel', 'Malawi/2019-20/Data/Panel/ag_mod_g_19.dta',
     'Malawi/2019-20/Data/Panel/ag_mod_d_19.dta', 'y4_hhid'),
]


def _key(df, hh):
    def s(c):
        return df[c].astype('string').str.strip()
    return s(hh) + '|' + s('gardenid') + '|' + s('plotid')


def module_d_crop_text(wave, half, g_rel, d_rel, hh):
    """How many G code-48 rows could be NAMED from Module D's plot-level
    'Other Crop' text?  A measurement of feasibility only."""
    g = get_dataframe(str(get_data_file(g_rel)), convert_categoricals=False)
    crop = _codes(g['crop_code'])
    g48 = g[crop.isin(SEASONAL_OTHER)].copy()
    d_path = str(get_data_file(d_rel))
    try:
        d = get_dataframe(d_path, convert_categoricals=False)
        how = 'get_dataframe'
    except Exception as e:  # 2016-17 XS ag_mod_d has a latin-1 bad byte (CONTENTS.org)
        try:
            d = get_dataframe(d_path, convert_categoricals=False,
                              encoding='latin-1')
            how = f'get_dataframe(encoding=latin-1) after {type(e).__name__}'
        except Exception as e2:
            return {'wave': wave, 'half': half, 'module_d': d_rel,
                    'unreadable': f'{type(e).__name__}; latin-1: {type(e2).__name__}: {e2}'}
    text_cols = [c for c in ('ag_d20_oth', 'ag_d20b_oth') if c in d.columns]
    code_cols = [c for c in ('ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e')
                 if c in d.columns]
    d = d[[hh, 'gardenid', 'plotid'] + code_cols + text_cols].copy()
    d = d.assign(_k=_key(d, hh))
    d['_n48'] = sum(_codes(d[c]).isin(SEASONAL_OTHER).astype(int) for c in code_cols)
    texts = pd.DataFrame({c: _norm_text(d[c]) for c in text_cols})
    d['_ntext'] = texts.notna().sum(axis=1)
    d['_text1'] = texts.bfill(axis=1).iloc[:, 0] if text_cols else pd.NA
    d = d.drop_duplicates('_k')
    g48 = g48.assign(_k=_key(g48, hh)).merge(
        d[['_k', '_n48', '_ntext', '_text1']], on='_k', how='left',
        indicator=True)
    matched = g48['_merge'] == 'both'
    has_text = matched & (g48['_ntext'] > 0)
    unambiguous = has_text & (g48['_ntext'] == 1)
    prof = _string_profile(g48.loc[unambiguous, '_text1'].dropna())
    prof['top'] = prof['top'][:20]
    return {
        'wave': wave, 'half': half, 'module_d': d_rel, 'read': how,
        'd_text_columns': text_cols,
        'g_code48_rows': int(len(g48)),
        'matched_to_d_plot': int(matched.sum()),
        'd_plot_lists_code48': int((matched & (g48['_n48'] > 0)).sum()),
        'd_plot_has_other_crop_text': int(has_text.sum()),
        'd_plot_has_exactly_one_other_text': int(unambiguous.sum()),
        'unambiguous_text_says_cowpea': int(g48.loc[unambiguous, '_text1']
                                            .str.contains(COWPEA_RX, regex=True)
                                            .fillna(False).sum()),
        'profile_unambiguous_text': prof,
    }


def main():
    results = [measure_cell(*cell) for cell in CELLS]
    module_d = [module_d_crop_text(*m) for m in MODULE_D]
    (OUT_DIR / 'measure_856.json').write_text(
        json.dumps({'cells': results, 'module_d_crop_text_for_g48': module_d},
                   indent=1, default=str) + '\n')
    for m in module_d:
        print({k: v for k, v in m.items() if k != 'profile_unambiguous_text'})
        if 'profile_unambiguous_text' in m:
            print('   top:', m['profile_unambiguous_text']['top'])

    fields = ['wave', 'half', 'module', 'n_raw_rows', 'n_builder_rows',
              'n_qty_pos',
              'unit13_rows', 'unit13_share', 'unit13_qty_pos', 'unit13_qty_pos_share',
              'unit13_with_text', 'unit13_no_text', 'unit_text_on_other_codes',
              'unit_text_distinct', 'unit_text_k80', 'unit_text_k90',
              'crop_other_codes', 'crop_other_rows', 'crop_other_share',
              'crop_other_qty_pos', 'crop_other_qty_pos_share',
              'crop_other_with_text', 'crop_other_no_text',
              'crop_text_distinct', 'crop_text_k80', 'crop_text_k90',
              'crop_other_kg_qty_share', 'either_rows', 'either_share']
    rows = []
    for r in results:
        us, cs = r['unit_specify'], r['crop_specify']
        rows.append({
            'wave': r['wave'], 'half': r['half'], 'module': r['module'],
            'n_raw_rows': r['n_raw_rows'], 'n_builder_rows': r['n_builder_rows'],
            'n_qty_pos': r['n_qty_pos'],
            'unit13_rows': r['unit13']['rows'],
            'unit13_share': r['unit13']['share_of_builder_rows'],
            'unit13_qty_pos': r['unit13']['rows_qty_pos'],
            'unit13_qty_pos_share': r['unit13']['share_of_qty_pos_rows'],
            'unit13_with_text': us['quadrant']['code_and_text'] if us else 'n/a',
            'unit13_no_text': us['quadrant']['code_no_text'] if us else 'n/a',
            'unit_text_on_other_codes': us['quadrant']['text_other_code'] if us else 'n/a',
            'unit_text_distinct': us['profile_on_code13']['n_distinct'] if us else 'n/a',
            'unit_text_k80': us['profile_on_code13']['strings_to_cover_80pct'] if us else 'n/a',
            'unit_text_k90': us['profile_on_code13']['strings_to_cover_90pct'] if us else 'n/a',
            'crop_other_codes': '/'.join(str(c) for c in r['confirmed_codes']['crop_other']),
            'crop_other_rows': r['crop_other']['rows'],
            'crop_other_share': r['crop_other']['share_of_builder_rows'],
            'crop_other_qty_pos': r['crop_other']['rows_qty_pos'],
            'crop_other_qty_pos_share': r['crop_other']['share_of_qty_pos_rows'],
            'crop_other_with_text': cs['quadrant_generic_other']['code_and_text'] if cs else 'n/a',
            'crop_other_no_text': cs['quadrant_generic_other']['code_no_text'] if cs else 'n/a',
            'crop_text_distinct': cs['profile_on_generic_other']['n_distinct'] if cs else 'n/a',
            'crop_text_k80': cs['profile_on_generic_other']['strings_to_cover_80pct'] if cs else 'n/a',
            'crop_text_k90': cs['profile_on_generic_other']['strings_to_cover_90pct'] if cs else 'n/a',
            'crop_other_kg_qty_share': r['crop_other_share_of_kilogram_quantity'],
            'either_rows': r['unit13_or_crop_other']['rows'],
            'either_share': r['unit13_or_crop_other']['share_of_builder_rows'],
        })
    with open(OUT_DIR / 'measure_856_summary.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(pd.DataFrame(rows).to_string())


if __name__ == '__main__':
    main()
