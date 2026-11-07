"""Approved IHS5-to-2024 food-factor borrowing; ledger food-followups §§3–5.

Keep native quantities/units and the existing kg ladder. No item aliases,
coarse-label matches, factor averaging or new kg estimator are defined here.
"""
from pathlib import Path
import json
import re
import sys
import contextlib

import numpy as np
import pandas as pd

from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.transformations import KNOWN_METRIC, U_UNKNOWN, _parse_explicit_metric

HERE = Path(__file__).resolve().parent
COUNTRY = HERE.parents[1]
sys.path.append(str(COUNTRY / '_'))
from malawi import food_acquired_to_canonical, _metric_kg_factor  # noqa: E402

BORROW_KEY = 'Malawi::food_acquired::borrow-ihs5-factor'
FACTOR_FILE = '2019-20/Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta'
FACTOR_MD5 = 'dc1bed57a31e20232d04bd1ac5bb0f01'
SOURCES = {'consumed': 'hh_g03', 'bought': 'hh_g04',
           'produced': 'hh_g06', 'gifted': 'hh_g07'}
KEYS = ['region', 'item_code', 'unit_code', 'Otherunit']
wave = '2024-25'


def canonical_food(wide):
    """Run the existing Malawi producer in its documented wave cwd."""
    with contextlib.chdir(HERE):
        return food_acquired_to_canonical(wide, wave='2024-25')


def _code(value):
    return format_id(value).upper() if pd.notna(format_id(value)) else pd.NA


def _label(value):
    """Spelling comparison without erasing numeric meaning.

    Ignore case/spacing and nonnumeric punctuation. Retain literal numeric
    tokens and separators: 1/2, 12, 1-2 and 30.0 are different. Unicode numeric
    fractions remain literal tokens. No numeric equivalences are inferred.
    The instrument's ordinal ``No. 10`` is the one explicit punctuation alias.
    """
    if pd.isna(value):
        return ''
    raw = re.sub(r'\bno\s*\.\s*(?=\d)', 'no ', str(value).casefold())
    spans = [m for m in re.finditer(
        r'[-+−]?(?:\d+(?:[.,٫٬]\d+)*|[.,٫٬]\d+)|[^\W\d_]', raw)
        if any(c.isnumeric() for c in m.group())]
    numeric = tuple(m.group() for m in spans)
    separators = tuple(re.sub(r'\s+', '', raw[a.end():b.start()])
                       for a, b in zip(spans, spans[1:]))
    # One string type also handles an entirely absent dictionary entry under
    # pandas' Arrow string comparisons; tuples and missing strings cannot mix.
    return json.dumps((re.sub(r'[^\w]+', '', raw), numeric, separators),
                      ensure_ascii=False, separators=(',', ':'))


def _other(value):
    # Text identity, not synonym matching; never a generic code-23 mean.
    return ' '.join(str(value).strip().upper().split()) if pd.notna(value) else ''


def _metric(value):
    if pd.isna(value):
        return False
    return (str(value).strip().lower() in KNOWN_METRIC
            or _metric_kg_factor(value) is not None
            or _parse_explicit_metric(value) is not None)


def prepare_ihs5_factors(factors, dictionaries=None):
    """Keep the full source key; mark conservative item/unit compatibility.

    Every source factor/origin is retained, including refused rows. The exact
    released key must be unique even if duplicate rows happen to agree.
    """
    if dictionaries is None:
        dictionaries = json.loads((HERE / 'borrow_dictionaries.json').read_text())
    out = factors.copy()
    for key in KEYS:
        out[f'_raw_{key}'] = out[key]
    for key in KEYS[:-1]:
        out[key] = out[key].map(_code)
    out['Otherunit'] = out['Otherunit'].map(_other)
    if out[KEYS].isna().any(axis=None) or out.duplicated(KEYS).any():
        raise ValueError('IHS5 factor source requires unique, nonmissing full keys')
    current = out.item_code.map(dictionaries['current_items']).map(_label)
    previous = out.item_code.map(dictionaries['ihs5_items']).map(_label)
    unit = out.unit_code.map(dictionaries['current_units']).map(_label)
    out['borrow_compatible'] = (
        current.ne('') & current.eq(previous) & current.eq(out.item_name.map(_label))
        & unit.ne('') & unit.eq(out.unit_name.map(_label)))
    factor = pd.to_numeric(out.factor, errors='coerce')
    out['borrow_compatible'] &= np.isfinite(factor) & factor.gt(0)
    return out


def match_ihs5_factors(wide, factors):
    """Many-to-one exact native-key lookup, preserving row order and count."""
    out = wide.copy()
    admitted = prepare_ihs5_factors(factors)
    admitted = admitted[admitted.borrow_compatible].set_index(KEYS)
    for source in SOURCES:
        unit = out[f'unitcode_{source}'].map(_code)
        raw_other = f'{SOURCES[source]}b_oth'
        detail = out[raw_other] if raw_other in out else out[f'unitsdetail_{source}']
        other = detail.map(_other).where(unit.eq('23'), '')
        idx = pd.MultiIndex.from_arrays([
            out['_borrow_region'].map(_code), out['hh_g02'].map(_code), unit, other],
            names=KEYS)
        matched = admitted.reindex(idx)
        allowed = (out[f'u_{source}'].notna()
                   & out[f'u_{source}'].ne(U_UNKNOWN)
                   & ~out[f'u_{source}'].map(_metric))
        # Existing metric conversion remains the one native conversion point.
        out[f'cfactor_{source}'] = np.where(allowed, matched.factor, np.nan)
        for col in ('region', 'item_code', 'unit_code', 'Otherunit',
                    'factor', 'source', 'item_name', 'unit_name'):
            values = (matched[f'_raw_{col}'].to_numpy()
                      if col in KEYS else matched[col].to_numpy())
            out[f'ihs5_{source}_{col}'] = values
    return out


def derive_borrowed_food(wide, factors):
    """Existing canonical reducer with complete quantity coverage and a stamp.

    The count companion uses that same reducer/key map. A collapsed row with
    any contributing quantity lacking a factor keeps Quantity_kg missing.
    """
    matched = match_ihs5_factors(wide, factors)
    out = canonical_food(matched)
    companion = matched.copy()
    for source in ('bought', 'produced', 'gifted'):
        q = pd.to_numeric(matched[f'quantity_{source}'], errors='coerce')
        active = q.gt(0)
        if source == 'bought':
            active |= pd.to_numeric(matched.expenditure, errors='coerce').gt(0)
        # Every retained contributor counts, including an expenditure-only
        # answer with unknown quantity. It cannot certify a whole kg sum.
        companion[f'quantity_{source}'] = np.where(active, 1., np.nan)
        # Metric rows remain under the original metric conversion; no borrowing.
        companion[f'cfactor_{source}'] = np.where(
            matched[f'u_{source}'].map(_metric), np.nan,
            (matched[f'cfactor_{source}'].notna() & q.notna()).astype(float))
    coverage = canonical_food(companion).reindex(out.index)
    complete = (coverage.Quantity.gt(0)
                & coverage.Quantity.eq(coverage.Quantity_kg))
    out['Quantity_kg'] = out.Quantity_kg.where(complete)
    # This recovered producer has no other derivation. Refuse silent overwrite.
    if 'Derivation' in out.columns and out.Derivation.notna().any():
        raise ValueError('combine pre-existing derivation keys explicitly')
    out['Derivation'] = pd.Series(pd.NA, index=out.index, dtype='string')
    out.loc[out.Quantity_kg.notna(), 'Derivation'] = BORROW_KEY
    return out


def load_ihs5_factors():
    """Read the declared released source, with its version bound explicitly."""
    import yaml
    sidecar = COUNTRY / (FACTOR_FILE + '.dvc')
    declared = yaml.safe_load(sidecar.read_text())['outs'][0]['md5']
    if declared != FACTOR_MD5:
        raise ValueError('IHS5 factor source changed; review borrowing/version metadata')
    return get_dataframe(str(COUNTRY / FACTOR_FILE), convert_categoricals=False)


def inputs_borrowed_food(wave='2024-25'):
    """Raw input rows/operands for every completely covered derived group.

    Original food-variable names remain numerical release answers. Each source
    contribution repeats its input line, full factor key/origin, year and MD5.
    The extra food_obs coordinate retains input grain after served-key collapse.
    """
    if wave != '2024-25':
        raise ValueError(f'{BORROW_KEY} applies only to 2024-25')
    wide = read_food_wide()
    factors = load_ihs5_factors()
    served = derive_borrowed_food(wide, factors)
    derived = served.index[served.Derivation.eq(BORROW_KEY).fillna(False)]
    matched = match_ihs5_factors(wide, factors).reset_index()
    raw_columns = [c for c in columns_dict if c in matched]
    pieces = []
    for source, canonical in [('bought', 'purchased'), ('produced', 'produced'),
                              ('gifted', 'inkind')]:
        raw = matched[raw_columns + ['_source_half', '_source_row']].copy()
        raw['region'] = matched['_borrow_region'].to_numpy()
        for col in ('region', 'item_code', 'unit_code', 'Otherunit', 'factor',
                    'source', 'item_name', 'unit_name'):
            raw[f'ihs5.{col}'] = matched[f'ihs5_{source}_{col}'].to_numpy()
        raw['ihs5_source_year'] = '2019-20'
        raw['ihs5_source_md5'] = FACTOR_MD5
        raw['source_file'] = np.where(matched._source_half.eq('xs'),
                                     'Cross_Sectional/hh_mod_g1.dta',
                                     'Panel/hh_mod_g1_24.dta')
        raw.index = pd.MultiIndex.from_arrays([
            matched.t, matched.j, matched.i,
            matched[f'u_{source}'].map(_norm_unit_code),
            np.full(len(matched), canonical)], names=['t', 'i', 'j', 'u', 's'])
        with contextlib.chdir(HERE):
            raw = apply_harmonize_food(raw, wave, level='j')
        q = pd.to_numeric(matched[f'quantity_{source}'], errors='coerce')
        active = q.gt(0)
        if source == 'bought':
            active |= pd.to_numeric(matched.expenditure, errors='coerce').gt(0)
        take = raw.index.isin(derived) & active.to_numpy()
        raw = raw.iloc[np.flatnonzero(take)]
        raw['food_obs'] = raw._source_half + ':' + raw._source_row.astype(str)
        pieces.append(raw.set_index('food_obs', append=True))
    return pd.concat(pieces)


# Native producer recovered from 83a; raw operands additionally retained.
from malawi import (CS_24, drop_ihps24_refreshed, handling_unusual_units,
                    normalize_food_label, apply_harmonize_food, _norm_unit_code)

columns_dict = {'case_id': 'j', 'y5_hhid': 'j', 'hh_g02': 'i', 'hh_g05': 'expenditure'}
for source, stem in SOURCES.items():
    columns_dict.update({f'{stem}a': f'quantity_{source}',
                         f'{stem}b': f'unitcode_{source}',
                         f'{stem}b_label': f'units_{source}',
                         f'{stem}b_oth': f'unitsdetail_{source}'})
UNIT_LABELS = [f'units_{s}' for s in SOURCES]


PANEL = str(HERE.parent / 'Data/Panel/hh_mod_g1_24.dta')


def _read(path):
    raw_columns = [c for c in columns_dict if c not in ('case_id', 'y5_hhid')]
    id_column = 'y5_hhid' if '/Panel/' in str(path) else 'case_id'
    raw_columns.append(id_column)
    labelled = get_dataframe(path, convert_categoricals=True)[raw_columns].rename(
        columns_dict, axis=1)
    raw = get_dataframe(path, convert_categoricals=False)[raw_columns]
    if len(raw) != len(labelled):
        raise ValueError('numeric/labelled food source row counts disagree')
    for col in raw_columns:
        labelled[col] = raw[col].to_numpy()
    labelled['_source_half'] = 'panel' if id_column == 'y5_hhid' else 'xs'
    labelled['_source_row'] = np.arange(len(raw))
    return labelled


def _unit_list():
    """The instrument's unit list, numeric code -> label, off the Panel file."""
    columns = [f'{stem}b_label' for stem in SOURCES.values()]
    coded = get_dataframe(PANEL, convert_categoricals=False)[columns]
    labelled = get_dataframe(PANEL, convert_categoricals=True)[columns]
    pairs = pd.concat([
        pd.DataFrame({'code': pd.to_numeric(coded[f'{stem}b_label'], errors='coerce'),
                      'label': labelled[f'{stem}b_label'].astype('string')})
        for stem in SOURCES.values()]).dropna().drop_duplicates()
    assert not pairs['code'].duplicated().any(), 'a unit code with two labels'
    return dict(zip(pairs['code'], pairs['label']))


def _decode_unit_labels(frame, unit_list):
    """Replace a bare numeric code with its label; a label stays as it is."""
    for col in UNIT_LABELS:
        raw = frame[col].astype('string')
        code = pd.to_numeric(raw, errors='coerce')
        frame[col] = code.map(unit_list).astype('string').fillna(raw)
    return frame




def read_food_wide():
    df = _read(str(HERE.parent / 'Data/Cross_Sectional/hh_mod_g1.dta'))
    # The IHS6 file writes Stata's extended missing .a as a literal 'a' in these
    # string columns, on 7 to 98 rows each; the Panel copy holds a true missing.
    READ = [c for c in df.columns if c in set(columns_dict.values()) - {'i', 'j'}]
    df[READ] = df[READ].replace('a', pd.NA)
    df = _decode_unit_labels(df, _unit_list())
    panel_df = drop_ihps24_refreshed(_read(PANEL), 'j')

    keep = sorted(set(columns_dict.values()) | (set(columns_dict) - {'case_id', 'y5_hhid'}) | {'_source_half', '_source_row'})
    # Preserve the two original ID names as raw input operands too.
    df['y5_hhid'] = pd.NA
    panel_df['case_id'] = panel_df.get('case_id', pd.NA)
    keep = sorted(set(keep) | {'case_id', 'y5_hhid'})
    df = df.loc[:, keep]
    panel_df = panel_df.loc[:, keep]
    df['j'] = CS_24 + df['j'].apply(format_id)
    panel_df['j'] = panel_df['j'].apply(format_id)
    df = pd.concat([df, panel_df], axis=0)
    df['i'] = normalize_food_label(df['i'].astype(str).str.capitalize())

    # The IHS6 file stores these as strings; the Panel file as numbers.
    cols = ['expenditure'] + [f'quantity_{s}' for s in SOURCES]
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

    df = df.set_index(['j', 'i'])
    # Raw-named operands stay byte-for-value source answers, including blanks.
    cleaned = [c for c in columns_dict.values() if c in df.columns]
    df[cleaned] = df[cleaned].replace(r'^\s*$', pd.NA, regex=True)
    for s in SOURCES:
        df[f'unitcode_{s}'] = df[f'unitcode_{s}'].str.upper()

    df = handling_unusual_units(df, suffixes=list(SOURCES))
    # A quantity with no unit at all (the 3 rows whose label was that literal 'a') is
    # "no unit recorded" (GH #1042).  A NaN here would be deleted by the first groupby.
    for s in SOURCES:
        df[f'u_{s}'] = df[f'u_{s}'].fillna(U_UNKNOWN)

    df['t'] = wave
    df = df.reset_index()
    regions = []
    for half, name, idcol, prefix in [('Cross_Sectional', 'hh_mod_a_filt.dta', 'case_id', CS_24),
                                      ('Panel', 'hh_mod_a_filt_24.dta', 'y5_hhid', '')]:
        cover = get_dataframe(str(HERE.parent / 'Data' / half / name),
                              convert_categoricals=False)[[idcol, 'region']]
        regions.append(pd.Series(cover.region.to_numpy(),
                                 index=prefix + cover[idcol].map(format_id)))
    region = pd.concat(regions)
    if not region.index.is_unique:
        raise ValueError('2024 cover region lookup must have unique household ids')
    df['_borrow_region'] = df['j'].map(region)
    return df.set_index(['j', 't', 'i'])
