#!/usr/bin/env python
"""GhanaLSS 2016-17 food_acquired -> canonical long form.

Emits the canonical schema (per DESIGN_ghanalss_food_acquired_2026-06-15.org,
decisions D1/D5/D6):

  Index   : (t, i, j, u, s, visit)   -- NO `v` (joined from sample() at API time)
  Columns : [Quantity, Expenditure, Price]
  s       : {'purchased', 'produced'} as an index level (the two source modules
            are STACKED into rows, not kept as wide produced_/purchased_ columns)
  visit   : KEPT as a country-local index level (D1 override -- do NOT fold into t)

Field design (GLSS7 Main Report, "Interviewer Workload"): the interviewer
visited each household "every sixth day i.e. after every five days ... seven (7)
times in order to complete the cycle", within a 35-day cycle, supported by a
household diary.  Visit 1 is intake (sections 1, 2, 5, 6A, 7 + diary training);
9B and 8H are asked at the 2nd..7th visits -- SIX five-day recalls.  `visit`
holds the CALENDAR VISIT (2..7), not the source question number; there are
deliberately no visit-1 rows, because visit 1 collected no consumption.  See
_stack_visits for why calendar and not a per-module ordinal.

Sources (the question numbers below are stems, NOT visits):
  - Purchases : g7sec9b.dta, Section 9B.  q1..q6 -> visits 2..7.
        s9bq{n}a = amount spent  -> Expenditure
        s9bq{n}b = quantity      -> Quantity
        s9bq{n}c = unit code     -> u (decoded via Stata value labels + unit_label)
    -> s='purchased'.  No reported Price.
  - Production: g7sec8h.dta, Section 8H.  q3..q8 -> visits 2..7
        (q1/q2 are screeners: "consume any own produce?" / "how many months?").
        s8hq{n}q = quantity      -> Quantity
        s8hq{n}u = unit code     -> u (label is code-prefixed "N. Label"; stripped)
        s8hq{n}p = selling price -> Price (farmgate)
    -> s='produced'.  No produced expenditure column -> Expenditure = NaN.

Unit decode (D-units / GH #348, #453, #384):
  - 9B (purchases): the full g7sec9b.dta is 3.2 GB (a dense food x household
    matrix), so we read the 284 MB g7sec9b_small.dta -- which carries the
    numeric s9bq*c unit codes but NO Stata value labels.  The code -> native
    label map was lifted once from the full file's value labels (metadata-only
    read) into categorical_mapping.org (#+name: unit_9b); decode_unit_code_9b
    turns each numeric code into its native label.
  - 8H (production): g7sec8h.dta is only 99 MB, so it is read categorically
    (convert_categoricals=True) to get native unit label strings directly.
    Its labels are prefixed with the numeric code ("7. Bowl"); the prefix is
    stripped.
  Both paths then map the native label through the country-level unit_label org
  table (raw label string -> canonical Preferred Label) so `u` holds clean
  labels with no leading digits (leak audit clean).
"""
import re
import sys
import warnings

import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, get_categorical_mapping,
                                       get_dataframe, format_id, _to_numeric,
                                       to_parquet)
from lsms_library.transformations import U_UNKNOWN

# The country's own helpers, imported RELATIVE to this script (the house
# pattern).  Deliberately not `from lsms_library.countries.GhanaLSS._.ghanalss
# import ...`: that absolute PACKAGE import binds to whichever `lsms_library`
# won sys.path rather than to the config tree LSMS_COUNTRIES_ROOT selects
# (GH #948 / #436; CONTENTS.org Trap 6).  `../../_` is correct from this
# script's own directory, which is where the Makefile runs it
# (`cd $(@D) && python food_acquired.py`) and which every `../Data/...` read
# in this file already assumes.
sys.path.append('../../_')
from ghanalss import derive_produced_farmgate_value, to_country_food_labels

t = '2016-17'


def hh_id(clust, nh):
    """Canonical household id: clust + '/' + zero-padded nh (matches sample/roster)."""
    c = format_id(clust)
    n = format_id(nh, zeropadding=2)
    if c is None or n is None:
        return pd.NA
    return c + '/' + n


# --- unit decode: native label string -> canonical Preferred Label ----------
_ul = df_from_orgfile('../../_/unit_labels.org', name='unit_label').dropna()
_unit_map = dict(zip(_ul['u'].astype(str).str.strip(),
                     _ul['Preferred Label'].astype(str).str.strip()))


def decode_unit(label):
    """Strip any leading 'N. ' code prefix, then map the native label -> PL."""
    if pd.isna(label):
        return pd.NA
    s = re.sub(r'^\s*\d+\.\s*', '', str(label)).strip()
    if s == '':
        return pd.NA
    return _unit_map.get(s, pd.NA)


# --- 9B units: numeric code -> native label, lifted once from the full
# g7sec9b.dta's Stata value labels into categorical_mapping.org (unit_9b) so the
# build reads the 284 MB g7sec9b_small.dta (no value labels) instead of the
# 3.2 GB full file.  See #453 / #384.
_u9b = df_from_orgfile('../../_/categorical_mapping.org', name='unit_9b').dropna()
_unit_code_9b = dict(zip(_u9b['Code'].astype(int),
                         _u9b['Label'].astype(str).str.strip()))


def decode_unit_code_9b(code):
    """9B numeric unit code -> canonical Preferred Label (code -> native -> PL)."""
    if pd.isna(code):
        return pd.NA
    try:
        native = _unit_code_9b.get(int(code))
    except (ValueError, TypeError):
        return pd.NA
    return decode_unit(native) if native is not None else pd.NA


def _blank_code(raw):
    """True where a RAW unit code is blank: missing, or an empty string.

    Only this case is the missing-unit sentinel (GH #1008; the #1041 rule).  A
    code that is present but unlabelled (9B codes 75/77 are absent from
    ``unit_9b``) or a label ``unit_labels.org`` does not map is a DECODE
    failure: it stays NaN so the null-index audit (Site I) still reports it.
    """
    s = pd.Series(raw).reset_index(drop=True)
    blank = s.isna()
    if not pd.api.types.is_numeric_dtype(s.dtype):
        blank = blank | s.astype('string').str.strip().eq('').fillna(False)
    return blank.to_numpy(dtype=bool)


def _stack_visits(df_num, df_cat, visits, qty_stem, unit_stem, label_map,
                  item_col, exp_stem=None, price_stem=None, unit_decoder=None):
    """Reshape one source module (one row per HH x item) into long form.

    `df_num` (convert_categoricals=False) supplies the numeric food codes and
    amounts.  Units are decoded either from numeric codes in `df_num` via
    `unit_decoder` (a code -> Preferred Label callable -- used for 9B, which
    reads the small label-less file) or, when `unit_decoder` is None, from the
    decoded *label strings* in `df_cat` (convert_categoricals=True; used for 8H,
    whose source is small enough to read categorically).  `df_num`/`df_cat` are
    the same source rows in the same order, so columns line up positionally.

    Stacks each visit into rows; emits columns Quantity/u (+ optional
    Expenditure, Price) with i (household), j (harmonized item), visit.

    `visits` is an explicit {source question number -> CALENDAR VISIT} map, NOT
    a range: the two modules number their questions from different origins (9B
    q1..q6, 8H q3..q8 -- 8H q1/q2 are the screeners "consume any own produce?"
    / "how many months?"), and both are asked at the same six calendar visits,
    the 2nd..7th.  Writing the question number into `visit` -- what this script
    did before -- put a purchase and the own-production consumed at the same
    visit two apart, so `visit` was not sliceable across `s`.

    Calendar visit, not a per-module 1..6 ordinal, because a per-module ordinal
    does not generalise: in GLSS5 (2005-06) 9B runs from the 2nd visit but 8H's
    first visit-specific column is the 3rd, so position-in-module would leave
    the two modules misaligned by one.  The calendar number is also what every
    wave's own Stata variable labels state, so the map is checkable against the
    source rather than inferred.

    `u` is ``U_UNKNOWN`` exactly where the raw unit code (the column the
    decoder reads) is blank -- see `_blank_code` -- and NaN where a code is
    present but does not decode.
    """
    df_num = df_num.copy()
    i = pd.Series([hh_id(c, n) for c, n in zip(df_num['clust'], df_num['nh'])])
    j = df_num[item_col].apply(lambda x: label_map.get(format_id(x), ''))

    frames = []
    for v, calendar_visit in sorted(visits.items()):
        if unit_decoder is not None:
            raw_u = df_num[unit_stem.format(v=v)]
            u_vals = raw_u.apply(unit_decoder).values
        else:
            raw_u = df_cat[unit_stem.format(v=v)]
            u_vals = raw_u.apply(decode_unit).values
        u_vals = np.where(_blank_code(raw_u), U_UNKNOWN, u_vals)
        cols = {'Quantity': _to_numeric(df_num[qty_stem.format(v=v)]).values,
                'u': u_vals}
        if exp_stem is not None:
            cols['Expenditure'] = _to_numeric(df_num[exp_stem.format(v=v)]).values
        if price_stem is not None:
            cols['Price'] = _to_numeric(df_num[price_stem.format(v=v)]).values
        part = pd.DataFrame(cols)
        part['i'] = i.values
        part['j'] = j.values
        part['visit'] = calendar_visit
        frames.append(part)
    out = pd.concat(frames, ignore_index=True)
    # Treat 0 as missing for amounts (matches the legacy script's convention).
    out = out.replace({'': pd.NA, 0: np.nan})
    return out


# Food-code -> harmonized item maps (config reads only; no microdata).
# Food codes (freqcd / foodcd) stay numeric for the harmonize_food lookup.
purch_labels = get_categorical_mapping(
    tablename='harmonize_food',
    idxvars={'Code': ('Code_9b', format_id)}, **{'Label': 'Preferred Label'})
prod_labels = get_categorical_mapping(
    tablename='harmonize_food',
    idxvars={'Code': ('Code_8h', format_id)}, **{'Label': 'Preferred Label'})


DERIVATION_8H = 'GhanaLSS::food_acquired::8h-farmgate'


def screen_8h(num8h, cat8h):
    """Keep the 8H rows whose screener ``s8hq1`` is 1 ("1. Yes").

    GH #1008, ruling @ligon 2026-10-19: serve own produce only where the
    screener says the household consumed this item from own production -- the
    questionnaire's own skip logic, and what 1998-99, 2005-06 and 2012-13
    already do.  ``s8hq1`` is asked per (household, item) row.  Applied BEFORE
    the visit stack, by position on both views (they are the same source rows
    in the same order).

    It removes 868,975 of 934,584 source rows: 868,255 "2. No" and 720 blank.
    None of the "No" rows carries a quantity or a unit at any visit; 12,038
    carry a price only, and each is one constant per (visit, item) -- 11,934 at
    visit 2 (Cassava 2, Tomato 3, Pepper 2, Okra 7) and 104 at visits 3..7 -- a
    fill, not an answer.  Of the 720 blank screeners, 39 rows (11 households)
    DO carry quantities; they go too, as in the sibling waves.  Census and
    costs: ``_/CONTENTS.org``.
    """
    keep = num8h['s8hq1'].eq(1).to_numpy()
    return (num8h[keep].reset_index(drop=True),
            cat8h[keep].reset_index(drop=True))


def build(num9b, num8h, cat8h):
    """The canonical wave frame from the three source views.

    ``num9b`` is ``g7sec9b_small.dta`` and ``num8h`` / ``cat8h`` are
    ``g7sec8h.dta``, read with ``convert_categoricals=False`` / ``True``.  A
    function, not module-level code, so the decode, the screener gate and the
    sentinel can be tested on a synthetic frame without the 284 MB read
    (``tests/test_gh1008_ghanalss_null_keys.py``).
    """
    ####################
    # Purchases (Section 9B) -> s='purchased'
    ####################
    # 9B q1..q6 are asked at the 2nd..7th visits ("amount spent by second visit"
    # .. "by seventh visit" in the GLSS4/5/6 labels for the same stems).
    fe = _stack_visits(num9b, None, {n: n + 1 for n in range(1, 7)},
                       qty_stem='s9bq{v}b', unit_stem='s9bq{v}c',
                       label_map=purch_labels, item_col='freqcd',
                       exp_stem='s9bq{v}a', unit_decoder=decode_unit_code_9b)
    fe['s'] = 'purchased'

    ####################
    # Home production (Section 8H) -> s='produced'
    ####################
    num8h, cat8h = screen_8h(num8h, cat8h)
    # 8H q3..q8 are the same six visits: this wave's own Stata labels read
    # "Quantity of own produced food consumed at 2nd visit" (s8hq3) through
    # "...now-7th visit" (s8hq8).  q1/q2 are screeners, not visits.
    hp = _stack_visits(num8h, cat8h, {n: n - 1 for n in range(3, 9)},
                       qty_stem='s8hq{v}q', unit_stem='s8hq{v}u',
                       label_map=prod_labels, item_col='foodcd',
                       price_stem='s8hq{v}p')
    hp['s'] = 'produced'

    ####################
    # Combine, harmonize, canonicalize
    ####################
    df = pd.concat([fe, hp], ignore_index=True)

    # Drop rows whose food code did not harmonize (no '' / NA j leakage; D6).
    df = df[df['j'].notna() & (df['j'] != '')]

    # LCU-only convention: a monetary value with no physical quantity/unit ->
    # u='Value' with Quantity = Expenditure (food-acquired skill).  2016-17
    # records quantities, so this is typically empty, but apply it where it
    # occurs.  "No unit" is a blank code (U_UNKNOWN, written by _stack_visits)
    # or a code that did not decode (NaN) -- the same rows this rescue reached
    # when it tested `u.isna()` alone, before the sentinel moved upstream.
    if 'Expenditure' in df.columns:
        no_unit = df['u'].isna() | df['u'].eq(U_UNKNOWN).fillna(False)
        value_only = df['Quantity'].isna() & no_unit & df['Expenditure'].notna()
        df.loc[value_only, 'u'] = 'Value'
        df.loc[value_only, 'Quantity'] = df.loc[value_only, 'Expenditure']

    # A blank unit code is already the missing-unit sentinel U_UNKNOWN
    # ('Unknown'), never a NaN `u` (GH #1008; the #1041 rule) -- written in
    # _stack_visits from the RAW code, so a present-but-undecodable code stays
    # NaN for the null-index audit to report.  Census of the served rows: every
    # blank is a missing s9bq{n}c / s8hq{n}u; 422 are 9B purchases with a
    # quantity and an amount, the rest 8H produced rows that survive the
    # screener gate (mostly a selling price only; CONTENTS.org).  A NaN `u` is
    # deleted by every downstream groupby (Feature assembly, the derived food
    # tables) while Country serves it; the sentinel keeps the row in both and
    # the kg ladder serves it `none`.

    df['t'] = t

    # Sentinel guard: some Stata extracts use a huge value for missing numerics.
    num = df.select_dtypes(exclude=['object', 'category'])
    if len(num.columns):
        na = num.max().max()
        if pd.notna(na) and na > 1e99:
            warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
            df = df.replace(na, np.nan)

    idx = ['t', 'i', 'j', 'u', 's', 'visit']
    value_cols = [c for c in ['Quantity', 'Expenditure', 'Price'] if c in df.columns]

    # Collapse duplicate (t,i,j,u,s,visit) rows (multiple source records of the
    # same item/unit within a visit) by summing the amounts.
    df = df.groupby(idx, dropna=False)[value_cols].sum(min_count=1)

    df = df.dropna(how='all')

    # ---- own-production value: DERIVED, not an answer (GH: 8h-farmgate) ----
    # Section 8H reports a quantity and a farmgate price but never a total, so
    # the served Expenditure is a CONSTRUCTION.  Registry: _/derivations.yml,
    # key GhanaLSS::food_acquired::8h-farmgate; ledger
    # .coder/ledger/ghanalss-produced-qp.md.
    #
    # Stamped AFTER the uniqueness assert (CLAUDE.md "Derived Values", step 3:
    # core SUMs the additive measures and re-derives Price, so a string must not
    # reach the .sum()), and ONLY where BOTH factors exist -- a produced row
    # missing either keeps Expenditure NaN and carries NO key, so the labelled
    # rows are exactly the computed ones.
    _prod = (df.index.get_level_values('s') == 'produced')
    _both = (_prod
             & pd.to_numeric(df['Quantity'], errors='coerce').notna().to_numpy()
             & pd.to_numeric(df['Price'], errors='coerce').notna().to_numpy())
    _kept = pd.to_numeric(df['Expenditure'], errors='coerce').to_numpy(dtype=float)
    _qp = derive_produced_farmgate_value(df['Quantity'], df['Price'])
    df['Expenditure'] = pd.array(np.where(_both, _qp, _kept), dtype='Float64')
    df['Derivation'] = pd.Series(np.where(_both, DERIVATION_8H, None),
                                    index=df.index, dtype='string')
    assert df.loc[~_both, 'Derivation'].isna().all(), (
        'a row with no computed value carries the 8h-farmgate key')
    return df


if __name__ == '__main__':
    # Read the 284 MB g7sec9b_small.dta (NOT the 3.2 GB full g7sec9b.dta):
    # _small lacks value labels, but units are decoded from numeric s9bq*c codes
    # via the unit_9b org table (decode_unit_code_9b).  See #453 / #384.
    # g7sec8h.dta is read twice: numerically for codes and amounts, and
    # categorically for its native unit label strings.
    df = build(get_dataframe('../Data/g7sec9b_small.dta', convert_categoricals=False),
               get_dataframe('../Data/g7sec8h.dta', convert_categoricals=False),
               get_dataframe('../Data/g7sec8h.dta', convert_categoricals=True))
    # Lcp: the wave vocabulary -> the COUNTRY harmonize_food axis.
    # Pure rename; an unmapped label raises rather than passing through.
    df = to_country_food_labels(df, '2016-17')
    to_parquet(df, 'food_acquired.parquet')
