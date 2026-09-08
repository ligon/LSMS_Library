"""Build nonfood_expenditures for Togo EHCVM 2018 (item-level).
SELF-CONTAINED: the id / map helpers are inlined here, so this script does
NOT import togo (matching the sibling wave scripts livestock.py,
crop_production.py, plot_inputs.py, plot_labor.py).

*** SOURCE LOCATION: the EHCVM non-food modules live in 2018/Data1/ (NOT
2018/Data/, which held only the `_forEthan` extracts).  The five
`Togo_survey2018_nonfooditems{7days,30days,3months,6months,12months}.csv`
extracts this script's PREDECESSOR read were `dvc add`ed in 2021, never
pushed, and are unrecoverable (GH #750); they have been retired.  Do not
resurrect them -- Data1/ is the published EHCVM distribution and covers the
same five recall windows. ***

WHAT SECTION 9 IS (established from the instrument, not from convention).
2018/Documentation/tgo_ehcvm1_qnr_household_excel_vague1.xls, sheet
S9b__Conso_NA row 0:

    SECTION 9: DÉPENSES RÉTROSPECTIVES NON ALIMENTAIRES DU MÉNAGE
    PARTIE B: DEPENSES NON ALIMENTAIRES DES 7 DERNIERS JOURS

and correspondingly PARTIE C / D / E / F for 30 DERNIERS JOURS, 3 / 6 / 12
DERNIERS MOIS.  So s09b..s09f ARE recall-window modules, one file per
window:

    file    part   recall window     items  rows
    s09b     B     last 7 days          17   17,932
    s09c     C     last 30 days         22   27,871
    s09d     D     last 3 months        18   10,999
    s09e     E     last 6 months        12   32,326
    s09f     F     last 12 months       53   19,317

Each row is one reported purchase:
    s09?q01  Code Produit/Service  (the item; -> j via `nonfood_items`)
    s09?q02  bought in the window? (1=Oui / 2=Non) -- GATE
    s09?q03  Montant (CFA) dépensé au cours de [WINDOW]  -> Expenditure

`Expenditure` IS NOT ANNUALISED.  It is the FCFA amount the household
reported for THAT window, exactly as recorded, and `RecallWindow` names the
window.  Annualising is a transformation, so it is the analyst's to make and
not a column here (same rule as harvest_kg in crop_production).  For the
record, one defensible scaling is

    annual = Expenditure * 365.25 / {'7 days': 7, '30 days': 30,
                                     '3 months': 91.3125, '6 months': 182.625,
                                     '12 months': 365.25}[RecallWindow]

but the library ships the reported number and the window, not the product.

PARTIE A (s09a) IS DELIBERATELY EXCLUDED -- see _/CONTENTS.org
"nonfood_expenditures (EHCVM section 9)".  In one line: it is keyed by
EVENT (13 fêtes/cérémonies) rather than by product, its five amount columns
are fixed categories two of which are FOOD (alimentation, boissons), and the
five retired CSVs did not contain it either.  Its non-food categories are
therefore NOT in this table, and because 9E/9F instruct the enumerator to
exclude ceremony clothing, this table UNDERCOUNTS clothing and jewellery.

GRAIN (t, i, j).  No `v` level: the framework joins v from sample() at API
time (CLAUDE.md, "sample() and Cluster Identity"), so emitting one here
would be wrong.  No `vague` level: each grappe is visited in exactly one
vague, so (grappe, menage) already identifies a household uniquely (CLAUDE.md
"EHCVM countries"; _/CONTENTS.org "Sampling Design").

`i` is Togo's composite id (grappe + '0' + zero-padded menage), inlined
VERBATIM from togo.i(), matching sample() and the other Togo features.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet

T = '2018'

# module letter -> (source file, recall window verbatim from the instrument)
MODULES = {
    'b': ('../Data1/s09b_me_tgo2018.dta', '7 days'),
    'c': ('../Data1/s09c_me_tgo2018.dta', '30 days'),
    'd': ('../Data1/s09d_me_tgo2018.dta', '3 months'),
    'e': ('../Data1/s09e_me_tgo2018.dta', '6 months'),
    'f': ('../Data1/s09f_me_tgo2018.dta', '12 months'),
}


def i(value):
    """Composite household id from (grappe, menage), matching Togo's sample().
    Inlined VERBATIM from togo.i() (Togo/_/togo.py:7): grappe + '0' separator
    + zero-padded (2-digit) menage.  NO 'E_' prefix."""
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a {int code -> Preferred Label} dict from categorical_mapping.org.
    Blank / '---' Preferred Labels map to NA.  (Inlined from livestock.py.)"""
    raw = tools.get_categorical_mapping(tablename=tablename, idxvars=key,
                                        **{value: value})
    out = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v) or str(v).strip() in ('---', ''):
            out[int_k] = pd.NA
        else:
            out[int_k] = str(v).strip()
    return out


item_map = _harmonized_codes('nonfood_items')

# `j` is an index level, so a non-injective label map silently POOLS two
# distinct reported items (GH #323, harmonize_seed_crop).  Refuse to build.
labels = [v for v in item_map.values() if not pd.isna(v)]
assert len(labels) == len(set(labels)), (
    'nonfood_items Preferred Labels are not unique: '
    f'{sorted({x for x in labels if labels.count(x) > 1})}')

frames = []
for letter, (fn, window) in MODULES.items():
    src = get_dataframe(fn, convert_categoricals=False)
    q01, q02, q03 = (f's09{letter}q0{n}' for n in (1, 2, 3))

    # As distributed, every shipped row already has the gate == 1 (Oui): the
    # 'Non' rows are not published.  Keep the gate anyway, for parity and so
    # a future re-release that ships them cannot leak 'did not buy' rows in.
    src = src[src[q02] == 1]

    codes = src[q01].astype('Int64')
    unknown = sorted(set(codes.dropna().unique()) - set(item_map))
    assert not unknown, (
        f'{fn}: item codes with no nonfood_items row: {unknown}')

    hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                         index=['grappe', 'menage'])), axis=1)

    frames.append(pd.DataFrame({
        't':            T,
        'i':            hh.values,
        'j':            codes.map(item_map).astype('string').values,
        'RecallWindow': window,
        'Expenditure':  pd.to_numeric(src[q03], errors='coerce').values,
    }))

df = pd.concat(frames, axis=0, ignore_index=True)
df['Expenditure'] = df['Expenditure'].astype('Float64')
df['RecallWindow'] = df['RecallWindow'].astype('string')
df = df[df['i'].notna() & df['j'].notna()]

# The item codes are disjoint across the five modules (201-217 / 301-322 /
# 401-418 / 501-512 / 601-653), so j determines RecallWindow and (t, i, j) is
# already unique in the source -- no reduction is needed or wanted.  Assert
# both, so a future release that reuses a code cannot pass silently.
per_item_windows = df.groupby('j', dropna=False)['RecallWindow'].nunique()
assert (per_item_windows == 1).all(), (
    'an item appears under more than one recall window: '
    f'{per_item_windows[per_item_windows > 1].index.tolist()}')
dups = df.duplicated(subset=['t', 'i', 'j']).sum()
assert dups == 0, f'{dups} duplicate (t, i, j) rows in nonfood_expenditures'

df = df.set_index(['t', 'i', 'j'])[['Expenditure', 'RecallWindow']]

assert len(df) > 0, 'nonfood_expenditures 2018 produced no rows'
to_parquet(df, 'nonfood_expenditures.parquet')
