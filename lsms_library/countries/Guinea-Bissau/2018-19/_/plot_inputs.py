"""Build plot_inputs for Guinea-Bissau EHCVM 2018-19 (GAP 2, item-level).

Cloned from Niger/2018-19/_/plot_inputs.py but SELF-CONTAINED: the label
maps, the i() builder, the CROP_NA sentinel, and the
(t, i, input, crop, u) finish tail are inlined here so this script does
not import the niger module.

Single source file: s16b_me_gnb2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Sim / 2=Não) — application gate
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   received as a GIFT? (Sim / Não)        -> Received_as_gift
  s16bq07a / s16bq07b   gift quantity + unit        -> Quantity_received_as_gift, Unit_received_as_gift
  s16bq08   PURCHASED? (Sim / Não)                 -> Purchased
  s16bq09a / s16bq09b   purchased quantity + unit   -> Quantity_purchased, Unit_purchased

We keep s16bq02==1 (applied) rows (the held file ships only those: 5,534 of
5,534).  The EHCVM roster has NO crop column — the seed's crop is embedded in
the input-type label ('Sementes de arroz').  A row is a SEED record when its
s16bq01 CODE is 11-20 (the file's own value labels: 11 'Sementes de  milho
preto' ... 19 'Plantas, cordas/caules de tuberculos', 20 'Outras
sementes'); its crop is harmonize_seed_crop's label for it, and a seed label
the table does not name passes through as recorded (GH #1119's contract: a
recorded but unmapped crop passes through).  Codes 1-10 (fertilizers and
phytosanitary products) are structural non-crop records and carry the
CROP_NA sentinel.  i = EHCVM composite via the Guinea-Bissau helper (grappe +
'0' + zero-padded menage, NO 'E_' prefix).  No plot column; grain is
(t, i, input, crop, u).

LABEL NOTE (silent-failure gotcha): a bare get_categorical_mapping on a
missing table silently returns the FIRST org table (`u`), which would mis-map
inputs to unit labels.  _table_exists() still guards every lookup.  Since
2026-11-13 _/categorical_mapping.org has harmonize_input (the three inorganic
labels with a product named, GH #1107) and harmonize_seed_crop (GH #1119);
every other input label passes through as the raw Portuguese label, and the
lookup folds runs of whitespace because the source spells code 11 with a
double space.  The `u` table exists for Guinea-Bissau, so units harmonize
normally; 'Quilograma' and 'Grama' take their mass from the country's u_kg
declaration at read time, not from a relabel here.

`Purchased` / `Quantity_purchased` read the PURCHASE, s16bq08 / s16bq09a (ruling
2026-11-13, "Fix all three now", with Togo 2018 and Benin 2018-19).  Until then they
read s16bq05 / s16bq07a, which the variable labels make the GIFT question and quantity
("16B.05. Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?"; s16bq06 names the
giver); those are now served in their own columns, under Benin's and Togo's names.
"""
import os

import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific.  A non-null token keeps all reported rows through the
# framework's NaN-key de-dup collapse (groupby().first() drops NaN keys).
CROP_NA = '(not crop-specific)'

_ORG = os.path.join(os.path.dirname(__file__), '..', '..', '_',
                    'categorical_mapping.org')


def _table_exists(tablename):
    """True iff categorical_mapping.org declares a ``#+name:`` /
    ``#+NAME:`` header for ``tablename``.  Guards against
    get_categorical_mapping's silent fall-through to the first org table."""
    target = ('#+name: ' + tablename).lower()
    try:
        with open(_ORG, encoding='utf-8') as fh:
            for line in fh:
                if line.strip().lower() == target:
                    return True
    except OSError:
        return False
    return False


def i(value):
    """Guinea-Bissau EHCVM household id from (grappe, menage).  No 'E_'
    prefix (single EHCVM wave).  Inlined for self-containment."""
    if isinstance(value, pd.Series):
        grappe = tools.format_id(value.iloc[0])
        menage = tools.format_id(value.iloc[1], zeropadding=2)
        if grappe is None or menage is None:
            return None
        return grappe + '0' + menage
    return tools.format_id(value)


def _safe_map(tablename):
    """Load a {Original Label -> Preferred Label} dict only if the table
    really exists; otherwise return {} (identity passthrough)."""
    if not _table_exists(tablename):
        return {}
    return tools.get_categorical_mapping(
        tablename=tablename, idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})


def _fold(label):
    """A label with runs of whitespace folded to one space, for lookups only."""
    return ' '.join(str(label).split())


def _input_labels(source_labels, input_map):
    """Map an input-type column through input_map.  Unmapped labels pass
    through unchanged (kept visible, flagged by the sanity checker)."""
    folded = {_fold(k): v for k, v in input_map.items()}
    lab = source_labels.astype('string')
    return lab.map(lambda x: folded.get(_fold(x), x) if pd.notna(x) else pd.NA).astype('string')


#: s16bq01 codes of the seed records (value labels 11-20, all 'Sementes ...',
#: 'Plantas, cordas/caules de tuberculos' or 'Outras sementes').
SEED_CODES = range(11, 21)


def _seed_crop_labels(source_labels, source_codes, seed_crop_map):
    """The crop of each SEED record (code in SEED_CODES): seed_crop_map's label,
    else the seed label as recorded (GH #1119: a recorded but unmapped crop
    passes through).  NA on the non-seed rows; _finish fills those with the
    CROP_NA sentinel."""
    folded = {_fold(k): v for k, v in seed_crop_map.items()}
    lab = source_labels.astype('string')
    seed = pd.to_numeric(source_codes, errors='coerce').isin(list(SEED_CODES)).to_numpy()
    crop = lab.map(lambda x: folded.get(_fold(x), x) if pd.notna(x) else pd.NA).astype('string')
    return crop.where(seed, pd.NA)


def _unit_labels(unit_series, unit_map):
    """Map a unit column through the u table.  Unmapped labels pass
    through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Coerce numeric columns, guarantee the schema column set, fill the
    crop / u index levels with non-null tokens where absent, drop rows with
    no input identity, build the (t, i, input, crop, u) index."""
    for col in ['Quantity', 'Quantity_purchased', 'Quantity_received_as_gift']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    for col in ['Purchased', 'Received_as_gift']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype('boolean')
    for col in ['Unit_purchased', 'Unit_received_as_gift']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype('string')
    df['t'] = t
    df['input'] = df['input'].astype('string')
    if 'crop' not in df.columns:
        df['crop'] = pd.NA
    df['crop'] = df['crop'].astype('string').fillna(CROP_NA)
    # GH #1118 / #1041: a missing unit is the missing-unit sentinel `Unknown`
    # (transformations.U_UNKNOWN, spelled here so the script imports nothing from
    # transformations), never a minted label.  It used to be 'Manquant'; the held
    # file has no applied row with a missing unit, so no served row changes.
    df['u'] = df['u'].astype('string').fillna('Unknown')
    df = df[df['input'].notna()]
    keep = ['t', 'i', 'input', 'crop', 'u',
            'Quantity', 'Purchased', 'Quantity_purchased', 'Unit_purchased',
            'Received_as_gift', 'Quantity_received_as_gift', 'Unit_received_as_gift']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'input', 'crop', 'u'])
    return df


src = get_dataframe('../Data/s16b_me_gnb2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_gnb2018.dta', convert_categoricals=False)

input_map = _safe_map('harmonize_input')        # the three inorganic labels (GH #1107)
unit_map = _safe_map('u')                        # present in GNB
seed_crop_map = _safe_map('harmonize_seed_crop')  # seed label -> crop (GH #1119)

# Keep only inputs the household actually applied (s16bq02 == 1 = Sim).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

# SEED_CODES is read off the value labels; refuse a file whose labels moved.
_seed_rows = pd.to_numeric(srcn['s16bq01'], errors='coerce').isin(list(SEED_CODES)).to_numpy()
_not_seed = {str(x) for x in src.loc[_seed_rows, 's16bq01'].dropna().unique()
             if not any(w in _fold(x).lower() for w in ('sementes', 'plantas'))}
assert not _not_seed, f'plot_inputs 2018-19: seed codes carry non-seed labels {_not_seed}'

hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                     index=['grappe', 'menage'])), axis=1)

# 'Sim' / 'Não' (loaded as labels with convert_categoricals).  s16bq08 is the purchase
# question, s16bq05 the gift question (their variable labels; ruling 2026-11-13).
YES_NO = {'Sim': True, 'Não': False}
purchased = src['s16bq08'].astype('string').map(YES_NO)
gift = src['s16bq05'].astype('string').map(YES_NO)

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['s16bq01'], input_map).values,
    'crop':               _seed_crop_labels(src['s16bq01'], srcn['s16bq01'], seed_crop_map).values,
    'u':                  _unit_labels(src['s16bq03b'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['s16bq03a'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['s16bq09a'], errors='coerce').values,
    'Unit_purchased':     _unit_labels(src['s16bq09b'], unit_map).values,
    'Received_as_gift':   gift.values,
    'Quantity_received_as_gift': pd.to_numeric(srcn['s16bq07a'], errors='coerce').values,
    'Unit_received_as_gift': _unit_labels(src['s16bq07b'], unit_map).values,
})

df = _finish_plot_inputs(df, '2018-19')

assert len(df) > 0, 'plot_inputs 2018-19 produced no rows'
# GH #323: a label table that made two reported line-items share a key would let
# the framework's collapse destroy one.  Fail the build instead (Togo's guard).
assert df.index.is_unique, 'plot_inputs 2018-19: (t, i, input, crop, u) is not unique'
to_parquet(df, 'plot_inputs.parquet')
