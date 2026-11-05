"""Household-crop sales the single-plot rule leaves out (GH #1457).

Used by ``2019-20/_/crop_production.py`` and ``2020-21/_/crop_production.py``, after
``tanzania.crop_production_for_wave``, and named by ``_/derivations.yml``.  It is its
own module because ``tests/test_gh905_tanzania_signed_sums.py`` pins the digest of the
whole of ``tanzania.py``, so that file must not change for this.

AG_SEC_5A / 7A / 7B record a sale per household and crop code, each in the sale module
asking about one harvest module (5A -> 4A, 7A -> 6A, 7B -> 6B).  ``tanzania.py`` attaches
one to a crop code on exactly ONE plot of its own module.  On the ruling on GH #843,
option (d), extended in bulk on 2026-11-04, every other sale goes on ONE row with
``plot_id = 'Farm-level'``, no harvest ``Quantity``.  Nothing is split, summed or
dropped.  Sale modules ask no unit: "(KG)" on every sold quantity, so ``Unit_sold`` is
``kg`` by the instrument, and it is what gives the Farm-level row's kilograms a carrier,
because ``u`` here describes the harvest and is NA where there is none.

The 2008-15 folder builds no ``crop_production`` (no agriculture source on disk), so
its four rounds never reach this.
"""
import sys
from pathlib import Path

import pandas as pd

from lsms_library.derivations import union_keys
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

# Loaded by importlib from an absolute path when derivations.yml resolves it, so a
# relative sys.path entry does not find the country module.  As `_upd_district.py`.
_COUNTRY = str(Path(__file__).resolve().parent)
if _COUNTRY not in sys.path:
    sys.path.insert(0, _COUNTRY)
import tanzania

HH_CROP_SALE = 'Tanzania::crop_production::hh-crop-sale-single-row'
FARM_LEVEL = 'Farm-level'
UNIT_SOLD = 'kg'
#: Harvest module key -> its sale module's section id.
SALE_SECTION = {'seasonal': '5A', 'fruit': '7A', 'perennial': '7B'}


def _sale_records(frame, cm, module):
    """One sale module: one row per (i, crop code), as ``tanzania.py`` reads it."""
    out = pd.DataFrame({
        'i': frame[cm['hhid']].apply(format_id).values,
        '_code': pd.to_numeric(frame[cm['crop']], errors='coerce').astype('Int64').values,
        'Quantity_sold': pd.to_numeric(frame[cm['qty']], errors='coerce').astype('Float64').values,
        'Value_sold': pd.to_numeric(frame[cm['value']], errors='coerce').astype('Float64').values,
    }).dropna(subset=['_code'])
    out = (out.groupby(['i', '_code'], dropna=False)[['Quantity_sold', 'Value_sold']]
           .sum(min_count=1).reset_index())
    out['_module'] = tanzania._CROP_MODULES[module]
    out['_section'] = SALE_SECTION[module]
    return out


def _harvest_token(records):
    """The ``production_obs`` of the harvest rows a sale record asks about."""
    return 'reported:' + records['_module'] + ':' + records['_code'].astype('string')


def _is_sale(frame):
    """A sale is a record with a positive sold quantity or a positive value."""
    return (frame['Quantity_sold'].astype('float64').gt(0)
            | frame['Value_sold'].astype('float64').gt(0))


def derive_hh_crop_sale_attachment(df, t, sales, colmaps, crop_labels=None):
    """``df`` with ONE Farm-level row per sale ``tanzania.py`` did not attach.

    ``df`` is what ``crop_production_for_wave`` returned, with its index; ``sales`` and
    ``colmaps`` are what the wave script gave it.  Returns the frame with two new
    columns: ``Unit_sold``, the instrument's ``kg`` on a row with a positive sold
    quantity; and ``Derivation``, :data:`HH_CROP_SALE` on a plot's row holding a sale.
    A Farm-level row's index discloses it, so it carries no key.
    """
    labels = tanzania._crop_labels() if crop_labels is None else crop_labels
    records = pd.concat([_sale_records(sales[k], colmaps[f'sales_{k}'], k)
                         for k in SALE_SECTION if sales.get(k) is not None], ignore_index=True)
    records = records[_is_sale(records)]
    flat = df.reset_index()
    rows = flat.groupby(['i', 'production_obs'], dropna=False).size().rename('_rows').reset_index()
    records = records.assign(production_obs=_harvest_token(records)).merge(
        rows, on=['i', 'production_obs'], how='left')
    alone = records['_rows'].fillna(0).eq(1)

    farm = records.loc[~alone, ['i', 'Quantity_sold', 'Value_sold']].copy()
    code = records.loc[~alone, '_code']
    # The sale's own record, unlike the harvest rows it asks about: never a repeat of
    # a harvest token, and two modules of one code stay apart.
    farm['production_obs'] = ('reported:' + records.loc[~alone, '_section'] + ':'
                              + code.astype('string')).astype('string')
    # As crop_production_for_wave keeps an unmapped code.
    farm['j'] = code.map(labels).astype('string').where(
        code.map(labels).notna(), code.astype('string').radd('crop_'))
    farm['t'] = t
    farm['plot_id'] = FARM_LEVEL
    farm['u'] = pd.Series(pd.NA, index=farm.index, dtype='string')
    farm['Quantity'] = pd.Series(pd.NA, index=farm.index, dtype='Float64')
    farm['harvest_month'] = pd.Series(pd.NA, index=farm.index, dtype='Int64')
    farm['intercropped'] = pd.Series(pd.NA, index=farm.index, dtype='boolean')
    # A property of the module that asks, as on every harvest row.
    farm['perennial'] = records.loc[~alone, '_module'].ne('4A').astype('boolean')

    # Beside any key a row already has; crop_production_for_wave writes none today.
    derivation = (flat['Derivation'].astype('string') if 'Derivation' in flat.columns
                  else pd.Series(pd.NA, index=flat.index, dtype='string')).copy()
    hit = _is_sale(flat).fillna(False).to_numpy(dtype=bool)
    derivation[hit] = pd.array([union_keys((key, HH_CROP_SALE)) for key in derivation[hit]],
                               dtype='string')
    flat['Derivation'] = derivation
    farm['Derivation'] = pd.Series(pd.NA, index=farm.index, dtype='string')

    out = pd.concat([flat, farm[flat.columns]], ignore_index=True)
    out['Unit_sold'] = pd.Series(pd.NA, index=out.index, dtype='string').mask(
        out['Quantity_sold'].astype('float64').gt(0).to_numpy(dtype=bool), UNIT_SOLD)
    out = out.set_index(tanzania.CROP_PRODUCTION_INDEX)
    if not out.index.is_unique:
        raise ValueError(f'crop_production {t}: a sale record repeats a native coordinate')
    # Conservation: every sale record is on exactly one row, unchanged.
    on_a_row = out[_is_sale(out).fillna(False).to_numpy(dtype=bool)]
    if (len(on_a_row) != len(records)
            or on_a_row['Value_sold'].sum() != records['Value_sold'].sum()
            or on_a_row['Quantity_sold'].sum() != records['Quantity_sold'].sum()):
        raise ValueError(f'crop_production {t}: a sale moved or doubled')
    return out


#: Each wave's household id, as the wave scripts name it (they stay the source of truth;
#: a test checks these are named there).
_HOUSEHOLD = {'2019-20': 'sdd_hhid', '2020-21': 'y5_hhid'}
#: The wave's own spelling of a file name: 2020-21 ships them in lower case.
_FILE_NAME = {'2019-20': 'AG_SEC_{}.dta', '2020-21': 'ag_sec_{}.dta'}
_SALE_FILES = {'seasonal': ('5a', 'ag5a_02', 'ag5a_03'),
               'fruit': ('7a', 'ag7a_03', 'ag7a_04'),
               'perennial': ('7b', 'ag7b_03', 'ag7b_04')}
_HARVEST_FILES = {'seasonal': '4a', 'fruit': '6a', 'perennial': '6b'}


def inputs_hh_crop_sale_single_row(wave):
    """The raw sale lines behind one wave's keyed rows, at (t, i, production_obs), under
    their own names, keyed to join the served row of the crop's one plot.  Re-reads the
    sources; nothing is cached."""
    if wave not in _HOUSEHOLD:
        raise ValueError(f'hh-crop-sale-single-row is {sorted(_HOUSEHOLD)}, not {wave!r}')
    hhid = _HOUSEHOLD[wave]
    data = Path(countries_root()) / 'Tanzania' / wave / 'Data'
    pieces = []
    for module, (name, qty, value) in _SALE_FILES.items():
        spell = _FILE_NAME[wave].format
        case = str.upper if wave == '2019-20' else str.lower
        raw = get_dataframe(str(data / spell(case(name))), convert_categoricals=False)
        rows = get_dataframe(str(data / spell(case(_HARVEST_FILES[module]))),
                             convert_categoricals=False)
        section = tanzania._CROP_MODULES[module]
        rows = pd.DataFrame({
            'i': rows[hhid].apply(format_id).values,
            '_code': pd.to_numeric(rows['cropid'], errors='coerce').astype('Int64').values,
        }).dropna(subset=['_code'])
        rows = rows.groupby(['i', '_code']).size().rename('_rows').reset_index()
        out = pd.DataFrame({
            't': wave, 'i': raw[hhid].apply(format_id).values,
            '_code': pd.to_numeric(raw['cropid'], errors='coerce').astype('Int64').values,
            'cropid': raw['cropid'].values, 'section': SALE_SECTION[module],
            'Quantity_sold': pd.to_numeric(raw[qty], errors='coerce').astype('Float64').values,
            'Value_sold': pd.to_numeric(raw[value], errors='coerce').astype('Float64').values,
        }).dropna(subset=['_code'])
        out = out[_is_sale(out).to_numpy(dtype=bool)].merge(rows, on=['i', '_code'], how='left')
        out = out[out['_rows'] == 1].drop(columns='_rows')
        out['production_obs'] = ('reported:' + section + ':' + out['_code'].astype('string')).astype('string')
        pieces.append(out.drop(columns='_code'))
    return pd.concat(pieces, ignore_index=True).set_index(['t', 'i', 'production_obs'])
