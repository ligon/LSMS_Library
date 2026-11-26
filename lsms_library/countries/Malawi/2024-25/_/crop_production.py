"""Reported rainy-season harvest (G13) and household-crop sales (I).

Use the IHS6 interview for shared refreshment households and keep the
distinct retained IHPS interviews. This is not dry-season or tree coverage.
See the source and provenance boundaries in ledger 1724-malawi-modern-rainy-crop.
"""
import sys

import pandas as pd

from lsms_library.local_tools import format_id, get_dataframe, to_parquet

sys.path.append('../../_/')
from malawi import (CS_24, _farm_level_sales, _harvest_block, _sale_block,
                    assemble_crop_production, ihps24_refreshed)


WAVE = '2024-25'
# Native 8507/8520 G and I agree on these codes. Unlike historical Other,
# modern code 50 names cowpea directly and does not carry a text derivation.
_MODERN_CROPS = {
    48: 'Other (Specify) 1',
    49: 'Other (Specify) 2',
    50: 'Cowpea (Khobwe)',
}
_GRAIN = ['t', 'i', 'plot_id', 'crop', 'u', 'condition', 'production_obs']
_HARVEST_KEY = ['hhid', 'gardenid', 'plotid', 'crop_code']
_SALE_KEY = ['hhid', 'crop_code']


def _ids(frame, field):
    return frame[field].map(format_id).astype('string')


def _cover_ids(frame, field):
    ids = _ids(frame, field)
    if ids.isna().any() or not ids.is_unique:
        raise ValueError('Malawi 2024-25 crop_production has a missing or repeated cover key')
    return ids


def _select(frame, field, own_cover, *, omit=(), prefix=''):
    ids = _ids(frame, field)
    if ids.isna().any() or not ids.isin(own_cover).all():
        raise ValueError('Malawi 2024-25 crop_production has a record outside its own cover')
    keep = ~ids.isin(omit)
    # Omit the full IHPS copy. Missing values in IHS6 are not filled from
    # the panel, and harvest and sales use the same interview precedence.
    return frame.loc[keep].assign(hhid=prefix + ids.loc[keep])


def _native_key(frame, fields):
    keys = pd.DataFrame({field: _ids(frame, field) for field in fields})
    if keys.isna().any(axis=None) or keys.duplicated().any():
        raise ValueError('Malawi 2024-25 crop_production has a missing or repeated native key')
    return keys


def _label_modern_crops(frame):
    modern = frame['_crop_code'].map(_MODERN_CROPS).astype('string')
    for col in ('crop', 'crop_variety'):
        frame[col] = modern.where(modern.notna(), frame[col].astype('string'))
    return frame


def _harvest(frame):
    keys = _native_key(frame, _HARVEST_KEY)
    frame = frame.assign(plotkey=keys['gardenid'] + '_' + keys['plotid'])
    out = _harvest_block(
        frame, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
        qty='ag_g13a', unit='ag_g13b', condition='ag_g13c', plant_m='ag_g05a',
        plant_y='ag_g05b', harv_m='ag_g12b', intercrop='ag_g01', t=WAVE,
    )
    out = _label_modern_crops(out)
    if len(out) != len(frame) or out['crop'].isna().any():
        raise ValueError('Malawi 2024-25 crop_production has an unlabelled harvest record')
    return out


def _sales(frame):
    qty = pd.to_numeric(frame['ag_i02a'], errors='raise')
    value = pd.to_numeric(frame['ag_i03'], errors='raise')
    positive = qty.gt(0).fillna(False) | value.gt(0).fillna(False)
    frame = frame.loc[positive]
    if not pd.to_numeric(frame['ag_i01'], errors='raise').eq(1).fillna(False).all():
        raise ValueError('Malawi 2024-25 crop_production has a positive I record without Sale Yes')
    _native_key(frame, _SALE_KEY)
    sale = _sale_block(
        frame, hhid='hhid', cropcode='crop_code', sold_flag='ag_i01',
        qty_sold='ag_i02a', value_sold='ag_i03', unit_sold='ag_i02b',
        condition='ag_i02c',
    )
    # Use the established Farm-level shape and native quantity/value masks.
    # No plot-level attachment was attempted, so discard the old ladder's
    # disposition tally. The constructor's unset outcome adds no derivation.
    farm, _ = _farm_level_sales(WAVE, sale.assign(outcome=pd.NA))
    if len(farm) != len(frame):
        raise ValueError('Malawi 2024-25 crop_production lost a positive I record')
    farm = _label_modern_crops(farm)
    return farm.assign(t=WAVE, Derivation=pd.Series(pd.NA, index=farm.index, dtype='string'))


def build_crop_production():
    xs_cover = get_dataframe('../Data/Cross_Sectional/hh_mod_a_filt.dta', convert_categoricals=False)
    pn_cover = get_dataframe('../Data/Panel/hh_mod_a_filt_24.dta', convert_categoricals=False)
    xs_ids = _cover_ids(xs_cover, 'case_id')
    pn_ids = _cover_ids(pn_cover, 'y5_hhid')
    refresh = ihps24_refreshed()
    blank_previous = _ids(pn_cover, 'y4_hhid').isna()
    shared = _ids(pn_cover.loc[blank_previous], 'case_id')
    if (shared.isna().any() or not shared.is_unique or not shared.isin(xs_ids).all()
            or refresh != dict(zip(pn_ids.loc[blank_previous], shared))
            or (CS_24 + xs_ids).isin(pn_ids).any()):
        raise ValueError('Malawi 2024-25 crop_production has an invalid refreshment cover bridge')

    xs_g = get_dataframe('../Data/Cross_Sectional/ag_mod_g.dta', convert_categoricals=False)
    pn_g = get_dataframe('../Data/Panel/ag_mod_g_24.dta', convert_categoricals=False)
    xs_i = get_dataframe('../Data/Cross_Sectional/ag_mod_i.dta', convert_categoricals=False)
    pn_i = get_dataframe('../Data/Panel/ag_mod_i_24.dta', convert_categoricals=False)
    harvest = [
        _harvest(_select(xs_g, 'case_id', xs_ids, prefix=CS_24)),
        _harvest(_select(pn_g, 'y5_hhid', pn_ids, omit=refresh)),
    ]
    plot_rows = assemble_crop_production(WAVE, harvest, []).reset_index()
    sales = [
        _sales(_select(xs_i, 'case_id', xs_ids, prefix=CS_24)),
        _sales(_select(pn_i, 'y5_hhid', pn_ids, omit=refresh)),
    ]
    # Nothing about a modern Farm-level record is a sale-ladder decision.
    # Country's historical diagnostics are left unchanged.
    plot_rows.attrs = {}
    out = pd.concat([plot_rows, *(s[plot_rows.columns] for s in sales)], ignore_index=True)
    if len(plot_rows) != sum(map(len, harvest)) or out.duplicated(_GRAIN).any():
        raise ValueError('Malawi 2024-25 crop_production changed the native record grain')
    return out.set_index(['t', 'i', 'plot_id', 'crop'])


if __name__ == '__main__':
    to_parquet(build_crop_production(), 'crop_production.parquet')
