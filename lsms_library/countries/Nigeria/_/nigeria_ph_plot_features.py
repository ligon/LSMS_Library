"""Own-source W1–W3 PH plot reports (GH587), distinct from the PP cards.

The admitted sources are PH A1 exports, not copies of the PP rows. Raw bound
operands and native routing survive even when canonical attributes are unknown.
W4 current-area units and W5 prior-area representation remain separate holds.
"""
from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from lsms_library.local_tools import format_id
from lsms_library.paths import countries_root
from lsms_library.countries.Nigeria._.nigeria import (
    HECTARES_PER_ACRE, SQM_PER_HECTARE, PP_QUARTER,
)

# Exact own PH sidecars and independently sealed typed header dictionaries.
# A new release must be reviewed before these dictionaries can be applied.
_SOURCES = {'2010-11': {'t': '2011Q1',
             'role': 'W1',
             'path': '2010-11/Data/Post Harvest Wave 1/Agriculture/secta1_harvestw1.dta',
             'md5': '79dd6900d209b11330d952d5588d4ed1',
             'bytes': 1289303,
             'header_sha256': '614e547c00ae71b204c11c515a8970d58db5d2f48153c47a701d50396f499e28',
             'raw_columns': ['hhid',
                             'plotid',
                             'sa1q3',
                             'sa1q4',
                             'sa1q5a',
                             'sa1q7a',
                             'sa1q9a',
                             'sa1q9b',
                             'sa1q9c',
                             'sa1q9d',
                             'sa1q12'],
             'estimate': 'sa1q9a',
             'unit': 'sa1q9b',
             'gps': 'sa1q9d',
             'other_unit': 'sa1q9c',
             'acquire': 'sa1q12',
             'unit_labels': {1: 'heaps',
                             2: 'ridges',
                             3: 'stands',
                             4: 'plots',
                             5: 'acres',
                             6: 'hectares',
                             7: 'square metres',
                             8: 'other specify'},
             'acquire_labels': {1: 'outright purchase',
                                2: 'rented for cash or inkind goods from other household',
                                3: 'used free of charge',
                                4: 'distributed by community or family',
                                5: 'family/inheritance'}},
 '2012-13': {'t': '2013Q1',
             'role': 'W2',
             'path': '2012-13/Data/Post Harvest Wave 2/Agriculture/secta1_harvestw2.dta',
             'md5': '4eafe18762b5c2c6ef7b64c307388974',
             'bytes': 1118612,
             'header_sha256': '1f0f58968c367f465a620401f380461b8432d5be75f4531c9d6013a3901287ab',
             'raw_columns': ['hhid',
                             'plotid',
                             'sa1q3',
                             'sa1q4',
                             'sa1q5',
                             'sa1q7',
                             'sa1q9a',
                             'sa1q9b',
                             'sa1q9c',
                             'sa1q12'],
             'estimate': 'sa1q9a',
             'unit': 'sa1q9b',
             'gps': 'sa1q9c',
             'other_unit': None,
             'acquire': 'sa1q12',
             'unit_labels': {1: 'HEAPS',
                             2: 'RIDGES',
                             3: 'STANDS',
                             4: 'PLOTS',
                             5: 'ACRES',
                             6: 'HECTARES',
                             7: 'SQUARE METRES'},
             'acquire_labels': {1: 'OUTRIGHT PURCHASE',
                                2: 'RENTED FOR CASH OR INKIND GOODS FROM OTHER HOUSEHOLD',
                                3: 'USED FREE OF CHARGE',
                                4: 'DISTRIBUTED BY COMMUNITY OR FAMILY',
                                5: 'FAMILY/INHERITANCE'}},
 '2015-16': {'t': '2016Q1',
             'role': 'W3',
             'path': '2015-16/Data/secta1_harvestw3.dta',
             'md5': 'a010e45cc651a983d420553e951f870d',
             'bytes': 1023413,
             'header_sha256': 'a201c5e5e0186f110efa6a80d1b73359f4b69f99871e51345d4937d13ecbfe68',
             'raw_columns': ['hhid',
                             'plotid',
                             'sa1q1a',
                             'sa1q3',
                             'sa1q4',
                             'sa1q4b',
                             'sa1q5',
                             'sa1q7',
                             'sa1q9a',
                             'sa1q9b',
                             'sa1q9c',
                             'sa1q10a',
                             'sa1q10b',
                             'sa1q10c',
                             'sa1q12'],
             'estimate': 'sa1q9a',
             'unit': 'sa1q9b',
             'gps': 'sa1q9c',
             'other_unit': None,
             'acquire': 'sa1q12',
             'unit_labels': {1: '1. HEAPS',
                             2: '2. RIDGES',
                             3: '3. STANDS',
                             4: '4. PLOTS',
                             5: '5. ACRES',
                             6: '6. HECTARES',
                             7: '7. SQUARE METRES'},
             'acquire_labels': {1: '1. OUTRIGHT PURCHASE',
                                2: '2. RENTED FOR CASH OR INKIND GOODS FROM OTHER HOUSEHOLD',
                                3: '3. USED FREE OF CHARGE',
                                4: '4. DISTRIBUTED BY COMMUNITY OR FAMILY',
                                5: '5. FAMILY/INHERITANCE'}}}


def ph_plot_source(folder):
    """Return an isolated copy of this source's admitted binding/cards."""
    return deepcopy(_SOURCES[folder])


def admit_ph_plot_source(folder, filename):
    """Refuse an unreviewed path/sidecar before the normal source getter."""
    spec = _SOURCES[folder]
    expected = Path(countries_root()) / 'Nigeria' / spec['path']
    if Path(filename).resolve() != expected.resolve():
        raise RuntimeError('Nigeria PH plot_features: source path is not admitted')
    sidecar = Path(str(expected) + '.dvc')
    declaration = yaml.safe_load(sidecar.read_text(encoding='utf-8'))
    outs = declaration.get('outs') if isinstance(declaration, dict) else None
    if not isinstance(outs, list) or len(outs) != 1 or not isinstance(outs[0], dict):
        raise RuntimeError('Nigeria PH plot_features: invalid source declaration')
    out = outs[0]
    if (out.get('md5') != spec['md5'] or out.get('size') != spec['bytes']
            or out.get('path') != expected.name):
        raise RuntimeError('Nigeria PH plot_features: source version needs dictionary review')


def _raw_operand(series):
    # Nullable storage prevents PP's absent optional cells from converting an
    # integer native code into a float or mixing W1 text with W2 GPS values.
    if pd.api.types.is_integer_dtype(series.dtype):
        return pd.array(series, dtype=str(series.dtype).capitalize())
    if pd.api.types.is_float_dtype(series.dtype):
        return pd.array(series, dtype=str(series.dtype).capitalize())
    if not all(isinstance(v, str) for v in series.dropna()):
        raise TypeError('Nigeria PH plot_features: unexpected native operand type')
    return pd.array(series, dtype='string')


def ph_plot_features_for_wave(folder, raw):
    """Preserve each PH plot record with its own area/card/routing operands.

    No current-access/new-plot filter, PP join, dedup, ID walk or local unit
    factor. Only the existing metric area policy is used. Family/inheritance
    and community-or-family remain their literal native reports; neither is
    forced into a narrower canonical tenure category.
    """
    spec = _SOURCES[folder]
    missing = set(spec['raw_columns']) - set(raw.columns)
    if missing:
        raise KeyError('Nigeria PH plot_features: missing admitted operand columns')
    for key in ['hhid', 'plotid']:
        values = raw[key]
        number = pd.to_numeric(values, errors='coerce')
        if (values.map(lambda v: isinstance(v, (bool, np.bool_))).any()
                or number.isna().any() or not np.isfinite(number).all()
                or not number.eq(np.floor(number)).all()):
            raise ValueError('Nigeria PH plot_features: incomplete or nonintegral native key')
    index = pd.MultiIndex.from_arrays(
        [[spec['t']] * len(raw), raw['hhid'].map(format_id), raw['plotid'].map(format_id)],
        names=['t', 'i', 'plot_id'])
    if not index.is_unique:
        raise ValueError('Nigeria PH plot_features: duplicate normalized plot key')

    number = lambda name: pd.to_numeric(raw[name], errors='coerce').astype('Float64')
    unit = number(spec['unit'])
    gps = number(spec['gps'])
    estimate = number(spec['estimate'])
    factor = unit.map({5: HECTARES_PER_ACRE, 6: 1.0, 7: 1.0 / SQM_PER_HECTARE})
    area = (gps / SQM_PER_HECTARE).where(gps.notna(), estimate * factor)
    # Existing PP metric plausibility policy; the raw operands remain intact.
    area = area.where((area <= 1000) | area.isna(), pd.NA)
    units = {1: 'heaps', 2: 'ridges', 3: 'stands', 4: 'plots', 5: 'acres',
             6: 'hectares', 7: 'square metres'}
    if 8 in spec['unit_labels']:
        units[8] = 'other specify'  # PH W1: never PP's square-foot code8.
    acquire = number(spec['acquire'])
    # Only purchase/rent establishes these canonical relationships. Free use
    # can concern a private plot; community-or-family and family/inheritance
    # likewise do not establish a narrower ownership category.
    tenure = acquire.map({1: 'owned', 2: 'rented_in'})
    out = pd.DataFrame(index=index)
    out['Area'] = pd.array(area, dtype='Float64')
    out['AreaUnit'] = pd.array(unit.map(units), dtype='string')
    out['Tenure'] = pd.array(tenure, dtype='string')
    for name in ['TenureSystem', 'SoilType']:
        out[name] = pd.array([pd.NA] * len(raw), dtype='string')
    for name in ['Irrigated', 'PlotCertificate', 'ErosionProtection', 'Fallow']:
        out[name] = pd.array([pd.NA] * len(raw), dtype='boolean')
    out['PlotSlope'] = pd.array([pd.NA] * len(raw), dtype='Float64')
    out['PHReportedAreaUnit'] = pd.array(unit.map(spec['unit_labels']), dtype='string')
    out['PHReportedAcquisition'] = pd.array(acquire.map(spec['acquire_labels']), dtype='string')
    for name in spec['raw_columns']:
        out[f'NativePH_{spec["role"]}_{name}'] = _raw_operand(raw[name])
    out.attrs = deepcopy(raw.attrs)
    return out


def append_ph_plot_features(pp, raw, folder):
    """Append this source's PH rows; retain the existing PP block unchanged."""
    ph = ph_plot_features_for_wave(folder, raw)
    if pp.index.names != ph.index.names or not pp.index.is_unique:
        raise ValueError('Nigeria PH plot_features: incompatible PP plot index')
    if set(pp.index.get_level_values('t')) - {PP_QUARTER[folder]}:
        raise ValueError('Nigeria PH plot_features: unexpected PP quarter')
    return pd.concat([pp, ph])
