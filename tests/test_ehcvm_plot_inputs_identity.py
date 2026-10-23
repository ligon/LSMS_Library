"""GH #1123: keep every native EHCVM input slot (Niger, Senegal, CotedIvoire).

`crop` is an index level of plot_inputs (t, i, input, crop, u) and every seed
label maps to the one input 'Seed', so a non-injective harmonize_seed_crop puts
two native seed slots on one key and core keeps one observed row.  Until
GH #1123 the catch-all slots -- 15 "Semences d'autres céréales", 'Plants/
boutures de tubercules' and 'Autres semences' -- all mapped to 'Autre crop' in
these three countries (Burkina_Faso's equivalent was PR #1189).  Measured on
the held sources: Niger 2018-19 lost 6 lines, Niger 2021-22 3, Senegal 15,
CotedIvoire 9 -- of which 5 (Senegal) and 8 (CotedIvoire) carried EQUAL
payloads, so the loss was silent even to the grain audit.

The fix splits the two named catch-alls (Benin/Togo/Burkina labels) and guards
each wave's declared index before the parquet is written.  NOT covered here:
Niger's ECVMA waves (2011-12, 2014-15), whose collisions are a repeated seed
SLOT naming the same crop -- a different defect, left open (see the ledger).

As in the Burkina/Benin tests, nothing here asserts post-collapse API
uniqueness: core makes that true by construction.  The tests count rows and
compare payloads against the native source lines.
"""
from pathlib import Path
import collections
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root


CEREAL, TUBER, OTHER = 'Autre céréale', 'Autre tubercule', 'Autre crop'
SEED_LABELS = {
    15: "Semences d'autres céréales",
    19: 'Plants/boutures de tubercules',
    20: 'Autres semences',
}
# (country, wave) -> the catch-all s16bq01 codes present in the held source,
# their value labels (2018-19 Niger/CotedIvoire label 19 'Autres semences';
# Niger 2018-19 code 20 has NO value label) and the crop each must serve.
CASES = {
    ('Niger', '2018-19'): {
        'source': 's16b_me_ner2018.dta',
        'labels': {15: SEED_LABELS[15], 19: 'Autres semences', 20: 20},
        'crops': {15: CEREAL, 19: OTHER, 20: OTHER},
        'colliding': [15, 19]},
    ('Niger', '2021-22'): {
        'source': 's16b_me_ner2021.dta',
        'labels': dict(SEED_LABELS),
        'crops': {15: CEREAL, 19: TUBER, 20: OTHER},
        'colliding': [15, 19, 20]},
    ('Senegal', '2018-19'): {
        'source': 's16b_me_sen2018.dta',
        'labels': dict(SEED_LABELS),
        'crops': {15: CEREAL, 19: TUBER, 20: OTHER},
        'colliding': [15, 19, 20]},
    ('CotedIvoire', '2018-19'): {
        'source': 'Menage/s16b_me_CIV2018.dta',
        'labels': {15: SEED_LABELS[15], 19: 'Autres semences'},
        'crops': {15: CEREAL, 19: OTHER},
        'colliding': [15, 19]},
}
IDS = [f'{c}-{w}' for c, w in CASES]

# Applied native lines, before -> after (development 9a0ed5f3c -> this fix).
# Niger 2021-22 also has 4 lines whose whole payload is NA; _finalize_result's
# dropna(how='all') removes them, as it did before (not a collapse).
SERVED = {('Niger', '2018-19'): 13401, ('Niger', '2021-22'): 12435,
          ('Senegal', '2018-19'): 9916, ('CotedIvoire', '2018-19'): 11259}
HOLLOW = {('Niger', '2021-22'): 4}


def _seed_map(country):
    table = df_from_orgfile(countries_root() / country / '_' / 'categorical_mapping.org',
                            name='harmonize_seed_crop')
    return table.set_index('Original Label')['Preferred Label'].to_dict()


@pytest.mark.parametrize('country,wave', list(CASES), ids=IDS)
def test_present_catchall_slots_map_to_their_expected_crops(country, wave):
    seed = _seed_map(country)
    case = CASES[(country, wave)]
    got = {code: seed[str(label)] for code, label in case['labels'].items()}
    assert got == case['crops']
    colliding = [got[c] for c in case['colliding']]
    assert len(set(colliding)) == len(colliding), (
        f'{country} {wave}: the seed slots that used to collide share a crop: '
        f'{dict(zip(case["colliding"], colliding))}')


def _run_script(monkeypatch, country, wave, codes, *, seed_override=None):
    """Run the ACTUAL wave script on source-shaped lines, spying on the write."""
    case = CASES[(country, wave)]
    n = len(codes)
    numeric = pd.DataFrame({
        'vague': [1] * n, 'grappe': [7] * n, 'menage': [3] * n,
        's16bq01': codes, 's16bq02': [1] * n,
        's16bq03a': [1.25] * n, 's16bq03b': [2] * n,
        's16bq05': [2] * n, 's16bq07a': [np.nan] * n,
    })
    labelled = numeric.copy()
    labelled['s16bq01'] = labelled['s16bq01'].map(case['labels'])
    labelled['s16bq03b'] = 'Kilogramme'
    labelled['s16bq05'] = 'Non'
    real_read = local_tools.get_categorical_mapping
    written = []

    def mapping(*args, tablename=None, **kwargs):
        out = real_read(*args, tablename=tablename, **kwargs)
        if tablename == 'harmonize_seed_crop' and seed_override:
            out = {**out, **seed_override}
        return out

    def source(filename, convert_categoricals=True, **kwargs):
        assert Path(filename).name == Path(case['source']).name
        return (labelled if convert_categoricals else numeric).copy()

    root = countries_root() / country
    monkeypatch.setattr(local_tools, 'get_dataframe', source)
    monkeypatch.setattr(local_tools, 'get_categorical_mapping', mapping)
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, *a, **k: written.append(frame.copy()))
    monkeypatch.chdir(root / wave / '_')
    monkeypatch.syspath_prepend(str(root / '_'))
    monkeypatch.delitem(sys.modules, country.lower(), raising=False)
    try:
        runpy.run_path(str(root / wave / '_' / 'plot_inputs.py'),
                       run_name='__gh1123_regression__')
    except AssertionError:
        assert not written, 'uniqueness must be checked before the parquet write'
        raise
    assert len(written) == 1
    return written[0]


@pytest.mark.parametrize('country,wave', list(CASES), ids=IDS)
def test_equal_payloads_on_distinct_catchall_slots_all_survive(monkeypatch, country, wave):
    case = CASES[(country, wave)]
    codes = case['colliding']
    out = _run_script(monkeypatch, country, wave, codes)
    assert len(out) == len(codes)
    assert out.index.is_unique
    assert sorted(out.index.get_level_values('crop')) == sorted(case['crops'][c] for c in codes)
    assert set(out.index.get_level_values('input')) == {'Seed'}
    assert out['Quantity'].tolist() == [1.25] * len(codes)


@pytest.mark.parametrize('country,wave', list(CASES), ids=IDS)
def test_the_pre_fix_catchall_fails_before_writing(monkeypatch, country, wave):
    case = CASES[(country, wave)]
    lossy = {str(case['labels'][c]): OTHER for c in case['colliding']}
    with pytest.raises(AssertionError, match=f'{country} plot_inputs {wave}:'):
        _run_script(monkeypatch, country, wave, case['colliding'], seed_override=lossy)


@pytest.mark.parametrize('country,wave', list(CASES), ids=IDS)
def test_a_repeated_native_key_fails_before_writing_and_names_no_household(
        monkeypatch, country, wave):
    with pytest.raises(AssertionError, match=r'1 duplicate tuple\(s\)') as error:
        _run_script(monkeypatch, country, wave, [15, 15])
    # The production message is aggregate-only: no household id, no source key.
    assert _HHID[country](7, 3) not in str(error.value)


# ---------------------------------------------------------------------------
# Real held source vs the API (cache-gated)
# ---------------------------------------------------------------------------

_INPUTS = {1: 'Organic manure', 2: 'Organic compost', 3: 'Urea', 4: 'Phosphate',
           5: 'NPK', 6: 'DAP', 7: 'Pesticide', 8: 'Herbicide', 9: 'Fungicide',
           10: 'Other phytosanitary'}
_NAMED = {11: 'Mil', 12: 'Sorgho', 13: 'Maïs en grain', 14: 'Riz Paddy',
          16: 'Coton', 17: 'Sésame', 18: 'Niébé/Haricots secs'}
_HHID = {
    'Niger': lambda g, m: 'E_' + local_tools.format_id(g) + '0' + local_tools.format_id(m, zeropadding=2),
    'Senegal': lambda g, m: local_tools.format_id(g) + '0' + local_tools.format_id(m, zeropadding=2),
    'CotedIvoire': lambda g, m: local_tools.format_id(g) + local_tools.format_id(m, zeropadding=3),
}


def _expected_input_crop(country, wave, code):
    code = int(code)
    if code in _INPUTS:
        return _INPUTS[code], '(not crop-specific)'
    if code in _NAMED:
        return 'Seed', _NAMED[code]
    if country == 'Senegal' and code == 21:
        return 'Seed', 'Arachide'
    return 'Seed', CASES[(country, wave)]['crops'][code]


@pytest.fixture(scope='module')
def built():
    cache = {}

    def get(country):
        if country not in cache:
            cache[country] = Country(country).plot_inputs()
        return cache[country]
    return get


@pytest.mark.requires_s3
@pytest.mark.parametrize('country,wave', list(CASES), ids=IDS)
def test_every_real_native_line_and_payload_is_served(built, country, wave):
    """Every applied (household, s16bq01) line of the held source is served,
    on the expected (input, crop), with its exact Quantity / Purchased /
    Quantity_purchased (the existing API bindings, s16bq03a / s16bq05 /
    s16bq07a).  Multisets per (i, input, crop), so Niger 2018-19's two
    'Autre crop' slots (19 and the unlabelled 20) are each counted."""
    case = CASES[(country, wave)]
    path = countries_root() / country / wave / 'Data' / case['source']
    raw = local_tools.get_dataframe(path, convert_categoricals=False)
    raw = raw.loc[raw['s16bq02'] == 1]
    native = ['grappe', 'menage', 's16bq01']
    assert not raw[native].isna().any(axis=None)
    assert not raw.duplicated(native).any()
    qty = pd.to_numeric(raw['s16bq03a'], errors='coerce')
    if (country, wave) == ('Niger', '2021-22'):
        qty = qty.mask(qty == 9999)          # GH #865 sentinel, as the script does
    purchased = raw['s16bq05'].map({1: True, 2: False})
    qpur = pd.to_numeric(raw['s16bq07a'], errors='coerce')
    keep = ~(qty.isna() & purchased.isna() & qpur.isna())
    assert int((~keep).sum()) == HOLLOW.get((country, wave), 0)
    ic = [_expected_input_crop(country, wave, c) for c in raw['s16bq01']]
    expected = collections.Counter(
        (_HHID[country](g, m), inp, crop, _f(q), _b(p), _f(qp))
        for g, m, (inp, crop), q, p, qp, k in zip(
            raw['grappe'], raw['menage'], ic, qty, purchased, qpur, keep) if k)

    api = built(country)
    api = api.loc[api.index.get_level_values('t') == wave].reset_index()
    assert len(api) == SERVED[(country, wave)] == sum(expected.values())
    got = collections.Counter(
        (i, inp, crop, _f(q), _b(p), _f(qp))
        for i, inp, crop, q, p, qp in zip(api['i'], api['input'], api['crop'], api['Quantity'],
                                          api['Purchased'], api['Quantity_purchased']))
    assert got == expected


def _f(x):
    return None if pd.isna(x) else float(x)


def _b(x):
    return None if pd.isna(x) else bool(x)
