"""New Module U coverage preserves measured roles, ranks and interview grain."""
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import data_root


WAVE = '2024-25'
TABLE = 'shocks'
IHS_U = 'hh_mod_u.dta'
IHPS_U = 'hh_mod_u_24.dta'
COVER = 'hh_mod_a_filt_24.dta'
INDEX = ['t', 'i', 'Shock']
COLUMNS = [
    'AffectedIncome', 'AffectedAssets', 'AffectedProduction',
    'AffectedConsumption', 'AffectedFoodStock',
    'HowCoped0', 'HowCoped1', 'HowCoped2',
]
NATIVE = [
    'hh_u0a', 'hh_u03a', 'hh_u03b', 'hh_u03c', 'hh_u03d', 'hh_u03e',
    'hh_u04a', 'hh_u04b', 'hh_u04c',
]
NOTHING = 'DID NOT DO ANYTHING'
OTHER = 'OTHER (SPECIFY)'
HELP = 'RECEIVED HELP'
SAVED = 'RELIED ON SAVINGS'


@pytest.fixture
def modules(tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    data_root.cache_clear()
    empty = ['Floods', *[None] * 8]
    inputs = {
        IHS_U: pd.DataFrame([
            ['shared', 'Drought', 'Increase', 'Decrease', None,
             'Did Not Change', 'Decrease', SAVED, NOTHING, HELP],
            ['shared', *empty],
            ['retained-key', 'Drought', None, None, None, 'Increase',
             'Did Not Change', NOTHING, None, None],
        ], columns=['case_id', *NATIVE]),
        IHPS_U: pd.DataFrame([
            ['retained-key', 'Drought', None, None, 'Decrease', 'Increase',
             None, OTHER, None, SAVED],
            ['retained-key', *empty],
            # Different native order and a different coded response are both
            # allowed. The IHS6 row survives whole, not merged slot by slot.
            ['refresh-key', *empty],
            ['refresh-key', 'Drought', 'Increase', 'Decrease', None,
             'Did Not Change', 'Decrease', OTHER, NOTHING, HELP],
        ], columns=['y5_hhid', *NATIVE]),
        COVER: pd.DataFrame({
            'y5_hhid': ['refresh-key', 'retained-key'],
            'y4_hhid': ['', 'prior-round'],
            # Retained case_id is an older baseline key, even when it equals
            # today's IHS6 shared case_id. Only refresh accounts for overlap.
            'case_id': ['shared', 'shared'],
        }),
    }
    reads = []

    def read(fn, **kwargs):
        name = Path(fn).name
        if name not in inputs:
            raise FileNotFoundError(name)
        reads.append(name)
        return inputs[name].copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    yield Country('Malawi')[WAVE], inputs, reads
    data_root.cache_clear()


def test_public_effects_coping_and_component_precedence(modules):
    wave, _, reads = modules
    built = wave.grab_data(TABLE)
    assert list(built.index.names) == INDEX
    assert built.index.is_unique
    assert list(built.columns) == COLUMNS
    assert set(built.index.get_level_values('i')) == {
        'cs-24-shared', 'cs-24-retained-key', 'retained-key',
    }
    assert reads == [IHS_U, IHPS_U, COVER]

    # The raw blank is kept blank through construction; the usual finalizer
    # omits wholly hollow payload. It must not mint a 'None' response.
    blank = (WAVE, 'retained-key', 'Floods')
    assert built.loc[blank].isna().all()
    c = wave.country
    public = c._finalize_result(built.copy(), c.resources['Data Scheme'][TABLE], TABLE)
    assert len(public) == 3
    assert set(public.index.names) == set(INDEX)
    public = public.reorder_levels(INDEX)
    assert blank not in public.index
    shared = public.loc[(WAVE, 'cs-24-shared', 'Drought')]
    retained = public.loc[(WAVE, 'retained-key', 'Drought')]
    ihs6_only = public.loc[(WAVE, 'cs-24-retained-key', 'Drought')]
    assert shared.AffectedConsumption and not shared.AffectedFoodStock
    assert not ihs6_only.AffectedConsumption and ihs6_only.AffectedFoodStock
    assert pd.isna(retained.AffectedConsumption) and retained.AffectedFoodStock
    assert shared.AffectedIncome and shared.AffectedAssets
    assert pd.isna(shared.AffectedProduction) and retained.AffectedProduction
    assert shared.HowCoped0 == SAVED
    assert shared.HowCoped1 == NOTHING
    assert shared.HowCoped2 == HELP
    assert shared.HowCopedSet == '+'.join(sorted([SAVED, HELP]))
    assert retained.HowCoped0 == OTHER
    assert pd.isna(retained.HowCoped1)
    assert retained.HowCoped2 == SAVED
    assert retained.HowCopedSet == '+'.join(sorted([OTHER, SAVED]))
    assert ihs6_only.HowCopedSet == NOTHING
    pd.testing.assert_frame_equal(wave.grab_data(TABLE), built)
    assert reads == [IHS_U, IHPS_U, COVER]


@pytest.mark.parametrize('file', [IHS_U, IHPS_U])
def test_rejects_duplicate_or_multiplied_keys(modules, file):
    wave, inputs, _ = modules
    inputs[file] = pd.concat([inputs[file], inputs[file].iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match='missing or duplicate household-shock keys'):
        wave.grab_data(TABLE)


def test_rejects_shared_grid_mismatch_even_for_blank_row(modules):
    wave, inputs, _ = modules
    inputs[IHPS_U] = inputs[IHPS_U].drop(index=2)
    with pytest.raises(ValueError, match='disagree on shared shock keys'):
        wave.grab_data(TABLE)


def test_rejects_module_without_its_panel_cover_identity(modules):
    wave, inputs, _ = modules
    inputs[COVER] = inputs[COVER].iloc[:1]
    with pytest.raises(ValueError, match='disagree on household identity'):
        wave.grab_data(TABLE)


def test_rejects_cover_interview_without_its_module_grid(modules):
    wave, inputs, _ = modules
    inputs[IHPS_U] = inputs[IHPS_U].iloc[2:]
    with pytest.raises(ValueError, match='missing or duplicate household-shock keys'):
        wave.grab_data(TABLE)


def test_rejects_missing_cover_data(modules):
    wave, inputs, _ = modules
    del inputs[COVER]
    with pytest.warns(UserWarning, match='could not load sub-df'):
        with pytest.raises(ValueError, match='require the IHPS panel cover'):
            wave.grab_data(TABLE)


@pytest.mark.parametrize('provider', [IHS_U, IHPS_U, COVER])
def test_declared_sources_are_shocks_cache_inputs(modules, monkeypatch, provider):
    from lsms_library import country as country_module

    wave, _, _ = modules
    original = country_module.source_fingerprint
    wave_before = wave._input_hash(TABLE)
    country_before = wave.country._table_cache_hash(TABLE, wave.country.waves)
    sample_before = wave.country._table_cache_hash('sample', wave.country.waves)

    def revised(path):
        fingerprint = original(path)
        return fingerprint + '-revised' if Path(path).name == provider else fingerprint

    monkeypatch.setattr(country_module, 'source_fingerprint', revised)
    assert wave._input_hash(TABLE) != wave_before
    assert wave.country._table_cache_hash(TABLE, wave.country.waves) != country_before
    if provider != COVER:
        assert wave.country._table_cache_hash('sample', wave.country.waves) == sample_before
