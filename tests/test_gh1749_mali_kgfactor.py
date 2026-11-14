"""GH #1749: Mali 2017-18 serves ``s7fq13d`` ("Equivalent en Kg") as ``crop_production.KgFactor``.

Ruling (``isa-decisions.org``, 2026-11-13, "Mali 2017-18 s7fq13d, GH1749"): "Okay,
let's wire it."  Q7F13's fourth sub-column is the instrument's kilograms per ONE
harvest unit (the P2 enumerator manual, PDF p. 30, 7F.13: "on écrit le poids en Kg
d’une unité de l’UML reportée"), so it is the canonical ``KgFactor``, the ``reported``
rung of ``transformations.harvest_kg_factors``, as UNPS q6d is for Uganda.  It is
served as recorded (a 0 reads NA), carries no ``Derivation`` key, and goes through
today's ladder unchanged: the reported-factor screen refuses a factor above
``KG_FACTOR_MAX`` and that row falls to ``survey_median``.

The source oracle reads ``eaci17_s7fp2.dta`` through ``get_dataframe`` and the crop
map through ``get_categorical_mapping``; the synthetic tests drive
``mali.crop_production_finalize`` on invented rows.
"""
import ast
import copy
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import country as C, local_tools as L, transformations as T
from lsms_library.build_transforms import GrainConflictWarning
from lsms_library.countries.Mali._ import mali as M

COUNTRY_DIR = Path(M.__file__).resolve().parents[1]
KEY = ('Mali', 'crop_production')
WAVE = '2017-18'
FARM = 'Farm-level'

#: Measured cold at 1a2bd6bd9 (burst jobs 41288942, 41289107; base cd199d4a3 served
#: none 23,160 and metric 1,761).  A movement here is a data change to explain, not a
#: pin to bump.
PARTITION_2017_18 = {'reported': 21039, 'survey_median': 2345, 'none': 1535, 'metric': 2}
REFUSED_2017_18 = 2167
REFUSED_BY_UNIT = {'Charretée': 1581, 'Autre': 534, 'Grenier': 21, 'Sac': 31}


# --- the wiring, statically ------------------------------------------------------

def _kgfactor_entries(wave):
    tree = ast.parse((COUNTRY_DIR / wave / '_' / 'crop_production.py').read_text())
    return [v for n in ast.walk(tree) if isinstance(n, ast.Dict)
            for k, v in zip(n.keys, n.values) if isinstance(k, ast.Constant) and k.value == 'KgFactor']


def test_only_s7fq13d_is_wired_as_the_harvest_factor():
    entry, = _kgfactor_entries(WAVE)
    literals = {n.value for n in ast.walk(entry) if isinstance(n, ast.Constant)}
    assert 's7fq13d' in literals
    # The tree "Equivalent en Kg" fields stay unread (the sold ones are sometimes a total, GH #824).
    assert not {'s11fq11c', 's11fq14c'} & literals
    # 2014-15's sibling mixes factors and totals; it is not read at all.
    assert _kgfactor_entries('2014-15') == []
    assert 's3aq08c' not in (COUNTRY_DIR / '2014-15' / '_' / 'crop_production.py').read_text()


def test_kgfactor_is_declared_optional():
    import yaml

    class _SchemeLoader(yaml.SafeLoader):   # the scheme carries `!make` tags
        pass
    _SchemeLoader.add_multi_constructor('!', lambda loader, suffix, node: None)
    scheme = yaml.load((COUNTRY_DIR / '_' / 'data_scheme.yml').read_text(), Loader=_SchemeLoader)
    col = scheme['Data Scheme']['crop_production']['KgFactor']
    assert col == {'type': 'float', 'optional': True}


# --- the fold, on invented rows --------------------------------------------------

@pytest.fixture
def invented(monkeypatch):
    saved = copy.deepcopy(C._GRAIN_LEDGER.get(KEY))
    C._GRAIN_LEDGER.pop(KEY, None)
    monkeypatch.delenv('LSMS_GRAIN_STRICT', raising=False)
    monkeypatch.setattr(M, '_crop_labels', lambda x: x.astype('string'))
    monkeypatch.setattr(M, '_unit_labels', lambda x: x.astype('string'))

    def deny(*a, **kw):
        raise AssertionError('invented factor test attempted source acquisition')
    monkeypatch.setattr(L, '_ensure_dvc_pulled', deny)
    try:
        yield
    finally:
        C._GRAIN_LEDGER.pop(KEY, None)
        if saved is not None:
            C._GRAIN_LEDGER[KEY] = saved


def _rows(factors, *, plots=('1_1', '1_1')):
    return pd.DataFrame({
        't': WAVE, 'i': 'recorded-invented-household', 'plot_id': list(plots),
        'crop': 'Invented millet', 'u': 'Sac', 'Quantity': [3., 4.],
        'KgFactor': factors, 'Quantity_sold': np.nan, 'Value_sold': np.nan,
        'planting_month': pd.NA, 'harvest_month': pd.NA,
        'intercropped': pd.NA, 'perennial': False})


@pytest.mark.parametrize('factors,expected,conflict', [
    ([100., 100.], 100., False),
    ([100., 50.], np.nan, True),
    ([100., np.nan], np.nan, True),      # a missing factor disagrees: the Quantity is summed
    ([np.nan, np.nan], np.nan, False),
])
def test_a_repeated_key_keeps_only_an_agreed_factor(invented, factors, expected, conflict):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = M.crop_production_finalize(_rows(factors))
    assert len(out) == 1 and out['Quantity'].iloc[0] == 7
    got = out['KgFactor'].iloc[0]
    assert (pd.isna(got) and pd.isna(expected)) or got == expected
    assert bool([w for w in caught if issubclass(w.category, GrainConflictWarning)]) == conflict
    reports = C.grain_reports(*KEY)
    assert bool(reports) == conflict
    if conflict:
        report, = reports
        assert report['columns'] == {'KgFactor': 1} and report['kind'] == 'cells_blanked'


def test_distinct_rows_keep_their_own_factor_and_a_wave_without_one_gets_nan(invented):
    out = M.crop_production_finalize(_rows([100., 50.], plots=('1_1', '1_2')))
    assert sorted(out['KgFactor'].tolist()) == [50., 100.]
    bare = M.crop_production_finalize(_rows([1., 1.], plots=('1_1', '1_2')).drop(columns='KgFactor'))
    assert 'KgFactor' in bare.columns and bare['KgFactor'].isna().all()
    assert pd.api.types.is_float_dtype(bare['KgFactor'])
    assert not C.grain_reports(*KEY)


# --- the served table ------------------------------------------------------------

@pytest.fixture(scope='module')
def built():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Country('Mali').crop_production()


def _flat(built):
    flat = built.reset_index()
    flat['t'] = flat['t'].astype(str)
    return flat


@pytest.mark.requires_s3
def test_the_factor_is_on_2017_18_seasonal_rows_only(built):
    flat = _flat(built)
    kf = pd.to_numeric(flat['KgFactor'], errors='coerce')
    assert kf[flat['t'] != WAVE].isna().all()
    farm = flat['plot_id'].astype('string').eq(FARM).fillna(False)
    tree = flat['perennial'].astype('boolean').fillna(False)
    assert kf[farm | tree].isna().all()
    assert (kf.dropna() > 0).all()
    kg = (flat['t'] == WAVE) & flat['u'].astype('string').eq('Kg').fillna(False) & ~farm & ~tree
    assert kg.sum() > 1000 and (kf[kg] == 1).all()


@pytest.mark.requires_s3
def test_the_served_factors_are_the_files_answers(built):
    """The multiset of served factors equals the file's positive s7fq13d on rows the crop map keeps."""
    root = COUNTRY_DIR / WAVE
    s7f = L.get_dataframe(str(root / 'Data' / 'eaci17_s7fp2.dta'))
    crop_map = L.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Code', dirs=[f'{root / "_"}/', f'{COUNTRY_DIR / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})
    kept = s7f['s7fq03'].astype('string').str.strip().map(crop_map).notna()
    d = pd.to_numeric(s7f.loc[kept, 's7fq13d'], errors='coerce')
    flat = _flat(built)
    seasonal = flat[(flat['t'] == WAVE) & ~flat['plot_id'].astype('string').eq(FARM).fillna(False)
                    & ~flat['perennial'].astype('boolean').fillna(False)]
    served = pd.to_numeric(seasonal['KgFactor'], errors='coerce')
    assert len(seasonal) == int(kept.sum())
    assert Counter(served.dropna()) == Counter(d[d > 0])
    assert int(served.isna().sum()) == int((~(d > 0)).sum())


@pytest.mark.requires_s3
def test_no_derivation_key_is_minted_for_the_factor(built):
    flat = _flat(built)
    has = flat['KgFactor'].notna()
    keys = set(flat.loc[has, 'Derivation'].dropna().astype(str))
    assert keys <= {M.DERIVATION_HH_CROP_SALE}, keys


@pytest.mark.requires_s3
def test_the_ladder_reads_it_through_the_unchanged_screen(built):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        fac = T.harvest_kg_factors(built)
    t = built.index.get_level_values('t').astype(str)
    src = fac['KgFactorSource'].astype(str)
    assert not (src[t != WAVE] == 'reported').any()
    assert dict(Counter(src[t == WAVE])) == PARTITION_2017_18
    rej = fac['kg_reported_rejected'].fillna(False).to_numpy(dtype=bool)
    kf = pd.to_numeric(built['KgFactor'], errors='coerce').to_numpy()
    assert int(rej.sum()) == REFUSED_2017_18
    # Every refusal is the 250 kg ceiling (a Kg row is 1, and the year signature is above it).
    assert (kf[rej] > T.KG_FACTOR_MAX).all()
    u = built['u'].astype(str).to_numpy()
    assert dict(Counter(u[rej])) == REFUSED_BY_UNIT
    # A refused row is never left unserved when its unit has a median of accepted factors.
    assert set(src[rej]) <= {'survey_median', 'metric', 'inferred', 'none'}


#: harvest_kg by wave, measured cold (burst job 41289107).  2017-18 was 1,656,240.75 kg
#: on 1,736 plot-crops at base cd199d4a3.  The other three waves are unchanged by the
#: wiring and are pinned as TRIPWIRES: unrelated Mali work that moves them re-pins them
#: with its own measurement.
HARVEST_KG = {'2014-15': 292_888.0, '2017-18': 32_791_070.26,
              '2018-19': 14_881_091.45, '2021-22': 7_062_290.5}


@pytest.mark.requires_s3
def test_harvest_kg_by_wave(built):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        hk = T.harvest_kg(built)
    t = hk.index.get_level_values('t').astype(str)
    got = {w: float(pd.to_numeric(hk['Harvest_kg'])[np.asarray(t == w)].sum()) for w in HARVEST_KG}
    assert got == pytest.approx(HARVEST_KG, rel=1e-9)
    assert int((t == WAVE).sum()) == 23_359
