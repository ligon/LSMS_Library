"""Invented FIELD-factor folds and real parquet audit transport; no survey inputs.

The 2017 fragment is compiled from the actual native script between its sale
constructor and next source read. This tests its fold/gates, not a rewritten
reducer. Compiler transport executes the actual compiler over owned parquets.
"""
import ast
import copy
import hashlib
import runpy
import warnings
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import country as C, local_tools as L, paths
from lsms_library.build_transforms import GrainConflictWarning, reduce_to_agreed
from lsms_library.countries.Mali._ import mali as M

COUNTRY = Path(M.__file__).resolve().parents[1]
KEY = ('Mali', 'crop_production')


@pytest.fixture(autouse=True)
def invented_only(monkeypatch):
    repo = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(repo)
    saved = copy.deepcopy(C._GRAIN_LEDGER.get(KEY))
    C._GRAIN_LEDGER.pop(KEY, None)
    monkeypatch.delenv('LSMS_GRAIN_STRICT', raising=False)
    monkeypatch.delenv('LSMS_READ_STRICT', raising=False)
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


def rows(factors, *, wave='2014-15', plot='1_1'):
    return pd.DataFrame({
        't': wave, 'i': 'recorded-invented-household', 'plot_id': plot,
        'crop': 'Invented maize', 'u': 'Kg', 'Quantity': [10., 20.],
        'Quantity_sold': [2., 3.], 'Unit_sold': 'Bag',
        'KgFactor_sold': factors, 'Value_sold': [100., 200.],
        'planting_month': pd.NA, 'harvest_month': pd.NA,
        'intercropped': pd.NA, 'perennial': False})


@pytest.mark.parametrize('factors,expected,conflict', [
    ([25., 25.], 25., False),
    ([25., 30.], np.nan, True),
    ([25., np.nan], np.nan, True),
    ([np.nan, np.nan], np.nan, False),
    ([25., np.nextafter(25., 26.)], np.nan, True),
])
@pytest.mark.parametrize('plot', ['1_1', pd.NA])
def test_final_fold_blanks_only_conflicted_factor(factors, expected, conflict, plot):
    operand = rows(factors, plot=plot)
    before = operand.copy(deep=True)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        result = M.crop_production_finalize(operand)
    reports = copy.deepcopy(C.grain_reports(*KEY))
    baseline = M.crop_production_finalize(operand.drop(columns='KgFactor_sold'))
    pd.testing.assert_frame_equal(result.drop(columns='KgFactor_sold'),
                                  baseline.drop(columns='KgFactor_sold'), check_exact=True)
    pd.testing.assert_frame_equal(operand, before, check_exact=True)
    assert len(result) == 1 and result.Quantity.iloc[0] == 30
    assert result.Quantity_sold.iloc[0] == 5 and result.Value_sold.iloc[0] == 300
    if pd.isna(expected):
        assert pd.isna(result.KgFactor_sold.iloc[0])
    else:
        assert result.KgFactor_sold.iloc[0] == expected
    assert bool([w for w in caught if issubclass(w.category, GrainConflictWarning)]) == conflict
    assert bool(reports) == conflict
    if conflict:
        report, = reports
        assert report['columns'] == {'KgFactor_sold': 1}
        assert report['kind'] == 'cells_blanked'
        assert report['destroyed'] == report['nan_key_rows'] == 0
        assert report['wave'] == '2014-15'


def sold_fragment(sales, *, plots=('1_1',)):
    path = COUNTRY / '2017-18/_/crop_production.py'
    body = ast.parse(path.read_text()).body
    def assigns(node, name):
        return isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
    start = next(i for i,n in enumerate(body) if assigns(n, 'sold'))
    stop = next(i for i,n in enumerate(body[start:], start) if assigns(n, 's11c'))
    module = ast.fix_missing_locations(ast.Module(body=body[start:stop], type_ignores=[]))
    seasonal = pd.DataFrame({'i': 'h', 'crop': 'Invented maize', 'plot_id': list(plots),
                             'Quantity': 10., 'u': 'Kg', 'Quantity_sold': pd.NA,
                             'Unit_sold': pd.NA, 'Value_sold': pd.NA})
    scope = {'pd': pd, 'WAVE': '2017-18', 's7g': sales,
             'seasonal': seasonal, 'reduce_to_agreed': reduce_to_agreed}
    exec(compile(module, str(path), 'exec'), scope)
    return scope['seasonal']


def sale_rows(factors, *, unit='Bag', answer='Oui', qty=(2., 3.), value=(100., 200.)):
    return pd.DataFrame({'i': 'h', 's7gq01': 'Invented maize', 's7gq20': answer,
                         's7gq21a': list(qty), 's7gq21b': unit,
                         's7gq21d': factors, 's7gq22': list(value)})


@pytest.mark.parametrize('factors,conflict', [([25., 25.], False), ([25., np.nan], True), ([25., 30.], True)])
def test_actual_2017_sale_fold_preserves_amounts(factors, conflict):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', GrainConflictWarning)
        result = sold_fragment(sale_rows(factors))
    assert len(result) == 1 and result.Quantity.iloc[0] == 10
    assert result.Quantity_sold.iloc[0] == 5 and result.Value_sold.iloc[0] == 300
    assert result.Unit_sold.iloc[0] == 'Bag'
    if conflict:
        assert result.KgFactor_sold.isna().all()
    else:
        assert result.KgFactor_sold.iloc[0] == 25
    if conflict:
        report, = C.grain_reports(*KEY)
        assert report['levels'] == ['i', 'crop', 'Unit_sold']
        assert report['columns'] == {'KgFactor_sold': 1} and report['wave'] == '2017-18'


def test_existing_sale_gates_apply_to_new_factor_without_changing_quantities():
    result = sold_fragment(sale_rows([25., 25.]), plots=('1_1', '1_2'))
    assert len(result) == 2 and result.Quantity.eq(10).all()
    assert result[['Quantity_sold','Unit_sold','Value_sold','KgFactor_sold']].isna().all().all()
    no = sold_fragment(sale_rows([25., 25.], answer='Non'))
    assert no.Quantity_sold.iloc[0] == no.Value_sold.iloc[0] == 0
    assert no.Unit_sold.isna().all() and no.KgFactor_sold.isna().all()
    missing = sold_fragment(sale_rows([np.nan, np.nan], qty=(np.nan,np.nan), value=(np.nan,np.nan)))
    assert missing[['Quantity_sold','Value_sold','KgFactor_sold']].isna().all().all()


def test_compiler_relays_real_native_footers_once_and_replay_survives_empty_ledger(tmp_path, monkeypatch):
    data = tmp_path / 'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(COUNTRY.parent))
    paths.data_root.cache_clear(); paths.countries_root.cache_clear(); L._resolve_data_path.cache_clear()
    original_get = L.get_dataframe
    def owned_get(fn, *args, **kwargs):
        path = Path(L._resolve_data_path(str(fn), stack_depth=2))
        assert path.resolve().is_relative_to(data) and path.suffix == '.parquet'
        return original_get(path, *args, **kwargs)
    monkeypatch.setattr(L, 'get_dataframe', owned_get)
    waves = ('2014-15', '2017-18', '2018-19', '2021-22')
    class DeclaredOnly:
        def __init__(self): self.waves = waves
        def __getitem__(self, wave):
            return SimpleNamespace(wave_folder=wave, data_scheme={'crop_production': {}} if wave in waves[:2] else {})
    monkeypatch.setattr(ll, 'Country', lambda *a, **kw: DeclaredOnly())
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', GrainConflictWarning)
            first = M.crop_production_finalize(rows([25., np.nan]))
        factor_report, = copy.deepcopy(C.grain_reports(*KEY))
        existing = {**factor_report, 'columns': {'preexisting_field': 1}, 'wave': None, 'wave_folder': None}
        native_paths = []
        for wave in waves[:2]:
            native = data/'Mali'/wave/'_'/ 'crop_production.parquet'
            native.parent.mkdir(parents=True)
            frame = first.copy()
            frame.index = pd.MultiIndex.from_arrays([[wave], ['recorded-invented-household'], ['1_1'], ['Invented maize']], names=first.index.names)
            audit = [factor_report, existing, existing] if wave == waves[0] else [existing]
            L.to_parquet(frame, native, absolute_path=True, grain_audit=audit)
            native_paths.append(native)
        C._GRAIN_LEDGER.pop(KEY, None)
        monkeypatch.chdir(COUNTRY/'_')
        runpy.run_path(str(COUNTRY/'_'/ 'crop_production.py'), run_name='__factor_compiler__')
        target = data/'Mali/var/crop_production.parquet'
        reports = L.read_parquet_grain_audit(target)
        assert reports == [factor_report, existing]
        expected = pd.concat([owned_get(p) for p in native_paths])
        pd.testing.assert_frame_equal(owned_get(target), expected, check_exact=True)
        before = hashlib.sha256(target.read_bytes()).hexdigest()
        C._GRAIN_LEDGER.pop(KEY, None)
        with pytest.warns(C.GrainCollapseWarning):
            C._replay_wave_stamp(target, 'Mali', 'crop_production', None)
        replayed = C.grain_reports(*KEY)
        assert replayed == [{**r, 'country':'Mali', 'table':'crop_production', 'from_cache':True} for r in reports]
        assert hashlib.sha256(target.read_bytes()).hexdigest() == before
    finally:
        paths.data_root.cache_clear(); paths.countries_root.cache_clear(); L._resolve_data_path.cache_clear()


@pytest.mark.parametrize('wave,field', [('2014-15','s3aq23c'),('2017-18','s7gq21d')])
def test_only_field_factor_is_wired_and_native_writer_stamps(wave, field):
    path = COUNTRY/wave/'_'/ 'crop_production.py'
    tree = ast.parse(path.read_text())
    values = [v for n in ast.walk(tree) if isinstance(n,ast.Dict)
              for k,v in zip(n.keys,n.values) if isinstance(k,ast.Constant) and k.value=='KgFactor_sold']
    assert len(values)==1
    literals = [n.value for n in ast.walk(values[0]) if isinstance(n,ast.Constant)]
    assert field in literals
    assert not {'s3bq13c','s11fq14c'} & {n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str)}
    calls = [n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='to_parquet']
    call, = calls
    audit, = [k.value for k in call.keywords if k.arg=='grain_audit']
    assert isinstance(audit,ast.Call) and isinstance(audit.func,ast.Name) and audit.func.id=='grain_reports'
    assert [a.value for a in audit.args] == ['Mali','crop_production']
