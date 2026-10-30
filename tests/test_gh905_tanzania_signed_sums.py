"""New #905 adverse populations; actual public old helper versus extracted candidate.

These invented inputs add signed-cancellation coverage missing from the old
nonnegative comparison. Never remove the source helper's existing clamps.
"""
import importlib.util
import hashlib
from types import ModuleType
import json
import os
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from tests.test_gh905_tanzania_vectorised_sums import SOURCE, load_helper, stub_maps, source_case
from tests.tanzania_crop_marker_oracle import restore_original_function, relabel_expected

KINDS = ['plot_inputs', 'livestock', 'plot_labor']
POPULATIONS = ['signed_cancellation', 'nonnegative_mixed', 'nullable', 'all_na']


OLD_PUBLIC_SHA256 = 'c61473b1b743025357078c6bd324bc598b7c4a52b122476b98377b959cb7e44c'
VECTORIZED_LIVESTOCK = """        out = (out.groupby(level=['t', 'i', 'animal'])[LIVESTOCK_COLUMNS]
               .sum(min_count=1))"""
LEGACY_LIVESTOCK = """        out = out.groupby(level=['t', 'i', 'animal']).agg({
            'HeadCount': lambda s: s.sum(min_count=1),
            'HeadAcquired': lambda s: s.sum(min_count=1),
            'HeadSold': lambda s: s.sum(min_count=1),
        })"""


def load_old():
    external = os.environ.get('TZ905_OLD_PUBLIC_HELPER')
    if external:
        path = Path(external)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == OLD_PUBLIC_SHA256
        spec = importlib.util.spec_from_file_location('_tz905_actual_old_public', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    # CI has no private old-helper file. Restore the frozen pre-#1119 function
    # and reverse only the livestock optimization. The historical digest is
    # UNCHANGED: marker work must not repin the numerical reference.
    # the complete digest proves this is the exact independently attested public
    # source, rather than an approximation or a second full helper fixture.
    current = SOURCE.read_text(encoding='utf-8')
    assert current.count(VECTORIZED_LIVESTOCK) == 1
    public = restore_original_function(current).replace(VECTORIZED_LIVESTOCK, LEGACY_LIVESTOCK, 1)
    assert hashlib.sha256(public.encode('utf-8')).hexdigest() == OLD_PUBLIC_SHA256
    module = ModuleType('_tz905_actual_old_public')
    module.__file__ = str(SOURCE)
    exec(compile(public, str(SOURCE), 'exec'), module.__dict__)
    return module


def operands(kind, population):
    values = {
        'signed_cancellation': [1e16, 1., 1., -1e16],
        'nonnegative_mixed': [100., 0.1, 0.2, 1e-12],
        'nullable': [1.5, pd.NA, 0., 2.25],
        'all_na': [pd.NA, pd.NA, pd.NA, pd.NA],
    }[population]
    args = list(source_case(kind, wave='2020-21'))
    if kind == 'plot_inputs':
        raw = pd.concat([args[2].iloc[[0]]] * 4, ignore_index=True)
        raw['ag4a_10_1'] = pd.array(values, dtype='Float64')
        args[2] = raw
    elif kind == 'livestock':
        raw = pd.concat([args[1].iloc[[0]]] * 4, ignore_index=True)
        raw['indigenous'] = pd.array(values, dtype='Float64')
        raw['bought'] = pd.array(values, dtype='Float64')
        # Existing actual range clamps stay active: signed/outsize head counts
        # become missing. That is a healthy policy control, not cancellation.
        args[1] = raw
    else:
        raw = pd.concat([args[1].iloc[[0]]] * 4, ignore_index=True)
        raw['ag3a_74_1d'] = pd.array(values, dtype='Float64')
        # Wage is un-clamped source numeric; do not turn it into day/counts.
        args[1] = raw
    for value in args:
        if isinstance(value, pd.DataFrame):
            value.attrs = {'source': 'invented-905-adverse', 'population': population}
    return args


@pytest.mark.parametrize('kind', KINDS)
@pytest.mark.parametrize('population', POPULATIONS)
def test_actual_helpers_preserve_adverse_population(monkeypatch, tmp_path, kind, population):
    old, new = load_old(), load_helper()
    stub_maps(old, monkeypatch)
    stub_maps(new, monkeypatch)
    args = operands(kind, population)
    before = getattr(old, kind + '_for_wave')(*args)
    after = getattr(new, kind + '_for_wave')(*args)
    if kind == 'plot_inputs':
        # Only the structural crop-NA expectation is superseded by the ruling
        # #1119 comment6368100317 / data_info.yml:250–265. All seed crops in
        # this cohort are mapped; keep legacy payload/order and other domains.
        before = relabel_expected(before, [
            '(not crop-specific)' if pd.isna(crop) else crop
            for crop in before.index.get_level_values('crop')])
    assert before.index.equals(after.index)
    assert list(before.columns) == list(after.columns)
    assert before.dtypes.equals(after.dtypes)
    assert before.attrs == after.attrs
    assert before.isna().equals(after.isna())
    comparisons = []
    for col in before:
        if str(before[col].dtype) != 'Float64':
            pd.testing.assert_series_equal(before[col], after[col])
            continue
        a = before[col].to_numpy(dtype=float, na_value=np.nan)
        b = after[col].to_numpy(dtype=float, na_value=np.nan)
        mask = np.isfinite(a)
        assert np.array_equal(mask, np.isfinite(b))
        av, bv = a[mask], b[mask]
        comparisons.append((col, av, bv))
    receipt = {'kind': kind, 'population': population, 'rows': len(before),
               'old': {c: a.tolist() for c, a, b in comparisons},
               'new': {c: b.tolist() for c, a, b in comparisons},
               'old_new_index_order_dtypes_attrs_NA_exact': True,
               'source_clamps_unchanged': True}
    receipt['crop_axis_oracle'] = ('GH1119 structural marker only' if kind == 'plot_inputs'
                                  else 'unchanged')
    evidence = Path(os.environ.get('TZ905_EVIDENCE', str(tmp_path)))
    with (evidence/'adverse-observations.jsonl').open('a') as stream:
        stream.write(json.dumps(receipt) + '\n')
    for col, a, b in comparisons:
        np.testing.assert_array_max_ulp(a, b, maxulp=4)
