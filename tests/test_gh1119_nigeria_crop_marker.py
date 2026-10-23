"""GH #1119 -- Nigeria plot_inputs: the "not crop-specific" marker is canonical.

``crop`` is a declared index level of Nigeria's ``plot_inputs``: seed rows
carry the seed's crop, every other input (fertilizer, pesticide, herbicide,
animal traction) is not crop-specific.  Nigeria spelled that marker ``n/a``
(34,641 rows).  Ruling (@ligon, 2026-10-23): ONE marker everywhere,
``(not crop-specific)`` -- the EHCVM / Mali spelling, registered in
``lsms_library/data_info.yml`` -- distinct from the structural ``plot_id``
sentinel ``Farm-level``.  No other Nigeria plot table carries a crop marker:
``crop_production``'s crop is always a crop, and ``plot_labor`` /
``plot_features`` have no crop level.  No assertion prints an id.
"""
import importlib.util
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MARKER = '(not crop-specific)'


@pytest.fixture(scope='module')
def ng():
    path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_1119', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_marker_constant(ng):
    assert ng.NO_CROP == MARKER


def test_assembly_fills_the_marker_for_a_non_seed_row(ng):
    part = pd.DataFrame({'i': ['1', '1'], 'plot_id': ['1', '1'],
                         'input': ['Urea', 'Seed'], 'crop': [pd.NA, 'Maize'],
                         'Quantity': [10.0, 2.0], 'u': ['Kg', 'Kg']})
    out = ng.assemble_plot_inputs('2012Q3', [part])
    got = dict(zip(out.index.get_level_values('input'),
                   out.index.get_level_values('crop')))
    assert got == {'Urea': MARKER, 'Seed': 'Maize'}


def test_no_script_writes_the_old_spelling():
    root = countries_root() / 'Nigeria' / '_'
    for name in ('nigeria.py', 'plot_inputs.py'):
        src = (root / name).read_text(encoding='utf-8')
        assert "NO_CROP = 'n/a'" not in src and "crop = 'n/a'" not in src


def _aws_creds_available() -> bool:
    spec = importlib.util.spec_from_file_location(
        '_shared_conftest', Path(__file__).with_name('conftest.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


@pytest.mark.slow
@pytest.mark.requires_s3
@pytest.mark.skipif(not _aws_creds_available(),
                    reason='needs S3 credentials to build Nigeria')
def test_built_plot_inputs_marker_count():
    from lsms_library.country import Country
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = Country('Nigeria').plot_inputs()
    crop = df.index.get_level_values('crop')
    inputs = df.index.get_level_values('input')
    n_marker = int((crop == MARKER).sum())
    n_old = int((crop == 'n/a').sum())
    n_seed_marker = int(((crop == MARKER) & (inputs == 'Seed')).sum())
    n_nonseed_other = int(((crop != MARKER) & (inputs != 'Seed')).sum())
    assert n_old == 0, n_old
    assert n_marker == 34641, n_marker
    assert n_seed_marker == 0, n_seed_marker
    assert n_nonseed_other == 0, n_nonseed_other
