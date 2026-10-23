"""GH #637 -- Nigeria W1/W2 community_prices: one row per SOURCE item code.

W1 (2010-11) and W2 (2012-13) price Section C8 on the community
questionnaire's OWN item scheme, which ``nigeria._resolve_w1w2_price_item``
maps by name onto the shared food label ``j``.  Two things share a ``j``:

* SIZE VARIANTS of one item, coded ``base*10 + size`` (1511 / 1512 are item
  151 in two sizes), and
* DISTINCT ITEMS the name map lumps under one label (codes 3 and 4 are both
  ``Maize``; 900 / 901 both ``Beer (local and imported)``; ...).

``community_prices_for_wave`` collapsed ``(t, v, j, u, obs)`` to the FIRST
price: 2,568 W1 / 2,951 W2 multi-row groups, 2,303 / 2,605 of them
disagreeing on Price (GH #637 tail diagnosis, 2026-10-23).  Ruling (@ligon,
2026-10-23), following GH #840 for Tanzania: ``variety`` is the source item
code, so each item and size code keeps its own row under the same ``j`` and
no price is picked.  W3-W5 carry their consumption-scheme code as
``variety`` too; it is 1:1 with ``j`` there, so no W3-W5 row moves.

Every builder and built test here fails on the base (the collapse, and an
index without ``variety``).  No assertion prints an id.
"""
import ast
import importlib.util
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


@pytest.fixture(scope='module')
def ng():
    path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_637_variety', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _names(ng, rows):
    """A W1-shaped C8 frame of (ea, item_cd, price) rows, built in 'names' mode."""
    raw = pd.DataFrame({'state': [1] * len(rows), 'lga': [101] * len(rows),
                        'ea': [r[0] for r in rows],
                        'item_cd': [r[1] for r in rows],
                        'sc8q2': [r[2] for r in rows]})
    fr = dict(df=raw, dec=raw, ea='ea', item='item_cd', price='sc8q2')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ng.community_prices_for_wave('2011Q1', [fr], 'names')


class TestDeclaration:
    def test_index_declares_variety(self):
        scheme = load_yaml(countries_root() / 'Nigeria' / '_' / 'data_scheme.yml')
        spec = scheme['Data Scheme']['community_prices']['index']
        assert [t.strip() for t in spec.strip('() ').split(',')] == \
            ['t', 'v', 'j', 'u', 'variety', 'obs']

    def test_builder_selects_nothing(self):
        # No groupby-first left in the builder: the key is asserted unique.
        path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
        src = path.read_text(encoding='utf-8')
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef)
                  and n.name == 'community_prices_for_wave')
        body = ast.get_source_segment(src, fn)
        assert "'first'" not in body and '.first(' not in body
        assert "'variety'" in body


class TestBuilder:
    def test_size_variants_keep_their_own_rows(self, ng):
        out = _names(ng, [(1, 1511, 50.0), (1, 1512, 90.0)])
        assert list(out.index.names) == ['t', 'v', 'i', 'j', 'u', 'variety', 'obs']
        r = out.reset_index()
        assert r['j'].nunique() == 1          # one food label ...
        assert sorted(r['variety']) == ['1511', '1512']   # ... two codes
        assert r.set_index('variety')['Price'].to_dict() == {'1511': 50.0, '1512': 90.0}

    def test_lumped_items_keep_their_own_rows(self, ng):
        # Codes 3 and 4 both resolve to 'Maize' / 'Kg'.
        out = _names(ng, [(1, 3, 100.0), (1, 4, 140.0)])
        r = out.reset_index()
        assert r['j'].tolist() == ['Maize', 'Maize']
        assert r.set_index('variety')['Price'].to_dict() == {'3': 100.0, '4': 140.0}

    def test_variety_is_the_code_as_a_string(self, ng):
        out = _names(ng, [(1, 1511, 50.0)])
        var = out.index.get_level_values('variety')
        assert var.tolist() == ['1511']
        assert pd.api.types.is_string_dtype(var)

    def test_a_repeated_code_is_a_broken_key(self, ng):
        with pytest.raises(AssertionError, match='broken key'):
            _names(ng, [(1, 1511, 50.0), (1, 1511, 55.0)])

    def test_codes_mode_carries_the_code_too(self, ng):
        raw = pd.DataFrame({'state': [1], 'lga': [101], 'ea': [1],
                            'item_cd': [11], 'c8q2': [1], 'c8q3': [100.0]})
        dec = raw.copy()
        dec['c8q2'] = pd.Series(['Heap Small'], dtype='string')
        fr = dict(df=raw, dec=dec, ea='ea', item='item_cd', price='c8q3',
                  unit='c8q2')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = ng.community_prices_for_wave('2019Q1', [fr], 'codes',
                                               {11: 'Millet'})
        assert out.index.get_level_values('variety').tolist() == ['11']


def _aws_creds_available() -> bool:
    spec = importlib.util.spec_from_file_location(
        '_shared_conftest', Path(__file__).with_name('conftest.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


# Served rows per post-harvest round (cold builds, 2026-10-23).  W1/W2 serve
# every priced, resolvable C8 record (the raw count of (ea, item_cd) cells
# with a positive price and a food label); W3-W5 are unchanged.
EXPECTED_ROWS = {'2011Q1': 30628, '2013Q1': 31239, '2016Q1': 23153,
                 '2019Q1': 31757, '2024Q1': 30064}
# (t, v, j, u, obs) cells that hold more than one source code -- the groups the
# old `first` collapse selected from.
EXPECTED_MULTI = {'2011Q1': 2568, '2013Q1': 2951}


@pytest.mark.slow
@pytest.mark.requires_s3
@pytest.mark.skipif(not _aws_creds_available(),
                    reason='needs S3 credentials to build Nigeria')
class TestBuilt:
    @pytest.fixture(scope='class')
    def cp(self):
        from lsms_library.country import Country
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return Country('Nigeria').community_prices()

    def test_index_and_uniqueness(self, cp):
        assert list(cp.index.names) == ['t', 'v', 'j', 'u', 'variety', 'obs']
        assert cp.index.is_unique

    def test_rows_per_round(self, cp):
        got = {str(t): int(n) for t, n in cp.reset_index().groupby('t').size().items()}
        assert got == EXPECTED_ROWS, got

    def test_multi_code_cells(self, cp):
        r = cp.reset_index()
        size = r.groupby(['t', 'v', 'j', 'u', 'obs']).size()
        multi = size[size > 1]
        got = {str(t): int(n) for t, n in
               multi.groupby(level='t').size().items()}
        assert got == EXPECTED_MULTI, got
