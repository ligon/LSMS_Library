"""Togo 2018 plot_inputs: the purchase is s16bq08, and a missing unit is Unknown.

GH #1118 (the minted ``Manquant`` unit) and the 2026-11-13 ruling "Fix all three
now": ``Purchased`` / ``Quantity_purchased`` read ``s16bq08`` / ``s16bq09a`` (the
purchase), not ``s16bq05`` / ``s16bq07a`` (the gift question), which now ride in
``Received_as_gift`` / ``Quantity_received_as_gift``.

The first class reads only the script source.  ``TestServed`` builds the real
table and carries ``requires_s3``; its oracle reads the ``.dta`` through
``get_dataframe`` by CODE and imports nothing from ``Togo/``.
"""
from __future__ import annotations

import ast
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

COUNTRY = countries_root() / 'Togo'
SCRIPT = COUNTRY / '2018' / '_' / 'plot_inputs.py'
SOURCE = COUNTRY / '2018' / 'Data1' / 's16b_me_tgo2018.dta'

#: column -> the s16b variable it must be read from (labels or codes)
BINDINGS = {
    'Purchased': 's16bq08', 'Quantity_purchased': 's16bq09a', 'Unit_purchased': 's16bq09b',
    'Received_as_gift': 's16bq05', 'Quantity_received_as_gift': 's16bq07a',
    'Unit_received_as_gift': 's16bq07b',
}


def _frame_literal():
    """The keyword -> source-variable pairs of the script's pd.DataFrame({...})."""
    tree = ast.parse(SCRIPT.read_text())
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and getattr(node.func, 'attr', None) == 'DataFrame'
                and node.args and isinstance(node.args[0], ast.Dict)):
            out = {}
            for key, value in zip(node.args[0].keys, node.args[0].values):
                subs = [n.slice.value for n in ast.walk(value)
                        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant)]
                names = [n.id for n in ast.walk(value) if isinstance(n, ast.Name)]
                out[key.value] = (subs, names)
            return out, tree
    raise AssertionError('no pd.DataFrame({...}) literal in the 2018 script')


class TestSource:
    def test_each_acquisition_column_reads_its_own_question(self):
        literal, tree = _frame_literal()
        assigned = {t.id: n.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                    for t in n.targets if isinstance(t, ast.Name)}
        for column, var in BINDINGS.items():
            subs, names = literal[column]
            # A flag is built one step earlier (purchased = src['s16bq08']...).
            for name in names:
                if name in assigned:
                    subs += [n.slice.value for n in ast.walk(assigned[name])
                             if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant)]
            assert var in subs, f'{column} must read {var}; it reads {subs}'
        # The gift question never feeds a purchase column.
        for column in ('Purchased', 'Quantity_purchased', 'Unit_purchased'):
            subs, _ = literal[column]
            assert not {'s16bq05', 's16bq07a', 's16bq07b'} & set(subs), column

    def test_missing_unit_is_the_corpus_sentinel(self):
        tree = ast.parse(SCRIPT.read_text())
        consts = {t.id: n.value.value for n in tree.body if isinstance(n, ast.Assign)
                  for t in n.targets if isinstance(t, ast.Name)
                  and isinstance(n.value, ast.Constant)}
        assert consts['UNIT_NA'] == 'Unknown'
        # Comments may name the retired spelling; no code literal may.
        assert not [n for n in ast.walk(tree)
                    if isinstance(n, ast.Constant) and n.value == 'Manquant']


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope='class')
    def served(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return ll.Country('Togo').plot_inputs(waves=['2018'])

    @pytest.fixture(scope='class')
    def raw(self):
        codes = get_dataframe(SOURCE, convert_categoricals=False)
        codes = codes[codes['s16bq02'] == 1]
        hollow = codes['s16bq03a'].isna() & codes['s16bq05'].isna() & codes['s16bq08'].isna()
        return codes[~hollow], codes[hollow]

    def test_rows_are_the_applied_lines_less_the_hollow_ones(self, served, raw):
        kept, hollow = raw
        assert len(hollow) == 3
        assert len(served) == len(kept) == 13730

    def test_flags_and_quantities_are_the_purchase_and_the_gift(self, served, raw):
        kept, _ = raw
        assert served['Purchased'].astype('boolean').sum() == (kept['s16bq08'] == 1).sum() == 7128
        assert (served['Purchased'].astype('boolean') == False).sum() == (kept['s16bq08'] == 2).sum()  # noqa: E712
        assert served['Received_as_gift'].astype('boolean').sum() == (kept['s16bq05'] == 1).sum() == 804
        assert served['Quantity_purchased'].notna().sum() == kept['s16bq09a'].notna().sum() == 7128
        assert served['Quantity_purchased'].astype(float).sum() == pytest.approx(kept['s16bq09a'].sum())
        assert served['Quantity_received_as_gift'].astype(float).sum() == pytest.approx(kept['s16bq07a'].sum())
        assert served['Quantity'].astype(float).sum() == pytest.approx(kept['s16bq03a'].sum())
        # A unit is served exactly where its quantity question was answered.
        assert served['Unit_purchased'].notna().sum() == kept['s16bq09b'].notna().sum()
        assert served['Unit_received_as_gift'].notna().sum() == kept['s16bq07b'].notna().sum()

    def test_units_are_served_in_the_u_vocabulary(self, served):
        u = set(served.index.get_level_values('u'))
        assert 'Kg' in u and 'Kilogramme' not in u
        assert 'Kilogramme' not in set(served['Unit_purchased'].dropna())
        assert 'Kilogramme' not in set(served['Unit_received_as_gift'].dropna())

    def test_a_missing_unit_is_unknown_not_manquant(self, served, raw):
        kept, _ = raw
        u = pd.Series(served.index.get_level_values('u'), dtype=object)
        assert int((u == 'Manquant').sum()) == 0
        assert int((u == 'Unknown').sum()) == int(kept['s16bq03b'].isna().sum()) == 9
