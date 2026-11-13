"""A code-prefixed ``u`` label, and which spelling the kg ladder is handed (GH #1454).

Some EHCVM food files write a unit as ``<code>. <name>``.  Where the name is a
bare metric unit, ``_parse_explicit_metric`` reads the CODE as the size:
``'100. Kg'`` is 100.0 kg and ``'101. Litre'`` is 101.0.  Nothing serves that
today, for one reason: on the Country path ``_aggregate_wave_data`` has already
run ``_finalize_result``, so the ladder receives the country's ``Preferred
Label`` (``Kg``), not the label as written.  A direct call on
``var/food_acquired.parquet`` would read 100 kg.

So the parser's answer is pinned as a known defect rather than blessed: the
xfail states the right answer and fails loudly once someone fixes the parser.  A
fix was not made here because it moves every GhanaLSS L2 hash (22 of 22 tables,
154 of 154 wave hashes, no other country, measured with a no-op body edit):
``GhanaLSS/_/nutrition.py`` imports the transform.  (Since GH #1509's "Make the
dependency visible", 2026-11-12, Uganda's and Ethiopia's ``nutrition.py`` import
it too, so such a fix now re-warms those two countries as well.)  What actually
protects the
served numbers is the rule below, that every label the parser misreads is mapped
away, and that the ladder really does receive the mapped label.
"""
import re

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.country import _categorical_key_column
from lsms_library.paths import countries_root
from lsms_library.transformations import _parse_explicit_metric

CODE_PREFIX = re.compile(r'^\s*\d+\s*[.)]\s+')


def _strip_code(label):
    return CODE_PREFIX.sub('', label)


def _is_misread(label):
    """The parser's reading comes from the code, not from the unit's name."""
    if not CODE_PREFIX.match(label):
        return False
    return _parse_explicit_metric(label) != _parse_explicit_metric(_strip_code(label))


def _u_rows(country):
    table = ll.Country(country).categorical_mapping.get('u')
    if table is None or 'Preferred Label' not in getattr(table, 'columns', ()):
        return []
    key = _categorical_key_column(table)
    return [(str(k), str(v)) for k, v in zip(table[key], table['Preferred Label'])]


def _countries():
    return sorted(p.name for p in countries_root().iterdir()
                  if (p / '_' / 'data_scheme.yml').exists())


@pytest.mark.xfail(strict=True, reason="GH #1454: the code is read as the size; "
                                       "no served path reaches this, see module docstring")
@pytest.mark.parametrize('label', ['100. Kg', '100. kg', '101. Litre', '101. litre'])
def test_a_code_prefix_is_not_a_stated_size(label):
    assert _parse_explicit_metric(label) is None


@pytest.mark.parametrize('label, expected', [
    ('999. Paquet (1 Kg)', 1.0),
    ('1001. 12,5 cl (démi walatt)', 0.125),
    ('138. Sac (50 Kg)', 50.0),
])
def test_a_code_prefix_beside_a_stated_size_reads_the_size(label, expected):
    """The defect is confined to a bare unit: the code is not adjacent to a number."""
    assert _parse_explicit_metric(label) == pytest.approx(expected)
    assert not _is_misread(label)


def test_every_misread_label_in_a_u_table_is_mapped_to_one_the_parser_reads_correctly():
    misread = {}
    for country in _countries():
        for raw, preferred in _u_rows(country):
            if _is_misread(raw):
                misread[(country, raw)] = preferred
    # The rule is not vacuous: these are the cells that carry the hazard.
    assert {('Niger', '100. Kg'), ('Niger', '101. Litre'),
            ('Senegal', '100. kg'), ('Senegal', '101. litre')} <= set(misread)
    for (country, raw), preferred in misread.items():
        assert not CODE_PREFIX.match(preferred), (country, raw, preferred)
        assert _parse_explicit_metric(preferred) == _parse_explicit_metric(_strip_code(raw)), (
            country, raw, preferred)


@pytest.mark.requires_s3
def test_the_ladder_receives_served_labels_on_the_country_path(monkeypatch):
    """The prose this issue corrected said the opposite: the ladder saw the raw ``u``.

    Reads the labels the transform is actually handed, against the parquet's own
    labels for the wave, on the one wave whose file writes a misread label.
    """
    seen = {}
    real = T.food_quantities_from_acquired

    def spy(food_acquired, **kwargs):
        seen['u'] = food_acquired.index.get_level_values('u').astype(str).unique().tolist()
        seen['unit_kg'] = kwargs.get('unit_kg')
        return real(food_acquired, **kwargs)

    monkeypatch.setattr(T, 'food_quantities_from_acquired', spy)
    ll.Country('Niger').food_quantities(waves=['2021-22'])

    assert 'Kg' in seen['u']
    assert not [u for u in seen['u'] if CODE_PREFIX.match(u)]

    from lsms_library.paths import data_root
    written = pd.read_parquet(data_root() / 'Niger' / 'var' / 'food_acquired.parquet')
    written = written.xs('2021-22', level='t').index.get_level_values('u').astype(str)
    assert '100. Kg' in set(written), "the parquet no longer holds the raw label; re-read the docstring"
