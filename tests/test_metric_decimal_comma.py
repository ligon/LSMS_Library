"""``_parse_explicit_metric`` must read a DECIMAL COMMA, not the digits beside the unit.

GH #1143.  The patterns are ``(<number>)\\s*<unit>\\b`` and ``re.search`` takes
the leftmost match; the number group admitted no comma, so the match started
AFTER it and read only the digits adjacent to the unit token:

==========================  ===========  ========
label                       before       stated
==========================  ===========  ========
``Bouteille de 1,5 L``      5.0          1.5
``Sac (0,9 Kg)``            **9.0**      0.9
``Filet (2,5Kg)``           5.0          2.5
``12,5cl(demi walatt)``     0.05         0.125
``1,500 kg``                500.0        1500
==========================  ===========  ========

The French-style decimal comma is how every EHCVM unit vocabulary spells a
size, so the error was live on the ``metric`` rung of the served food tables
(Senegal, Benin, Cote d'Ivoire).  Same shape as GH #850 (plural) and the
fraction fix (``tests/test_metric_fraction_parse.py``): a regex that reads PART
of a self-describing label and asserts the result as a measurement.

The rule (``transformations._metric_scalar``):

- ``\\d+,\\d{1,2}`` is a decimal comma (``1,25`` -> 1.25);
- ``[1-9]\\d{0,2}(,\\d{3})+`` is a thousands separator (``10,000`` -> 10000);
- anything else with a comma (``0,500``, ``1,5000``, ``1,50,000``, the mixed
  ``1,500.5``) is ambiguous and the label declines (``None``), so the row falls
  to the inference ladder rather than being served a guess.
"""

from __future__ import annotations

import doctest
import re

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import (
    _metric_number,
    _metric_scalar,
    _parse_explicit_metric,
    _seeded_kg_factors,
    food_kg_factors,
)


# ---------------------------------------------------------------------------
# the number token
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label,expected", [
    # the issue's own list
    ('1,5 L', 1.5),
    ('0,5 L', 0.5),
    ('0,9 Kg', 0.9),
    ('2,5 Kg', 2.5),
    ('12,5 cl', 0.125),
    ('1,25 kg', 1.25),
    # the corpus spellings, verbatim (served and org-table labels)
    ('Bouteille de 1,5 L', 1.5),               # Senegal, served
    ('993. Bouteille de 1,5 litre', 1.5),      # Senegal 2021-22, served
    ('Bouteille de 1,5 litre', 1.5),           # ehcvm_units.org
    ('Filet (2,5 kg)', 2.5),                   # Senegal Preferred Label
    ('987. Filet (2,5Kg)', 2.5),               # unit abuts the number
    ('Bidon de 1,5 L', 1.5),                   # Benin, served
    ('Bidon de 0,5 L', 0.5),                   # Benin, served
    ('Sac (2,5 Kg)', 2.5),                     # Benin, served
    ('sac de 2,5kg', 2.5),                     # Benin raw / ehcvm_units.org
    ('Sac (0,9 Kg)', 0.9),                     # Cote d'Ivoire, served
    ('Sac (0,9 kg)', 0.9),                     # Cote d'Ivoire raw
    ('12,5cl(demi walatt)', 0.125),            # Senegal 2018-19, unit abuts
    ('12,5cl(Demi Walatt)', 0.125),            # Senegal units.org
    ('1001. 12,5 cl (démi walatt)', 0.125),    # Senegal 2021-22
])
def test_decimal_comma_is_read_as_a_decimal(label, expected):
    got = _parse_explicit_metric(label)
    assert got == pytest.approx(expected), (
        f"{label!r} -> {got!r}; a decimal comma is part of the number, not a "
        "separator before it")


@pytest.mark.parametrize("label,expected", [
    ('1,500 kg', 1500.0),
    ('10,000 g', 10.0),
    ('1,000,000 g', 1000.0),
    ('1500 kg', 1500.0),
    ('20kg', 20.0),
    ('50 KG', 50.0),
])
def test_thousands_separator_and_plain_integers(label, expected):
    assert _parse_explicit_metric(label) == pytest.approx(expected)


@pytest.mark.parametrize("label", [
    '0,500 kg',      # neither grammar: 3 decimals, or a thousands group led by 0
    '1,5000 kg',     # a 4-digit group
    '1,50,000 g',    # lakh grouping: not three-digit groups
    '1,500.5 kg',    # mixed separators
    '1.5.2 kg',      # a match may not start inside a number
])
def test_an_ambiguous_number_declines_rather_than_being_partly_read(label):
    """The pre-fix failure was reading the TRAILING digits of a number the
    pattern could not read whole; refusing the token must not reintroduce it
    through a later start position."""
    assert _parse_explicit_metric(label) is None


def test_volume_as_mass_false_still_declines_a_comma_volume():
    """The comma token must not let a fluid label sneak past the gate."""
    assert _parse_explicit_metric('Bouteille de 1,5 L', volume_as_mass=False) is None
    assert _parse_explicit_metric('12,5 cl', volume_as_mass=False) is None
    assert _parse_explicit_metric('Sac (0,9 Kg)', volume_as_mass=False) == pytest.approx(0.9)


@pytest.mark.parametrize("token,expected", [
    ('1,5', 1.5), ('0,9', 0.9), ('12,5', 12.5), ('1,25', 1.25),
    ('1,500', 1500.0), ('10,000', 10000.0), ('1,000,000', 1e6),
    ('50', 50.0), ('0.5', 0.5),
])
def test_metric_scalar_grammar(token, expected):
    assert _metric_scalar(token) == pytest.approx(expected)


@pytest.mark.parametrize("token", ['0,500', '1,5000', '1,50,000', '1,500.5', ',5', '1,'])
def test_metric_scalar_refuses_an_ambiguous_comma(token):
    with pytest.raises(ValueError):
        _metric_scalar(token)


def test_metric_number_reads_comma_fraction_parts():
    assert _metric_number('1,5') == pytest.approx(1.5)
    assert _metric_number('1/2') == pytest.approx(0.5)
    assert _metric_number('3 / 1,5') == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# the existing spellings are untouched
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fn", [_parse_explicit_metric, _metric_scalar],
                         ids=lambda f: f.__name__)
def test_docstring_examples_run(fn):
    """The docstring examples were documentation only (``tests/test_doctests.py``
    covers ``local_tools``); running them here pins every pre-#1143 example --
    plural, fraction, volume gate -- beside the new comma ones."""
    finder = doctest.DocTestFinder()
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS)
    for test in finder.find(fn, fn.__name__, globs={fn.__name__: fn}):
        runner.run(test)
    assert runner.failures == 0
    assert runner.tries > 0


@pytest.mark.parametrize("label,expected", [
    ('50 kg Bag', 50.0), ('500 g packet', 0.5), ('1L Carton', 1.0),
    ('500 ml Bottle', 0.5), ('2 lbs sack', 0.907184),
    ('Sack (50 kgs)', 50.0), ('Tin (Debe) (20 lts)', 20.0),
    ('Cup/Mug(0.5lt)', 0.5), ('Jerrican (5 ltrs)', 5.0),
    ('Plastic basin(5lts)', 5.0), ('Bottle(500ml)', 0.5),
    ('1/2 kg', 0.5), ('Packet(1/4lt / 1/4kg)', 0.25), ('3/4 kg', 0.75),
    ('Boite (500 milligrammes)', 0.5e-3), ('Bouteille (75 centilitres)', 0.75),
    ('0.5 tonne', 500.0), ('Basin/20 kg', 20.0),
    # range labels keep their pinned endpoint (test_food_kg_inference.py)
    ('Gourd (1-5lts)', 5.0), ('Fish Cut piece (1 - 2 kg)', 2.0),
    # Benin's RAW label: the instrument's words, read as its "1 litre" (the
    # curated Preferred Label `Bidon de 1,5 L` is what the comma fix reads)
    ('Bidon de 1 litre et démi', 1.0),
])
def test_existing_spellings_are_unchanged(label, expected):
    assert _parse_explicit_metric(label) == pytest.approx(expected)


@pytest.mark.parametrize("label", ['Heap (Small)', '20gallon drum', 'half kg',
                                   '2 quintaux', '1/0 kg', 'bidon de demi litre'])
def test_existing_declines_are_unchanged(label):
    assert _parse_explicit_metric(label) is None


# ---------------------------------------------------------------------------
# where the number is served
# ---------------------------------------------------------------------------

def test_seeded_factor_is_the_stated_size():
    """``_seeded_kg_factors`` is the seeding layer both food and crop ladders
    read; the comma labels must enter it at the stated size."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'h1', 'A', 'Bouteille de 1,5 L'), ('2020', 'h1', 'B', 'Sac (0,9 Kg)'),
         ('2020', 'h1', 'C', '12,5cl(demi walatt)')], names=['t', 'i', 'j', 'u'])
    f = _seeded_kg_factors(pd.DataFrame({'Quantity': [1.0, 1.0, 1.0]}, index=idx))
    assert f['bouteille de 1,5 l'] == pytest.approx(1.5)
    assert f['sac (0,9 kg)'] == pytest.approx(0.9)
    assert f['12,5cl(demi walatt)'] == pytest.approx(0.125)


def test_food_ladder_serves_the_stated_size_on_the_metric_rung():
    rows = [('2020', f'h{k}', 'A', 'kg', 3.0, 300.0) for k in range(20)]
    rows += [('2020', f'h{k}', 'A', 'Sac (0,9 Kg)', 2.0, 180.0) for k in range(20)]
    idx = pd.MultiIndex.from_tuples([r[:4] for r in rows], names=['t', 'i', 'j', 'u'])
    df = pd.DataFrame({'Quantity': [r[4] for r in rows],
                       'Expenditure': [r[5] for r in rows]}, index=idx)
    served = food_kg_factors(df)
    lab = served[served.index.get_level_values('u') == 'Sac (0,9 Kg)']
    assert set(lab['KgFactorSource']) == {'metric'}
    assert lab['kg_per_unit'].unique() == pytest.approx([0.9])


# ---------------------------------------------------------------------------
# every comma label the config tree ships parses to the size it spells
# ---------------------------------------------------------------------------

_COMMA_METRIC = re.compile(r'\d,\d{1,2}\s*(?:kg|l\b|litre|cl)', re.IGNORECASE)


def _org_comma_metric_cells():
    root = countries_root().parent
    cells = set()
    paths = (list(root.glob('categorical_mapping/*.org'))
             + list(root.glob('countries/*/_/*.org'))
             + list(root.glob('countries/*/_/categorical_mapping/*.org')))
    for path in paths:
        if path.name == 'CONTENTS.org':
            continue
        for line in path.read_text(errors='replace').splitlines():
            if not line.lstrip().startswith('|'):
                continue
            for cell in line.strip().strip('|').split('|'):
                cell = cell.strip()
                if _COMMA_METRIC.search(cell):
                    cells.add((path.relative_to(root).as_posix(), cell))
    return sorted(cells)


def test_the_config_tree_has_comma_labels_to_check():
    """Guards the census below from passing vacuously."""
    assert len(_org_comma_metric_cells()) >= 15


@pytest.mark.parametrize("where,label", _org_comma_metric_cells())
def test_every_shipped_comma_label_reads_its_stated_size(where, label):
    """Oracle: the same label with its decimal comma spelled as a point, which
    the parser has always read.  Independent of the comma grammar under test."""
    dotted = re.sub(r'(\d),(\d{1,2})(?!\d)', r'\1.\2', label)
    expected = _parse_explicit_metric(dotted)
    assert expected is not None, (where, label, dotted)
    assert _parse_explicit_metric(label) == pytest.approx(expected), (where, label)
