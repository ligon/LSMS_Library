"""Burkina Faso ``food_acquired`` labels: the crosswalk must PARSE, and the
served ``j`` must carry no encoding damage (GH #794).

THE DEFECT.  ``Burkina_Faso/_/categorical_mapping.org`` keys ``harmonize_food``
on the damage the reader produces for four cp1252 / double-encoded value labels
(strategy B, see ``_/CONTENTS.org``).  One key -- ``Cube (Maggi, Jumbo,<U+0085>)``
-- contains U+0085, a Unicode line separator, and an editor turned it into a
line break.  ``local_tools.df_from_orgfile`` ends a table at the FIRST line
that does not begin with ``|``, so the table stopped there and the four rows
below it (Cube, Feuilles, both beef spellings) were never loaded: 47,137
served rows carried a raw C1 label and beef was served as THREE ``j`` values.
Nothing warned -- an org row that is never read looks exactly like one that
is.

The config-only tests pin the parse so the truncation class cannot recur
silently; the data tests pin what the user sees.
"""
import re
import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

ORG = countries_root() / 'Burkina_Faso' / '_' / 'categorical_mapping.org'

# The damage spellings the reader emits, exactly as the crosswalk must key
# them (U+008C / U+009C / U+0085 are cp1252 bytes read as latin-1; the 2018-19
# beef is UTF-8 read as latin-1).
DAMAGE_KEYS = {
    '\u008cufs': 'Oeufs',
    'Cube (Maggi, Jumbo,\u0085)': 'Cube alimentaire',
    'Viande de bÅ\u0093uf': 'Viande de boeuf',
    'Viandes de b\u009cufs fraîches': 'Viande de boeuf',
}

MOJIBAKE = re.compile(r'[\x80-\x9f]|Ã|Å|Â')


# ---------------------------------------------------------------------------
# 1. Configuration -- no microdata needed.
# ---------------------------------------------------------------------------

def _table_lines():
    """Raw lines of the harmonize_food table, header to the first non-row."""
    with open(ORG, encoding='utf-8', newline='') as fh:
        lines = fh.read().split('\n')
    start = next(i for i, s in enumerate(lines)
                 if s.strip().lower() == '#+name: harmonize_food')
    i = start + 1
    while lines[i].strip().startswith('#+'):
        i += 1
    rows = []
    while i < len(lines) and lines[i].strip():
        rows.append(lines[i])
        i += 1
    return rows


def test_every_crosswalk_line_is_a_complete_org_row():
    """A row split across two lines ends the table for the org reader.
    Every line between the header and the blank line that closes the table
    must start AND end with ``|`` (or be a ``|-`` rule)."""
    bad = [(k, s[:60]) for k, s in enumerate(_table_lines())
           if not (s.lstrip().startswith('|') and s.rstrip().endswith('|'))]
    assert not bad, bad


def test_the_crosswalk_parses_to_completion_with_the_damage_keys():
    """The reader's own view: all four damage-keyed rows load and map to the
    Preferred Label the table intends."""
    t = df_from_orgfile(ORG, name='harmonize_food')
    d = t.set_index('Original Label')['Preferred Label'].to_dict()
    for key, preferred in DAMAGE_KEYS.items():
        assert d.get(key) == preferred, (repr(key), d.get(key))
    # No key may be a stripped fragment of a split row.
    assert ')' not in d and 'Cube (Maggi, Jumbo,' not in d


def test_the_cube_key_still_holds_its_nel_byte():
    """The failure mode, stated as a byte: the Cube key contains a literal
    U+0085 and the row is one line.  An editor that 'normalises' it to a
    newline turns this red before it turns the served data brown."""
    hits = [s for s in _table_lines() if 'Cube (Maggi, Jumbo,' in s]
    assert len(hits) == 1, hits
    assert '\u0085' in hits[0]
    assert hits[0].rstrip().endswith('|')


def test_the_2014_hook_repairs_what_the_crosswalk_cannot_reach():
    """``Feuilles (oseilles, baobab, boulvaka<U+0085>`` ENDS in U+0085; the org
    reader strips every cell so no key can carry it, while the build-time dict
    sees the raw label with it.  The hook's whole-value ``_J_FIX`` is the only
    route, and the cp1252 / double-encoding decoders are a safety net that
    must leave clean French alone."""
    import importlib.util

    path = countries_root() / 'Burkina_Faso' / '2014' / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location('bf2014_labels', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod._repair_label('Feuilles (oseilles, baobab, boulvaka') == 'Feuilles'
    assert mod._repair_label('Viandes de b\u009cufs fraîches') == \
        'Viandes de bœufs fraîches'
    assert mod._repair_label('Viande de bÅ\u0093uf') == 'Viande de bœuf'
    for clean in ('Viande séchée', 'Semoule de maïs',
                  'Cube alimentaire', 'Kg', 'Unknown'):
        assert mod._repair_label(clean) == clean
    assert mod._repair_label(3.0) == 3.0


# ---------------------------------------------------------------------------
# 2. Delivered data.
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def delivered():
    from lsms_library.country import Country
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return Country('Burkina_Faso').food_acquired()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Burkina_Faso food_acquired could not be built: '
                    f'{type(exc).__name__}: {exc}')


_data = pytest.mark.requires_s3


@_data
def test_no_served_label_carries_encoding_damage(delivered):
    for level in ('j', 'u'):
        values = pd.Series(delivered.index.get_level_values(level).astype(str))
        leaked = values[values.str.contains(MOJIBAKE)]
        assert leaked.empty, leaked.value_counts().head(10).to_dict()


@_data
def test_beef_cube_and_feuilles_are_one_label_each(delivered):
    """Before the repair beef was served as THREE ``j`` values (660 / 1,397 /
    1,901 rows), and 38,982 Cube rows plus 16,610 Feuilles rows leaked raw."""
    j = pd.Series(delivered.index.get_level_values('j').astype(str))
    beef = j[j.str.contains('Viande') & j.str.contains('uf')].unique().tolist()
    assert beef == ['Viande de boeuf'], beef
    assert j[j.str.startswith('Cube')].unique().tolist() == ['Cube alimentaire']
    wave = delivered.xs('2014', level='t')
    j14 = pd.Series(wave.index.get_level_values('j').astype(str))
    assert j14[j14.str.startswith('Feuilles')].unique().tolist() == ['Feuilles']
    assert (j14 == 'Feuilles').sum() > 15_000
