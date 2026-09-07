"""The canonical-``j`` guard for GhanaLSS (GH #782).

GhanaLSS shipped 149,047 rows of 2005-06 own production carrying the survey's
own raw labels as ``j`` -- for years, silently -- because the decode matched
nothing and ``Series.replace()`` passes an unmatched value through unchanged.
Nothing checked.  CLAUDE.md names the remedy exactly: *"a plausible-looking
distribution therefore proves nothing; the discriminating check is a subset
assertion against the canonical vocabulary."*  This module is that assertion.

Two tiers, deliberately:

* **Config-only** (``TestVocabulary``) -- reads the org tables and nothing else.
  No cache, no microdata, no skip, milliseconds.  These catch the whole of the
  cross-wave drift class *statically*, which matters because it is invisible to
  any per-wave check: every GhanaLSS wave is internally clean, and the seven
  spelling drifts existed only in the assembled frame.
* **Data-gated** (``TestDelivered``) -- the end-to-end subset assertion on the
  built table, skipped when the GhanaLSS cache is cold.

The config tier is the one that stops recurrence.  A guard that only runs when
the microdata happens to be present is a guard that does not run in CI.
"""
import re

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'
WAVES = ['1987-88', '1988-89', '1991-92', '1998-99', '2005-06',
         '2012-13', '2016-17']
# 2005-06 ships no market price survey, so it has no harmonize_price_item.
PRICE_WAVES = ['1987-88', '1988-89', '1991-92', '1998-99', '2012-13', '2016-17']
ENC = 'ISO-8859-1'


def _collision_key(label):
    """casefold + punctuation-to-space + singularise every word.

    This is the key #782 used to census the corpus; it deliberately equates
    'Tea bag'/'Tea bags', 'Egg'/'Eggs'/'eggs' and 'Maize (flour/dough)'/
    'maize-flour/dough'.  Two labels sharing a key are the same commodity
    spelled two ways -- never two distinct foods.
    """
    s = re.sub(r'[^a-z0-9]+', ' ', str(label).casefold()).strip()
    return ' '.join(w[:-1] if len(w) > 3 and w.endswith('s') and not w.endswith('ss')
                    else w for w in s.split())


def _wave_table(wave, name):
    path = countries_root() / COUNTRY / wave / '_' / 'categorical_mapping.org'
    return df_from_orgfile(path, name=name, encoding=ENC)


def _values(frame, column):
    return {v.strip() for v in frame[column].dropna().astype(str) if v.strip()}


def _collisions(labels):
    lab = pd.Series(sorted(labels))
    groups = lab.groupby(lab.map(_collision_key)).apply(list)
    return {k: v for k, v in groups.items() if len(v) > 1}


@pytest.fixture(scope='module')
def food_axis():
    """{wave: {'Preferred Label': {...}, 'Aggregate Label': {...}}}."""
    out = {}
    for w in WAVES:
        t = _wave_table(w, 'harmonize_food')
        out[w] = {c: _values(t, c) for c in ('Preferred Label', 'Aggregate Label')}
    return out


class TestVocabulary:
    """Config-only. Runs everywhere, needs no data."""

    @pytest.mark.parametrize('axis', ['Preferred Label', 'Aggregate Label'])
    def test_no_cross_wave_spelling_drift(self, food_axis, axis):
        """The defect #782 reported: one commodity, two spellings, by wave.

        Invisible per-wave -- it exists only in the union.  Before the fix the
        union carried 7 such groups on each axis (Egg/Eggs, Tea bag/Tea bags,
        Condiment/Condiments, and four Other X/Other Xs).
        """
        union = set().union(*(food_axis[w][axis] for w in WAVES))
        bad = _collisions(union)
        assert not bad, (
            f'{COUNTRY} harmonize_food {axis!r}: {len(bad)} label(s) spelled two '
            f'ways across waves -- {dict(sorted(bad.items()))}.  The country-level '
            f'_/food_items.org `food_label` table is the authority; use its form '
            f'in every wave.')

    @pytest.mark.parametrize('wave', WAVES)
    @pytest.mark.parametrize('axis', ['Preferred Label', 'Aggregate Label'])
    def test_wave_vocabulary_internally_clean(self, food_axis, wave, axis):
        """No single wave may carry two spellings of one commodity.

        Pinned because it is what makes a rename provably safe: if no wave holds
        both variants, canonicalising one to the other cannot merge two distinct
        foods.
        """
        bad = _collisions(food_axis[wave][axis])
        assert not bad, f'{COUNTRY} {wave} harmonize_food {axis!r}: {bad}'

    @pytest.mark.parametrize('wave', PRICE_WAVES)
    def test_price_items_share_the_food_vocabulary(self, food_axis, wave):
        """One vocabulary, not two (GH #782, @ligon's direction).

        ``harmonize_price_item`` carries its own ``Preferred Label`` column
        parallel to ``harmonize_food``'s.  A price item flagged ``Food: yes``
        asserts it is *on the wave's food axis* -- so its label must be a label
        that axis actually has, or ``community_prices`` cannot join
        ``food_acquired`` on ``j`` however plausible both sides look.

        The two tables agreed when #782 was filed but nothing enforced it, so a
        one-sided edit drifted silently.  ``Food: own`` (a food the axis does not
        name) and ``Food: no`` (a non-food -- 400 of 2016-17's 643 items) are
        exempt by construction: they are the author's signed statement that the
        item is deliberately off the food axis.
        """
        t = _wave_table(wave, 'harmonize_price_item')
        t = t.apply(lambda c: c.astype(str).str.strip())
        flagged = t[t['Food'] == 'yes']
        assert len(flagged), f'{wave}: no Food=yes price items -- table decoded empty?'
        off = sorted(set(flagged['Preferred Label']) - food_axis[wave]['Preferred Label'])
        assert not off, (
            f'{COUNTRY} {wave}: {len(off)} price item(s) flagged Food=yes carry a '
            f'Preferred Label absent from this wave\'s harmonize_food axis -- {off}. '
            f'Either put the label on the food axis, or flag the item Food=own/no.')

    def test_country_table_is_a_usable_authority(self):
        """The country crosswalk must not itself carry two spellings of one food."""
        t = df_from_orgfile(countries_root() / COUNTRY / '_' / 'food_items.org',
                            name='food_label')
        bad = _collisions(_values(t, 'Preferred Label'))
        assert not bad, f'{COUNTRY} _/food_items.org food_label: {bad}'


@pytest.fixture(scope='module')
def delivered():
    """food_acquired's delivered j per wave, or skip if the cache is cold."""
    try:
        import lsms_library as ll
        fa = ll.Country(COUNTRY).food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} food_acquired not buildable here: {e}')
    return pd.DataFrame({
        't': fa.index.get_level_values('t').astype(str),
        'j': fa.index.get_level_values('j').astype(str)})


@pytest.mark.slow
class TestDelivered:
    """End-to-end. Skipped when the GhanaLSS cache is cold."""

    def test_delivered_j_is_on_the_wave_food_axis(self, delivered, food_axis):
        """THE subset assertion -- the check whose absence let #782 ship.

        Every delivered j must be a Preferred Label of the wave that produced
        it.  2005-06 failed this for 149,047 rows across 63 raw survey labels
        ('beef', 'maize-flour/dough', 'okro') because its own-production decode
        matched nothing and passed the source label through.
        """
        offenders = {}
        for wave, g in delivered.groupby('t'):
            off = sorted(set(g['j']) - food_axis[wave]['Preferred Label'])
            if off:
                offenders[wave] = (len(off), off[:15],
                                   int(g['j'].isin(off).sum()))
        assert not offenders, (
            f'{COUNTRY} food_acquired: delivered j is not a subset of the wave\'s '
            f'harmonize_food Preferred Labels -- {{wave: (n_labels, sample, rows)}} '
            f'{offenders}.  An unmatched decode is passing raw source labels '
            f'through; fix the decode, do not add the raw label to the axis.')

    def test_no_cross_wave_collisions_in_delivered_j(self, delivered):
        """The census #782 published: 35 groups over 27.9% of rows -> 0."""
        bad = _collisions(set(delivered['j']))
        rows = int(delivered['j'].isin(
            [x for v in bad.values() for x in v]).sum()) if bad else 0
        assert not bad, (
            f'{COUNTRY} food_acquired: {len(bad)} label(s) spelled two ways across '
            f'waves, over {rows:,} rows ({100 * rows / len(delivered):.1f}%) -- '
            f'{dict(sorted(bad.items()))}')
