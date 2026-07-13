"""
Regression tests for the ``roster_to_characteristics`` residence filter --
the silent per-wave deletion of ``household_characteristics``.

``Country.household_characteristics()`` hands the transform the *country-level*
concat of every wave's ``household_roster``, so a residence-duration column
contributed by one wave is unioned (all-NaN) onto every other wave.  Three
defects followed from that, each destroying an entire wave-cell while the
underlying roster stayed fully populated:

* **D1** the resolution was an ``if/elif`` chain over the columns *present*,
  not *populated*.  Ethiopia carries both ``MonthsAway`` (W1-W3) and
  ``WeeksAway`` (W4-W5, when the questionnaire switched to weeks -- see
  ``CLAUDE.md`` §"MonthsSpent / MonthsAway / WeeksAway").  ``MonthsAway`` won
  the ``elif``, resolved to all-NaN on W4-W5, and both waves vanished.
  Fixed by a per-**row** coalesce across the three sources.
* **D2** no all-NaN guard.  CotedIvoire 1985-89 and Mali 2021-22 never asked a
  residence question; their ``MonthsSpent`` column exists only because a later
  wave supplies it.  The keep-mask was all-False -> 5 waves vanished.  Fixed by
  a per-``t`` fallback to the documented "count everyone" behaviour.
* **D3** untranslated labels.  Burkina Faso 2014's ``MonthsSpent`` was the raw
  French ``'6 mois ou plus'`` / ``'moins de 6 mois'``; ``to_numeric(...,
  errors='coerce')`` turned both into NaN.  Fixed in *config* (the wave's
  ``data_info.yml`` ``mapping:``), following the EHCVM ``Oui``/``Non`` -> 12/0
  pattern -- asserted here so a config regression is caught.

The adversarial constraint: the filter exists because counting everyone
produced a 1315-household drift on Uganda vs. the replication pipeline.  The D2
fallback IS that old behaviour, so it must fire **only** for a wave with no
usable residence datum at all -- never for a wave where the filter works.
``TestFilterStillFiresWhereItShould`` pins that.

Synthetic rosters only -- no World Bank Microdata access required (except the
config assertion, which reads YAML).
"""
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root
from lsms_library.transformations import roster_to_characteristics

_IDX = ['t', 'v', 'i', 'pid']


def _roster(rows, columns):
    """rows: list of ((t, v, i, pid), (col values...))."""
    keys, vals = zip(*rows)
    idx = pd.MultiIndex.from_tuples(keys, names=_IDX)
    return pd.DataFrame(list(vals), index=idx, columns=columns)


def _hh(result):
    """Households present per wave in a household_characteristics frame."""
    return result.groupby(level='t').size().to_dict()


def _members(result):
    """Members counted per wave (sum of the sex x age buckets)."""
    buckets = [c for c in result.columns if c != 'log HSize']
    return result[buckets].sum(axis=1).groupby(level='t').sum().astype(int).to_dict()


def _quiet(*args, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return roster_to_characteristics(*args, **kwargs)


class TestD1PerRowCoalesce:
    """MonthsAway (W1) + WeeksAway (W2), disjointly populated -- the Ethiopia
    shape.  Both waves must survive, and both must still be filtered."""

    @pytest.fixture
    def roster(self):
        # w1 asks MonthsAway; w2 asks WeeksAway.  Each wave is NaN in the
        # other's column, exactly as the country-level concat produces.
        rows = [
            # ---- wave 1: MonthsAway populated, WeeksAway NaN
            (('w1', 'V1', 'HH1', 1), ('MALE',   35, 0.0, pd.NA)),   # resident
            (('w1', 'V1', 'HH1', 2), ('FEMALE', 33, 2.0, pd.NA)),   # resident
            (('w1', 'V1', 'HH1', 3), ('MALE',   20, 12.0, pd.NA)),  # away all year -> drop
            (('w1', 'V2', 'HH2', 1), ('FEMALE', 40, 1.0, pd.NA)),   # resident
            # ---- wave 2: WeeksAway populated, MonthsAway NaN
            (('w2', 'V1', 'HH1', 1), ('MALE',   36, pd.NA, 0.0)),   # resident
            (('w2', 'V1', 'HH1', 2), ('FEMALE', 34, pd.NA, 4.0)),   # resident
            (('w2', 'V1', 'HH1', 3), ('MALE',   21, pd.NA, 52.0)),  # away all year -> drop
            (('w2', 'V2', 'HH2', 1), ('FEMALE', 41, pd.NA, 2.0)),   # resident
        ]
        return _roster(rows, ['sex', 'age', 'monthsaway', 'weeksaway'])

    def test_both_waves_survive(self, roster):
        result = _quiet(roster)
        assert _hh(result) == {'w1': 2, 'w2': 2}, (
            "D1: the WeeksAway wave must not be deleted by an all-NaN "
            "MonthsAway column unioned in from the other wave"
        )

    def test_filter_still_fires_in_both_waves(self, roster):
        result = _quiet(roster)
        # The away-all-year member is dropped in each wave; nobody else is.
        assert _members(result) == {'w1': 3, 'w2': 3}

    def test_no_fallback_warning(self, roster):
        """Every wave has a usable datum -> the D2 fallback must NOT fire."""
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster)
        assert not [w for w in captured
                    if 'no usable residence duration' in str(w.message)]

    def test_monthsspent_wins_over_monthsaway_on_the_same_row(self):
        """Priority order when >1 source is populated: MonthsSpent first."""
        rows = [
            # MonthsSpent says resident (12); MonthsAway would say away (12 -> 0)
            (('w1', 'V1', 'HH1', 1), ('MALE',   35, 12.0, 12.0)),
            (('w1', 'V2', 'HH2', 1), ('FEMALE', 33, 12.0, 12.0)),
        ]
        roster = _roster(rows, ['sex', 'age', 'monthsspent', 'monthsaway'])
        result = _quiet(roster)
        assert _members(result) == {'w1': 2}


class TestD2AllNaNWaveGuard:
    """A wave whose residence column is entirely NaN reverts to counting
    everyone -- the documented behaviour for a country with no residence
    column, applied per-wave (CotedIvoire 1985-89 / Mali 2021-22 shape)."""

    @pytest.fixture
    def roster(self):
        rows = [
            # ---- old wave: residence question never asked (all NaN)
            (('w1', 'V1', 'HH1', 1), ('MALE',   35, pd.NA)),
            (('w1', 'V1', 'HH1', 2), ('FEMALE', 33, pd.NA)),
            (('w1', 'V2', 'HH2', 1), ('FEMALE', 40, pd.NA)),
            # ---- new wave: MonthsSpent asked
            (('w2', 'V1', 'HH1', 1), ('MALE',   36, 12.0)),
            (('w2', 'V1', 'HH1', 2), ('FEMALE', 34, 0.0)),   # non-resident -> drop
            (('w2', 'V2', 'HH2', 1), ('FEMALE', 41, 6.0)),
        ]
        return _roster(rows, ['sex', 'age', 'monthsspent'])

    def test_all_nan_wave_is_not_deleted(self, roster):
        result = _quiet(roster)
        assert 'w1' in _hh(result), (
            "D2: a wave with no usable residence datum must fall back to "
            "counting everyone, not vanish"
        )
        assert _hh(result) == {'w1': 2, 'w2': 2}

    def test_all_nan_wave_counts_everyone(self, roster):
        result = _quiet(roster)
        assert _members(result)['w1'] == 3

    def test_populated_wave_is_still_filtered(self, roster):
        """The fallback is per-``t``: the wave that DOES have data keeps its
        filter (this is the Uganda-drift guarantee)."""
        result = _quiet(roster)
        assert _members(result)['w2'] == 2

    def test_warns_naming_the_wave(self, roster):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster)
        msgs = [str(w.message) for w in captured
                if 'no usable residence duration' in str(w.message)]
        assert msgs, "the silent-deletion fallback must announce itself"
        assert "'w1'" in msgs[-1]
        assert "'w2'" not in msgs[-1]

    def test_single_wave_all_nan_without_t_level(self):
        """Same guard when the caller passes a roster with no ``t`` level."""
        idx = pd.MultiIndex.from_tuples(
            [('HH1', 1), ('HH1', 2)], names=['i', 'pid'])
        roster = pd.DataFrame(
            [('MALE', 35, pd.NA), ('FEMALE', 33, pd.NA)],
            index=idx, columns=['sex', 'age', 'monthsspent'])
        result = _quiet(roster, final_index=['i'])
        assert len(result) == 1
        assert result[[c for c in result.columns
                       if c != 'log HSize']].sum(axis=1).iloc[0] == 2


class TestFilterStillFiresWhereItShould:
    """The adversarial check: the residence filter exists to fix a 1315-HH
    Uganda drift.  Nothing above may weaken it on a wave that has data."""

    @pytest.fixture
    def roster(self):
        # Single Uganda-shaped wave: everyone has MonthsSpent.
        rows = [
            (('w1', 'V1', 'HH1', 1), ('MALE',   35, 12.0)),
            (('w1', 'V1', 'HH1', 2), ('FEMALE', 33, 12.0)),
            (('w1', 'V1', 'HH1', 3), ('MALE',    8, 0.0)),    # departed -> drop
            (('w1', 'V1', 'HH1', 4), ('FEMALE',  0.5, 0.0)),  # infant -> KEEP
            (('w1', 'V1', 'HH1', 5), ('MALE',   20, pd.NA)),  # not asked -> drop
        ]
        return _roster(rows, ['sex', 'age', 'monthsspent'])

    def test_zero_months_dropped_nan_dropped_infant_kept(self, roster):
        result = _quiet(roster)
        assert _members(result) == {'w1': 3}
        row = result.iloc[0]
        assert row['FEMALE 00-03'] == 1        # the infant survived
        # The departed 8-y-o and the not-asked 20-y-o are gone: their buckets
        # never even appear (``dummies`` emits a column only for an observed
        # sex x age combination).
        assert 'MALE 04-08' not in result.columns
        assert 'MALE 19-30' not in result.columns

    def test_no_fallback_warning_when_wave_has_data(self, roster):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster)
        assert not [w for w in captured
                    if 'no usable residence duration' in str(w.message)]

    def test_string_months_still_parse(self):
        """Country concat can leave MonthsSpent as strings ('12') or floats
        ('12.0') -- both must still resolve."""
        rows = [
            (('w1', 'V1', 'HH1', 1), ('MALE',   35, '12')),
            (('w1', 'V1', 'HH1', 2), ('FEMALE', 33, '0')),     # -> drop
            (('w1', 'V2', 'HH2', 1), ('FEMALE', 41, '12.0')),
        ]
        roster = _roster(rows, ['sex', 'age', 'monthsspent'])
        assert _members(_quiet(roster)) == {'w1': 2}


class TestD3BurkinaFaso2014Config:
    """The French 6-month binary must be mapped to 12/0 in *config*, exactly
    as the EHCVM waves map ``Oui``/``Non`` (CLAUDE.md §"EHCVM note").  Left
    raw, ``to_numeric(errors='coerce')`` NaNs the whole wave away."""

    def test_monthsspent_mapping_declared(self):
        path = (countries_root() / 'Burkina_Faso' / '2014' / '_'
                / 'data_info.yml')
        spec = yaml.safe_load(path.read_text())
        ms = spec['household_roster']['myvars']['MonthsSpent']
        assert isinstance(ms, list), (
            "Burkina Faso 2014 MonthsSpent (B3A) is the French 6-month binary "
            "'6 mois ou plus' / 'moins de 6 mois'; it needs a mapping: block, "
            "or roster_to_characteristics() coerces the wave to all-NaN and "
            "household_characteristics returns nothing for 2014."
        )
        assert ms[0] == 'B3A'
        mapping = ms[-1]['mapping']
        assert mapping['6 mois ou plus'] == 12
        assert mapping['moins de 6 mois'] == 0
