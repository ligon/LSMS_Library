"""
Regression test for GH #268 -- ``roster_to_characteristics`` must
preserve mover / split-off households (rows with NaN ``v`` in the
roster's index) under its new default ``mover_sentinel='Mover'``,
and recover the legacy drop behavior when explicitly opted out via
``mover_sentinel=None``.

Pre-#268, the function's final ``groupby(level=final_index).sum()``
relied on pandas' default ``dropna=True``, which silently excluded
rows whose index had NaN in any of ``('t', 'v', 'i')`` -- in
practice always ``v``, since ``v`` is joined onto the roster post-hoc
via ``_join_v_from_sample`` and ``sample``'s ``v`` column is NaN for
movers / split-offs that lack a cluster code.  ``sample()`` typically
carries a valid ``Region`` for those households, so dropping them at
this stage loses households we /can/ still assign to a market.  GH
#268 flips the default so the household survives as a distinguishable
``v == 'Mover'`` bucket; callers who want the strict legacy drop
pass ``mover_sentinel=None``.

These tests run against a synthetic 3-household roster -- no World
Bank Microdata access required.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import roster_to_characteristics


def _synthetic_roster():
    """3-HH roster: two with valid cluster codes, one mover (NaN v).
    All members report MonthsSpent=12 so the upstream filter doesn't
    drop any of them."""
    rows = [
        # HH1 in cluster V1: two adults
        (('2020', 'V1', 'HH1', 1), ('MALE',   35, 12)),
        (('2020', 'V1', 'HH1', 2), ('FEMALE', 33, 12)),
        # HH2 in cluster V2: one adult + one child
        (('2020', 'V2', 'HH2', 1), ('FEMALE', 40, 12)),
        (('2020', 'V2', 'HH2', 2), ('MALE',   10, 12)),
        # HH3 mover (NaN v): two adults
        (('2020', pd.NA, 'HH3', 1), ('MALE',   28, 12)),
        (('2020', pd.NA, 'HH3', 2), ('FEMALE', 27, 12)),
    ]
    keys, vals = zip(*rows)
    idx = pd.MultiIndex.from_tuples(keys, names=['t', 'v', 'i', 'pid'])
    return pd.DataFrame(list(vals), index=idx,
                        columns=['sex', 'age', 'monthsspent'])


@pytest.fixture
def roster():
    return _synthetic_roster()


@pytest.fixture
def roster_no_movers(roster):
    """Same roster minus the NaN-v rows; used to confirm the new
    default path is a no-op when there's nothing to fill."""
    keep = ~roster.index.get_level_values('v').isna()
    return roster.loc[keep]


class TestMoverSentinelDefault:
    """The new default behavior: NaN v gets ``'Mover'``."""

    def test_mover_household_survives(self, roster):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = roster_to_characteristics(roster)
        v_values = list(result.index.get_level_values('v'))
        assert 'Mover' in v_values, (
            "GH #268: mover HH (NaN v) should survive groupby as "
            "v='Mover'; got v values: %r" % v_values
        )
        # All three HHs (V1, V2, Mover) should be present.
        assert sorted(v_values) == ['Mover', 'V1', 'V2']

    def test_mover_household_has_correct_counts(self, roster):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = roster_to_characteristics(roster)
        mover_row = result.xs('Mover', level='v').iloc[0]
        # HH3 has one 28yo Male and one 27yo Female: both in 19-30.
        assert mover_row['MALE 19-30'] == 1
        assert mover_row['FEMALE 19-30'] == 1
        # log HSize = log(2)
        assert np.isclose(mover_row['log HSize'], np.log(2))

    def test_warning_message_mentions_replaced_and_sentinel(self, roster):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster)
        msgs = [str(w.message) for w in captured
                if 'household_characteristics' in str(w.message)]
        assert msgs, "Expected GH #268 warning naming the sentinel"
        msg = msgs[-1]
        assert 'replaced' in msg
        assert "'Mover'" in msg


class TestMoverSentinelLegacyDrop:
    """``mover_sentinel=None`` recovers the GH #197 drop behavior."""

    def test_mover_household_dropped(self, roster):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = roster_to_characteristics(roster, mover_sentinel=None)
        v_values = list(result.index.get_level_values('v'))
        assert 'Mover' not in v_values
        # Only the two non-mover HHs.
        assert sorted(v_values) == ['V1', 'V2']
        assert len(result) == 2

    def test_warning_message_says_dropped(self, roster):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster, mover_sentinel=None)
        msgs = [str(w.message) for w in captured
                if 'household_characteristics' in str(w.message)]
        assert msgs
        assert 'dropped' in msgs[-1]


class TestMoverSentinelIdempotenceWhenCleanInput:
    """No NaN v -> both code paths produce identical output."""

    def test_default_and_legacy_agree_when_no_movers(self, roster_no_movers):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            r_default = roster_to_characteristics(roster_no_movers)
            r_legacy = roster_to_characteristics(roster_no_movers,
                                                  mover_sentinel=None)
        assert r_default.equals(r_legacy)

    def test_no_warning_when_no_movers(self, roster_no_movers):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            roster_to_characteristics(roster_no_movers)
        msgs = [str(w.message) for w in captured
                if 'household_characteristics' in str(w.message)]
        # No NaN v means neither the replace nor the drop warning
        # should fire.
        assert msgs == []


class TestMoverSentinelCustomLabel:
    """The sentinel is configurable -- pass any string."""

    def test_custom_sentinel_label(self, roster):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = roster_to_characteristics(roster,
                                                mover_sentinel='split-off')
        v_values = list(result.index.get_level_values('v'))
        assert 'split-off' in v_values
        assert 'Mover' not in v_values
