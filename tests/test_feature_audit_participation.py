"""Whole-country exclusion differs from row collapse within kept countries."""
from __future__ import annotations

import pandas as pd
import pytest

from bench.feature_audit.scan import CANONICAL_FEATURES, _assembly_invariants


def _assembled(counts):
    keys = [(country, '2020', f'h{k}', 'Flood')
            for country, rows in counts.items() for k in range(rows)]
    index = pd.MultiIndex.from_tuples(keys, names=['country', 't', 'i', 'Shock'])
    return pd.DataFrame({'Quantity': 1.0}, index=index)


def _checks(result, rows, kwargs=None, check_rows=True):
    return _assembly_invariants('1b-assembly', 'shocks', result, rows,
                                kwargs or {}, check_rows=check_rows)


def test_registered_farm_tables_are_in_canonical_audit():
    assert {'crop_production', 'plot_labor', 'plot_inputs'} <= set(CANONICAL_FEATURES)


def test_a_whole_country_is_named_with_its_own_lost_rows():
    findings = _checks(_assembled({'A': 2}), {'A': 2, 'B': 7})
    missing = [f for f in findings if f.check == 'country_exclusion']
    assert len(missing) == 1
    finding = missing[0]
    assert finding.country == 'B'
    assert finding.severity == 'B' and finding.status == 'warn'
    assert finding.metrics['dropped'] == 7
    assert finding.fingerprint == 'shocks|B||country_exclusion'
    assert not any(f.check == 'row_conservation' for f in findings)


def test_country_exclusion_and_retained_collapse_are_counted_separately():
    findings = _checks(_assembled({'A': 1}), {'A': 2, 'B': 7})
    missing = next(f for f in findings if f.check == 'country_exclusion')
    collapse = next(f for f in findings if f.check == 'row_conservation')
    assert missing.country == 'B' and missing.metrics['dropped'] == 7
    assert collapse.metrics == {'assembled': 1, 'sum_parts': 2, 'dropped': 1}


def test_retained_additive_collapse_remains_a_warning_for_triage():
    out = _assembled({'A': 1, 'B': 7})
    out.iloc[0, 0] = 2.0  # two source rows summed into one retained row
    findings = _checks(out, {'A': 2, 'B': 7})
    assert not any(f.check == 'country_exclusion' for f in findings)
    collapse = next(f for f in findings if f.check == 'row_conservation')
    assert collapse.status == 'warn' and collapse.metrics['dropped'] == 1
    assert 'triage losslessness' in collapse.detail


def test_an_empty_assembly_names_all_positive_built_countries():
    findings = _checks(_assembled({}), {'A': 2, 'B': 7, 'Empty': 0})
    missing = {f.country: f.metrics['dropped'] for f in findings
               if f.check == 'country_exclusion'}
    assert missing == {'A': 2, 'B': 7}
    assert any(f.check == 'assembly_nonempty' for f in findings)


def test_zero_row_country_is_not_an_excluded_positive_build():
    findings = _checks(_assembled({'A': 2}), {'A': 2, 'Empty': 0})
    assert not any(f.check == 'country_exclusion' for f in findings)


@pytest.mark.parametrize('kwargs,check_rows', [
    ({}, False), ({'labels': 'Aggregate'}, False), ({'labels': 'Aggregate'}, True),
])
def test_explicitly_disabled_or_nonbaseline_checks_allow_country_degradation(kwargs, check_rows):
    findings = _checks(_assembled({'A': 2}), {'A': 2, 'B': 7}, kwargs, check_rows)
    assert not any(f.check == 'country_exclusion' for f in findings)


def test_missing_country_axis_is_structural_and_does_not_guess_attribution():
    malformed = _assembled({'A': 2}).droplevel('country')
    findings = _checks(malformed, {'A': 2, 'B': 7})
    structural = [f for f in findings if f.check == 'missing_country_level']
    assert len(structural) == 1 and structural[0].severity == 'B'
    assert not any(f.check in {'country_exclusion', 'row_conservation'} for f in findings)
