"""Chronological visit inference: assignments are derived, dates never are."""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


_PATH = Path(__file__).parents[1] / 'lsms_library/countries/GhanaLSS/_/visit_dates.py'
_SPEC = importlib.util.spec_from_file_location('glss_visit_inference', _PATH)
vd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(vd)


def raw_frame(wave, pages):
    rows = []
    for source, values in pages.items():
        for number, (code, date) in enumerate(values, 1):
            rows.append({'t': wave, 'i': '400502', 'source': source,
                         'source_record': number, 'visitcd': code,
                         'parsed_date': pd.to_datetime(date),
                         'original_answer': f'{source}:{number}'})
    return pd.DataFrame(rows).set_index(['t', 'i', 'source', 'source_record'])


def glss4(dates, codes=None):
    return raw_frame('1998-99', {
        'SEC0A.DTA': [(None, dates[0])],
        'SEC0C.DTA': list(zip([1, 2, 3], dates[:3])),
        'SEC0B.DTA': list(zip(codes or range(2, len(dates) + 1), dates[1:])),
    })


@pytest.mark.parametrize('start,codes', [
    ('1998-06-14', [3, 3, 4, 5, 6, 7]),
    ('1998-06-13', [3, 4, 4, 5, 6, 7]),
])
def test_glss4_duplicate_codes_recover_by_anchored_order(start, codes):
    dates = pd.date_range(start, periods=7, freq='5D')
    raw = glss4(dates, codes)
    result, inputs = vd._reconcile(raw, '1998-99')
    assert result.index.get_level_values('visit').tolist() == list(range(1, 8))
    assert result.Int_t.tolist() == list(dates)
    assert result.Derivation.iloc[:3].isna().all()
    assert result.Derivation.iloc[3:].eq(vd.KEY4).all()
    pd.testing.assert_frame_equal(inputs[raw.columns], raw)
    pd.testing.assert_frame_equal(vd.derive_glss4_visit_assignments(inputs), result)
    b = inputs.xs('SEC0B.DTA', level='source')
    assert b.assigned_visit.tolist() == list(range(2, 8))
    shuffled = raw.sample(frac=1, random_state=57)
    shuffled_result, shuffled_inputs = vd._reconcile(shuffled, '1998-99')
    pd.testing.assert_frame_equal(result, shuffled_result)
    pd.testing.assert_frame_equal(inputs.sort_index(), shuffled_inputs.sort_index())


def test_missing_visits_do_not_get_consecutive_numbers():
    dates = pd.to_datetime(['1998-06-01', '1998-06-02', '1998-06-04', '1998-06-11'])
    result, inputs = vd._reconcile(glss4(dates), '1998-99')
    # The last report can be visit 4,5,6,7; no median gap may choose one.
    assert result.index.get_level_values('visit').tolist() == [1, 2, 3]
    assert inputs.iloc[-1].assignment_reason == 'ambiguous_visit'
    assert pd.isna(inputs.iloc[-1].assigned_visit)


def test_partial_identification_with_missing_anchor_slot():
    raw = raw_frame('1998-99', {
        'SEC0A.DTA': [(None, '1998-06-01')],
        'SEC0C.DTA': [(1, '1998-06-01'), (3, '1998-06-03')],
        'SEC0B.DTA': [(99, '1998-06-02'), (99, '1998-06-09')],
    })
    result, inputs = vd._reconcile(raw, '1998-99')
    assert result.index.get_level_values('visit').tolist() == [1, 2, 3]
    assert result.loc[('1998-99', '400502', 2), 'Derivation'] == vd.KEY4
    assert inputs.iloc[-1].assignment_reason == 'ambiguous_visit'


def test_exact_duplicate_dates_preserve_every_original_record():
    raw = glss4(pd.date_range('1998-01-01', periods=7, freq='D'))
    extra = raw.iloc[[-1]].copy()
    extra.index = pd.MultiIndex.from_tuples(
        [('1998-99', '400502', 'SEC0B.DTA', 99)], names=raw.index.names)
    raw = pd.concat([raw, extra])
    result, inputs = vd._reconcile(raw, '1998-99')
    assert len(result) == 7
    assert len(inputs) == len(raw)
    assert inputs.iloc[[-1, -2]].assigned_visit.eq(7).all()
    pd.testing.assert_frame_equal(inputs[raw.columns], raw)


@pytest.mark.parametrize('problem', ['conflicting_intake', 'excess_dates', 'reversed_anchor'])
def test_impossible_assignment_preserves_reported_anchors(problem):
    dates = pd.date_range('1998-01-01', periods=8 if problem == 'excess_dates' else 7)
    raw = glss4(dates)
    c1 = ('1998-99', '400502', 'SEC0C.DTA', 1)
    c2 = ('1998-99', '400502', 'SEC0C.DTA', 2)
    if problem == 'conflicting_intake':
        raw.loc[c1, 'parsed_date'] = pd.Timestamp('1997-12-31')
    if problem == 'reversed_anchor':
        raw.loc[c2, 'parsed_date'] = pd.Timestamp('1997-12-31')
    result, inputs = vd._reconcile(raw, '1998-99')
    assert result.index.get_level_values('visit').tolist() == [1, 2, 3]
    assert result.iloc[0].Int_t == dates[0]
    assert result.Derivation.isna().all()
    assert inputs.xs('SEC0B.DTA', level='source').assigned_visit.isna().all()


def test_invalid_date_and_unmatched_household_survive_inputs():
    raw = glss4(pd.date_range('1998-01-01', periods=7))
    extra = raw.iloc[[-1]].copy()
    extra.index = pd.MultiIndex.from_tuples(
        [('1998-99', 'unmatched', 'SEC0B.DTA', 100)], names=raw.index.names)
    raw = pd.concat([raw, extra])
    raw.loc[('1998-99', '400502', 'SEC0B.DTA', 6), 'parsed_date'] = pd.NaT
    _, inputs = vd._reconcile(raw, '1998-99')
    assert len(inputs) == len(raw)
    assert inputs.iloc[-2].assignment_reason == 'invalid_or_missing_date'
    assert pd.isna(inputs.iloc[-1].assigned_visit)


def test_valid_typo_is_not_silently_repaired_or_rejected():
    dates = list(pd.date_range('1998-01-01', periods=7))
    dates[-1] = pd.Timestamp('2098-01-07')
    result = vd.derive_glss4_visit_assignments(glss4(dates))
    # Explicit modelling limitation: seven ordered valid dates fill seven
    # slots even when a reported year looks implausible. No date repair.
    assert result.iloc[-1].Int_t == dates[-1]
    assert result.iloc[-1].Derivation == vd.KEY4


def test_glss5_two_pages_recover_duplicate_codes_without_spacing_rule():
    dates = pd.to_datetime(['2006-01-01', '2006-01-02', '2006-01-05',
                            '2006-01-06', '2006-01-10', '2006-01-15',
                            '2006-01-16', '2006-01-17', '2006-01-20',
                            '2006-01-28', '2006-02-03'])
    raw = raw_frame('2005-06', {
        'parta/sec0.dta': [(None, dates[0])],
        'partb/secb0.dta': [(None, dates[0])],
        'partb/secb1.dta': list(zip([2, 3, 4, 5, 6, 7, 8, 9, 9, 11], dates[1:])),
        'partb/secb2.dta': list(zip(range(2, 12), dates[1:])),
    })
    result, inputs = vd._reconcile(raw, '2005-06')
    assert result.Int_t.tolist() == list(dates)
    assert result.Derivation.iloc[1:].eq(vd.KEY5).all()
    assert len(inputs) == len(raw)
    raw.loc[('2005-06', '400502', 'partb/secb0.dta', 1), 'parsed_date'] = dates[1]
    result, inputs = vd._reconcile(raw, '2005-06')
    assert len(result) == 1
    assert result.iloc[0].Int_t == dates[0]
    assert inputs.iloc[1:].assigned_visit.isna().all()


def test_original_reason_field_is_not_overwritten_by_diagnostics():
    raw = glss4(pd.date_range('1998-01-01', periods=7))
    raw['reason'] = range(len(raw))
    _, inputs = vd._reconcile(raw, '1998-99')
    pd.testing.assert_frame_equal(inputs[raw.columns], raw)
    assert 'assignment_reason' in inputs
