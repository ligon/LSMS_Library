"""Infer GLSS4/5 calendar visits from reported dates, without changing dates.

Modelling assumption: distinct valid dates are distinct scheduled contacts.
All monotone injections into the protocol's visit ordinals are considered;
missing visits are allowed. Only assignments common to every candidate are
served. No expected day spacing, date repair, or source-row tie-break is used.
An otherwise valid mistyped date can therefore acquire an inferred visit: this
is a disclosed limitation, not evidence that the date is correct. Raw records
remain available through the registered inputs functions, including duplicates.
See .coder/ledger/ghanalss-visit-dates.md, Authorized inference extension.
"""
from functools import lru_cache
from itertools import combinations

import pandas as pd

from lsms_library.local_tools import (format_id, get_dataframe,
                                      get_formatting_functions)
from lsms_library.paths import countries_root

KEY4 = 'GhanaLSS::interview_date::anchored-chronology-glss4'
KEY5 = 'GhanaLSS::interview_date::anchored-chronology-glss5'
_SPECS = {
    '1998-99': (
        ('SEC0A.DTA', ('dd', 'mm', 'yy')),
        ('SEC0C.DTA', ('s0vday', 's0vmonth', 's0vyear')),
        ('SEC0B.DTA', ('s0dy_sum', 's0mt_sum', 's0yr_sum')),
    ),
    '2005-06': (
        ('parta/sec0.dta', ('ddate', 'mdate', 'ydate')),
        ('partb/secb0.dta', ('day', 'month', 'year')),
        ('partb/secb1.dta', ('intday', 'intmon', 'intyr')),
        ('partb/secb2.dta', ('vday', 'vmon', 'vyr')),
    ),
}


def _load_raw(wave):
    """Read every original field; reuse the established wave date parser."""
    wave_dir = countries_root() / 'GhanaLSS' / wave
    parser = get_formatting_functions(wave_dir / '_' / 'mapping.py',
                                     f'visit_dates_{wave}')['Int_t']
    @lru_cache(maxsize=None)
    def parse_triplet(day, month, year):
        # Date components repeat across households and pages. Reuse the
        # existing scalar parser once per triplet, without changing its rules.
        return parser(pd.Series([day, month, year]))

    parts = []
    for filename, columns in _SPECS[wave]:
        raw = get_dataframe(str(wave_dir / 'Data' / filename),
                            convert_categoricals=False).copy()
        # Original components stay untouched, including impossible dates.
        raw['parsed_date'] = pd.to_datetime([
            parse_triplet(*values)
            for values in raw[list(columns)].itertuples(index=False, name=None)
        ])
        if wave == '1998-99':
            raw['i'] = [format_id(c) + format_id(h, zeropadding=2)
                        for c, h in zip(raw['clust'], raw['nh'])]
        else:
            raw['i'] = raw['hhid'].map(format_id)
        raw['t'] = wave
        raw['source'] = filename
        raw['source_record'] = range(1, len(raw) + 1)
        parts.append(raw.set_index(['t', 'i', 'source', 'source_record']))
    return pd.concat(parts, sort=False)


def _reconcile(raw, wave):
    """Return canonical dates and all annotated inputs; never mutate raw."""
    if set(raw.index.get_level_values('t')) - {wave}:
        raise ValueError(f'Visit inputs must be scoped to {wave}')
    n, key = (7, KEY4) if wave == '1998-99' else (11, KEY5)
    intake_source = _SPECS[wave][0][0]
    annotated = raw.copy()
    annotated['assigned_visit'] = pd.array([pd.NA] * len(raw), dtype='Int64')
    annotated['assignment_reason'] = pd.array(['unresolved'] * len(raw), dtype='string')
    flat = annotated.reset_index()
    # Accumulate scalar annotations outside pandas extension arrays; repeated
    # scalar writes can copy an entire Arrow string column for every record.
    assigned = [pd.NA] * len(flat)
    reasons = ['unresolved'] * len(flat)
    rows = []
    for (t, household), group in flat.groupby(['t', 'i'], sort=False, dropna=False):
        anchors = {}
        conflict = False
        intake = group[group['source'] == intake_source]
        intake_dates = set(intake['parsed_date'].dropna())
        if len(intake_dates) == 1:
            anchors[1] = next(iter(intake_dates))
        else:
            conflict = True
        anchor_rows = {}
        if 1 in anchors:
            anchor_rows[1] = intake[intake['parsed_date'] == anchors[1]].index
        if wave == '1998-99':
            page = group[group['source'] == 'SEC0C.DTA']
            for visit in (1, 2, 3):
                part = page[page['visitcd'] == visit]
                values = set(part['parsed_date'].dropna())
                if len(values) > 1:
                    conflict = True
                elif values:
                    date = next(iter(values))
                    if visit == 1:
                        if 1 not in anchors or date != anchors[1]:
                            conflict = True
                    else:
                        anchors[visit] = date
                        anchor_rows[visit] = part[part['parsed_date'] == date].index
        else:
            # Part B's intake is corroboration only, never a replacement for
            # the current Part A intake. Missing is not contradictory.
            other = group[group['source'] == 'partb/secb0.dta']
            values = set(other['parsed_date'].dropna())
            if values and (1 not in anchors or values != {anchors[1]}):
                conflict = True
        for visit, indices in anchor_rows.items():
            date = anchors[visit]
            rows.append((t, household, visit, date, pd.NA))
            for idx in indices:
                assigned[idx] = visit
                reasons[idx] = 'reported_anchor'

        # Preserve reported anchors even when their dates contradict the event
        # model. Such a household contributes no inferred assignments.
        dates = sorted(set(group['parsed_date'].dropna()) | set(anchors.values()))
        candidates = []
        if not conflict and 1 in anchors and len(dates) <= n:
            for ordinals in combinations(range(1, n + 1), len(dates)):
                candidate = dict(zip(dates, ordinals))
                if all(candidate[date] == visit for visit, date in anchors.items()):
                    candidates.append(candidate)
        assignments = {}
        if candidates:
            for date in dates:
                possible = {candidate[date] for candidate in candidates}
                if len(possible) == 1:
                    assignments[date] = next(iter(possible))
        for idx, record in group.iterrows():
            if reasons[idx] == 'reported_anchor':
                continue
            date = record['parsed_date']
            if pd.isna(date):
                reason = 'invalid_or_missing_date'
            elif conflict:
                reason = 'conflicting_or_missing_anchor'
            elif not candidates:
                reason = 'no_consistent_assignment'
            elif date not in assignments:
                reason = 'ambiguous_visit'
            else:
                assigned[idx] = assignments[date]
                reason = 'anchored_date' if assignments[date] in anchors else 'inferred_chronology'
            reasons[idx] = reason
        for date, visit in assignments.items():
            if visit not in anchors:
                rows.append((t, household, visit, date, key))
    result = pd.DataFrame(rows, columns=['t', 'i', 'visit', 'Int_t', 'Derivation'])
    result['visit'] = result['visit'].astype('Int64')
    result['Int_t'] = pd.to_datetime(result['Int_t'])
    result['Derivation'] = result['Derivation'].astype('string')
    result = result.set_index(['t', 'i', 'visit']).sort_index()
    assert result.index.is_unique, 'visit inference must produce one date per visit'
    flat['assigned_visit'] = pd.array(assigned, dtype='Int64')
    flat['assignment_reason'] = pd.array(reasons, dtype='string')
    inputs = flat.set_index(['t', 'i', 'source', 'source_record'])
    return result, inputs


def derive_glss4_visit_assignments(raw):
    """Fixed seven-visit GLSS4 construction; dates are reported, visit inferred."""
    return _reconcile(raw, '1998-99')[0]


def derive_glss5_visit_assignments(raw):
    """Fixed eleven-visit GLSS5 construction; Part A intake takes precedence."""
    return _reconcile(raw, '2005-06')[0]


def inputs_glss4(wave=None):
    """All SEC0A/B/C answers at source grain, including unresolved records."""
    if wave not in (None, '1998-99'):
        raise ValueError('GLSS4 visit inputs are available only for 1998-99')
    return _reconcile(_load_raw('1998-99'), '1998-99')[1]


def inputs_glss5(wave=None):
    """All sec0/secb0/secb1/secb2 answers and their inferred visit linkage."""
    if wave not in (None, '2005-06'):
        raise ValueError('GLSS5 visit inputs are available only for 2005-06')
    return _reconcile(_load_raw('2005-06'), '2005-06')[1]


def build_visit_dates(wave):
    """Source-backed integration entry point; no alternate inference options."""
    if wave not in _SPECS:
        raise ValueError(f'No registered chronological visit inference for {wave}')
    return _reconcile(_load_raw(wave), wave)[0]
