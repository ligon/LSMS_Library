"""NEW proposed25 audit-union controls; source prepared, never executed.

Reuse the frozen195 actor/row assertions. Actual cold report accessors inspect
private invented ledgers; only writer kwargs are observed, no parquet payload or
actual formatting/native helper is executed. A fresh finite root guard is needed.
"""
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import country as library_country, datetime_parse, local_tools
from tests.test_niger_producer_compiler_integration import actor, exact


A, B, MIDDLE = '2011-12', '2021-22', '2014-15'


def grain(table, selector, count=1, *, wave=None):
    return {
        'country': 'Niger', 'table': table, 'site': 'reduce_to_agreed',
        'kind': 'cells_blanked', 'wave': selector if wave is None else wave,
        'wave_folder': selector, 'levels': ['t', 'i'], 'rows': 4, 'groups': 2,
        'conflicting_groups': 1, 'cells_blanked': count,
        'columns': {'Reported': count}, 'examples': ['invented-only-key'],
        'collapsed_rows': 2, 'destroyed': 0, 'nan_key_rows': 0,
    }


def date(table, selector, parsed=2, *, wave=None):
    return {
        'country': 'Niger', 'table': table, 'site': 'parse_datetime',
        'column': 'Date', 'wave': selector if wave is None else wave,
        'wave_folder': selector, 'kind': 'date', 'calendar': 'gregorian',
        'n': parsed + 1, 'parsed': parsed, 'missing': 1,
        'unparsed': 0, 'out_of_range': 0, 'ambiguous': 0,
        'sentinel': 0, 'outside_window': 0, 'examples': {},
    }


def drop(table, year):
    return {
        'country': 'Niger', 'table': table, 'wave': year,
        'file': 'invented-only-' + year, 'missing_required': [],
        'missing_optional': ['invented_optional'],
        'cause': 'invented unavailable source; not an actual source observation',
    }


@pytest.mark.parametrize('profile', [
    'distinct', 'empty_missing', 'collision', 'duplicate', 'legacy',
])
def test_yaml_composite_writer_preserves_whole_diagnostics(actor, monkeypatch, profile):
    table, _, _, _, _, _, frames, _, _, outputs, install, execute = actor
    # Also cover a physical destination equal to the first logical cache name.
    folder = A if profile == 'duplicate' else 'shared'
    install(folders={A: folder, B: folder})
    ga, gb, gm = grain(table, A), grain(table, B, 2), grain(table, MIDDLE)
    da, db, dm = date(table, A), date(table, B, 3), date(table, MIDDLE)
    xa, xb, xm = drop(table, A), drop(table, B), drop(table, MIDDLE)
    grain_reports, date_reports, drops = [gb, gm, ga], [db, dm, da], [xm, xb, xa]
    expected_g, expected_d, expected_x = [ga, gb], [da, db], [xa, xb]
    middle_g, middle_d, middle_x = [gm], [dm], [xm]
    if profile == 'collision':
        # Same country/table/wave/site/column identities; differing whole
        # dictionaries must BOTH survive. A scalar-key merge would lose one.
        ga2, da2 = grain(table, A, 2), date(table, A, 4)
        grain_reports.append(ga2)
        date_reports.append(da2)
        expected_g, expected_d = [ga, ga2, gb], [da, da2, db]
    elif profile == 'duplicate':
        grain_reports.append(deepcopy(ga))
        date_reports.append(deepcopy(da))
        # Whole dict equality, not object identity, is the ledger rule.
    elif profile == 'legacy':
        gp = grain(table, folder, wave='invented-legacy-logical')
        dp = date(table, folder, wave='invented-legacy-logical')
        grain_reports.insert(0, gp)
        date_reports.insert(0, dp)
        expected_g, expected_d = [ga, gb, gp], [da, db, dp]
    if profile != 'empty_missing':
        # Actual accessor filtering must continue excluding unrelated/replayed
        # records. No invented selector accessor replaces the library API.
        grain_reports.extend([grain(table, '2099-00'), {**ga, 'from_cache': True}])
        date_reports.extend([date(table, '2099-00'), {**da, 'from_cache': True}])
        drops.extend([drop(table, '2099-00'), {**xa, 'from_cache': True}])
        gledger = {('Niger', table): grain_reports}
        dledger = {('Niger', table): date_reports}
        xledger = {('Niger', table): drops}
    else:
        # Empty-key versus absent-key accessors must both yield None at the
        # writer, with neither a fabricated report nor an empty list stamp.
        gledger, dledger, xledger = {('Niger', table): []}, {}, {}
        expected_g = expected_d = expected_x = None
        middle_g = middle_d = middle_x = None
    monkeypatch.setattr(library_country, '_GRAIN_LEDGER', gledger)
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', dledger)
    monkeypatch.setattr(library_country, '_BUILD_DROP_LEDGER', xledger)
    original_ledgers = deepcopy((gledger, dledger, xledger))
    writes = []
    original_writer = local_tools.to_parquet

    def observe_writer(frame, path, **kwargs):
        if kwargs.get('absolute_path') is True:
            writes.append((Path(path).parts[-3], deepcopy(kwargs)))
        return original_writer(frame, path, **kwargs)

    monkeypatch.setattr(local_tools, 'to_parquet', observe_writer)
    execute()
    assert writes == [
        (folder, {'absolute_path': True, 'grain_audit': expected_g,
                  'datetime_audit': expected_d, 'build_drops': expected_x}),
        (MIDDLE, {'absolute_path': True, 'grain_audit': middle_g,
                  'datetime_audit': middle_d, 'build_drops': middle_x}),
    ]
    assert (gledger, dledger, xledger) == original_ledgers
    assert len(outputs) == 1
    exact(outputs[0], pd.concat([frames[A], frames[B], frames[MIDDLE]]))
