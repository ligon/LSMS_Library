"""Uganda plot vocabulary reads don't depend on the process codec."""
import builtins
from pathlib import Path

import pytest

from lsms_library.countries.Uganda._ import uganda


ORG = Path(uganda.__file__).resolve().with_name('categorical_mapping.org')


def _with_org_default(monkeypatch, encoding, callback):
    original = builtins.open

    def open_org(file, mode='r', *args, **kwargs):
        if (isinstance(file, (str, Path)) and Path(file).name == ORG.name
                and Path(file).resolve() == ORG and 'b' not in mode
                and kwargs.get('encoding') is None):
            kwargs['encoding'] = encoding
        return original(file, mode, *args, **kwargs)

    with monkeypatch.context() as guard:
        guard.setattr(builtins, 'open', open_org)
        return callback()


def test_acquire_map_is_locale_independent(monkeypatch):
    expected = _with_org_default(
        monkeypatch, 'utf-8', uganda._harmonize_acquire_table)
    assert expected
    assert _with_org_default(
        monkeypatch, 'ascii', uganda._harmonize_acquire_table) == expected


def test_plot_features_prefix_reads_all_four_tables_under_ascii(monkeypatch):
    monkeypatch.chdir(ORG.parents[1] / '2005-06' / '_')

    class ReachedFrame(Exception):
        pass

    class NoSurveyFrame:
        @property
        def empty(self):
            raise ReachedFrame

    def read_prefix():
        with pytest.raises(ReachedFrame):
            uganda.plot_features_for_wave('2005-06', NoSurveyFrame(), None, {})

    _with_org_default(monkeypatch, 'utf-8', read_prefix)
    _with_org_default(monkeypatch, 'ascii', read_prefix)
