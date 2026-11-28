"""Ethiopia food and community labels don't depend on the process codec."""
import builtins
from pathlib import Path

import pytest

from lsms_library.countries.Ethiopia._ import ethiopia


ORG = Path(ethiopia.__file__).resolve().with_name('categorical_mapping.org')


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


def test_food_label_union_is_locale_independent(monkeypatch):
    monkeypatch.chdir(ORG.parents[1] / '2011-12' / '_')
    expected = _with_org_default(
        monkeypatch, 'utf-8', ethiopia.harmonize_food_union_map)
    assert expected
    assert _with_org_default(
        monkeypatch, 'ascii', ethiopia.harmonize_food_union_map) == expected


def test_community_price_prefix_reads_all_three_tables_under_ascii(monkeypatch):
    class ReachedFrame(Exception):
        pass

    class NoSurveyFrame:
        @property
        def index(self):
            raise ReachedFrame

    def read_prefix():
        with pytest.raises(ReachedFrame):
            ethiopia.community_prices_for_wave('2011-12', NoSurveyFrame(), {})

    _with_org_default(monkeypatch, 'utf-8', read_prefix)
    _with_org_default(monkeypatch, 'ascii', read_prefix)
