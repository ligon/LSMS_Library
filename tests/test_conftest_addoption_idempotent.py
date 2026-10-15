"""GH #734: the root ``conftest.py`` may be loaded twice (worktree + the
``.pth``-pinned main checkout), so ``pytest_addoption`` must tolerate a
second registration of ``--rebuild`` / ``--rebuild-caches`` / ``--no-purge``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from _pytest.config.argparsing import Parser

ROOT_CONFTEST = Path(__file__).resolve().parent.parent / "conftest.py"


def _load_root_conftest():
    spec = importlib.util.spec_from_file_location("_root_conftest_for_test", ROOT_CONFTEST)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_pytest_addoption_is_idempotent():
    mod = _load_root_conftest()
    parser = Parser()
    mod.pytest_addoption(parser)
    mod.pytest_addoption(parser)          # used to raise "option names ... already added"
    registered = {n for opt in parser._anonymous.options for n in opt.names()}
    assert {"--rebuild", "--rebuild-caches", "--no-purge"} <= registered


def test_only_the_already_added_duplicate_is_swallowed():
    """A ValueError that is not the duplicate-name one propagates."""
    mod = _load_root_conftest()

    class _Parser:
        def addoption(self, *names, **kwargs):
            raise ValueError("some other argparse complaint")

    with pytest.raises(ValueError, match="some other"):
        mod._addoption_once(_Parser(), "--rebuild", action="store_true")

    class _DupParser:
        def addoption(self, *names, **kwargs):
            raise ValueError("option names {'--rebuild'} already added")

    mod._addoption_once(_DupParser(), "--rebuild", action="store_true")   # no raise
