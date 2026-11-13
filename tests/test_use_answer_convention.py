"""The plot_inputs USE-ANSWER convention, checked where it can drift.

An ``input`` label ending in ``'?'`` is the survey's own yes/no answer to a
use question asked once per plot about a GROUP of inputs, not an input
(maintainer decision on GH #1118, 2026-10-22;
``transformations.is_use_answer_label``).  The answer itself rides in
``Applied``.  Documented beside the ``plot_inputs`` index in
``lsms_library/data_info.yml``.

Three checks:

* every country whose ``harmonize_input`` table (at any categorical-mapping
  level: country ``_/``, wave ``{w}/_/``, file or directory spelling) serves a
  ``?`` label declares ``Applied`` in its ``plot_inputs`` scheme -- a ``?``
  row with no declared ``Applied`` column would be served without its answer.
  Scope: ``harmonize_input`` tables ONLY; a ``?`` label written inline in a
  script (GhanaLSS writes its ``input`` labels as constants in its wave
  ``plot_inputs.py``) or read from another table is not seen;
* Mali's ``Applied`` stays REQUIRED (plain ``bool``), unlike Ethiopia's and
  Malawi's optional one, so the built-table guards keep checking it;
* ``nitrogen_kg`` counts the use-answer rows it skips on
  ``attrs['nitrogen_input_match']['use_answer_rows']`` -- always present, so
  ``matched_rows + unmatched_rows + use_answer_rows`` is every input row.

Config-only and synthetic: no data build.
"""
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.local_tools import all_dfs_from_orgfile, categorical_mapping_files
from lsms_library.paths import countries_root


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that tolerates the ``!make`` tag (as test_schema_consistency)."""


_SchemeLoader.add_constructor('!make', lambda loader, node: {'__make__': True})


def _checkout_root():
    root = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
    return root


def _plot_inputs_spec(country):
    path = countries_root() / country / '_' / 'data_scheme.yml'
    with open(path, encoding='utf-8') as fh:
        data = yaml.load(fh, Loader=_SchemeLoader) or {}
    return ((data.get('Data Scheme') or {}).get('plot_inputs')), path


def _use_answer_labels_by_country():
    """{country: {label: [org file, ...]}} for every '?' harmonize_input label."""
    _checkout_root()
    found = {}
    for cdir in sorted(p for p in countries_root().iterdir() if p.is_dir()):
        levels = [cdir / '_'] + sorted(p / '_' for p in cdir.iterdir()
                                       if p.is_dir() and p.name != '_' and (p / '_').is_dir())
        for level in levels:
            for org in categorical_mapping_files(level):
                for name, table in all_dfs_from_orgfile(org).items():
                    if name.strip().lower() != 'harmonize_input':
                        continue
                    cols = ['Preferred Label'] if 'Preferred Label' in table.columns \
                        else list(table.columns)
                    for col in cols:
                        for value in table[col].dropna().astype(str):
                            if T.is_use_answer_label(value):
                                found.setdefault(cdir.name, {}).setdefault(
                                    value.strip(), []).append(str(org))
    return found


_FOUND = _use_answer_labels_by_country()


def test_the_sweep_is_not_vacuous():
    """Mali serves two use answers; if the sweep cannot see them it sees nothing."""
    assert set(_FOUND.get('Mali', {})) == {'Inorganic Fertilizer?',
                                           'Phytosanitary products?'}, _FOUND


@pytest.mark.parametrize('country', sorted(_FOUND))
def test_a_country_serving_use_answers_declares_applied(country):
    spec, path = _plot_inputs_spec(country)
    assert isinstance(spec, dict), (
        f"{country}: harmonize_input serves use-answer label(s) "
        f"{sorted(_FOUND[country])} but {path} declares no plot_inputs table")
    assert 'Applied' in spec, (
        f"{country}: harmonize_input serves use-answer label(s) "
        f"{sorted(_FOUND[country])}, whose answer rides in `Applied`, but the "
        f"plot_inputs scheme in {path} does not declare `Applied`")


def test_mali_applied_stays_required():
    """Mali's `Applied` is a REQUIRED column, unlike Ethiopia's and Malawi's
    `optional: true` one.  `optional: true` would remove it from
    `country._required_scheme_columns`, which switches off
    `Country._assert_built_required_columns` and Site B (null_read_audit) for
    it -- and buys nothing: Site B warns only when a wave's column is 100%
    null, both EAC-I waves serve answer rows, and the base build emitted 0
    `Applied` warnings.  Keep the guard."""
    from lsms_library.country import _required_scheme_columns
    spec, path = _plot_inputs_spec('Mali')
    assert spec['Applied'] == 'bool', (path, spec['Applied'])
    assert 'Applied' in _required_scheme_columns(spec), path


_NAMES = ['t', 'i', 'plot_id', 'input', 'crop']


def _frame(rows):
    idx = pd.MultiIndex.from_tuples([r[:5] for r in rows], names=_NAMES)
    return pd.DataFrame({'Quantity': [r[5] for r in rows], 'u': [r[6] for r in rows]},
                        index=idx)


def test_nitrogen_tally_always_carries_use_answer_rows():
    plain = _frame([('2014-15', 'h1', '1_1', 'Urea', 'x', 50.0, 'Kg'),
                    ('2014-15', 'h1', '1_1', 'Seed', 'Mil', 3.0, 'Kg')])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', T.NutrientCoverageWarning)
        tally = T.nitrogen_kg(plain).attrs['nitrogen_input_match']
    assert tally['use_answer_rows'] == 0
    assert tally['matched_rows'] + tally['unmatched_rows'] == len(plain)


def test_nitrogen_tally_counts_the_skipped_use_answers():
    rows = [('2014-15', 'h1', '1_1', 'Urea', 'x', 50.0, 'Kg'),
            ('2014-15', 'h1', '1_1', 'Inorganic Fertilizer?', 'x', pd.NA, 'Unknown'),
            ('2014-15', 'h1', '1_1', 'Phytosanitary products?', 'x', pd.NA, 'Unknown'),
            ('2014-15', 'h1', '1_2', 'Inorganic Fertilizer?', 'x', pd.NA, 'Unknown')]
    frame = _frame(rows)
    with warnings.catch_warnings():
        warnings.simplefilter('error', T.NutrientCoverageWarning)
        out = T.nitrogen_kg(frame)
    tally = out.attrs['nitrogen_input_match']
    assert tally['use_answer_rows'] == 3
    assert tally['matched_rows'] == 1 and tally['unmatched_rows'] == 0
    assert tally['matched_rows'] + tally['unmatched_rows'] + tally['use_answer_rows'] == len(frame)
    # The skipped rows add no label to either side of the label partition.
    assert not any(T.is_use_answer_label(k) for k in tally['unmatched_labels'])
    assert tally['matched_labels'] == ['Urea']
    assert out['Nitrogen_kg'].tolist() == pytest.approx([50.0 * 0.46])
