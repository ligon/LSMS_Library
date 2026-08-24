"""``get_data_file`` must accept the path conventions callers actually use.

GH #716.  ``get_data_file`` is documented as taking a *countries-relative*
path, but ``local_tools.get_dataframe`` -- the accessor ``CLAUDE.md`` points
everyone at first -- also accepts the *repo-relative* form, which is how paths
are written in scripts and in every ``CONTENTS.org`` example.  Passing that
form to ``get_data_file`` used to return ``None`` for a file on disk, and the
``None`` then propagated as the string ``'None'`` into whatever consumed it.

These tests pin the normalisation.  They are deliberately structural: the
whole point of ``get_data_file`` is to fetch files that are *not* on disk yet,
so the mapping must not depend on existence.
"""
from pathlib import Path

import pytest

from lsms_library.data_access import _as_countries_relative, _COUNTRIES_DIR

REL = Path('GhanaLSS/1998-99/Documentation/GHA_1998_GLSS_Report_EN.pdf')


@pytest.mark.parametrize('given', [
    REL,                                              # documented form
    Path('lsms_library/countries') / REL,             # repo-relative
    Path('countries') / REL,                          # partial prefix
    _COUNTRIES_DIR / REL,                             # absolute
])
def test_all_accepted_forms_normalise_to_countries_relative(given):
    assert _as_countries_relative(Path(given)) == REL


def test_normalisation_does_not_depend_on_the_file_existing():
    """The fetch path exists to get files that are absent -- so must this."""
    ghost = Path('Nowhere/2999-00/Data/absent.dta')
    assert not (_COUNTRIES_DIR / ghost).exists()
    assert _as_countries_relative(Path('lsms_library/countries') / ghost) == ghost


def test_uninterpretable_paths_pass_through_unchanged():
    """An absolute path outside the countries root is returned as-is, so a
    genuinely unavailable file behaves exactly as it did before #716."""
    outside = Path('/tmp/somewhere/else.dta')
    assert _as_countries_relative(outside) == outside


def test_longest_prefix_wins():
    """'lsms_library/countries' must be stripped in full, not just 'countries'."""
    got = _as_countries_relative(Path('lsms_library/countries') / REL)
    assert got.parts[0] == 'GhanaLSS', got


def test_a_country_directory_is_never_mistaken_for_the_prefix():
    """Stripping is anchored at the START of the path, so a wave or file named
    'countries' deeper in cannot trigger it."""
    tricky = Path('GhanaLSS/1998-99/Data/countries/x.dta')
    assert _as_countries_relative(tricky) == tricky


def test_peru_ssp_case_from_the_original_report():
    """The other path that silently returned None during the #713 work."""
    rel = Path('Peru/1990/Data/N00A.SSP')
    assert _as_countries_relative(Path('lsms_library/countries') / rel) == rel
