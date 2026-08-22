"""Regression coverage for the ``.DAT``/``.DCT`` parse path (GH #704).

The oldest surveys in the corpus (Côte d'Ivoire CILSS 1985-89, Ghana
GLSS1/GLSS2, Nicaragua EMNV 2001) ship 531 ``.DAT``/``.DCT`` pairs.  The
``.DCT`` declares a fixed-width layout; the shipped ``.DAT`` is in fact
comma-delimited with a header row.  *Both* readings fail silently on the
wrong file:

- a dictionary-driven ``read_fwf`` on the comma-delimited payload returns
  a right-shaped frame of shredded strings with an all-NaN numeric tail
  (the #704 finding), and
- a bare ``read_csv`` on a genuinely fixed-width payload returns a single
  column of whole lines.

So the reader must *decide*, not assume.  These tests are all synthetic
-- no DVC, no network, no fixtures from the corpus -- so they run in the
cache-gated fast tier.

Each of the first four tests fails on the pre-#704 reader:
``test_missing_value_sentinel_becomes_na`` (it shipped ``str`` columns
full of ``'.'``), ``test_true_fixed_width_is_not_read_as_csv`` (one
column), ``test_fixed_width_with_commas_in_a_text_field``
(comma-shredded), and ``test_undecidable_layout_raises`` (it silently
returned garbage).
"""

from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.local_tools import (DatLayoutError, _detect_dat_layout,
                                      _load_dct_for, _parse_dct,
                                      get_dataframe)

# A three-variable dictionary in the corpus's own idiom: start column,
# width, type code, name.  Fields are 12 wide on a stride of 13, so
# columns 12 and 25 (0-based) are inter-field gaps.
DCT = """dictionary
parmfile=ascii
variables
     1     12 R CLUST
    14     12 R POPUL
    27     12 c NAME
endvars
"""

NAMES = ['CLUST', 'POPUL', 'NAME']


def _pair(tmp_path, dat_text, dct_text=DCT, stem='COMM'):
    """Write a ``.DAT``/``.DCT`` pair and return the ``.DAT`` path."""
    dat = tmp_path / f'{stem}.DAT'
    dat.write_text(dat_text)
    if dct_text is not None:
        (tmp_path / f'{stem}.DCT').write_text(dct_text)
    return str(dat)


def _fw_row(clust, popul, name):
    """One fixed-width record matching ``DCT``: 12/1/12/1/12 = 38 chars."""
    return f'{clust:>12} {popul:>12} {name:<12}'


# ---------------------------------------------------------------------------
# The dictionary
# ---------------------------------------------------------------------------

def test_parse_dct_reads_names_and_colspecs():
    names, colspecs = _parse_dct(DCT)
    assert names == NAMES
    assert colspecs == [(0, 12), (13, 25), (26, 38)]


def test_parse_dct_accepts_the_separator_declaring_variant():
    """``CotedIvoire/1988-89/Data/SEC10A.DCT`` is the corpus's one
    dictionary that declares its own layout (``names=y``,
    ``separator=,``) instead of ``parmfile=ascii``.  It must still
    parse -- it carries the same ``variables``/``endvars`` block."""
    text = DCT.replace('parmfile=ascii', 'names=y\nseparator=,')
    names, _ = _parse_dct(text)
    assert names == NAMES


def test_parse_dct_rejects_an_unrecognised_grammar():
    """A dictionary we only half-understand must not license a read.
    Returning ``None`` leaves the caller on the legacy reader chain."""
    text = DCT.replace('    14     12 R POPUL', '  _column(14) float POPUL %12f')
    assert _parse_dct(text) is None


def test_parse_dct_rejects_a_file_with_no_variables_block():
    assert _parse_dct('dictionary\nparmfile=ascii\n') is None


# ---------------------------------------------------------------------------
# Layout detection
# ---------------------------------------------------------------------------

def test_detect_header_row():
    names, colspecs = _parse_dct(DCT)
    lines = ['CLUST,POPUL,NAME', '101,2000,Accra']
    assert _detect_dat_layout(lines, names, colspecs) == 'csv-header'


def test_detect_header_row_is_case_insensitive():
    names, colspecs = _parse_dct(DCT)
    lines = ['clust,popul,name', '101,2000,Accra']
    assert _detect_dat_layout(lines, names, colspecs) == 'csv-header'


def test_detect_fixed_width():
    names, colspecs = _parse_dct(DCT)
    lines = [_fw_row('101', '2000', 'Accra'), _fw_row('102', '750', 'Kumasi')]
    assert _detect_dat_layout(lines, names, colspecs) == 'fixed'


def test_detect_headerless_delimited():
    names, colspecs = _parse_dct(DCT)
    lines = ['101,2000,Accra', '102,750,Kumasi']
    assert _detect_dat_layout(lines, names, colspecs) == 'csv-noheader'


def test_a_comma_delimited_line_never_reads_as_fixed_width():
    """The load-bearing asymmetry: comma-delimited numeric payloads carry
    no spaces, so an inter-field gap column is never blank in one."""
    names, colspecs = _parse_dct(DCT)
    lines = ['101,2000,Accra']
    assert _detect_dat_layout(lines, names, colspecs) != 'fixed'


# ---------------------------------------------------------------------------
# End-to-end through ``get_dataframe``
# ---------------------------------------------------------------------------

def test_missing_value_sentinel_becomes_na(tmp_path):
    """The live #704 defect: ``.`` is Stata's ASCII missing marker.  Left
    unhandled, one missing value silently demotes a whole numeric column
    to ``str`` -- which is what every one of the corpus's 531 pairs did."""
    fn = _pair(tmp_path, 'CLUST,POPUL,NAME\n101,2000,Accra\n102,.,Kumasi\n')
    df = get_dataframe(fn)

    assert list(df.columns) == NAMES
    assert len(df) == 2
    assert pd.api.types.is_numeric_dtype(df['POPUL'])
    assert df['POPUL'].iloc[0] == 2000
    assert pd.isna(df['POPUL'].iloc[1])
    # A decimal point *inside* a value is not a missing marker.
    assert df['NAME'].tolist() == ['Accra', 'Kumasi']


def test_decimal_values_are_not_mistaken_for_the_sentinel(tmp_path):
    fn = _pair(tmp_path, 'CLUST,POPUL,NAME\n101,2.5,Accra\n')
    df = get_dataframe(fn)
    assert df['POPUL'].iloc[0] == pytest.approx(2.5)


def test_true_fixed_width_is_not_read_as_csv(tmp_path):
    """The mirror-image failure the detector also has to prevent: a bare
    ``read_csv`` returns a single column of whole lines here."""
    fn = _pair(tmp_path, '\n'.join([_fw_row('101', '2000', 'Accra'),
                                    _fw_row('102', '750', 'Kumasi')]) + '\n')
    df = get_dataframe(fn)

    assert list(df.columns) == NAMES
    assert df.shape == (2, 3)
    assert df['CLUST'].tolist() == [101, 102]
    assert df['POPUL'].tolist() == [2000, 750]
    assert df['NAME'].tolist() == ['Accra', 'Kumasi']


def test_fixed_width_sentinel_becomes_na(tmp_path):
    fn = _pair(tmp_path, _fw_row('101', '.', 'Accra') + '\n'
                         + _fw_row('102', '750', 'Kumasi') + '\n')
    df = get_dataframe(fn)
    assert pd.isna(df['POPUL'].iloc[0])
    assert df['POPUL'].iloc[1] == 750


def test_fixed_width_with_commas_in_a_text_field(tmp_path):
    """The stated misfire case.  A fixed-width record whose text field
    contains commas fails the header test (its first record is not the
    dictionary's name list) and passes the fixed-width test (the commas
    sit *inside* a declared field, so the gap columns are still blank).
    It is read fixed-width, and the comma stays in the value."""
    fn = _pair(tmp_path, _fw_row('101', '2000', 'Accra, GA') + '\n'
                         + _fw_row('102', '750', 'Kumasi, AS') + '\n')
    df = get_dataframe(fn)

    assert df.shape == (2, 3)
    assert df['NAME'].tolist() == ['Accra, GA', 'Kumasi, AS']
    assert df['CLUST'].tolist() == [101, 102]


def test_headerless_delimited_takes_names_from_the_dictionary(tmp_path):
    fn = _pair(tmp_path, '101,2000,Accra\n102,.,Kumasi\n')
    df = get_dataframe(fn)
    assert list(df.columns) == NAMES
    assert pd.isna(df['POPUL'].iloc[1])


def test_undecidable_layout_raises(tmp_path):
    """Neither hypothesis fits: five comma-separated fields against a
    three-variable dictionary, too short to be the declared record.
    Silently returning a plausible frame is what #704 was."""
    fn = _pair(tmp_path, '1,2,3,4,5\n6,7,8,9,10\n')
    with pytest.raises(DatLayoutError) as exc:
        get_dataframe(fn)
    assert 'cannot establish the layout' in str(exc.value)


def test_dat_without_a_dictionary_is_left_on_the_legacy_path(tmp_path):
    """Rwanda's ``PartA/B/C.dat`` and Pakistan's ``WEIGHTS.DAT`` ship with
    no ``.DCT``.  With no dictionary there are no colspecs to build, so
    behaviour must be exactly what it was."""
    fn = _pair(tmp_path, 'CLUST,POPUL\n101,2000\n', dct_text=None)
    df = get_dataframe(fn)
    assert list(df.columns) == ['CLUST', 'POPUL']
    assert _load_dct_for(fn) is None


def test_unparseable_dictionary_is_left_on_the_legacy_path(tmp_path):
    fn = _pair(tmp_path, 'CLUST,POPUL\n101,2000\n',
               dct_text='infile dictionary {\n  _column(1) int CLUST %12f\n}\n')
    assert _load_dct_for(fn) is None
    df = get_dataframe(fn)
    assert list(df.columns) == ['CLUST', 'POPUL']
