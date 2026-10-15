"""Magic-byte dispatch in ``local_tools.read_file`` (GH #699, GH #712).

``get_dataframe`` is a fallback chain, and a chain only works when a reader
handed the wrong kind of file FAILS.  The Stata reader does not reliably do
so: ``StataReader`` takes the first byte of any file as a release number, and
a byte in 102..115 (``'f'``..``'s'``) sends it into the old-format header
parser, which reads a garbage variable count and dies with whatever that
garbage dictates -- a ``KeyError`` from the type map on this stack, a
``MemoryError`` on another -- neither of which the chain's
``except (ValueError, struct.error)`` lists.  A CSV whose header begins
``hhid,`` was unreadable (#712); ``region,`` read fine.  Content-dependent.

And the chain had no SAS Transport (XPORT) branch at all, so Peru 1990's 39
``.SSP`` files were on disk and unreadable (#699); ``pyreadstat.read_xport``
is not the answer either -- it returns the right shape with 7 of 10 columns
NaN and no exception (``tests/test_null_read_guard.py``).

The fix sniffs rather than widening the ``except``: a wider ``except`` would
turn a genuinely broken ``.dta`` into a silent fall-through to ``read_csv``.
All tests here are synthetic (``tmp_path``), no DVC, no network; the one
real-blob test skips unless the Peru blob is already in the local cache.

Measured 2026-10-14 against every blob held locally (``_dvc_cache_path`` on
8,752 sidecars, 5,674 present): all 4,697 ``.dta`` blobs pass
``_is_stata_dta`` -- 67 of them ONLY because the fourth header byte is not
tested (Kyrgyz 1997, India 1997-98, GhanaLSS ``pov_gh5.dta`` carry
0x3f..0xff in the "unused" slot) -- and no ``.csv`` / ``.sav`` / ``.dat`` /
``.dct`` blob does.  Three ``.ssp`` blobs are XPORT; nothing else is.
"""

from __future__ import annotations

import io
import struct

import numpy as np
import pandas as pd
import pyreadstat
import pytest

from lsms_library.local_tools import (_dvc_cache_path, _is_sas_xport,
                                      _is_stata_dta, _sniff_head,
                                      get_dataframe)

XPORT_MAGIC = b'HEADER RECORD*******LIBRARY HEADER RECORD!!!!!!!'


# ---------------------------------------------------------------------------
# A hand-built SAS Transport (XPORT) V5 writer.  pandas reads XPORT but does
# not write it, so the fixture is assembled from the spec: 80-byte records,
# library + member + namestr headers, 140-byte NAMESTRs padded to a record
# boundary, an OBS header, then IBM-hex-float numerics and raw characters.
# ---------------------------------------------------------------------------

def _rec(b: bytes) -> bytes:
    assert len(b) <= 80, len(b)
    return b.ljust(80)


def _ibm_small_int(n: int) -> bytes:
    """IBM 360 hex float for 0 <= n <= 15: ``0.n (hex) * 16**1``."""
    if n == 0:
        return bytes(8)
    return bytes([0x41, n << 4]) + bytes(6)


_XPORT_MISSING = b'.' + bytes(7)


def write_xport(path, columns, rows):
    """*columns*: ``[(name, 'num' | ('char', width)), ...]``; *rows*: tuples.

    Numeric cells are small non-negative ints or ``None`` (SAS missing)."""
    hdr = b''
    hdr += _rec(XPORT_MAGIC + b'000000000000000000000000000000  ')
    hdr += _rec(b'SAS     SAS     SASLIB  6.06    bsd4.2                          09JUL92:16:50:38')
    hdr += _rec(b'09JUL92:16:50:38')
    hdr += _rec(b'HEADER RECORD*******MEMBER  HEADER RECORD!!!!!!!000000000000000001600000000140  ')
    hdr += _rec(b'HEADER RECORD*******DSCRPTR HEADER RECORD!!!!!!!000000000000000000000000000000  ')
    hdr += _rec(b'SAS     TESTDS  SASDATA 6.06    bsd4.2                          09JUL92:16:50:38')
    hdr += _rec(b'09JUL92:16:50:38' + b' ' * 16 + b'TEST DATASET'.ljust(40) + b' ' * 8)
    hdr += _rec(b'HEADER RECORD*******NAMESTR HEADER RECORD!!!!!!!000000%04d00000000000000000000  '
                % len(columns))
    namestrs = b''
    pos = 0
    for i, (name, kind) in enumerate(columns):
        ntype, length = (1, 8) if kind == 'num' else (2, kind[1])
        ns = struct.pack('>hhhh8s40s8shhh2s8shhl52s',
                         ntype, 0, length, i + 1, name.encode().ljust(8),
                         b'', b'', 0, 0, 0, b'', b'', 0, 0, pos, b'')
        assert len(ns) == 140
        namestrs += ns
        pos += length
    if len(namestrs) % 80:
        namestrs += b' ' * (80 - len(namestrs) % 80)
    data = b''
    for row in rows:
        for v, (name, kind) in zip(row, columns):
            if kind == 'num':
                data += _XPORT_MISSING if v is None else _ibm_small_int(v)
            else:
                data += v.encode('latin-1').ljust(kind[1])
    if len(data) % 80:
        data += b' ' * (80 - len(data) % 80)
    obs = _rec(b'HEADER RECORD*******OBS     HEADER RECORD!!!!!!!000000000000000000000000000000  ')
    with open(path, 'wb') as fh:
        fh.write(hdr + namestrs + obs + data)


def _stata_dta(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_stata(buf, write_index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# The predicates
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('head', [
    b'<stata_dta><header><release>118</release>',   # Stata 13+
    bytes([115, 2, 1, 0]) + bytes(12),               # Stata 12, LOHI
    bytes([114, 1, 1, 0]) + bytes(12),               # Stata 10/11, HILO
    bytes([105, 2, 1, 0x7f]) + bytes(12),            # Kyrgyz 1997: byte 3 junk
    bytes([110, 2, 1, 0xff]) + bytes(12),            # India 1997-98
    bytes([113, 2, 1, 0x74]) + bytes(12),            # GhanaLSS pov_gh5.dta
    bytes([102, 0, 1, 0]) + bytes(12),               # format 102 byteorder 0
])
def test_stata_sniff_accepts_every_header_shape_in_the_corpus(head):
    assert _is_stata_dta(head)


@pytest.mark.parametrize('head', [
    b'hhid,name,val\n0,x,0.0\n',    # 'h' = 104 is a Stata release number
    b'region,name,val\n',           # 'r' = 114 likewise
    b'id,name\n',
    b'$FL2@(#) SPSS DATA FILE',     # SPSS .sav
    b'PAR1',                        # parquet
    XPORT_MAGIC,
    b'',
    bytes([105, 2]),                # too short to say
    bytes([106, 2, 1, 0]),          # 106 is not a Stata release
    bytes([105, 3, 1, 0]),          # byteorder 3 does not exist
    bytes([105, 2, 2, 0]),          # filetype is always 1
])
def test_stata_sniff_rejects_what_is_not_stata(head):
    assert not _is_stata_dta(head)


def test_xport_sniff():
    assert _is_sas_xport(XPORT_MAGIC + b'000')
    assert not _is_sas_xport(b'HEADER RECORD*******MEMBER  HEADER RECORD')
    assert not _is_sas_xport(b'<stata_dta>')
    assert not _is_sas_xport(b'')


def test_sniff_head_leaves_a_stream_at_offset_zero(tmp_path):
    p = tmp_path / 'x.bin'
    p.write_bytes(b'0123456789' * 20)
    with open(p, 'rb') as f:
        f.read(5)
        assert _sniff_head(f, 8) == b'01234567'
        assert f.tell() == 0
    assert _sniff_head(str(p), 8) == b'01234567'
    assert _sniff_head(p, 8) == b'01234567'
    assert _sniff_head(str(tmp_path / 'absent.bin')) == b''


# ---------------------------------------------------------------------------
# #712 -- a CSV whose leading bytes look like a Stata header
# ---------------------------------------------------------------------------

def test_csv_whose_header_looks_like_stata_reads_as_csv(tmp_path):
    """Pre-fix: ``KeyError: np.uint8(249)`` out of ``StataReader`` (pandas
    3.0.6), escaping ``get_dataframe``.  Ten rows are enough on this stack."""
    n = 10
    df = pd.DataFrame({'hhid': np.arange(n), 'name': ['x'] * n,
                       'val': np.linspace(0, 1, n)})
    fn = tmp_path / 'hhid.csv'
    df.to_csv(fn, index=False)
    got = get_dataframe(str(fn))
    assert list(got.columns) == ['hhid', 'name', 'val']
    assert len(got) == n
    assert got['hhid'].tolist() == list(range(n))


@pytest.mark.parametrize('first', ['id', 'region', 'fid', 'sid', 'pid'])
def test_csv_first_column_name_no_longer_matters(tmp_path, first):
    df = pd.DataFrame({first: [1, 2, 3], 'val': [0.5, 1.5, 2.5]})
    fn = tmp_path / f'{first}.csv'
    df.to_csv(fn, index=False)
    got = get_dataframe(str(fn))
    assert list(got.columns) == [first, 'val']
    assert len(got) == 3


def test_a_real_dta_still_reads(tmp_path):
    df = pd.DataFrame({'hhid': [1, 2, 3], 'val': [0.5, 1.5, 2.5]})
    fn = tmp_path / 'real.dta'
    fn.write_bytes(_stata_dta(df))
    assert _is_stata_dta(_sniff_head(str(fn)))
    got = get_dataframe(str(fn))
    assert list(got.columns) == ['hhid', 'val']
    assert got['val'].tolist() == [0.5, 1.5, 2.5]


#: How an unreadable binary file leaves ``get_dataframe`` today.  The stream
#: pass ends in ``ValueError('Unknown file type')``; ``get_dataframe`` then
#: retries with the path string, and ``pd.read_spss`` on that path raises
#: ``ReadstatError``, which its ``except`` does not list.  Loud either way,
#: which is what these tests pin; the inconsistent TYPE is a pre-existing
#: sibling defect of #712 (same function, different reader) and is
#: deliberately not widened here.
_LOUD = (ValueError, pd.errors.ParserError, pyreadstat.ReadstatError)


def test_a_truncated_dta_still_fails_loudly(tmp_path):
    """The sniff passes (the header is genuine), the Stata readers fail, and
    the chain must end in an error -- never a frame of shredded bytes.  A
    widened ``except`` is exactly what would break this."""
    df = pd.DataFrame({'hhid': np.arange(50), 'val': np.linspace(0, 1, 50)})
    raw = _stata_dta(df)
    fn = tmp_path / 'truncated.dta'
    fn.write_bytes(raw[:len(raw) // 2])
    assert _is_stata_dta(_sniff_head(str(fn)))
    with pytest.raises(_LOUD):
        get_dataframe(str(fn))


def test_a_corrupt_header_dta_is_not_a_garbage_frame(tmp_path):
    """A ``.dta`` whose header is unreadable fails the sniff and falls to the
    text readers, as before; those must not quietly hand back the bytes as a
    one-column frame."""
    df = pd.DataFrame({'hhid': np.arange(50), 'val': np.linspace(0, 1, 50)})
    raw = bytearray(_stata_dta(df))
    raw[0] = 0  # release 0: not Stata by any reading
    fn = tmp_path / 'corrupt.dta'
    fn.write_bytes(bytes(raw))
    assert not _is_stata_dta(_sniff_head(str(fn)))
    with pytest.raises(_LOUD):
        get_dataframe(str(fn))


# ---------------------------------------------------------------------------
# #699 -- SAS Transport (XPORT)
# ---------------------------------------------------------------------------

@pytest.fixture
def xport_file(tmp_path):
    fn = tmp_path / 'SYNTH.SSP'
    write_xport(fn,
                [('HID', 'num'), ('NAME', ('char', 6)), ('WT', 'num')],
                [(1, 'ana', 4), (2, 'bo', None), (3, 'carlos', 7)])
    return fn


def test_synthetic_xport_round_trips_through_pandas(xport_file):
    """Pins the fixture itself: pandas reads what the writer wrote."""
    df = pd.read_sas(xport_file, format='xport', encoding='latin-1')
    assert df.shape == (3, 3)
    assert df['HID'].tolist() == [1.0, 2.0, 3.0]


def test_xport_reads_through_get_dataframe_with_every_column_populated(xport_file):
    assert _is_sas_xport(_sniff_head(str(xport_file)))
    df = get_dataframe(str(xport_file))
    assert list(df.columns) == ['HID', 'NAME', 'WT']
    assert df['HID'].tolist() == [1.0, 2.0, 3.0]
    assert df['NAME'].tolist() == ['ana', 'bo', 'carlos']
    assert df['WT'].tolist()[0] == 4.0 and df['WT'].tolist()[2] == 7.0
    assert pd.isna(df['WT'].iloc[1])
    assert (df.notna().sum() > 0).all()


def test_xport_character_columns_are_str_not_bytes(xport_file):
    """``pd.read_sas(..., encoding=None)`` returns ``bytes`` for character
    variables; no other reader in the chain does, so the branch defaults to
    latin-1.  (Peru 1990 is numeric-only; this is for the next XPORT survey.)"""
    raw = pd.read_sas(xport_file, format='xport')
    assert isinstance(raw['NAME'].iloc[0], bytes)
    df = get_dataframe(str(xport_file))
    assert isinstance(df['NAME'].iloc[0], str)


def test_xport_dispatch_does_not_depend_on_the_suffix(tmp_path):
    fn = tmp_path / 'noext'
    write_xport(fn, [('HID', 'num')], [(1,), (2,)])
    df = get_dataframe(str(fn))
    assert df['HID'].tolist() == [1.0, 2.0]


def test_a_broken_xport_fails_loudly(tmp_path):
    """Magic says XPORT, body is garbage: that is a broken XPORT file, and it
    must raise, not fall through to ``read_csv``."""
    fn = tmp_path / 'broken.ssp'
    fn.write_bytes(XPORT_MAGIC.ljust(80) + b'garbage' * 40)
    with pytest.raises(ValueError):
        get_dataframe(str(fn))


_PERU_EXPEND = 'lsms_library/countries/Peru/1990/Data/EXPEND.SSP'


def _peru_blob_present() -> bool:
    try:
        p = _dvc_cache_path(_PERU_EXPEND)
    except Exception:
        return False
    return p is not None and p.exists()


@pytest.mark.skipif(not _peru_blob_present(),
                    reason='Peru 1990 EXPEND.SSP blob not in the local DVC cache')
def test_peru_1990_expend_ssp_reads_fully_populated():
    """The live instance (#699).  Measured 2026-08-22 and again 2026-10-14:
    1509 x 5, every cell populated via ``pandas.read_sas``; ``pyreadstat``
    gave 2 of 5 columns all-NaN.  ``WT`` is the Peru series' only real
    sampling weight (``Peru/_/CONTENTS.org``)."""
    df = get_dataframe(_PERU_EXPEND)
    assert df.shape == (1509, 5)
    assert list(df.columns) == ['HID', 'HHSIZE', 'PCFDEXP', 'WT', 'TOTPCX']
    assert int(df.notna().sum().sum()) == 1509 * 5
