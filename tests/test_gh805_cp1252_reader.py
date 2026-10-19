"""GH #805: ``get_dataframe`` re-decodes the cp1252 strings of a pre-118 ``.dta``.

pandas ``StataReader`` decodes every format < 118 file as latin-1, so a cp1252
file (the EHCVM-1 2018-19 ``s01_me_*`` rosters, Burkina 2014 ``conso7jours``,
Uganda ``AGSEC5`` / ``GSEC16``, ...) served C1 controls where cp1252 has a
printable character: ``Frère, s<U+009C>ur`` for ``Frère, sœur``.  Ruling
(@ligon, 2026-10-18): fix at the reader, value labels and str columns.

The C1 signature is necessary, not sufficient, and these tests pin the two
narrowings the corpus census forced (``local_tools`` block comment above
``_C1_RE``):

* a UTF-8 double encoding (``b\\xc5\\x93uf``) is never re-decoded;
* the decision is per FILE: one hole byte, or one cp1252 symbol between two
  letters (the DOS code-page signature), and nothing in the file changes.

Plus the scope gates: format >= 118 untouched; an explicit ``encoding=``
untouched; a clean file byte-identical.  Synthetic files are written to
``tmp_path`` with ``DataFrame.to_stata`` (format 114 encodes str as latin-1,
so ``'\\x9c'`` lands in the file as the byte 0x9C -- exactly a cp1252 oe).
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

import lsms_library.local_tools as lt
from lsms_library.paths import countries_root


def _write(path, df, version=114, value_labels=None):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df.to_stata(path, write_index=False, version=version, value_labels=value_labels)
    return str(path)


def _read(path, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return lt.get_dataframe(path, **kw)


# ---------------------------------------------------------------------------
# The repair
# ---------------------------------------------------------------------------

def test_cp1252_str_column_is_decoded_and_dtype_kept(tmp_path):
    raw = pd.DataFrame({"rel": ["Frère, s\x9cur", "Père", "Men\x92s haircut"],
                        "n": [1, 2, 3]})
    p = _write(tmp_path / "a.dta", raw)
    before = pd.read_stata(p)
    df = _read(p)
    assert list(df["rel"]) == ["Frère, sœur", "Père", "Men’s haircut"]
    assert list(df.dtypes) == list(before.dtypes)
    assert df.shape == before.shape and list(df.columns) == list(before.columns)


def test_cp1252_value_labels_are_decoded_in_frame_and_in_categories(tmp_path):
    raw = pd.DataFrame({"s01q02": [1, 7, 7]})
    labels = {"s01q02": {1: "Chef de ménage", 7: "Frère, s\x9cur"}}
    p = _write(tmp_path / "b.dta", raw, value_labels=labels)
    df = _read(p)
    assert list(df["s01q02"]) == ["Chef de ménage", "Frère, sœur", "Frère, sœur"]
    cats = _read(p, categories_only=True)
    assert cats["s01q02"][7] == "Frère, sœur"
    # ``.title()`` of the decoded label is what the retired GH #801
    # _fix_relationship produced -- the formatter alone now suffices.
    assert cats["s01q02"][7].title() == "Frère, Sœur"


def test_ellipsis_label_survives_strip(tmp_path):
    """U+0085 is whitespace to ``str.strip``; U+2026 is not -- which is why the
    Burkina Feuilles label became keyable (``_J_FIX`` retired)."""
    raw = pd.DataFrame({"product": ["Feuilles (oseilles, baobab, boulvaka\x85"]})
    df = _read(_write(tmp_path / "c.dta", raw))
    assert df["product"].iloc[0].strip() == "Feuilles (oseilles, baobab, boulvaka…"


# ---------------------------------------------------------------------------
# The narrowings
# ---------------------------------------------------------------------------

def test_utf8_double_encoding_is_left_alone(tmp_path):
    """``b\\xc5\\x93uf`` is UTF-8 read as latin-1; 0x93 is a continuation byte.
    cp1252 would make it ``bÅ“uf`` and break every ``_decode_mojibake``."""
    raw = pd.DataFrame({"j": ["Viande de bÅ\x93uf", "Frère, s\x9cur"]})
    df = _read(_write(tmp_path / "d.dta", raw))
    assert df["j"].iloc[0] == "Viande de bÅ\x93uf"            # untouched
    assert df["j"].iloc[1] == "Frère, sœur"                # same file, decoded


@pytest.mark.parametrize("dos", ["KOR\x80E", "dom\x82sticos", "c\x82ntrico"])
def test_dos_code_page_file_is_refused_whole(tmp_path, dos):
    """cp437/cp850 accented letters land in 0x80-0x9F; cp1252 turns them into a
    symbol between two letters.  The whole file is served as before -- the
    genuine cp1252 string beside it included."""
    raw = pd.DataFrame({"x": [dos, "Men\x92s haircut"]})
    p = _write(tmp_path / "e.dta", raw)
    assert list(_read(p)["x"]) == list(pd.read_stata(p)["x"])


def test_cp1252_hole_refuses_the_file(tmp_path):
    """0x81 0x8D 0x8F 0x90 0x9D are undefined in cp1252: a file carrying one is
    not cp1252 (Kyrgyz / Kazakh cp866 Cyrillic)."""
    raw = pd.DataFrame({"x": ["\x8d\x85\x92", "Don\x92t know"]})
    p = _write(tmp_path / "f.dta", raw)
    assert list(_read(p)["x"]) == list(pd.read_stata(p)["x"])


def test_a_refusing_label_in_an_unused_value_label_still_refuses(tmp_path):
    """The verdict reads the file's WHOLE label table, not only labels some row
    uses: one writer, one code page."""
    raw = pd.DataFrame({"v": [1, 1], "s": ["Don\x92t know", "ok"]})
    labels = {"v": {1: "plain", 2: "KOR\x80E"}}                    # 2 is never used
    p = _write(tmp_path / "g.dta", raw, value_labels=labels)
    assert list(_read(p)["s"]) == ["Don\x92t know", "ok"]


# ---------------------------------------------------------------------------
# The gates
# ---------------------------------------------------------------------------

def test_clean_file_is_byte_identical_and_returned_unchanged(tmp_path):
    raw = pd.DataFrame({"x": ["Café", "naïve", None], "n": [1.5, 2.0, 3.0]})
    p = _write(tmp_path / "h.dta", raw)
    df = _read(p)
    pd.testing.assert_frame_equal(df, pd.read_stata(p, convert_categoricals=False))
    assert lt._redecode_cp1252(df, p) is df


def test_explicit_encoding_disables_the_redecode(tmp_path):
    raw = pd.DataFrame({"x": ["Frère, s\x9cur"]})
    p = _write(tmp_path / "i.dta", raw)
    assert _read(p, encoding="latin-1")["x"].iloc[0] == "Frère, s\x9cur"


def test_format_118_is_out_of_scope(tmp_path):
    """A format-118 file is UTF-8; a C1 code point in it is damage upstream of
    the file (Mali 2017-18), not the latin-1 read #805 repairs."""
    raw = pd.DataFrame({"x": ["Moussa N\x92FA"]})
    p = _write(tmp_path / "j.dta", raw, version=118)
    assert _read(p)["x"].iloc[0] == "Moussa N\x92FA"


def test_release_sniff():
    assert lt._stata_release(bytes([115, 1, 1, 0])) == 115
    assert lt._stata_release(b"<stata_dta><header><release>117</release>") == 117
    assert lt._stata_release(b"<stata_dta><header><release>118</release>") == 118
    assert lt._stata_release(b"PK\x03\x04") is None


def test_predicate_classes():
    assert lt._cp1252_candidate("s\x9cur")
    assert not lt._cp1252_candidate("bÅ\x93uf")       # double encoding
    assert not lt._cp1252_candidate("sœur")            # already decoded (>U+00FF)
    assert not lt._cp1252_candidate("plain")
    assert lt._cp1252_redecode_map({"Kru\x9aevac", "Ni\x9a"}) == {
        "Kru\x9aevac": "Kruševac", "Ni\x9a": "Niš"}
    assert lt._cp1252_redecode_map({"Kru\x9aevac", "KOR\x80E"}) == {}
    assert lt._cp1252_redecode_map({"CAPENTRY\x97FURNITURE"}) == {
        "CAPENTRY\x97FURNITURE": "CAPENTRY—FURNITURE"}


# ---------------------------------------------------------------------------
# The real files (skipped without the L1 blobs)
# ---------------------------------------------------------------------------

def _real(rel, col):
    path = countries_root() / rel
    if lt._dvc_cache_path(str(path)) is None:
        pytest.skip(f"{rel}: L1 blob not cached")
    return _read(str(path))[col]


@pytest.mark.requires_s3
@pytest.mark.parametrize("rel", [
    "Niger/2018-19/Data/s01_me_ner2018.dta",
    "Senegal/2018-19/Data/s01_me_sen2018.dta",
    "Benin/2018-19/Data/s01_me_ben2018.dta",
    "Togo/2018/Data1/s01_me_tgo2018.dta",
])
def test_ehcvm1_roster_label_is_served_decoded(rel):
    vals = set(_real(rel, "s01q02").dropna().astype(str))
    assert "Frère, sœur" in vals
    assert not any(lt._C1_RE.search(v) for v in vals)


@pytest.mark.requires_s3
def test_albania_dos_district_names_are_untouched():
    vals = set(_real("Albania/2004/Data/w3_hh_basic.dta", "m0_distr").dropna().astype(str))
    assert {"KOR\x80E", "KU\x80OVE"} <= vals
