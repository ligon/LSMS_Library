"""GH #682/#685 — query-time label selection beyond food's ``j`` index level.

Three blocks:

1. **Cache-independent unit tests** of ``Country._apply_categorical_mappings``
   with an inline fake country, plus the ``_split_labels_arg`` /
   ``_assert_label_targets_present`` contracts and the ``source_cols[0]``
   ordering pin.
2. **A structural cache test** — the label-selection machinery must stay
   *outside* the ``@build_transform`` closure, or a read-path feature would
   move every ``lsms_cache_hash`` in the corpus.
3. **Subprocess integration tests** against a synthetic country tree
   (``LSMS_COUNTRIES_ROOT`` + ``LSMS_DATA_DIR`` in a ``tmp_path``), covering the
   real ``Country(...).sample(labels=...)`` path cold *and* warm, the error
   taxonomy end to end, and ``Feature``'s graceful degradation.

Why the mapping site and not ``_relabel_j``: see ``docs/guide/label-selection.md``.
The real-world cells this exists for are the corpus audit's class A (GH #682):
Iraq 2006-07 folds 7 raw ``xstrat`` settlement tiers onto 2 delivered values
(12,194 households in one bucket), and Ethiopia ESS2/ESS3, Mali, Niger and
Kazakhstan 1996 do the same at smaller scale.  None is wired; the fixture
country below stands in for all of them so the test needs no microdata.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library.country import (
    Country as _CountryCls,
    _RESERVED_U_SENTINELS,
    _assert_label_targets_present,
    _label_selection,
    _label_targets_missing,
    _split_labels_arg,
    _LABEL_SELECTION,
)
from lsms_library.errors import LabelUnavailableError


# ---------------------------------------------------------------------------
# Fixtures shared by the unit block
# ---------------------------------------------------------------------------

def _settlement_table() -> pd.DataFrame:
    """A two-level ``Rural`` table: canonical binary + a 7-way ladder.

    The column ORDER matters (``Code`` first) — see
    ``test_source_cols_ordering_is_the_key_column``.
    """
    return pd.DataFrame({
        "Code": [1, 2, 3, 4, 5, 6, 7],
        "Preferred Label": ["Urban"] * 3 + ["Rural"] * 4,
        "Settlement Label": ["City", "Large Town", "Medium Town", "Small Town",
                             "Large Village", "Small Village", "Other"],
    })


def _fake_country(cat_maps, name="Fixtureland"):
    """Minimal stand-in exposing what ``_apply_categorical_mappings`` reads."""
    fake = SimpleNamespace(name=name, categorical_mapping=cat_maps)
    fake._apply_categorical_mappings = (
        _CountryCls._apply_categorical_mappings.__get__(fake))
    return fake


def _sample_frame(values=("1", "3", "6")):
    idx = pd.MultiIndex.from_tuples(
        [("2000", "v1", f"h{n}") for n in range(1, len(values) + 1)],
        names=["t", "v", "i"])
    df = pd.DataFrame({"Rural": list(values),
                       "weight": [float(n) for n in range(1, len(values) + 1)]},
                      index=idx)
    df.attrs["id_converted"] = True
    return df


def _stub_finalize(fake):
    """Bind the real ``_finalize_result`` onto a fake, stubbing only what the
    label-selection branch does not exercise (index augmentation, id_walk,
    the v-join)."""
    fake._augment_index_from_related_tables = lambda df, scheme_entry, wave: df
    fake.data_scheme = []            # no 'sample' -> no v-join re-entry
    fake._updated_ids_cache = None   # no id_walk
    fake._finalize_result = _CountryCls._finalize_result.__get__(fake)
    return fake


# ---------------------------------------------------------------------------
# 1. The selection itself
# ---------------------------------------------------------------------------

def test_default_still_maps_to_preferred_label():
    """No ``labels=`` -> historical behaviour, byte for byte."""
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        _sample_frame())
    assert list(out["Rural"]) == ["Urban", "Urban", "Rural"]


def test_selected_variant_resolves_the_raw_code():
    """The whole point: the FINE label, recovered from the raw code."""
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        _sample_frame(), labels={"Rural": "Settlement"})
    assert list(out["Rural"]) == ["City", "Medium Town", "Small Village"]


def test_relabel_j_cannot_do_this():
    """The proof that fixes the implementation point.

    ``_relabel_j`` keys canonical -> variant on ``Preferred Label``.  That is a
    function for food (one Preferred Label per item) and one-to-many here, so
    ``to_dict()`` is last-row-wins.  If someone "simplifies" the selection back
    onto the output side, this is what they get.
    """
    tbl = _settlement_table()
    collapsed = (tbl[["Preferred Label", "Settlement Label"]]
                 .dropna().set_index("Preferred Label")["Settlement Label"].to_dict())
    assert collapsed == {"Urban": "Medium Town", "Rural": "Other"}
    assert len(collapsed) == 2 < len(tbl)


def test_target_key_is_case_insensitive():
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        _sample_frame(), labels={"rural": "Settlement"})
    assert list(out["Rural"]) == ["City", "Medium Town", "Small Village"]


def test_selection_applies_to_index_levels_too():
    df = _sample_frame().reset_index().set_index(["t", "Rural", "i"])
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        df, labels={"Rural": "Settlement"})
    assert list(out.index.get_level_values("Rural")) == [
        "City", "Medium Town", "Small Village"]


def test_bare_variant_column_name_is_accepted():
    """``'X Label'`` first, then a bare ``'X'`` — the ``_relabel_j`` rule."""
    tbl = _settlement_table().rename(columns={"Settlement Label": "Settlement"})
    out = _fake_country({"Rural": tbl})._apply_categorical_mappings(
        _sample_frame(), labels={"Rural": "Settlement"})
    assert list(out["Rural"]) == ["City", "Medium Town", "Small Village"]


def test_attrs_survive_the_selection():
    """``id_converted`` must not be dropped — CLAUDE.md, panel-ID chains."""
    df = _sample_frame().reset_index().set_index(["t", "Rural", "i"])
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        df, labels={"Rural": "Settlement"})
    assert out.attrs.get("id_converted") is True


def test_numeric_code_key_augmentation_survives_selection():
    """``_augment_numeric_code_keys`` (GH #223 L2) still fires under selection."""
    out = _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
        _sample_frame(values=("1.0", "3.0", "6.0")), labels={"Rural": "Settlement"})
    assert list(out["Rural"]) == ["City", "Medium Town", "Small Village"]


def test_u_sentinels_still_protected_under_selection():
    """GH #361: a country ``u`` table must not remap 'kg'/'Value', selection or not."""
    u_tbl = pd.DataFrame({
        "Original Label": ["kg", "Value", "Tas"],
        "Preferred Label": ["Kg", "value", "Basket"],
        "Detail Label": ["Kilogramme", "Local currency", "Woven basket"],
    })
    idx = pd.MultiIndex.from_tuples(
        [("2000", "h1", "Maize", "kg"), ("2000", "h1", "Salt", "Value"),
         ("2000", "h1", "Yam", "Tas")], names=["t", "i", "j", "u"])
    df = pd.DataFrame({"Quantity": [1.0, 2.0, 3.0]}, index=idx)
    out = _fake_country({"u": u_tbl})._apply_categorical_mappings(
        df, protect_u_sentinels=True, labels={"u": "Detail"})
    got = list(out.index.get_level_values("u"))
    assert set(_RESERVED_U_SENTINELS) <= set(got), got
    assert got == ["kg", "Value", "Woven basket"]


# ---------------------------------------------------------------------------
# The source_cols[0] ordering hazard
# ---------------------------------------------------------------------------

def test_source_cols_ordering_is_the_key_column():
    """PIN: the key column is "the first column that is not a label column".

    Two halves, both deliberate:

    * *default path* — reordering the table so a label column precedes ``Code``
      makes the mapping key on the WRONG column and silently map nothing.  This
      is a pre-existing, unguarded hazard; the test pins it so a future change
      to the rule is a visible test failure rather than a quiet corpus-wide
      re-decode.  **Keep the code column first.**
    * *selection path* — the requested target is excluded from key selection,
      so the same reordered table still keys on ``Code``.  Excluding the target
      is a provable no-op when the target IS ``Preferred Label`` (the default),
      which the first assertion of each pair holds fixed.
    """
    good = _settlement_table()                                    # Code first
    bad = good[["Preferred Label", "Settlement Label", "Code"]]   # Code last

    assert list(_fake_country({"Rural": good})._apply_categorical_mappings(
        _sample_frame())["Rural"]) == ["Urban", "Urban", "Rural"]
    # Reordered + default target: keys on 'Settlement Label', matches nothing,
    # and the raw codes leak through untouched.  Silently wrong, by design of
    # the historical rule.
    assert list(_fake_country({"Rural": bad})._apply_categorical_mappings(
        _sample_frame())["Rural"]) == ["1", "3", "6"]

    # Selection excludes the target, so ordering cannot make it key on itself.
    for tbl in (good, bad):
        assert list(_fake_country({"Rural": tbl})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})["Rural"]) == [
                "City", "Medium Town", "Small Village"]


def test_no_key_column_left_is_a_loud_keyerror():
    """A two-column ``| Preferred Label | X Label |`` table has nothing to key on."""
    tbl = _settlement_table()[["Preferred Label", "Settlement Label"]]
    with pytest.raises(KeyError) as ei:
        _fake_country({"Rural": tbl})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})
    assert not isinstance(ei.value, LabelUnavailableError)
    assert "no key column left" in str(ei.value)


# ---------------------------------------------------------------------------
# Error taxonomy — Feature (feature.py:556) depends on this distinction
# ---------------------------------------------------------------------------

def test_no_mapping_table_is_label_unavailable():
    """Missing curation -> degradable by Feature."""
    with pytest.raises(LabelUnavailableError):
        _fake_country({"Roof": _settlement_table()})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})


def test_no_such_variant_column_is_label_unavailable():
    with pytest.raises(LabelUnavailableError) as ei:
        _fake_country({"Rural": _settlement_table()})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Nutrition"})
    assert "Settlement Label" in str(ei.value)      # names what IS available


def test_country_with_no_mapping_tables_at_all_is_label_unavailable():
    with pytest.raises(LabelUnavailableError):
        _fake_country({})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})


def test_table_without_preferred_label_is_a_plain_keyerror():
    """Malformed table (has the table, no canonical column) -> loud, not degraded."""
    tbl = _settlement_table().rename(columns={"Preferred Label": "Canonical Label"})
    with pytest.raises(KeyError) as ei:
        _fake_country({"Rural": tbl})._apply_categorical_mappings(
            _sample_frame(), labels={"Rural": "Settlement"})
    assert not isinstance(ei.value, LabelUnavailableError)
    assert "Preferred Label" in str(ei.value)


def test_malformed_table_is_still_silently_skipped_by_default():
    """Unchanged: with no ``labels=``, a Preferred-Label-less table is skipped."""
    tbl = _settlement_table().rename(columns={"Preferred Label": "Canonical Label"})
    out = _fake_country({"Rural": tbl})._apply_categorical_mappings(_sample_frame())
    assert list(out["Rural"]) == ["1", "3", "6"]


# ---------------------------------------------------------------------------
# _split_labels_arg: the scalar contract is non-negotiable
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("arg,expected", [
    ("Preferred", ("Preferred", None)),
    ("Aggregate", ("Aggregate", None)),
    (None, (None, None)),
    ({}, ("Preferred", None)),
    ({"j": "Aggregate"}, ("Aggregate", None)),
    ({"Rural": "Settlement"}, ("Preferred", {"Rural": "Settlement"})),
    ({"j": "Aggregate", "Rural": "Settlement"},
     ("Aggregate", {"Rural": "Settlement"})),
])
def test_split_labels_arg(arg, expected):
    assert _split_labels_arg(arg) == expected


def test_split_labels_arg_rejects_other_types():
    with pytest.raises(TypeError):
        _split_labels_arg(["Aggregate"])
    with pytest.raises(TypeError):
        _split_labels_arg({"Rural": 3})


def test_dict_j_key_is_the_scalar_request():
    """Documented equivalence: one mechanism per face."""
    assert _split_labels_arg({"j": "Aggregate"}) == _split_labels_arg("Aggregate")


# ---------------------------------------------------------------------------
# Absent target -> plain KeyError (consistent with _relabel_j's no-'j' rule)
# ---------------------------------------------------------------------------

def test_absent_target_is_a_plain_keyerror():
    df = _sample_frame()
    assert _label_targets_missing(df, {"Rural": "Settlement"}) == []
    assert _label_targets_missing(df, {"Roof": "Detail"}) == ["Roof"]
    with pytest.raises(KeyError) as ei:
        _assert_label_targets_present(df, {"Roof": "Detail"},
                                      country="Fixtureland", table="sample")
    assert not isinstance(ei.value, LabelUnavailableError)
    _assert_label_targets_present(df, {"Rural": "Settlement"},
                                  country="Fixtureland", table="sample")   # no raise
    _assert_label_targets_present(df, None,
                                  country="Fixtureland", table="sample")   # no raise


def test_index_levels_count_as_present_targets():
    df = _sample_frame().reset_index().set_index(["t", "Rural", "i"])
    assert "Rural" in df.index.names and "Rural" not in df.columns
    assert _label_targets_missing(df, {"Rural": "Settlement"}) == []   # index level
    assert _label_targets_missing(df, {"weight": "X"}) == []           # column
    assert _label_targets_missing(df, {"Roof": "X"}) == ["Roof"]


# ---------------------------------------------------------------------------
# ContextVar scoping — _finalize_result RE-ENTERS ITSELF
# ---------------------------------------------------------------------------

def test_label_selection_context_is_scoped_and_restored():
    assert _LABEL_SELECTION.get() is None
    with _label_selection("sample", {"Rural": "Settlement"}):
        assert _LABEL_SELECTION.get() == ("sample", {"Rural": "Settlement"})
    assert _LABEL_SELECTION.get() is None
    # An empty/None selection must not set the var at all.
    with _label_selection("sample", None):
        assert _LABEL_SELECTION.get() is None
    # ... and it is restored even when the body raises.
    with pytest.raises(RuntimeError):
        with _label_selection("sample", {"Rural": "Settlement"}):
            raise RuntimeError("boom")
    assert _LABEL_SELECTION.get() is None


def test_context_selection_is_keyed_on_the_requesting_table():
    """The load-bearing half of the scoping.

    ``_finalize_result`` re-enters itself: ``_join_v_from_sample`` fetches
    ``sample()`` from inside the finalize of some OTHER table.  If the stored
    selection were applied regardless of table, that nested read would be
    relabelled too — and worse, an error raised inside it is swallowed by
    ``_join_v_from_sample``'s ``except (…, KeyError, …)``, so the ``v`` join
    would silently vanish.
    """
    fake = _fake_country({"Rural": _settlement_table()})
    captured = {}

    def _spy(df, protect_u_sentinels=False, labels=None):
        captured["labels"] = labels
        return df

    fake._apply_categorical_mappings = _spy
    _stub_finalize(fake)

    with _label_selection("household_roster", {"Rural": "Settlement"}):
        fake._finalize_result(_sample_frame(), {}, "sample")
    assert captured["labels"] is None, "selection leaked into a nested table read"

    with _label_selection("household_roster", {"Rural": "Settlement"}):
        fake._finalize_result(_sample_frame(), {}, "household_roster")
    assert captured["labels"] == {"Rural": "Settlement"}


def test_explicit_labels_argument_beats_the_context():
    fake = _fake_country({"Rural": _settlement_table()})
    captured = {}
    def _spy(df, protect_u_sentinels=False, labels=None):
        captured["labels"] = labels
        return df

    fake._apply_categorical_mappings = _spy
    _stub_finalize(fake)
    with _label_selection("sample", {"Rural": "Settlement"}):
        fake._finalize_result(_sample_frame(), {}, "sample", labels={"Rural": "Other"})
    assert captured["labels"] == {"Rural": "Other"}


# ---------------------------------------------------------------------------
# 2. Cache: the selection is post-read and must stay out of the build closure
# ---------------------------------------------------------------------------

def test_label_selection_is_outside_the_build_fingerprint():
    """Structural proof that this feature cannot move any ``lsms_cache_hash``.

    ``lsms_cache_hash`` folds in ``build_transforms_fingerprint``, which is the
    *source AST* of every ``@build_transform``-tagged entry point and
    everything its closure walk reaches.  ``_aggregate_wave_data`` IS tagged:
    adding even one defaulted keyword argument to its signature was measured to
    change the fingerprint for every table (a corpus-wide rebuild).  That is
    precisely why the ``method() -> _finalize_result`` hop is carried by
    ``_LABEL_SELECTION`` rather than by a parameter.

    If a future refactor drags the label-selection machinery into the closure,
    this test fails — which is the warning, not a nuisance.
    """
    from lsms_library._build_registry import _BUILD_TRANSFORMS, _closure_parts

    seen: set = set()
    parts: list = []
    for _qn, (fn, _tables) in sorted(_BUILD_TRANSFORMS.items()):
        parts += _closure_parts(fn, seen)

    def _in_closure(qualname: str) -> bool:
        return any(p.startswith(f"lsms_library.country.{qualname}=") for p in parts)

    # sanity: the walk really does reach the tagged orchestrator
    assert _in_closure("Country._aggregate_wave_data")
    for qualname in ("Country._apply_categorical_mappings",
                     "Country._finalize_result",
                     "Country._relabel_j",
                     "Country.__getattr__"):
        assert not _in_closure(qualname), (
            f"{qualname} entered the build-transform closure: editing it now "
            f"invalidates every cached parquet in the corpus. Label selection "
            f"is a READ-path feature and must stay outside it.")


# ---------------------------------------------------------------------------
# 3. Integration — a synthetic country tree, in a subprocess
# ---------------------------------------------------------------------------

_SETTLEMENT_ORG = """\
#+name: Rural
| Code | Preferred Label | Settlement Label |
|------+-----------------+------------------|
|    1 | Urban           | City             |
|    2 | Urban           | Large Town       |
|    3 | Urban           | Medium Town      |
|    4 | Rural           | Small Town       |
|    5 | Rural           | Large Village    |
|    6 | Rural           | Small Village    |
|    7 | Rural           | Other            |
"""


def _make_country(countries_root: Path, name: str, cat_org: str | None) -> None:
    """A minimal one-wave country whose ``sample`` reads a local CSV.

    No ``#+begin_example`` blocks anywhere: ``all_dfs_from_orgfile`` parses the
    tables inside them as live mappings, so an illustrative block would become a
    real (and wrong) categorical table.
    """
    c = countries_root / name
    (c / "_").mkdir(parents=True)
    (c / "2000" / "_").mkdir(parents=True)
    (c / "2000" / "Data").mkdir(parents=True)
    (c / "_" / "data_scheme.yml").write_text(textwrap.dedent(f"""\
        Country: {name}

        Waves:
          - '2000'

        Data Scheme:
          sample:
            index: (i, t)
            v: str
            weight: float
            Rural: str
        """))
    if cat_org is not None:
        (c / "_" / "categorical_mapping.org").write_text(cat_org)
    (c / "2000" / "_" / "data_info.yml").write_text(textwrap.dedent(f"""\
        Country: {name}
        Wave: '2000'

        sample:
            file: ../Data/hh.csv
            idxvars:
                i: hhid
            myvars:
                v: clust
                weight: wt
                Rural: settlement
        """))
    pd.DataFrame({
        "hhid": [f"h{n}" for n in range(1, 8)],
        "clust": ["c1", "c1", "c2", "c2", "c3", "c3", "c3"],
        "wt": [float(n) for n in range(1, 8)],
        "settlement": [1, 2, 3, 4, 5, 6, 7],
    }).to_csv(c / "2000" / "Data" / "hh.csv", index=False)


@pytest.fixture(scope="module")
def synthetic_tree(tmp_path_factory):
    root = tmp_path_factory.mktemp("label_selection")
    countries = root / "countries"
    countries.mkdir()
    _make_country(countries, "Fixtureland", _SETTLEMENT_ORG)
    _make_country(countries, "Otherland", None)          # curates nothing
    _make_country(countries, "Malformedland",
                  _SETTLEMENT_ORG.replace("Preferred Label", "Canonical Label"))
    return {"LSMS_COUNTRIES_ROOT": str(countries),
            "LSMS_DATA_DIR": str(root / "data")}


def _run(script: str, env_extra: dict) -> str:
    env = dict(os.environ, **env_extra)
    env.pop("LSMS_NO_CACHE", None)
    r = subprocess.run([sys.executable, "-c",
                        "import warnings; warnings.simplefilter('ignore')\n" +
                        textwrap.dedent(script)],
                       env=env, capture_output=True, text=True)
    assert r.returncode == 0, f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    return r.stdout


LADDER = ["City", "Large Town", "Medium Town", "Small Town",
          "Large Village", "Small Village", "Other"]
BINARY = ["Urban"] * 3 + ["Rural"] * 4


def test_end_to_end_cold_then_warm(synthetic_tree):
    """Cold build, then a warm read off the L2-country parquet: same answer.

    Also asserts what "post-read" MEANS: the cached parquet holds the RAW codes,
    so both the canonical and the selected vocabulary are produced on read from
    the same bytes.
    """
    out = _run(f"""
        from pathlib import Path
        import pandas as pd
        from lsms_library import Country
        from lsms_library.paths import data_root
        LADDER = {LADDER!r}
        BINARY = {BINARY!r}
        assert list(Country('Fixtureland').sample()['Rural']) == BINARY
        p = data_root('Fixtureland') / 'var' / 'sample.parquet'
        assert p.exists(), p
        raw = pd.read_parquet(p)
        assert sorted(set(raw['Rural'].astype(str))) == list('1234567'), sorted(set(raw['Rural']))
        # warm reads, both vocabularies, from the same cached bytes
        assert list(Country('Fixtureland').sample()['Rural']) == BINARY
        assert list(Country('Fixtureland').sample(labels={{'Rural': 'Settlement'}})['Rural']) == LADDER
        print('COLD_WARM_OK')
        """, synthetic_tree)
    assert "COLD_WARM_OK" in out


def test_end_to_end_cache_hash_is_unchanged_by_the_selection(synthetic_tree):
    """The embedded ``lsms_cache_hash`` is identical before and after a
    ``labels=`` read, and the parquet is not rewritten."""
    out = _run("""
        import pyarrow.parquet as pq
        from lsms_library import Country
        from lsms_library.paths import data_root
        p = data_root('Fixtureland') / 'var' / 'sample.parquet'
        Country('Fixtureland').sample()
        before = (pq.read_schema(p).metadata or {}).get(b'lsms_cache_hash')
        mtime_before = p.stat().st_mtime_ns
        assert before is not None
        Country('Fixtureland').sample(labels={'Rural': 'Settlement'})
        after = (pq.read_schema(p).metadata or {}).get(b'lsms_cache_hash')
        assert after == before, (before, after)
        assert p.stat().st_mtime_ns == mtime_before, 'parquet was rewritten'
        print('HASH_STABLE_OK')
        """, synthetic_tree)
    assert "HASH_STABLE_OK" in out


def test_end_to_end_error_taxonomy(synthetic_tree):
    out = _run("""
        from lsms_library import Country
        from lsms_library.errors import LabelUnavailableError

        # (a) country curates no such table -> degradable
        try:
            Country('Otherland').sample(labels={'Rural': 'Settlement'})
        except LabelUnavailableError:
            print('OTHERLAND_LABEL_UNAVAILABLE')

        # (b) malformed table -> plain KeyError, loud
        try:
            Country('Malformedland').sample(labels={'Rural': 'Settlement'})
        except LabelUnavailableError:
            raise AssertionError('malformed table degraded instead of raising')
        except KeyError:
            print('MALFORMED_PLAIN_KEYERROR')

        # (c) target not in the frame -> plain KeyError
        try:
            Country('Fixtureland').sample(labels={'Roof': 'Detail'})
        except LabelUnavailableError:
            raise AssertionError('absent target degraded instead of raising')
        except KeyError:
            print('ABSENT_TARGET_PLAIN_KEYERROR')

        # (d) bad type
        try:
            Country('Fixtureland').sample(labels=['Settlement'])
        except TypeError:
            print('BAD_TYPE_TYPEERROR')
        """, synthetic_tree)
    for marker in ("OTHERLAND_LABEL_UNAVAILABLE", "MALFORMED_PLAIN_KEYERROR",
                   "ABSENT_TARGET_PLAIN_KEYERROR", "BAD_TYPE_TYPEERROR"):
        assert marker in out, out


def test_scalar_labels_never_touches_a_column(synthetic_tree):
    """Back-compat: a scalar targets ``j`` and only ``j``.

    ``sample`` has no ``j`` level and a mapped ``Rural`` column; a scalar must
    leave ``Rural`` canonical.  (On a registered table with no ``j`` level the
    scalar is a silent no-op — pre-existing behaviour, deliberately untouched;
    the DICT form is the loud one.)
    """
    out = _run(f"""
        from lsms_library import Country
        assert list(Country('Fixtureland').sample(labels='Settlement')['Rural']) == {BINARY!r}
        assert list(Country('Fixtureland').sample(labels='Preferred')['Rural']) == {BINARY!r}
        assert list(Country('Fixtureland').sample()['Rural']) == {BINARY!r}
        print('SCALAR_IS_J_ONLY_OK')
        """, synthetic_tree)
    assert "SCALAR_IS_J_ONLY_OK" in out


def test_feature_degrades_for_an_uncurated_country(synthetic_tree):
    """Contract B (feature.py:556): drop, warn once, mark ``attrs``."""
    out = _run(f"""
        import warnings
        from lsms_library import Feature
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            df = Feature('sample')(['Fixtureland', 'Otherland'],
                                   labels={{'Rural': 'Settlement'}})
        assert sorted(set(df.index.get_level_values('country'))) == ['Fixtureland'], df.index
        assert list(df['Rural']) == {LADDER!r}, list(df['Rural'])
        assert df.attrs['labels_unavailable'] == ['Otherland'], df.attrs
        msgs = [str(m.message) for m in w]
        assert any('unavailable' in m and 'Otherland' in m for m in msgs), msgs
        assert not any('Failed to load' in m for m in msgs), msgs
        print('FEATURE_DEGRADE_OK')
        """, synthetic_tree)
    assert "FEATURE_DEGRADE_OK" in out


def test_feature_default_is_unaffected(synthetic_tree):
    out = _run(f"""
        from lsms_library import Feature
        df = Feature('sample')(['Fixtureland', 'Otherland'])
        assert sorted(set(df.index.get_level_values('country'))) == ['Fixtureland', 'Otherland']
        assert 'labels_unavailable' not in df.attrs
        assert list(df.xs('Fixtureland', level='country')['Rural']) == {BINARY!r}
        print('FEATURE_DEFAULT_OK')
        """, synthetic_tree)
    assert "FEATURE_DEFAULT_OK" in out
