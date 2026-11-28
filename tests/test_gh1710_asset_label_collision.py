"""Source-free checks of the post-preferred-label assets warning."""
from contextlib import ExitStack
import warnings
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from lsms_library import Country, Feature, country as cm
from lsms_library import catalog, paths


W1, W2 = "2090-91", "2091-92"
H1, H2, H3 = "canary-hh1", "canary-hh2", "canary-hh3"
A, B, C, D = ("canary-item-" + x for x in "ABCD")
P, Q = "canary-preferred", "canary-other-label"
MAP = {A: P, B: P, C: Q, D: Q}
LEVELS = ("t", "i", "j")
SCOPED = ("Burkina_Faso", "Mali", "Niger", "Uganda")
OUTSIDE = ("Fictive", "Togo", "Benin", "CotedIvoire")
ROWS = [
    (W1, H1, A, 17, 2),
    (W1, H2, A, 29, 3),
    (W2, H1, C, 31, 5),
    (W1, H1, B, 17, 2),  # identical payload, distinct item
    (W1, H2, B, 41, 8),
    (W2, H1, D, None, None),  # dropped, so no Q collision
    (W1, H2, P, 43, 10),  # unmapped key passes through and joins the group
    (W2, H1, A, 47, 12),  # same preferred label, different wave
    (W1, H3, B, 53, 13),  # same preferred label, different household
]
ATTRS = {"fixture_metadata": {"names": ["synthetic"]}}
SCHEME = {"index": "(t, i, j)", "Value": "float", "Quantity": "int"}
WARNING_NAME = "AssetLabelCollisionWarning"
MESSAGE = (
    "Preferred asset labels create duplicate index keys from distinct item keys "
    "({groups} groups, {rows} affected rows). The relabel does not collapse or remove rows."
)


def asset_frame(rows=ROWS, names=LEVELS):
    n = len(names)
    index = pd.MultiIndex.from_tuples([r[:n] for r in rows], names=names)
    frame = pd.DataFrame(index=index)
    frame["Value"] = pd.array([r[n] for r in rows], dtype="Float64")
    frame["Quantity"] = pd.array([r[n + 1] for r in rows], dtype="Int64")
    frame.attrs = ATTRS.copy()
    return frame


def mapping_table(mapping=MAP, *, explicit_key=False):
    out = pd.DataFrame({"Original Label": list(mapping), "Preferred Label": list(mapping.values())})
    if explicit_key:
        out.insert(0, "Decoy", ["canary-not-a-key"] * len(out))
        out.attrs[cm.CATEGORICAL_KEY_ATTR] = "Original Label"
    return out


def capture(load):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = load()
    return out, caught


def new_signal(caught, counts=None):
    signals = [w for w in caught if w.category.__name__ == WARNING_NAME]
    if counts is None:
        assert not signals
        return
    assert len(signals) == 1, "missing or repeated preferred asset label collision warning"
    warning = signals[0]
    assert warning.category is cm.AssetLabelCollisionWarning
    assert not issubclass(warning.category, cm.GrainCollapseWarning)
    expected = MESSAGE.format(groups=counts[0], rows=counts[1])
    assert str(warning.message) == expected
    assert warning.message.args == (expected,)
    assert not vars(warning.message)
    assert "canary" not in expected and W1 not in expected and W2 not in expected


@pytest.fixture
def registrations(tmp_path, monkeypatch):
    root = tmp_path / "countries"
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    monkeypatch.setenv("LSMS_DATA_DIR", str(data))
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    for option in ("LSMS_GRAIN_STRICT", "LSMS_NO_CACHE", "LSMS_NULL_INDEX_STRICT"):
        monkeypatch.delenv(option, raising=False)
    caches = (paths.countries_root, paths.data_root, catalog._country_dirs, catalog._all_features)
    for cached in caches:
        cached.cache_clear()
    for name in SCOPED + OUTSIDE:
        scheme = root / name / "_" / "data_scheme.yml"
        scheme.parent.mkdir(parents=True)
        # Uganda's empty index declaration is intentional (the real !make path).
        declaration = ("!make\n" if name == "Uganda" else
                       "\n    index: (t, i, j)\n    Value: float\n    Quantity: int\n")
        scheme.write_text(f"Waves: [{W1}, {W2}]\nData Scheme:\n  assets: {declaration}")
    with patch.dict(cm._GRAIN_LEDGER, clear=True):
        yield
    for cached in caches:
        cached.cache_clear()


def finalise(name, frame, table, *, method="assets", scheme=SCHEME, currency=None):
    country = Country(name)
    tables = {f"harmonize_{method}": table} if table is not None else {}
    with patch.object(Country, "categorical_mapping", property(lambda self: tables)):
        return capture(lambda: country._finalize_result(frame, scheme, method, currency=currency))


@pytest.mark.parametrize("name", SCOPED)
def test_distinct_preferred_collisions(registrations, name):
    original = asset_frame()
    result, caught = finalise(name, original.copy(), mapping_table(explicit_key=True))
    kept = [r for r in ROWS if r[3] is not None]
    expected = asset_frame([r[:2] + (MAP.get(r[2], r[2]),) + r[3:] for r in kept])
    expected = expected.reorder_levels(["i", "t", "j"])
    reference, _ = finalise(name, original.copy(), None)
    assert_frame_equal(result, expected, check_exact=True, check_index_type=True)
    assert result.attrs == reference.attrs
    assert result.attrs["fixture_metadata"] == ATTRS["fixture_metadata"]
    assert not cm.grain_reports(country=name, table="assets")
    assert len(result) == 8
    new_signal(caught, (2, 5))
    assert len(caught) == 1


@pytest.mark.parametrize("table", [None, pd.DataFrame({"Preferred Label": [P]}),
                                  pd.DataFrame({"Original Label": [A]}),
                                  mapping_table({}), mapping_table({A: Q, B: "different"})],
                         ids=["absent", "no-source", "no-preferred", "empty", "injective"])
def test_no_confirmed_collision_without_effective_alias(registrations, table):
    result, caught = finalise("Mali", asset_frame(), table)
    assert result.index.is_unique
    new_signal(caught)


@pytest.mark.parametrize("name", OUTSIDE)
def test_unapproved_countries_keep_silent_relabel(registrations, name):
    result, caught = finalise(name, asset_frame(), mapping_table())
    assert len(result) == 8 and not result.index.is_unique
    new_signal(caught)


def test_unapproved_table_keeps_silent_relabel(registrations):
    result, caught = finalise("Niger", asset_frame(), mapping_table(), method="test_other_table")
    assert len(result) == 8 and not result.index.is_unique
    new_signal(caught)


def test_distinct_waves_and_households_and_hollow_rows(registrations):
    rows = [(W1, H1, A, 17, 2), (W1, H2, B, 29, 3), (W2, H1, B, 31, 5),
            (W1, H1, B, None, None)]
    result, caught = finalise("Burkina_Faso", asset_frame(rows), mapping_table())
    assert len(result) == 3 and result.index.is_unique
    new_signal(caught)


def test_repeats_keep_legacy_warning_and_strict_behavior(registrations, monkeypatch):
    rows = [(W1, H1, A, 17, 2), (W1, H1, A, 29, 3)]
    original, original_warnings = finalise("Niger", asset_frame(rows), None)
    reports = cm.grain_reports(country="Niger", table="assets")
    mapped, mapped_warnings = finalise("Niger", asset_frame(rows), mapping_table())
    assert len(original) == len(mapped) == 1
    assert mapped.iloc[0].tolist() == original.iloc[0].tolist() == [46, 2]
    assert [w.category for w in mapped_warnings] == [cm.GrainCollapseWarning]
    assert [str(w.message) for w in mapped_warnings] == [str(w.message) for w in original_warnings]
    assert cm.grain_reports(country="Niger", table="assets") == reports
    assert len(reports) == 1
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    with pytest.raises(cm.GrainCollapseError):
        finalise("Niger", asset_frame(rows), mapping_table())
    affected, caught = finalise("Niger", asset_frame(), mapping_table())
    assert len(affected) == 8
    new_signal(caught, (2, 5))


def test_final_grain_and_currency_index(registrations):
    rows = [(W1, H1, A, "x", 17, 2), (W1, H1, B, "y", 29, 3),
            (W1, H1, C, "z", 31, 5), (W1, H1, D, "z", 41, 8)]
    frame = asset_frame(rows, (*LEVELS, "slot"))
    result, caught = finalise("Uganda", frame, mapping_table(), scheme={}, currency="index")
    assert result.index.names == ["i", "t", "j", "slot", "currency"]
    assert result.index.duplicated().sum() == 1
    new_signal(caught, (1, 2))


@pytest.mark.parametrize("names", [("t", "j"), ("t", "i", "j", None)],
                         ids=["missing-household", "unnamed-extra"])
def test_incomplete_index_identity_does_not_prove_alias(registrations, names):
    ids = {"t": W1, "i": H1, None: "canary-extra"}
    rows = [tuple(j if lvl == "j" else ids[lvl] for lvl in names) + (17, 2) for j in (A, B)]
    result, caught = finalise("Uganda", asset_frame(rows, names), mapping_table(), scheme={})
    assert len(result) == 2 and not result.index.is_unique
    new_signal(caught)


@pytest.mark.parametrize("rows,mapping", [
    ([(W1, None, A, 17, 2), (W1, None, B, 29, 3)], MAP),
    ([(W1, H1, A, 17, 2), (W1, H1, B, 29, 3)], {A: None, B: None}),
    ([(W1, H1, None, 17, 2), (W1, H1, A, 29, 3)], {float("nan"): P, A: P}),
], ids=["missing-household", "missing-preferred", "missing-item"])
def test_missing_key_cannot_prove_alias(registrations, rows, mapping):
    result, caught = finalise("Mali", asset_frame(rows), mapping_table(mapping))
    assert len(result) == 2
    new_signal(caught)


def synthetic_public_inputs(countries=None, *, assumed=False):
    """Supply decoded operands at the cache boundary without reading a cache."""
    if countries is None:
        countries = {"Uganda": asset_frame(), "Fictive": asset_frame(ROWS[:1])}

    def supply(self, method, waves):
        assert method == "assets"
        return self._restrict_to_waves(countries[self.name].copy(deep=True), waves)

    def checked(self, method, cache_path, waves):
        assert self.name in countries and cache_path.name == "assets.parquet"
        return supply(self, method, waves), None, None

    stack = ExitStack()
    stack.enter_context(patch.object(Country, "categorical_mapping",
                                     property(lambda self: {"harmonize_assets": mapping_table()})))
    if assumed:
        stack.enter_context(patch.object(Country, "_read_assumed_fresh_cache", supply))
    else:
        stack.enter_context(patch.object(Country, "_read_l2_country_cache", checked))
    return stack


@pytest.mark.parametrize("assumed", [False, True], ids=["checked-gate", "assumed-fresh-gate"])
def test_country_and_feature_forward_once(registrations, assumed):
    with synthetic_public_inputs(assumed=assumed):
        country, caught_country = capture(
            lambda: Country("Uganda", assume_cache_fresh=assumed).assets())
        clear_wave, caught_clear_wave = capture(
            lambda: Country("Uganda", assume_cache_fresh=assumed).assets(waves=[W2]))
        other, caught_other = capture(lambda: Country("Fictive", assume_cache_fresh=assumed).assets())
        feature, caught_feature = capture(
            lambda: Feature("assets", trust_cache=assumed)(["Fictive", "Uganda"], currency=None))
    assert list(feature.index.names) == ["country", "t", "i", "j"]
    assert feature.index.get_level_values("country").tolist() == ["Fictive"] + ["Uganda"] * 8
    assert_frame_equal(feature.xs("Uganda", level="country"),
                       country.reorder_levels(LEVELS), check_exact=True)
    assert_frame_equal(feature.xs("Fictive", level="country"),
                       other.reorder_levels(LEVELS), check_exact=True)
    assert not cm.grain_reports(country="Uganda", table="assets")
    assert len(clear_wave) == 2 and clear_wave.index.is_unique
    new_signal(caught_clear_wave)
    new_signal(caught_other)
    new_signal(caught_country, (2, 5))
    new_signal(caught_feature, (2, 5))
    assert len(caught_country) == 1
    other_categories = [w.category for w in caught_feature if w.category.__name__ != WARNING_NAME]
    assert other_categories == ([DeprecationWarning] * 2 if assumed else [])


@pytest.mark.parametrize("keep_other", [True, False], ids=["with-alias", "repeat-only"])
def test_surviving_same_item_repeats_are_not_a_separate_group(registrations, keep_other):
    # No declared grain in this configuration, so both A rows survive the
    # earlier normalizer. Only the distinct B can establish an alias collision.
    rows = [(W1, H1, A, 17, 2), (W1, H1, A, 29, 3),
            (W1, H1, B, 31 if keep_other else None, None)]
    result, caught = finalise("Uganda", asset_frame(rows), mapping_table(), scheme={})
    assert len(result) == (3 if keep_other else 2)
    assert result.index.duplicated().sum() == len(result) - 1
    assert not cm.grain_reports(country="Uganda", table="assets")
    new_signal(caught, (1, 3) if keep_other else None)


def test_later_spelling_alone_does_not_prove_preferred_alias(registrations):
    rows = [(W1, H1, A, 17, 2), (W1, H1, B, 29, 3)]
    with patch.object(cm, "_load_canonical_spellings", return_value={"assets": {"j": {Q: P}}}):
        result, caught = finalise("Mali", asset_frame(rows), mapping_table({A: P, B: Q}))
    assert result.index.get_level_values("j").tolist() == [P, P]
    new_signal(caught)


def test_bad_explicit_mapping_key_keeps_legacy_error(registrations):
    table = mapping_table(explicit_key=True)
    table.attrs[cm.CATEGORICAL_KEY_ATTR] = "not-a-column"
    with pytest.raises(KeyError, match="categorical key 'not-a-column' is not a column"):
        finalise("Mali", asset_frame(), table)


def test_warning_filter_stays_caller_policy(registrations):
    category = getattr(cm, WARNING_NAME, None)
    assert category is not None, "missing preferred asset label collision warning type"
    with synthetic_public_inputs(), warnings.catch_warnings():
        warnings.simplefilter("error", category)
        with pytest.raises(category, match=r"2 groups, 5 affected rows"):
            Country("Uganda").assets()
