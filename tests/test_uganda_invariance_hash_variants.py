"""Auth-free controls for the finite Uganda nutrition CPU hash allowance."""
from copy import deepcopy

import pandas as pd
import pytest

from tests import test_uganda_invariance as invariance


NUTRITION = "var/nutrition.parquet"
PRIMARY = "12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a"
HASWELL = "f3d3c34d3fad5253d6b026d3d397d2895bc92ce3124dbdd16a92da1553d89fd6"
UNMEASURED = "0" * 64


@pytest.fixture
def check_parquet(tmp_path, monkeypatch):
    """Call the actual raw-parquet test; no country build or credentials."""
    from lsms_library import local_tools

    frame = pd.DataFrame(
        {"Energy": [10.0, 20.0]},
        index=pd.MultiIndex.from_tuples(
            [("h1", "2019-20"), ("h2", "2019-20")], names=["i", "t"]))
    monkeypatch.setattr(local_tools, "read_parquet_cache_waves", lambda p: None)

    def check(entry, *, actual_hash=None, actual_fields=None, rel_path=NUTRITION,
              data=None):
        parquet = tmp_path / rel_path
        parquet.parent.mkdir(parents=True, exist_ok=True)
        (frame if data is None else data).to_parquet(parquet)
        monkeypatch.setattr(invariance, "BASELINE", {rel_path: entry})
        if actual_hash is not None or actual_fields:
            # Substitute fingerprint metadata only for policy branches; the
            # changed-value control below hashes the real parquet unchanged.
            fingerprint = invariance._fingerprint(frame)
            if actual_hash is not None:
                fingerprint["content_hash"] = actual_hash
            fingerprint.update(actual_fields or {})
            monkeypatch.setattr(invariance, "_fingerprint", lambda df: fingerprint)
        invariance.test_parquet_matches_baseline(
            tmp_path, ["2019-20"], rel_path)

    return frame, check


def _entry(frame):
    entry = invariance._fingerprint(frame)
    entry["content_hash"] = PRIMARY
    entry["content_hash_variants"] = [HASWELL]
    return entry


@pytest.mark.parametrize("actual_hash", [PRIMARY, HASWELL])
def test_measured_exact_hashes_pass_actual_invariance_test(check_parquet, actual_hash):
    frame, check = check_parquet
    check(_entry(frame), actual_hash=actual_hash)


def test_unmeasured_hash_still_fails(check_parquet):
    frame, check = check_parquet
    with pytest.raises(AssertionError, match="content hash mismatch"):
        check(_entry(frame), actual_hash=UNMEASURED)


def test_known_nutrition_hash_does_not_apply_to_another_entry(check_parquet):
    frame, check = check_parquet
    entry = _entry(frame)
    entry.pop("content_hash_variants")
    with pytest.raises(AssertionError, match="content hash mismatch"):
        check(entry, actual_hash=HASWELL, rel_path="var/fct.parquet")


@pytest.mark.parametrize("variants", [None, []])
def test_primary_hash_remains_sufficient_without_variants(check_parquet, variants):
    frame, check = check_parquet
    entry = _entry(frame)
    if variants is None:
        entry.pop("content_hash_variants")
    else:
        entry["content_hash_variants"] = variants
    check(entry, actual_hash=PRIMARY)


@pytest.mark.parametrize("field,wrong,reason", [
    ("shape", [1, 1], "shape mismatch"),
    ("columns", ["Wrong"], "column mismatch"),
    ("index_names", ["wrong", "t"], "index names mismatch"),
    ("dtypes", {"Energy": "int64"}, "dtype mismatch"),
])
def test_known_variant_cannot_bypass_structure(check_parquet, field, wrong, reason):
    frame, check = check_parquet
    with pytest.raises(AssertionError, match=reason):
        check(_entry(frame), actual_hash=HASWELL, actual_fields={field: wrong})


@pytest.mark.parametrize("with_variants", [False, True])
def test_null_primary_remains_explicitly_unpinned(check_parquet, with_variants):
    frame, check = check_parquet
    entry = _entry(frame)
    entry["content_hash"] = None
    if not with_variants:
        entry.pop("content_hash_variants")
    check(entry, actual_hash=UNMEASURED)


def test_null_hash_does_not_unpin_structure(check_parquet):
    frame, check = check_parquet
    entry = _entry(frame)
    entry["content_hash"] = None
    with pytest.raises(AssertionError, match="shape mismatch"):
        check(entry, actual_hash=UNMEASURED, actual_fields={"shape": [1, 1]})


def test_deliberately_changed_values_fail_real_parquet_fingerprint(check_parquet):
    frame, check = check_parquet
    entry = invariance._fingerprint(frame)
    entry["content_hash_variants"] = [HASWELL]
    changed = frame.copy()
    changed.iloc[0, 0] += 1.0
    with pytest.raises(AssertionError, match="content hash mismatch"):
        check(entry, data=changed)


def test_shipped_manifest_keeps_primary_and_limits_variants_to_nutrition():
    manifest = deepcopy(invariance._load_baseline())
    assert manifest[NUTRITION]["content_hash"] == PRIMARY
    variants = {path: entry["content_hash_variants"]
                for path, entry in manifest.items()
                if "content_hash_variants" in entry}
    assert variants == {NUTRITION: [HASWELL]}
