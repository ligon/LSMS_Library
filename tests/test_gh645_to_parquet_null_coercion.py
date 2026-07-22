"""GH #645 -- ``to_parquet`` must not destroy values that spell like nulls.

``to_parquet`` used to serialize object columns as::

    col.astype(str).astype('string[pyarrow]').replace(
        {'nan': None, 'None': None, '<NA>': None})

i.e. it stringified **first** and then tried to recover the nulls by matching
the resulting characters.  After ``astype(str)`` a genuine missing value and a
legitimate value that happens to spell ``'None'`` are the same three-to-four
characters, so the recovery cannot be right -- and it was not.  ``None`` was
the canonical library label for "no education", so every person the survey
recorded as never-schooled was nulled **in the act of writing the cache**, and
``Country._finalize_result``'s ``dropna(how='all')`` then deleted the row
outright (``individual_education`` has exactly one column).

Two independent things are pinned here.

1. **The write path** (unit, data-free): the null/not-null distinction must be
   captured *before* the cast and restored after, so a literal ``'None'`` /
   ``'nan'`` / ``'<NA>'`` survives a round-trip and only genuinely-missing
   cells come back null.  The pyarrow guard the block exists for -- genuinely
   mixed-type object columns -- must still work.

2. **The vocabulary** (unit, data-free): level 0 of the canonical education
   ladder is no longer spelled ``None``.  A canonical value that collides with
   the null literal under ``str()``, YAML, CSV and parquet round-trips is a
   trap that re-arms itself; the write-path fix alone would leave it loaded.
   ``None`` survives as an accepted *variant* so historical caches and any
   not-yet-updated country still resolve.

3. **The recovered rows** (integration, needs S3 + a cold cache): the three
   countries measured on the issue.  These MUST run against a genuinely fresh
   ``LSMS_DATA_DIR`` -- ``LSMS_NO_CACHE=1`` alone actually *masks* this bug,
   because it skips the wave-level parquet write where the destruction
   happened.  Each case therefore runs in its own subprocess with its own
   empty data root.

Negative control (2026-07-21, this worktree, cold isolated data dirs, measured
against the merge-base for each arm):

===============  ==================  ============  =============
country          before (cold/warm)  after         rows recovered
===============  ==================  ============  =============
Guatemala        20,678 / 20,678     29,527        8,849  (30%)
Tajikistan       59,297 / 54,462     59,790        493 / 5,328
Ethiopia         63,139 / 62,939     63,181        42 / 242
===============  ==================  ============  =============

Ethiopia's pair is exactly the 63,139 vs 62,939 reported on the issue.  (An
earlier arm of this work measured 59,059 / 58,859 / 59,092 for Ethiopia; those
predate PR #644, which re-keyed 2013-14 `individual_education` onto the
wave-native ids and recovered 5,247 people independently of this bug.)

Guatemala is the reason this went unnoticed for so long: cold == warm == wrong,
so no A/B comparison could see it, and the coverage matrix graded it ``sane``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

try:                                  # `tests` is a package, so the bare name
    from tests.conftest import requires_s3   # `conftest` resolves to the ROOT
except ImportError:                   # conftest.py, which has no requires_s3.
    from conftest import requires_s3

import lsms_library.local_tools as lt
from lsms_library.local_tools import get_dataframe, to_parquet


REPO_ROOT = Path(__file__).resolve().parent.parent
COL = "Educational Attainment"


# ---------------------------------------------------------------------------
# 1. The write path
# ---------------------------------------------------------------------------

def _edu_frame() -> pd.DataFrame:
    """One-column frame in ``individual_education`` shape.

    Deliberately ``dtype=object``: that is the only dtype the coercion block
    fires on, and under pandas 3.0 a *pure* string column is inferred as ``str``
    and skips it entirely -- which is why the bug was so patchy across
    countries (Guatemala/Tajikistan/Ethiopia had object columns; 22 others did
    not, and were fine only by accident of dtype inference).
    """
    idx = pd.MultiIndex.from_tuples(
        [("2000", f"h{k}", f"p{k}") for k in range(6)], names=["t", "i", "pid"])
    return pd.DataFrame({COL: pd.Series(
        ["Primary complete", "None", "Upper secondary", np.nan, "nan", "<NA>"],
        dtype=object, index=idx)})


def _roundtrip(df: pd.DataFrame) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "t.parquet")
        to_parquet(df.copy(), p, absolute_path=True)
        return get_dataframe(p)


def test_literal_none_survives_the_parquet_roundtrip():
    """The minimal repro from the issue: 5 rows in, 5 rows out."""
    df = _edu_frame()
    assert len(df.dropna(how="all")) == 5
    back = _roundtrip(df)
    assert len(back.dropna(how="all")) == 5, (
        "rows were deleted by the write path; values that spell like nulls are "
        f"being coerced to NULL: {list(back[COL])}")
    assert back[COL].iloc[1] == "None"


@pytest.mark.parametrize("literal", ["None", "nan", "<NA>"])
def test_null_spelling_literals_are_not_coerced(literal):
    idx = pd.Index(["a", "b"], name="i")
    df = pd.DataFrame({"c": pd.Series([literal, "other"], dtype=object, index=idx)})
    back = _roundtrip(df)
    assert back["c"].iloc[0] == literal
    assert back["c"].notna().all()


def test_genuine_missing_values_still_become_null():
    """The block's legitimate job: real NaN / None / NaT round-trip as NULL."""
    idx = pd.Index(list("abcd"), name="i")
    df = pd.DataFrame({"c": pd.Series(
        [np.nan, None, pd.NaT, "kept"], dtype=object, index=idx)})
    back = _roundtrip(df)
    assert list(back["c"].isna()) == [True, True, True, False]


def test_mixed_type_object_column_is_still_stringified():
    """The reason the coercion block exists at all.

    Under pandas 3.0 / pyarrow 23 a genuinely mixed object column still raises
    ``ArrowTypeError`` ("Expected bytes, got a 'int' object"), so the guard is
    load-bearing and must not be narrowed away.  Pinned so a future "this is
    obsolete under pandas 3" cleanup has to confront the evidence.
    """
    import pyarrow.lib as _pa

    idx = pd.Index(list("abc"), name="i")
    mixed = pd.DataFrame({"c": pd.Series(["a", 3, None], dtype=object, index=idx)})
    with tempfile.TemporaryDirectory() as d:
        with pytest.raises((_pa.ArrowTypeError, _pa.ArrowInvalid)):
            pd.DataFrame({"c": mixed["c"].reset_index(drop=True)}).to_parquet(
                os.path.join(d, "raw.parquet"), engine="pyarrow", index=False)
    back = _roundtrip(mixed)
    assert list(back["c"][:2]) == ["a", "3"]
    assert back["c"].isna().iloc[2]


def test_cache_schema_was_bumped_for_this_fix():
    """The fix changes cached parquet CONTENT, not any per-table input hash.

    Without the schema bump every warm machine keeps serving the nulls and the
    fix is invisible on exactly the machines where the corruption already
    exists.  See ``LSMS_CACHE_SCHEMA`` in ``local_tools.py``.
    """
    assert lt.LSMS_CACHE_SCHEMA >= 5


# ---------------------------------------------------------------------------
# 2. The vocabulary
# ---------------------------------------------------------------------------

def _harmonize_education_tables():
    """Yield ``(path, header, rows)`` for every ``harmonize_education`` table."""
    paths = sorted((REPO_ROOT / "lsms_library" / "countries").glob(
        "*/_/categorical_mapping.org"))
    paths.append(REPO_ROOT / "lsms_library" / "categorical_mapping"
                 / "harmonize_education.org")
    for path in paths:
        if not path.exists():
            continue
        in_tbl, header, rows = False, None, []
        for line in path.read_text(encoding="utf-8").splitlines():
            m = re.match(r"\s*#\+name:\s*(\S+)", line)
            if m:
                if in_tbl and header:
                    yield path, header, rows
                in_tbl, header, rows = m.group(1) == "harmonize_education", None, []
                continue
            if in_tbl and line.strip().startswith("|"):
                if set(line.strip()) <= set("|-+ "):
                    continue
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                if header is None:
                    header = cells
                else:
                    rows.append(cells)
            elif line.strip() and not line.strip().startswith("|"):
                if in_tbl and header:
                    yield path, header, rows
                in_tbl, header, rows = False, None, []
        if in_tbl and header:
            yield path, header, rows


def test_no_harmonize_education_table_still_emits_the_label_None():
    """``None`` must not be a *target* of any education mapping.

    It may remain on the left (a raw survey label spelled ``None``); what must
    go is the canonical value.
    """
    offenders = []
    for path, header, rows in _harmonize_education_tables():
        if "Preferred Label" not in header:
            continue
        col = header.index("Preferred Label")
        for cells in rows:
            if col < len(cells) and cells[col] == "None":
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {cells}")
    assert not offenders, (
        "'None' is still a canonical education label. It is indistinguishable "
        "from a null under str()/YAML/CSV/parquet round-trips -- use "
        "'No education' (GH #645):\n  " + "\n  ".join(offenders))


def test_harmonize_education_targets_stay_inside_the_declared_vocabulary():
    """Every mapping target must be a canonical value in ``data_info.yml``.

    This is what makes the rename safe to declare: the ``spellings`` block for
    ``Educational Attainment`` is an assertion, and
    ``diagnostics._check_declared_vocabularies`` FAILS a country whose values
    fall outside it.
    """
    vocab, variants = _declared_education_vocabulary()
    offenders = []
    for path, header, rows in _harmonize_education_tables():
        if "Preferred Label" not in header:
            continue
        col = header.index("Preferred Label")
        for cells in rows:
            if col >= len(cells):
                continue
            v = variants.get(cells[col], cells[col])
            if v and v not in vocab:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {cells[col]!r}")
    assert not offenders, (
        "education mapping targets outside the declared vocabulary "
        f"{sorted(vocab)}:\n  " + "\n  ".join(sorted(set(offenders))))


def _declared_education_vocabulary():
    info = yaml.safe_load(
        (REPO_ROOT / "lsms_library" / "data_info.yml").read_text(encoding="utf-8"))
    spellings = info["Columns"]["individual_education"][COL]["spellings"]
    variants = {v: canonical
                for canonical, vs in spellings.items() for v in (vs or [])}
    return frozenset(spellings), variants


def test_None_is_a_declared_variant_of_No_education():
    """Historical parquets and un-updated countries must still resolve.

    ``_enforce_canonical_spellings`` runs at API time on columns AND index
    levels, so a cache written before the rename maps forward without a
    rebuild.
    """
    vocab, variants = _declared_education_vocabulary()
    assert "No education" in vocab
    assert "None" not in vocab, "the null-colliding spelling is canonical again"
    assert variants.get("None") == "No education"


def test_declared_education_vocabulary_matches_the_canonical_org_file():
    """``canonical_education_labels.org`` is the documentation of record."""
    text = (REPO_ROOT / "lsms_library" / "categorical_mapping"
            / "canonical_education_labels.org").read_text(encoding="utf-8")
    vocab, _ = _declared_education_vocabulary()
    documented = set()
    for line in text.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        documented.update(cells)
    missing = {v for v in vocab if v not in documented}
    assert not missing, (
        f"declared but undocumented education levels: {sorted(missing)}")


# ---------------------------------------------------------------------------
# 3. The recovered rows (cold cache, real data)
# ---------------------------------------------------------------------------

# (country, expected rows, rows recovered vs. the pre-fix warm cache)
RECOVERED = [
    ("Guatemala", 29527, 8849),
    ("Tajikistan", 59790, 5328),
    ("Ethiopia", 63181, 242),
]

_COLD_PROBE = """
import json, warnings
warnings.filterwarnings("ignore")
import lsms_library as ll
c = %r
cold = len(ll.Country(c).individual_education())
warm = len(ll.Country(c).individual_education())
df = ll.Country(c).individual_education()
print("PROBE" + json.dumps({
    "cold": cold, "warm": warm,
    "no_education": int((df["Educational Attainment"] == "No education").sum()),
}))
"""


def _cold_probe(country: str) -> dict:
    """Build *country* in a subprocess with a genuinely empty data root.

    ``LSMS_NO_CACHE=1`` is deliberately NOT used: it skips the wave-level
    parquet write, which is where the destruction happened, so it *hides* the
    bug this test exists to catch.
    """
    with tempfile.TemporaryDirectory() as d:
        blobs = Path.home() / ".local" / "share" / "lsms_library" / "dvc-cache"
        if blobs.exists():                       # reuse L1 blobs, never L2
            os.symlink(blobs, Path(d) / "dvc-cache")
        env = dict(os.environ)
        env.pop("LSMS_NO_CACHE", None)
        env["LSMS_DATA_DIR"] = d
        env["PYTHONPATH"] = os.pathsep.join(
            [str(REPO_ROOT), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        r = subprocess.run([sys.executable, "-c", _COLD_PROBE % country],
                           capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))
    for line in r.stdout.splitlines():
        if line.startswith("PROBE"):
            return json.loads(line[len("PROBE"):])
    pytest.fail(f"cold probe for {country} produced no result:\n"
                f"stdout:\n{r.stdout[-3000:]}\nstderr:\n{r.stderr[-3000:]}")


@requires_s3
@pytest.mark.parametrize("country,expected,recovered", RECOVERED)
def test_individual_education_row_count_is_restored(country, expected, recovered):
    got = _cold_probe(country)
    assert got["cold"] == expected, (
        f"{country}.individual_education(): expected {expected} rows, got "
        f"{got['cold']}. Before GH #645 this returned {expected - recovered} on a "
        "warm cache -- every missing row a person the survey recorded as having "
        "no education.")
    assert got["no_education"] >= 1, (
        f"{country} has no 'No education' rows at all, which is what the bug "
        "looked like")


@requires_s3
@pytest.mark.parametrize("country,expected,recovered", RECOVERED)
def test_cold_and_warm_builds_agree(country, expected, recovered):
    """The invariant the destruction broke.

    ``to_parquet`` REBINDS its return value at the wave level
    (``country.py`` ``Wave.grab_data``) but the country level DISCARDS it, so a
    write-path coercion made the building call and every later call disagree.
    With a value-preserving write they must be identical.
    """
    got = _cold_probe(country)
    assert got["cold"] == got["warm"], (
        f"{country}.individual_education() is call-order / cache-state "
        f"dependent: cold {got['cold']} vs warm {got['warm']}")
