"""GH #1063 -- a ``dfs:`` outer merge must not take its row order from the hash seed.

EHCVM ``household_roster`` builds of the same code came back in different row
orders (CotedIvoire, Mali and Togo on 2026-10-14; Benin and Guinea-Bissau on
2026-10-19): same values, same index set, same dtypes, different order.  The
cause was one line of ``Wave.grab_data``::

    merge_on = list(set('t').union(data_info.get('merge_on')))

Python randomises ``str`` hashing per process (``PYTHONHASHSEED``), so the
order of that list was drawn at random each time the interpreter started, and
``pd.merge(..., how='outer')`` sorts its result lexicographically by the join
keys *in the order given*.  With two or more non-``t`` keys -- the EHCVM
rosters merge on ``[v, i]`` -- a process that drew ``v`` before ``i`` wrote the
wave parquet sorted by cluster, and one that drew ``i`` before ``v`` wrote it
sorted by household id.  "Cold vs warm" and "re-seed vs earlier reads" were
just two processes with different seeds.  Measured on development
(``9a0ed5f3c``): Benin cold builds under ``PYTHONHASHSEED=1`` and ``=3`` differ
at 14,491 of 42,343 positions; Mali at 6,474 of 220,732, all inside the two
EHCVM waves.  Builds under the same seed were order-identical, whatever the
backend or ``LSMS_BUILD_WORKERS``.

The hash seed is fixed when the interpreter starts, so the discriminating
tests run ``grab_data`` in SUBPROCESSES under seeds chosen to put ``v`` and
``i`` in opposite orders in the old set.  The synthetic tests need no
microdata; the Benin test (``requires_s3``) repeats the measurement on the
served table.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]

# Seeds to probe for one of each class (v before i, i before v) in the old set.
_SEEDS = tuple(str(s) for s in range(12))


def _old_key_order(seed: str) -> list[str]:
    """The merge-key order the pre-#1063 line produced under *seed*."""
    code = "print(','.join(set('t').union(['v', 'i'])))"
    # Not `-I`/`-E`: isolated mode ignores PYTHON* variables, PYTHONHASHSEED
    # included, and the probe would silently draw a random seed.
    out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                         text=True, env={**os.environ, "PYTHONHASHSEED": seed}, check=True)
    return out.stdout.strip().split(",")


@pytest.fixture(scope="module")
def opposed_seeds() -> tuple[str, str]:
    """One seed that put ``v`` before ``i`` in the old set, one that put ``i``
    before ``v`` -- the two row orders development drew between.  Chosen by
    probing, not hard-coded, because the seed->order map is an interpreter
    detail."""
    v_first = i_first = None
    for seed in _SEEDS:
        order = _old_key_order(seed)
        if order.index("v") < order.index("i"):
            v_first = v_first or seed
        else:
            i_first = i_first or seed
        if v_first and i_first:
            return v_first, i_first
    pytest.fail(f"no seed in {_SEEDS} separates v and i; the test cannot discriminate")


# ---------------------------------------------------------------------------
# Synthetic country: a two-key outer `dfs:` block whose v-order and i-order
# disagree.  Sorting by (v, i) puts household h2 (cluster c1) first; sorting
# by (i, v) puts household h1 (cluster c2) first.
# ---------------------------------------------------------------------------

_SCHEME = """\
Waves:
  - '2020'
Data Scheme:
  household_roster:
    index: (t, v, i, pid)
    Sex: str
"""

_INFO = """\
household_roster:
  dfs:
    - df_main
    - df_cover
  df_main:
    file: members.csv
    idxvars:
      v: ea
      i: hh
      pid: line
    myvars:
      Sex: sex
  df_cover:
    file: cover.csv
    idxvars:
      v: ea
      i: hh
    myvars:
      Region: reg
  merge_on:
    - v
    - i
  final_index:
    - v
    - i
    - pid
"""

_BUILD = """\
import json, sys
from lsms_library import country as C
df = C.Country('Synthland', preload_panel_ids=False)['2020'].grab_data('household_roster')
json.dump([list(map(str, k)) for k in df.index], sys.stdout)
"""


def _write_synthland(croot: Path) -> None:
    c = croot / "Synthland"
    for d in (c / "_", c / "2020" / "_", c / "2020" / "Data"):
        d.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ea": ["c2", "c2", "c1", "c1", "c3"],
                  "hh": ["h1", "h1", "h2", "h2", "h0"],
                  "line": [1, 2, 1, 2, 1],
                  "sex": ["M", "F", "F", "M", "F"]}).to_csv(
        c / "2020" / "Data" / "members.csv", index=False)
    pd.DataFrame({"ea": ["c1", "c2", "c3"], "hh": ["h2", "h1", "h0"],
                  "reg": ["N", "S", "E"]}).to_csv(
        c / "2020" / "Data" / "cover.csv", index=False)
    (c / "_" / "data_scheme.yml").write_text(_SCHEME)
    (c / "2020" / "_" / "data_info.yml").write_text(_INFO)


def _env(seed: str, croot: Path, droot: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(PYTHONHASHSEED=seed, PYTHONPATH=str(REPO),
               LSMS_COUNTRIES_ROOT=str(croot), LSMS_DATA_DIR=str(droot),
               # raw files in the config tree with no .dvc sidecar (GH #831)
               LSMS_ALLOW_INTREE_SOURCES="1",
               OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
    env.pop("LSMS_NO_CACHE", None)
    return env


def _build_in_subprocess(seed: str, croot: Path, droot: Path) -> list[list[str]]:
    # cwd=REPO: for `python -c`, sys.path[0] is the cwd and beats PYTHONPATH
    # (CLAUDE.md, scrum-master addendum 3).
    r = subprocess.run([sys.executable, "-c", _BUILD], cwd=REPO, capture_output=True,
                       text=True, env=_env(seed, croot, droot))
    assert r.returncode == 0, r.stderr[-3000:]
    return json.loads(r.stdout)


@pytest.fixture
def synthland(tmp_path):
    croot = tmp_path / "countries"
    _write_synthland(croot)
    return croot


def test_dfs_outer_merge_order_does_not_depend_on_the_hash_seed(synthland, tmp_path,
                                                              opposed_seeds):
    """Two COLD builds under seeds that drew opposite key orders on development
    must return the same rows in the same order.

    Fails on development (``9a0ed5f3c``): the ``v``-first seed returns the
    frame sorted by cluster, the ``i``-first seed sorted by household.
    """
    orders = {seed: _build_in_subprocess(seed, synthland, tmp_path / f"data-{seed}")
              for seed in opposed_seeds}
    a, b = (orders[s] for s in opposed_seeds)
    assert sorted(a) == sorted(b), "precondition: the same rows either way"
    assert a == b, (f"row order depends on PYTHONHASHSEED: seed {opposed_seeds[0]} "
                    f"-> {a}, seed {opposed_seeds[1]} -> {b}")


def test_dfs_outer_merge_rows_follow_t_then_the_declared_merge_on_order(synthland,
                                                                      tmp_path,
                                                                      opposed_seeds):
    """The order is not merely stable, it is the DECLARED one: ``t`` first,
    then ``merge_on`` as written (``[v, i]``), which for the EHCVM rosters is
    the index prefix ``(t, v, i)``."""
    got = _build_in_subprocess(opposed_seeds[1], synthland, tmp_path / "data")
    assert [tuple(k[:3]) for k in got] == [
        ("2020", "c1", "h2"), ("2020", "c1", "h2"),
        ("2020", "c2", "h1"), ("2020", "c2", "h1"),
        ("2020", "c3", "h0"),
    ]
    # Ties within a (v, i) key keep the left sub-frame's (member file) order.
    assert [k[3] for k in got] == ["1", "2", "1", "2", "1"]


def test_merge_key_order_is_spelled_without_a_set():
    """Static pin on the build site: the merge keys are an ordered list.  A
    ``set`` over ``str`` keys anywhere in that expression reintroduces the
    seed dependence, whatever the surrounding code does."""
    import inspect

    from lsms_library import country as C

    src = inspect.getsource(C.Wave.grab_data)
    line = next(ln for ln in src.splitlines() if ln.strip().startswith("merge_on ="))
    assert "set(" not in line, line


# ---------------------------------------------------------------------------
# The served table, on real microdata: Benin household_roster built cold twice.
# ---------------------------------------------------------------------------

_SERVE = """\
import sys, warnings
warnings.simplefilter('ignore')
import lsms_library as ll
ll.Country('Benin').household_roster().to_pickle(sys.argv[1])
"""


@pytest.mark.requires_s3
def test_benin_household_roster_cold_builds_are_order_identical(tmp_path, opposed_seeds):
    """The issue's own symptom: two cold builds of Benin's served
    ``household_roster`` (one wave, EHCVM ``merge_on: [v, i]``) in fresh data
    roots, under the two opposed seeds, compared WITHOUT sorting.  On
    development these differ at 14,491 of 42,343 positions."""
    from lsms_library.paths import data_root

    shared_blobs = data_root() / "dvc-cache"
    procs, outs = [], []
    for seed in opposed_seeds:
        root = tmp_path / f"data-{seed}"
        root.mkdir()
        if shared_blobs.exists():
            (root / "dvc-cache").symlink_to(shared_blobs.resolve(), target_is_directory=True)
        env = dict(os.environ)
        env.update(PYTHONHASHSEED=seed, PYTHONPATH=str(REPO), LSMS_DATA_DIR=str(root),
                   LSMS_BUILD_WORKERS="1", OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
        env.pop("LSMS_NO_CACHE", None)
        out = tmp_path / f"roster-{seed}.pkl"
        outs.append(out)
        procs.append(subprocess.Popen([sys.executable, "-c", _SERVE, str(out)], cwd=REPO,
                                      env=env, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True))
    for p in procs:
        _, err = p.communicate(timeout=900)
        assert p.returncode == 0, err[-3000:]
    a, b = (pd.read_pickle(o) for o in outs)
    assert len(a) > 40_000
    pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())   # same content
    pd.testing.assert_frame_equal(a, b)                             # and same order
    for root in (tmp_path / f"data-{s}" for s in opposed_seeds):
        shutil.rmtree(root, ignore_errors=True)
