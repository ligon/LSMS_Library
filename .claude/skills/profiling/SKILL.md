---
name: profiling
description: Use this skill when you need to attribute time or memory cost inside the LSMS Library hot paths — Country.<feature>() calls, Feature() cross-country aggregation, _finalize_result, load_dataframe_with_dvc, id_walk, kinship expansion. Covers pyinstrument, cProfile + snakeviz, and tracemalloc recipes, plus how to interpret the layered cache → DVC → waves → finalize architecture in a profile.
---

# Profiling Common LSMS Code Paths

Wall-clock phase timing (`bench/build_feature.py` + `bench/run_bench.sh`)
tells you *how long* a call took. Use this skill when you need to know
*where* the time went inside one of those phases — or where memory is
going on the large DataFrames (`food_acquired`, `Feature('household_roster')()`).

## Setup (once)

```sh
poetry install --with profile
```

The `profile` group is `optional = true` in `pyproject.toml`, so a plain
`poetry install` stays lean. The group adds `pyinstrument` (sampler) and
`snakeviz` (cProfile viewer); `cProfile` and `tracemalloc` are stdlib.

## When to reach for which tool

| Tool                  | Use when…                                                                   | Overhead | Install      |
|-----------------------|-----------------------------------------------------------------------------|----------|--------------|
| **pyinstrument**      | First-pass triage. "Where is time going?" Readable call tree.               | Low      | `profile`    |
| **cProfile + snakeviz** | You need per-call counts and a flame view, or pyinstrument missed a short call. | Medium   | `profile`    |
| **tracemalloc** (stdlib) | Peak memory on large DataFrames — derived tables, cross-country Features.    | High     | built-in     |

Default is pyinstrument. Escalate to cProfile only when the sampler view
looks suspicious (flat, too-short, or doesn't match wall-clock numbers).

## Canonical recipe — via the bench harness

`bench/build_feature.py` grew a `--profile {pyinstrument,cprofile,none}`
flag. Use it instead of writing one-off instrumentation.

```sh
# pyinstrument, cache in whatever state it's in
make profile country=Niger feature=household_roster

# Force a cold rebuild (LSMS_NO_CACHE=1 is set for you)
make profile-cold country=Niger feature=household_roster

# Deterministic cProfile — open the .prof with snakeviz
make profile-cprofile country=Niger feature=food_acquired
poetry run snakeviz bench/results/$(date -u +%Y-%m-%d)/Niger-food_acquired-phase3-cold.prof
```

Outputs land in `bench/results/{YYYY-MM-DD}/`:

- pyinstrument → `{Country}-{feature}-phase3-cold.html` and `…-phase4-warm.html`
- cProfile → `…-phase3-cold.prof` and `…-phase4-warm.prof`

A one-line JSON record is appended to `bench/results/{YYYY-MM-DD}.jsonl`
with `profile_paths` pointing at the artefacts.

## Canned recipes for the 8 common code paths

All paths exercised through the same harness. Pick cold vs. warm based
on what you're investigating.

| Code path                                         | File:line (anchor)                     | Command                                                                 |
|---------------------------------------------------|----------------------------------------|-------------------------------------------------------------------------|
| `Country.food_acquired()` end-to-end              | `lsms_library/country.py:1332`         | `make profile-cold country=Niger feature=food_acquired`                 |
| `Feature('household_roster')()` cross-country     | `lsms_library/feature.py:106`          | See **Feature recipe** below                                            |
| Derived `food_expenditures` (from `food_acquired`)| `lsms_library/transformations.py:177`  | `make profile country=Niger feature=food_expenditures`                  |
| Derived `household_characteristics`               | `lsms_library/transformations.py:62`   | `make profile country=Niger feature=household_characteristics`          |
| `Wave.grab_data()` + `df_data_grabber()`          | `lsms_library/country.py:512`, `local_tools.py:522` | `make profile-cold country=Niger feature=household_roster` (phase 3)    |
| `load_dataframe_with_dvc()` DVC-stage path        | `lsms_library/country.py:1670`         | `make profile-cold country=Uganda feature=household_roster` (stage-layer) |
| `_finalize_result()` pipeline                     | `lsms_library/country.py:1259`         | Any `make profile …` — phase 4 (warm) isolates finalize overhead        |
| `id_walk()` + transitive panel chains             | `lsms_library/local_tools.py:1059`     | `make profile country=BurkinaFaso feature=household_roster`             |

### Feature recipe (no Makefile target yet — small wrapper)

`bench/build_feature.py` only exercises one country. For cross-country
`Feature(...)` profiling, invoke pyinstrument directly:

```sh
poetry run python -m pyinstrument -r html \
    -o bench/results/$(date -u +%Y-%m-%d)/Feature-household_roster.html \
    -c "import lsms_library as ll; ll.Feature('household_roster')()"
```

## Profiling memory (tracemalloc)

```python
import tracemalloc, lsms_library as ll
tracemalloc.start(25)                          # 25-frame traceback depth
df = ll.Country('Niger').food_acquired()
snap = tracemalloc.take_snapshot()
for stat in snap.statistics('lineno')[:20]:
    print(stat)
```

Look for top stats pointing into `transformations.py` (derived tables),
`_expand_kinship` in `country.py`, or pandas `groupby`/`merge` internals.

## Interpretation guide — what you'll see in a profile

Profiles of a Country call show a layered architecture. Identify which
layer dominates before optimizing.

1. **Layer 0 — parquet cache hit (top-of-function read).** Near-zero on a
   warm profile: `load_dataframe_with_dvc` returns at `country.py:1699–1706`
   after reading `~/.local/share/lsms_library/{Country}/var/{table}.parquet`.
   Expect <100 ms total on phase 4 (warm in-proc). See `docs/guide/caching.md`
   for empirical numbers.
2. **Layer 1 — DVC stage reproduce.** Stage-layer countries (Uganda, Senegal,
   Malawi, Togo, Kazakhstan, Serbia, GhanaLSS) go through `stage.reproduce()`
   on cold. Expect `dvc.repo.reproduce` to dominate cold profiles for these.
3. **Layer 2 — wave rebuild.** Non-stage countries call `load_from_waves`
   → `Wave.grab_data` → `df_data_grabber` → `get_dataframe` (the `local →
   DVC → WB NADA` fallback in `local_tools.py:300`). Expect `pyreadstat` or
   `pd.read_stata` on the critical path for `.dta` inputs.
4. **Layer 3 — finalize.** Runs on every call, including warm hits:
   `_join_v_from_sample`, `id_walk`, `_expand_kinship`,
   `_enforce_canonical_spellings`, `_apply_categorical_mappings` (all inside
   `_finalize_result()` at `country.py:1259`). Should collectively stay
   <10 % of total for large tables. If it's >50 % on phase 4 of a big table,
   that's the target.

**Known data shapes** (roughly, for sanity-checking a profile):

- `household_roster`: 5 K – 50 K rows per wave
- `food_acquired`: 100 K – 500 K rows
- `Feature('household_roster')()`: 0.5 M – 2 M rows across ~40 countries

## Gotchas with teeth

- **Cold means cold.** The top-of-function cache read at
  `country.py:1699–1706` returns in milliseconds. If you profile without
  clearing the parquet, you are profiling that read and nothing else. Clear
  first:
  ```sh
  poetry run lsms-library cache clear --country Niger
  # or: rm -f ~/.local/share/lsms_library/Niger/var/*.parquet
  ```
  `make profile-cold` sets `LSMS_NO_CACHE=1` for you, which bypasses the top
  read in-process but does NOT delete the parquet — use the explicit clear
  for cross-process cold.
- **`LSMS_BUILD_BACKEND=make`** forces a full source rebuild through the
  legacy make backend, bypassing DVC entirely. Use to isolate wave-rebuild
  cost from DVC overhead.
- **pyinstrument misses short calls.** If the flame view is suspiciously
  flat, rerun with `make profile-cprofile …`. Deterministic profilers catch
  the short-but-frequent calls the sampler misses.
- **`_finalize_result` runs on every call.** If it's hot in a warm-call
  profile, that's the target — not the data read.
- **`_log_issue` pollutes the tree.** `country.py:168` appends to
  `lsms_library/ISSUES.md` on any cache/materialization hiccup during a
  cold run. After profiling: `git checkout HEAD -- lsms_library/ISSUES.md`.
- **Profile artefacts aren't gitignored.** They land in `bench/results/`;
  don't commit the `.html` / `.prof` files. Commit the JSON record only
  if it adds useful baseline data for a PR.
- **Fresh subprocess matters.** In-process caches on the Country object
  (`_sample_v_cache`, `_updated_ids_cache`, `_market_lookup_cache_*`) hide
  real work. Phase 4 is specifically the in-process warm case; if you want
  cross-process warm, run `bench/run_bench.sh` twice.

## If you need more than this

Not in the `profile` group; reach for these ad hoc and don't commit deps:

- `line_profiler` — per-line attribution inside a single function. Add a
  `@profile` decorator and run under `kernprof -l -v …`.
- `scalene` — CPU + memory + GPU, per-line. Heavier, good for memory regressions.
- `py-spy` / `memray` — sampling profilers that can attach to a running process.
