# Prior-Art Ledger — §0 Standing Baseline (LSMS Library)

> Repo-wide baseline that every per-task ledger inherits. It captures the
> library's most **reuse-prone machinery**, the **definitions in force**, and the
> **load-bearing invariants** — the things an agent most often reinvents or
> contradicts. Per-task ledgers should *cite* this file (and `CLAUDE.md`,
> `lsms_library/data_info.yml`) rather than re-copy it. Edit in place; git is the
> journal.

**Search tier used:** ripgrep + git (floor). gitnexus was read-only this
session. Re-grep before trusting a `path:line` for a finer-grained edit.
**Line anchors as of:** `b23b42ac` (drift expected — match on symbol name).

---

## §2 Existing machinery (reuse before you rebuild)

The canonical, tested machinery that already touches the areas tasks most often
re-enter. If your task computes one of these, the default is **reuse**, not a new
implementation.

| symbol | path:line | what it does | tested? |
|--------|-----------|--------------|---------|
| `Country._FOOD_DERIVED` | `lsms_library/country.py:3318` | runtime derivation of `food_expenditures` / `food_prices` / `food_quantities` from `food_acquired` | via per-country `test`/build + schema tests |
| `Country._ROSTER_DERIVED` | `lsms_library/country.py:3325` | runtime derivation of `household_characteristics` from `household_roster` | as above |
| `Country._join_v_from_sample` | `lsms_library/country.py:1633` | joins cluster `v` from `sample()` at API time for household-level tables | exercised by every roster/feature build |
| `_expand_kinship` | `lsms_library/country.py:3787` | Kroeber decomposition `Relationship → (Sex, Generation, Distance, Affinity)` via `categorical_mapping/kinship.yml` | kinship YAML + roster tests |
| `_enforce_canonical_spellings` | `lsms_library/country.py:3931` | replaces variant values/index labels with canonical forms from `data_info.yml` `spellings` | `tests/test_schema_consistency.py` |
| `_finalize_result` | `lsms_library/country.py:2098` | the single post-read pipeline: kinship, spellings, dtype coercion, `id_walk`, `v`-join | the integration test surface |
| `get_dataframe` | `lsms_library/local_tools.py:805` | the **only** sanctioned reader: local → DVC → WB NADA fallback chain | yes |
| `to_parquet` | `lsms_library/local_tools.py:1570` | the **only** sanctioned writer: redirects to `data_root()` via call-stack inference | yes |
| `cache_freshness` / `stamp_parquet_hash` | `lsms_library/local_tools.py:1515` / `1536` | content-hash cache invalidation gates (v0.8.0) | cache tests |
| `Wave._input_hash` / `Country._table_cache_hash` | `lsms_library/country.py:585` / `2230` | compute the embedded `lsms_cache_hash` over pre-finalize inputs | cache tests |
| `Country._assert_built_required_columns` | `lsms_library/country.py:2355` | post-build guard: raises if a script-path table is missing a required declared column | regression net (PR #243) |

**Reuse-search hint:** these methods rarely share a name with a naive
reimplementation. Search by *what it computes* — e.g. "derive expenditures",
"join cluster", "kilogram factor", "months present" — not just by identifier.

## §3 Definitions & conventions in force (cite, don't paraphrase)

The authoritative sources. Quote these; do not restate schema rules from memory.

- **Canonical schema** = `lsms_library/data_info.yml` (single source of truth:
  required columns, accepted values, rejected spellings). Tests read it directly
  — `tests/test_schema_consistency.py:22`. **Never hardcode schema rules.**
- **Kinship** = four columns `(Sex, Generation, Distance, Affinity)`, *not* a
  `Relationship` string (Kroeber 1909). `Generation` 0/±1, `Distance` 0=lineal /
  1=sibling line / 2=cousin. Per `data_info.yml` + `categorical_mapping/kinship.yml`.
- **`v`** = sampling-cluster id, owned by `sample()` and `cluster_features`;
  joined at API time. Two weights: `weight` (cross-sectional) vs `panel_weight`
  (longitudinal). See `CLAUDE.md` §"`sample()` and Cluster Identity".
- **Derived tables** (`food_expenditures`, `food_prices`, `food_quantities`,
  `household_characteristics`) are runtime-derived, *not* registered tables. See
  `CLAUDE.md` §"Derived Tables".
- **`food_prices(units=)` / `food_quantities(units=)`** — the `'kgvalue'` default
  is `Expenditure/Quantity_kg`, deliberately *not* the literature's "unit value".
  See `slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org`.
- **MonthsSpent / MonthsAway / WeeksAway** residence-duration semantics: see
  `CLAUDE.md` §"MonthsSpent / MonthsAway / WeeksAway".

## §4 Invariants & assumptions (the landmines)

- **IO is sanctioned-only.** Read with `get_dataframe`, write with `to_parquet`.
  Never `pd.read_stata`, `pyreadstat`, raw `dvc.api.open`, or absolute paths.
  Never `dvc pull`/`dvc fetch` from the CLI (global lock; fails under concurrency).
  See `CLAUDE.md` §"Data Access" anti-pattern table.
- **Do NOT bake `v` into feature parquets**; write `(t, i, …)` and let the
  framework join. Do NOT add `v` to feature `data_scheme.yml` indexes except
  `cluster_features`.
- **Do NOT register derived tables** in `data_scheme.yml` (`_FOOD_DERIVED` /
  `_ROSTER_DERIVED` auto-surface them).
- **`attrs['id_converted']` must survive** any `merge`/`set_index` downstream of
  `id_walk` in `_finalize_result` — copy `attrs` explicitly or transitive panel
  chains double-apply (BF 2021-22 dup bug, commit `4db41a27`).
- **Config paths resolve via `countries_root()` / `Wave.file_path`**, never a
  hardcoded `files("lsms_library")/"countries"` (ignores `LSMS_COUNTRIES_ROOT`).
- **Cached parquets store pre-finalize data** — kinship/spellings/dtype happen on
  every read, not at write time.
- **Targets pandas 3.0**: no `inplace=`, `pd.NA` for string/id/categorical
  missing, `.loc[mask, col]=` not chained assignment. See `CLAUDE.md` §"Pandas 3.0".

## §5 Default reuse decisions

For the recurring quantities, the standing decision is **reuse** — a per-task
ledger marking any of these "new" must justify why the existing tested path
doesn't fit, in the ledger and the commit message.

| quantity / behavior | decision | anchor |
|---------------------|----------|--------|
| food expenditures / prices / quantities | reuse `_FOOD_DERIVED` | §2 |
| household characteristics | reuse `_ROSTER_DERIVED` | §2 |
| cluster `v` on a household table | reuse `_join_v_from_sample` (or declare index without `v`) | §2 |
| kinship decomposition | reuse `_expand_kinship` + `kinship.yml` | §2 |
| reading any source file | reuse `get_dataframe` | §2/§4 |
| writing any wave/country parquet | reuse `to_parquet` | §2/§4 |
| cache invalidation | reuse the content-hash gates; do not hand-roll | §2/§4 |
