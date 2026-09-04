# Prior-Art Ledger — Togo `nonfood_expenditures` from the EHCVM s09 modules (GH #750)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`, `CLAUDE.md` and
> `lsms_library/data_info.yml` — cited, not re-copied.

**Search tier used:** ripgrep + git floor. gitnexus MCP failed to connect this
session (CONNECT_TIMEOUT), as did the github MCP; `gh` CLI used instead.

## §1 Task, restated

Register a `nonfood_expenditures` table for the country `Togo`, wave `2018`
(the single EHCVM wave), built from the standardized EHCVM distribution in
`2018/Data1/` — section-9 modules `s09a..s09f_me_tgo2018.dta`. Retire the five
orphan `Togo_survey2018_nonfooditems*.csv.dvc` sidecars in `2018/Data/`, whose
blobs were `dvc add`ed in 2021 and never pushed (GH #750) and are therefore
unrecoverable, together with their only consumer `2018/_/nonfood_expenditures.py`
(dead: not declared in `data_scheme.yml`, pre-canonical `j`/`i`/`m` index names,
imports `cfe.df_utils`, which the library no longer carries).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Uganda/_/nonfood_expenditures.py` | `countries/Uganda/_/nonfood_expenditures.py:1` | country-level concat of 8 wave parquets; emits a WIDE matrix (items as columns), index `(i,t,m)` | no | precedent only — **not** reused (see §5) |
| `uganda.nonfood_expenditures()` | `countries/Uganda/_/uganda.py:533` | single-`.dta` → wide (HHID × item) sum over purchased/away/produced/given | no | precedent only |
| `Nigeria/_/nonfood_expenditures.py` | `countries/Nigeria/_/nonfood_expenditures.py:1` | country-level concat of 4 wave parquets; WIDE, index `(j,t,m)` in the script | no | precedent only |
| `Togo/2018/_/livestock.py` | `countries/Togo/2018/_/livestock.py:1` | the modern Togo EHCVM wave-script template: self-contained, `get_dataframe` → build → `to_parquet` | via `Feature()` audit | **reuse the shape** (id helper, `_harmonized_codes`, `_map_codes`) |
| `togo.i()` | `countries/Togo/_/togo.py` | composite household id `format_id(grappe) + '0' + format_id(menage, zeropadding=2)` | — | **reuse** (inlined verbatim, as `livestock.py` does) |
| `local_tools.code_label_map` | `local_tools.py:1478` | `Code -> Label` org lookup with dual int/str keys | yes | **reuse** in preference to bare `get_categorical_mapping` |
| `local_tools.df_from_orgfile` | `local_tools.py:1621` | org-table reader; the `#+name:` matcher | yes (GH #461) | reuse |
| `Country._join_v_from_sample` | `country.py` | joins `v` from `sample()` at API time | yes | **reuse** — do not emit `v` |
| `Country._audit_index_collapse` | `country.py` | GH #323 grain audit | yes | reuse (verify 0 warnings) |

## §3 Definitions & conventions in force

- **EHCVM key convention**: `v: grappe`, `i: [grappe, menage]`, no `vague` level —
  each grappe is visited in exactly one vague. `CLAUDE.md` §"Gotchas with Teeth";
  `Togo/_/CONTENTS.org` §"Sampling Design".
- **`v` is never emitted by a feature**: "Do NOT put `v` in feature
  `data_scheme.yml` indexes other than `cluster_features`", `CLAUDE.md`
  §"`sample()` and Cluster Identity". Confirmed empirically here: `Togo.assets`,
  declared `(t,i,j)`, delivers `['i','t','j']` (auto-exempt, `assets` is in
  `index_info` without `v`); `Uganda.nonfood_expenditures` delivers `['i','t','v']`
  (NOT in `index_info` → the join fires).
- **Core never aggregates**; a wave *script* may reduce to its declared grain
  (`livestock.py:87` does). `CLAUDE.md` §"Grain Collapse"; decision D1 of
  `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`.
- **REPORTED values only**; derived quantities are transformations, never
  columns (`Togo/_/data_scheme.yml` `crop_production`, `plot_labor` comments).
- **A closing "module absent" claim requires evidence** — `CLAUDE.md`
  §"Adjudicating `absent` cells" (the Albania mistake).
- **Sanctioned IO**: `get_dataframe` / `to_parquet` from `local_tools`; never
  `pd.read_stata` on an absolute path, never the `dvc` CLI. `CLAUDE.md` §"Data Access".

## §4 Invariants & assumptions

- `#+name:` **trap** (`local_tools.py:1636`): the org-table header is matched by
  *exact string equality* against `f'#+name: {name}'.lower()` after
  `.strip().lower()`. Case is free (`#+NAME:` works — Togo's file mixes both),
  but the **single space after the colon is mandatory** and nothing may follow
  the name. A miss raises `KeyError` (GH #461) rather than silently returning
  the file's first table — the failure that once labelled every crop with a unit.
- `get_categorical_mapping` with no value kwarg returns `{}` silently; use
  `code_label_map` (`local_tools.py:1478`, GH #372/#377/#348).
- `data_scheme.yml` is hashed **by name** into `Country._table_cache_hash`, so
  editing it cold-rebuilds every Togo table. Expected, not a defect.
- Item codes in EHCVM s09 are globally unique across the sub-modules
  (2xx/3xx/4xx/5xx/6xx), so `j` determines the recall window — the window need
  not be an index level to be recoverable.
- `s09b..s09f` as distributed are **already filtered to the gate `q02 == 1`
  (Oui)**: every shipped row is a reported purchase. Absence of a
  `(household, item)` pair therefore means "did not buy", NOT "not asked".
  `s09a` is the opposite: a complete 6,171 × 13 grid with a 1/2 gate.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household id `i` | reuse | inline `togo.i()` verbatim, as `livestock.py` does; matches `sample()` 1:1 |
| cluster id `v` | reuse | framework joins it from `sample()`; the script must not emit it |
| item labels `j` | new (`nonfood_items` org table) | no existing Togo table decodes s09 codes; Stata value labels are truncated at ~110 chars, the instrument is not |
| `Expenditure` | new column | reported FCFA over the item's own recall window; name matches `food_acquired.Expenditure` |
| `RecallWindow` | new column | the reported recall period, verbatim from the instrument. NOT annualised — annualisation is a transformation |
| index shape | **new (long)**, deviating from the two precedents | Uganda `(i,t,v)` and Nigeria `(i,t,v,m)` are WIDE and disagree with *each other*; wide cannot carry the window at all. Long `(t,i,j)` matches `assets: (t,i,j)`, the canonical item-level shape |
| s09a (fêtes/cérémonies) | **excluded**, documented | different grain (event × 5 fixed categories, 2 of them food); the five retired CSVs were s09b–f only, so this is replacement parity. Follow-up named: a `ceremony_expenditures` feature keyed `(t,i,event)` |

## §6 Open questions for the human

- `nonfood_expenditures` has no block in `lsms_library/data_info.yml`
  (`Index Info > index_info`, `Columns`). One is proposed in the PR body but NOT
  applied — that file is out of scope for this task. Until it lands,
  `Feature('nonfood_expenditures')` cannot assemble Togo with Uganda/Nigeria on a
  named MultiIndex, and the two precedents will need migrating to the long shape.
- `Togo/2018/_/food_expenditures.py` is dead by the same test as the nonfood
  script (not in `data_scheme.yml`; its Makefile rule cannot fire) and is the
  only live reference to `Togo_survey2018_fooditems_forEthan.dta`. Left alone —
  out of scope — but it should be retired next.

---
### Phase 3 — verification (fill at task end)
