# Prior-Art Ledger — GH #783, stranded food vocabularies -> `harmonize_food`

**Search tier used:** ripgrep + `git grep` floor. **GitNexus MCP was
unreachable** (`CONNECT_TIMEOUT`), so the substitutes sanctioned in `AGENTS.md`
above the `<!-- gitnexus:start -->` marker were used throughout: `git grep` over
call sites for the reader census, `git diff --stat` + the covering tests for
blast radius, and the `gh` CLI for the issue thread. Declaring the substitution,
per the rule.

## §1 Task, restated

Six countries — GhanaLSS, GhanaSPS, Cambodia, Serbia, Guatemala, Panama — curate
a country-level food-item vocabulary in `countries/{C}/_/food_items.org`.
`Country.categorical_mapping` (`country.py:1781`) reads only the global
`lsms_library/categorical_mapping/*.org` and `countries/{C}/_/categorical_mapping.org`;
it **never opens `food_items.org` under any table name**. `Country._relabel_j`
(`country.py:2569`) — the `labels=` resolver — looks up a table named
`food_items` or `harmonize_food` in that property, so every one of those six
countries raised `LabelUnavailableError` for every `labels=` value. Move each
vocabulary into the country's `categorical_mapping.org` as `#+name:
harmonize_food`, repoint every reader, retire `food_items.org`, and change
nothing about the data those countries return.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country.categorical_mapping` | `country.py:1781` | merges global + country `categorical_mapping.org`; the *only* file the API reads food labels from | `tests/test_categorical_merge.py` | **reuse unchanged** |
| `Wave.categorical_mapping` | `country.py:1006` | `dict(country)` then `.update(wave)` — whole-table wave override | `tests/test_categorical_merge.py` | **reuse unchanged** (§6 decision) |
| `Country._relabel_j` | `country.py:2569` | the `labels=` resolver; `'<X> Label'` then bare `'<X>'` | `tests/test_label_selection.py`, `tests/test_food_labels.py` | **reuse unchanged** — the fix is config, not code |
| `tanzania.harmonized_food_labels` | `Tanzania/_/tanzania.py:369` | reads `../../_/categorical_mapping.org`, `name='harmonize_food'` | via build | **PRECEDENT** — Unit #0, completed |
| `ethiopia.harmonized_food_labels` | `Ethiopia/_/ethiopia.py:51` | same, with a legacy bare-pipe fallback | via build | **PRECEDENT** — Unit #0, completed |
| `local_tools.df_from_orgfile` | `local_tools.py:1621` | `name=None` means "first table in the file"; raises `KeyError` for a named table that is absent (GH #461) | yes | **reuse**; every repointed reader now passes `name='harmonize_food'` |
| `local_tools.all_dfs_from_orgfile` | `local_tools.py:2904` | returns `{name: df}` for **named** tables only | yes | why an unnamed table is unreachable |
| `ghanalss.harmonized_food_labels{,2}` | `GhanaLSS/_/ghanalss.py:148,157` | `pd.read_csv(delimiter='|')` over the `.org` | no | **DELETE** — zero callers (§4) |
| `ghanasps.harmonized_food_labels{,2}` | `GhanaSPS/_/ghanasps.py:12,21` | identical dead pair | no | **DELETE** — zero callers |
| `GhanaLSS/_/nutrition.py:135` | `_load_food_codes` | the single live reader of GhanaLSS `food_items.org`; needs `FCT Code` | `tests/test_ghanalss_nutrition.py` | **repoint** |
| `guatemala.harmonized_food_labels` | `Guatemala/_/guatemala.py:178` | reads `../../_/food_items.**csv**` — a file that exists nowhere in the tree | no | **out of scope** (issue body scopes it out); left alone |
| `Panama/_/nutrition.py:22` | raw `pd.read_csv(sep='|')` over the `.org` | unreachable script (§4) | no | **repoint**, verified pair-identical |

## §3 Definitions & conventions in force

- Three-level merge `global -> country -> wave`: `CLAUDE.md` → "Automatic
  categorical mappings"; code at `country.py:1781` and `country.py:1006`.
- `_ADDITIVE_CATEGORICAL_TABLES` row-unions instead of replacing, and applies
  **only** to the global→country merge — per `country.py` / `_merge_categorical_tables`.
- `labels=` contract, incl. `LabelUnavailableError` vs plain `KeyError`:
  `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md`.
- Designing an `Aggregate Label` requires the CFE β-spread test
  (`max(β) − min(β) < 1.348 × σ_min`): parent
  `.claude/skills/add-feature/food-acquired/SKILL.md`.
- "Unit #0" migration definition and per-country sub-case list:
  `slurm_logs/2026-06-13_wb_incidence_map/PARITY_LOOP_DESIGN.md`.
- Cache-hash inputs incl. `_/*.org`: `CLAUDE.md` → "Automatic content-hash
  staleness"; code at `country.py:2927-2934` (`corg:` parts).

## §4 Invariants & assumptions

- **`labels='Preferred'` short-circuits before table resolution**
  (`country.py:2578`: `if labels in (None, 'Preferred'): return df`). It
  therefore does **not** demonstrate the fix — it never raised in the first
  place. Any before/after demo must use a value that forces resolution.
- **A country-level `harmonize_food` cannot auto-rewrite `j`.**
  `_apply_categorical_mappings` matches a table whose *name* equals an
  index/column name (`j` ≠ `harmonize_food`), and the other route is
  `harmonize_{method_name}` (`country.py:2755`) = `harmonize_food_acquired`.
  Documented already at `Tanzania/_/categorical_mapping.org:512-521`. **This is
  what makes the move data-invariant.**
- `df_from_orgfile(name=None)` = "first table in the file". Safe in a
  single-table `food_items.org`; **wrong** in a multi-table
  `categorical_mapping.org`. Every repointed reader passes `name=` explicitly.
- The dead `harmonized_food_labels` pairs: `git grep harmonized_food_labels`
  returns only their `def` lines in both Ghana countries — zero callers.
- `Panama/_/nutrition.py` is unreachable for reasons predating this task:
  `nutrition` absent from `data_scheme.yml`; the Makefile's `var` target is
  `food_acquired.parquet` only; it reads `../var/food_quantities.parquet`,
  which the Makefile's own comment forbids materializing; and `eep153_tools` is
  not an installed dependency (measured: `ModuleNotFoundError`).
- A `%.parquet: %.py food_items.org …` Makefile prerequisite makes deletion
  fatal for **every** table of that country, not just the food ones. Present in
  GhanaLSS, Guatemala, Panama; absent in GhanaSPS, Cambodia, Serbia.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the merge semantics | **reuse, unchanged** | already `dict.update`, already what @ligon specified |
| `_relabel_j` | **reuse, unchanged** | the defect is config placement, not resolver code |
| table name | **reuse `harmonize_food`** | the name `_relabel_j` looks for; matches Tanzania/Ethiopia |
| the table content | **move byte-for-byte** | invariance is the hard constraint; asserted line-by-line |
| `Aggregate Label` | **new — REFUSED** | out of scope; needs the β-spread test (§3). Sizing recorded in §6 |
| the dead label helpers | **delete, not repoint** | zero callers, and they would raise if called |

## §6 Open questions / decisions on the record

- **Whole-table vs row-union for the country→wave override — decided: keep
  whole-table replacement.** Only GhanaLSS has wave-level `harmonize_food` (7
  waves; Tanzania and Ethiopia have none, so precedent is silent). No wave
  consumer needs `FCT Code`, and `_relabel_j` reads the *Country* property, so
  the two consumers are already separated by the level they ask at. No code
  change; recorded in `GhanaLSS/_/categorical_mapping.org`.
- **GhanaLSS `Aggregate Label`, deferred with sizing.** The 7 wave tables carry
  one; a union is *not* mechanical — 14 Preferred Labels take two different
  Aggregate values across waves, 9 of the country table's 195 distinct Preferred
  Labels appear in no wave table, and one wave value is mojibake (`Cooked Rice
  and Stew Ê Ê`). This is the remaining blocker for #770's item-axis price
  ladder on GhanaLSS.

---
### Phase 3 — verification

- `GhanaLSS/_/categorical_mapping.org#harmonize_food` — **OK (anchored on §5)**:
  207 table lines asserted byte-identical to the retired `food_label`.
- `GhanaSPS`, `Cambodia`, `Serbia`, `Guatemala`, `Panama` `#harmonize_food` —
  **OK (§5)**: 100 / 66 / 102 / 100 / 83 lines, each asserted byte-identical.
- `GhanaLSS/_/nutrition.py::_load_food_codes` — **OK (§2)**: its
  `waves = [c for c in lab.columns if c not in ('Preferred Label', 'FCT Code')]`
  is preserved exactly because no column was added or renamed.
- `Panama/_/nutrition.py` label read — **OK (§4)**: repointed parse measured to
  yield the same 79 `(Preferred Label, FCT ID)` pairs as the raw
  `pd.read_csv(sep='|')` it replaces.
- deleted `harmonized_food_labels{,2}` × 2 — **OK (§4)**: zero callers.
- `Cambodia/_/CONTENTS.org` "No `categorical_mapping.org`" and
  `Serbia/_/CONTENTS.org` "No `categorical_mapping.org`" — **CONTRADICTION
  found in the docs, not introduced by this change**: both files exist and both
  claims were already false. Corrected in place, quoted rather than deleted.

### Phase 3 addendum — a defect SURFACED (not introduced) by switching `labels=` on

`tests/test_ghanalss_nutrition.py::test_food_labels_carry_fct_codes` pinned the
old file location and had to move with it; two new pins were added to
`tests/test_food_labels.py` (the table is reachable with all its columns; the
standalone file stays retired).

**`_relabel_j` renames to the EMPTY STRING for a blank crosswalk cell.**
`all_dfs_from_orgfile` builds cells with `s.strip()` and nulls nothing, so a
blank cell arrives as `''`, survives `_relabel_j`'s `.dropna()`
(`country.py:2634`), and every item whose variant is blank collapses into a
single `j=''`. Measured on GhanaSPS `labels='FCT Label'`: 94 distinct `j` → 20,
of which one is `''`, because 81 of 98 rows have a blank `FCT Label`. Confirmed
by `j_head[0] == ''` on GhanaSPS `FCT Label`, GhanaLSS `2016-17` and GhanaLSS
`FCT Code`.

Corpus-wide exposure, config-only sweep — **17 columns in 4 countries**:

| country | columns with blank cells |
|---|---|
| GhanaLSS | all 7 per-round columns (37–147 of 205) + `FCT Code` (24) |
| GhanaSPS | `2009-10` 13, `2013-14` 5, `2017-18` 7, `Food Codes` 3, `FCT Label` 81 (of 98) |
| Mali | `2014-15`, `2018-19`, `2021-22` — 2 each of 327 |
| Panama | `2003` — 1 of 81 |

**Pre-existing, and provably so**: Mali already resolves `labels=` on
`development` and carries the same blank cells, so
`Country('Mali').food_acquired(labels='2014-15')` reproduces it without any of
this PR's changes. GH #783 does not touch `_relabel_j` or
`all_dfs_from_orgfile`; it makes four more countries able to *reach* the code
path that has this bug. Numeric columns are immune — `to_numeric=True` turns
their blanks into NaN, which `.dropna()` then removes correctly (Guatemala
`FCT code`: 5 NaN, no `''`).

This is the exact complement of #787 finding 2 ("unmatched `j` passes through
unrenamed, silently"): unmatched passes through, blank-matched collapses. Both
are silent. Reported to #787; **deliberately not fixed here** — a fix changes
returned data for Mali, and for Nigeria/Malawi/Uganda/Ethiopia/Tanzania if
their tables ever gain a blank, which is squarely against this PR's invariance
constraint.

**Therefore the headline `labels=` demonstration uses fully-populated columns**
(GhanaSPS `2017-18` 94→94, Cambodia `Code`, Serbia `proizvod`, Guatemala
`2000`, Panama `2008`), and the sparse-column results are reported as evidence
of the defect rather than as successes.
