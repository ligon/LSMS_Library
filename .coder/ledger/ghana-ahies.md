# Prior-Art Ledger — GhanaAHIES (new country: Ghana AHIES 2022-2024)

> Per-task ledger. Living, git-tracked snapshot of the machinery, definitions,
> and conventions that bear on THIS task. Inherits `STANDING.md`; cites
> `CLAUDE.md`, `lsms_library/data_info.yml` and the findings note
> `slurm_logs/ghana_ahies/README.org` rather than re-copying.

**Search tier used:** ripgrep + git floor. gitnexus MCP failed to connect this
session; no `.gitnexus/` index in the mirror. Line anchors as of `c39eba7f6`.

## §1 Task, restated
Add a new country `GhanaAHIES` (sibling of `GhanaLSS` / `GhanaSPS`) holding the
Ghana Statistical Service's Annual Household Income and Expenditure Survey,
2022-2024: a 10,800-household quarterly panel (600 EAs x 18) shipped by GSS as
three per-year **person x quarter** `.dta` files (Sections 1-4, 838 columns) and
three per-year **household x quarter** `SEC567_edt.dta` files (FIES, respondent
ids, housing). Wave label `t` is the quarter `YYYYQn` (12 values); wave
*folders* are calendar years (`2022/`, `2023/`, `2024/`) mapped through
`wave_folder_map`, so every table is **script-path** (`materialize: make`) and
each wave script emits all four `t` values from one file. Pilot on 2024 with
`sample`, `household_roster`, `individual_education`, `food_security`,
`housing`; then 2022/2023. `food_acquired` and every consumption feature are
**not** wired: Sections 8-13 are in no distributed file (findings note §"What
is NOT distributed"). Source blobs go to S3 through `push_to_cache_batch`;
provenance is `external` (GSS NADA; no WB id).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `Country.waves` + `wave_folder_map` | `country.py:2114-2132`, `:2383` | `{country}.py` `waves` list + `wave_folder_map` -> `Wave(year, wave_folder)`; `grab_data` filters the folder parquet to `t == self.year` (`:1727`) | Tanzania `2008-15`, Nigeria pp/ph | **reuse** — 12 quarters over 3 folders |
| Tanzania `2008-15/_/household_roster.py` | `countries/Tanzania/2008-15/_/household_roster.py:1-48` | one multi-round file with a `round` column -> `t` via a dict, `(t, i, pid)` parquet | GH #637 key review | **reuse as template** (AHIES ships `quarter` already spelled `YYYYQn`) |
| Nigeria `_/nigeria.py` `waves` / `wave_folder_map` | `countries/Nigeria/_/nigeria.py:15-35` | round labels `2018Q3` / `2019Q1` over year folders | yes | reuse the `YYYYQn` spelling |
| `get_dataframe(fn, convert_categoricals, encoding, categories_only)` | `local_tools.py:1183` | sanctioned reader; reads the 2.4 GB 2024 person file in 20.6 s / ~3 GB RAM with categoricals decoded (measured 2026-09-17) | yes | **reuse**; no `usecols`, so every table build re-reads the whole file — build serially (`LSMS_BUILD_WORKERS=1`) |
| `to_parquet` | `local_tools.py:1570` | sanctioned writer, redirects to `data_root()` | yes | reuse |
| `push_to_cache_batch(paths)` | `data_access.py:1368` | batched `dvc add` + `dvc push`, lock-retry; needs `_check_write_access` (`:1169`) | yes | **reuse** for the six blobs; writer creds resolved from `.dvc/s3_write_creds` (`_resolve_write_credentialpath`, `:1200`) |
| `add_wave` / `populate_and_push` | `data_access.py:1823`, `:1503` | WB-NADA download path; needs `_COUNTRY_CATALOG` spec to derive labels and `_find_stata_zip_url` (`api/resources`) | yes | **not usable**: GSS refuses `api/resources` even keyed, and files are already local. Scaffold dirs by hand, push with `push_to_cache_batch` |
| `provenance.WaveProvenance` / `render_source_org` / `write_provenance` | `provenance.py:128`, `:241`, `:309` | `SOURCE.org` record; `source='external'` writes `#+CATALOG_ID: none` | `tests/test_provenance.py` | **reuse** to write three `SOURCE.org` (as GLSS7's, `GhanaLSS/2016-17/Documentation/SOURCE.org`) |
| `_COUNTRY_CATALOG` | `data_access.py:191-297` | per-country WB discovery spec; `discoverable=False, reason=` for non-WB sources (EthiopiaRHS, KenyaLPS) | `tests/test_wave_provenance.py` | **extend**: add `GhanaAHIES` non-discoverable (GSS host); revisit if WB mirrors it |
| `capability.SeriesCapability` | `lsms_library/capability.py` | what a series measures, recorded at acquisition; `lacks` -> `absent_verdicts.csv` rows (`unsure` unless questionnaire-validated) | `tests/test_capability.py` | **extend**: AHIES record; the questionnaire in hand permits `questionnaire-validated` for Sections 8-13 = `asked-not-distributed`, NOT `not-asked` |
| `population.yml` + `scripts/promote_population_records.py` | `CLAUDE.md` §"Population Record" | generated from `slurm_logs/POPULATION_STATEMENTS_2026-07-21.org` | `tests/test_population.py` | **extend later**: universe statement is in the study doc (findings note); needs the evidence base edited, not the yml |
| `_normalise_sample_weights` | `country.py` (see `CLAUDE.md` §sample) | divides `weight` / `panel_weight` by within-`t` mean | yes | reuse; this is why `t` must be the quarter |
| `_expand_kinship` + `kinship.yml` | `country.py:3787`, `categorical_mapping/kinship.yml` | `Relationship` -> four columns | yes | reuse; AHIES `s1aq2` has 17 labels incl. `Househelp`, `Not member anymore` — add to `kinship.yml` if unmatched |
| `harmonize_education` / `canonical_education_labels.org` | `data_info.yml:422-` | ordinal education vocabulary | schema tests | reuse for `s2aq3`/`s2aq4` |
| Guinea-Bissau `_/data_scheme.yml` `food_security`, `housing` | `countries/Guinea-Bissau/_/data_scheme.yml` | FIES 8-item bool + `FIES_score`; `Roof`/`Floor` housing | GNB build | **reuse as schema template** (AHIES s5q1-8 is the same FAO instrument) |

## §3 Definitions & conventions in force
- Canonical schema: `lsms_library/data_info.yml` (`sample`, `household_roster`
  Sex/Age/Relationship required, `individual_education`), per STANDING.md §3.
- `sample` = `(i, t)` with `v`, `weight`, `panel_weight`, `strata`, `Rural`;
  raw weight wired, normalised at read (`.claude/skills/add-feature/sample/SKILL.md`).
- `v` owned by `sample`/`cluster_features`, joined at API time (STANDING.md §3).
- Provenance vocabulary `worldbank | external | unknown` (`provenance.py:42-78`);
  GSS-sourced waves are `external` (`GhanaLSS/2016-17/Documentation/SOURCE.org`).
- Absent-cell verdicts and C4: `CLAUDE.md` §"Adjudicating `absent` cells" —
  closing verdicts need evidence; only the questionnaire separates
  `not-asked` from `asked-not-distributed`.
- Country naming: series-suffixed for a shared ISO code (`GhanaLSS`, `GhanaSPS`;
  `data_access.py:210-212`).

## §4 Invariants & assumptions
- **`t` is the quarter, not the year** (EL, 2026-09-17). `hh_weight` differs by
  quarter; one `weight` per `(i, t)` in `sample`; a year-grained `t` would
  make `_normalize_dataframe_index` select one of four weights (GH #323).
- **`i = '{cluster}-{HholdID}'`**, stable across quarters and years (measured:
  9,637-10,042 of ~10,000 pairs present in all four quarters of each year);
  `hhid` is quarter-scoped and must NOT be `i`. `pid = new_pid`. Keys are
  unique and non-null on `(cluster, HholdID, quarter, new_pid)` in all three
  person files (findings note §"Survey design").
- One wave script serves four `t` values from one folder; use
  `self.wave_folder` for paths (`.claude/skills/multi-round-waves.md`).
- Each table build re-reads a 2.4-2.7 GB file (20 s, 3 GB): never build
  tables in parallel on the 4-core / 24 GB slice.
- `.dta` value labels are not UTF-8; `get_dataframe` default succeeded on
  2024 (pandas path), `pyreadstat` needs `encoding='latin1'`.
- 2023Q3 `hh_weight` is at half scale (mean 402 vs 720-854); record in
  `CONTENTS.org`, do not "fix" in config.
- 2022 `SEC567` carries `WTPP*`/`WTHH_*` columns; 2023's carries no weight;
  weights for `sample` come from the **person** file (`hh_weight`, constant
  within household-quarter), not `SEC567`.
- IO sanctioned-only; never the `dvc` CLI (STANDING.md §4). `push_to_cache_batch`
  for the six blobs.
- `countries_root()` is read-only at runtime; the raw files are placed in
  `Data/` only for the `dvc add` step (the acquisition exception, `CLAUDE.md`
  §"countries_root() is READ-ONLY").
- No `aggregation:` keys; no `v` in feature indexes; no derived tables in
  `data_scheme.yml` (STANDING.md §4).

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| wave/round handling | reuse `waves` + `wave_folder_map` | Nigeria/Tanzania pattern, tested |
| `sample` (v, weight, strata, Rural) | new wave script (per the skill) | `v=cluster`, `weight=panel_weight=hh_weight`, `strata` = region x urbrur (no explicit stratum var; the design stratifies EAs by region and locality), `Rural` from `urbrur` |
| `household_roster` | new wave script; kinship via `_expand_kinship` | `s1aq1`, `s1aq4y` (+`s1aq4m` months for <1), `s1aq2` labels -> `kinship.yml` |
| `individual_education` | new wave script + `harmonize_education` table | `s2aq3`/`s2aq4` |
| `food_security` | new wave script, GNB schema | `s5q1-s5q8` from `SEC567` |
| `housing` | new wave script, GNB/Malawi categorical schema | `s7*` from `SEC567` |
| `household_characteristics` | reuse `_ROSTER_DERIVED` | auto |
| `food_acquired` & consumption | **not wired**; `asked-not-distributed` verdict | Sections 8-13 undistributed |
| blob publication | reuse `push_to_cache_batch` | sanctioned writer |
| provenance | reuse `write_provenance(source='external')` | GLSS6/7 precedent |

## §6 Open questions for the human
- Sections 8-13 from GSS (blocks every consumption feature).
- WB catalog mirror check (needs the WB key restored in `config.yml`).
- `population.yml`: add AHIES to `POPULATION_STATEMENTS` evidence base and
  re-promote, or leave `unrecorded` for the pilot?

---
### Phase 3 — verification (fill at task end)
- (pending)
