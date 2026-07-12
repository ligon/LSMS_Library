# Prior-Art Ledger — GH #503 (Guyana household conflation)

**Search tier used:** ripgrep + git (floor), plus direct source interrogation via
`get_dataframe` (the only sanctioned reader — `STANDING.md §4`). gitnexus not used.

## §1 Task, restated

Guyana is a single-wave country (`1992/`). Its wave config
(`lsms_library/countries/Guyana/1992/_/data_info.yml`) declares the household
identifier as `i: [ED, HH]` for `sample`, `household_roster` and
`individual_education`, and `i: [ed_dvsn, smpl_hh]` for `housing`; the two
script-path tables (`1992/_/interview_date.py`, `_/assets.py`) rebuild the same
`ED-HH` key by hand.

That key is **not** the household. The source records households at
`(ED, SN, HH)` granularity — `COVERN.dta` has 1807 rows, 1807 unique
`(ED, SN, HH)` but only **1502** unique `(ED, HH)`. Distinct households
therefore collide on `i`, and `_normalize_dataframe_index`
(`country.py:4176`) collapses the collision with `groupby().first()`. The
consequence is not merely dropped rows: surviving households are **chimeras**
(members of two real households under one `i`), and `assets` **sums two
households' durables together**. Class 1 — silently wrong.

The fix is config-only, inside `countries/Guyana/`: make `SN` part of the
household identity, and make `v` the real geographic cluster `(ED, SN)`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `mapping.py:i()` | `countries/Guyana/1992/_/mapping.py:4` | hyphen-joins an **arbitrary-length** list of `idxvars` parts into one string id | via every Guyana build | **reuse as-is** — it already handles 3 parts; no signature change |
| `Wave.column_mapping` / `map_formatting_function` | `country.py:783-792` | binds a `mapping.py` function whose **name matches the idxvar** as the row-wise formatter when the YAML value is a *list* (falls back to `format_id` for a scalar) | yes | **reuse** — this is why `v: [ED, SN]` + `def v()` works with no library change |
| `df_data_grabber` "Trickier" form | `local_tools.py:1062-1068` | `df[cols].apply(f, axis=1)` for `{name: (listofcols, fn)}` | yes | reuse (the mechanism behind the above) |
| `dfs:` / `merge_on:` / `final_index:` / `drop:` | `country.py:986-1053` | multi-source sub-df merge, then `set_index(final_index)`, then `drop:` removes leftover helper columns | yes (Uganda, Malawi, EHCVM) | **reuse** — `drop:` is the sanctioned way to shed a merge-only helper column |
| `df_edit` hook (function named after the table) | `country.py:801` (`final_mapping['df_edit']`), applied `country.py:1054` | per-table post-merge hook, looked up in the wave/country `mapping.py` | yes (used by several countries) | **reuse** — used here to drop the `how='outer'` phantom rows |
| `Wave.cluster_features()` collapse | `country.py:1167-1189` | collapses an HH-grain `cluster_features` to `(t, v)` with `.first()` (`.mean()` for GPS) | yes | reuse (unchanged); Guyana's cure is a *correct* `v`, not a weaker collapse |
| `_join_v_from_sample` | `country.py:1633` | joins `v` from `sample()` at API time; **skips if `v` already present** | yes | reuse — so `v` must be removed from `household_roster` idxvars, not duplicated |
| `_normalize_dataframe_index` | `country.py:4176` | `groupby().first()` collapse + GH#323 RuntimeWarning | yes | **do not touch** — it is the *detector*, not the bug |
| `to_parquet` / `get_dataframe` | `local_tools.py:1570` / `805` | sanctioned writer/reader | yes | reuse in `assets.py` / `interview_date.py` |

**Not reinvented:** no new id-formatting helper, no new merge primitive, no new
collapse policy. Every mechanism above already existed and is used by other
countries; this task is a config change that *uses* them correctly.

## §3 Definitions & conventions in force

- **`i` is a single string index level**, however many source columns compose it
  — `format_id('-'.join(parts))`, `mapping.py:4`. Changing `[ED, HH]` →
  `[ED, SN, HH]` changes the *values* (`'1-1'` → `'1-37-1'`), **not the index
  shape**. This is why the v-join, kinship expansion, `_finalize_result` and
  `Feature()` assembly survive untouched.
- **`v` is the sampling cluster, owned by `sample`/`cluster_features` and joined
  at API time** — `CLAUDE.md` §"`sample()` and Cluster Identity"; `STANDING.md §3`.
  Corollary in force: *"Do NOT put `v` in feature `data_scheme.yml` indexes other
  than `cluster_features`."* Guyana's `household_roster` currently violates this
  (`v: ED` in its idxvars); this task removes it.
- **Canonical schema** = `lsms_library/data_info.yml` (per `STANDING.md §3`).
  Guyana's declared indexes (`countries/Guyana/_/data_scheme.yml`) are unchanged
  by this task: `sample (i, t)`, `household_roster (t, i, pid)`,
  `assets (t, i, j)`, `cluster_features (t, v)`, …
- **Two build paths** (`CLAUDE.md` §"Two Build Paths"): `interview_date` and
  `assets` are `materialize: make` script-path tables and write **hashless**
  L2-wave parquets, so they cannot self-invalidate → a manual
  `lsms-library cache clear --country Guyana` is mandatory after this change.

## §4 Invariants & assumptions

Repo-wide ones per `STANDING.md §4`. Task-specific:

- **The survey declares its own household key.** `COVERN.NEWID == ED*100000 +
  SN*100 + HH` holds for **1807/1807** rows. The source itself says the triple is
  the household. (Beware `int16` overflow when checking this — cast to `int64`.)
- **`ed_smpl` (HHCHAR) == `SN` (COVERN).** Verified: 1765/1819 HHCHAR triples are
  present in COVERN's `(ED, SN, HH)` set, and the *independent* corroboration —
  `HHCHAR.hhsize` vs the roster member count — matches **98.1%** on the triple
  vs only **81.1%** on the pair. The pair is not the household; the triple is.
- **`newid` in the second-questionnaire family (`HHCHAR`, `DRBLS`) is CORRUPT.**
  Both files carry 1616 unique `newid` for ~1818 rows; 240 HHCHAR rows violate
  the NEWID formula that COVERN satisfies exactly. **Therefore the existing
  `assets.py` join `DRBLS.newid ↔ COVERN.NEWID` is unsound** — 317 DRBLS rows
  carry an ambiguous (duplicated) `newid`, and that join recovers only 1529
  distinct households from 1730 rows. *This defeats the plan of record for
  `assets` (which assumed the newid join "correctly recovers the true
  household").*
- **HHCHAR is a positional crosswalk for DRBLS.** After
  `drop_duplicates(subset=(ed_dvsn, ed_smpl, smpl_hh), keep='first')` — HHCHAR
  contains exactly one exact duplicate record (row 1093/1094, ED 123 / SN 722 /
  HH 6) — HHCHAR has 1818 rows that are **row-for-row parallel to DRBLS**:
  `newid` matches 1818/1818 **and** `id_nmbr` matches 1818/1818. This yields
  1818 rows → **1818 distinct households** (perfectly 1:1), and it agrees with
  the newid join on **1415/1415 (100%)** of the rows where `newid` is
  unambiguous. That 100% agreement on the trustworthy subset is what validates
  the instrument (per the task's standard of evidence); the crosswalk is
  **guarded by asserts** so a source change crashes loudly instead of silently
  misattributing assets.
- **`id_nmbr` is NOT a household key** (only 160 distinct values across 1818
  rows). Do not be tempted.
- **Weight is a function of `ED` alone.** `WEIGHT.dta` is `(ED, WEIGHT)`, one
  weight per ED (max distinct weights per ED = 1), and joining it on `ED`
  reproduces `WEIGHTID.dta`'s authoritative per-household weight for
  **1795/1795** rows. So the weight merge **must stay keyed on plain `ED`** even
  after `v` becomes `[ED, SN]`. `WEIGHTID.dta` is *not* a usable substitute: only
  1742/1795 of its triples are in COVERN and it has a duplicate triple.
- **`ED` is NOT a cluster** (the issue's implicit assumption, and a second class-1
  defect it does not name). Within the 130 EDs, `RGN` varies in 22, `SECTOR` in
  10, `STNO` in 24. Under `v = (ED, SN)`: 168 clusters, `SECTOR` varies in
  **0/168**, `RGN` in **3/168**. `(ED, SN)` is the geographic cluster.
- **`country.py:1032` `how='outer'` is shared surface — do not touch.** It is why
  the sample merge injects phantom rows (`WEIGHT.dta` lists 616 EDs; only 130
  were enumerated → 488 phantom `i=NaN` rows). Handled *locally* with a Guyana
  `sample` df_edit hook, not by changing the shared merge.
- **`country.py:1189` `.first()` cluster collapse is shared surface — do not
  touch.** Its "invariant within a cluster" premise is false for Guyana only
  because Guyana's `v` was wrong.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| composite household id `i = ED-SN-HH` | **reuse** `mapping.py:i()` | already joins N parts; only the YAML list changes |
| composite cluster id `v = ED-SN` | **extend** — add `def v()` to the same `mapping.py`, delegating to `i()` | the framework binds a formatter *by idxvar name*; a scalar `v:` still falls back to `format_id`, so no other country is affected |
| dropping the merge-only `ED` helper column | **reuse** the YAML `drop:` key | sanctioned; already applied post-`set_index` in the `dfs:` branch |
| dropping the `how='outer'` phantom rows | **reuse** the `df_edit` hook | country-local; avoids touching shared `country.py:1032` |
| DRBLS → household identity | **new (but validated)**: positional HHCHAR crosswalk, assert-guarded | the existing `newid` join is *unsound* (see §4); no existing machinery does this, and the newid route cannot be repaired |
| collapsing duplicate `(t,i,j)` in assets | **delete** the `groupby().sum()` | with the correct identity, `(t,i,j)` is unique by construction (1818 rows → 1818 distinct households, DRBLS is WIDE = one row per household). The summation was 100% cross-household contamination; its docstring rationale ("several detail rows per household-item") was itself an artifact of the conflation |
| Region/Rural per household | **no change** | `cluster_features` stays `(t, v)` per `data_scheme.yml`; a correct `v` cuts wrong-Region households 287 → 13 |

## §6 Open questions for the human

- **Residual 13 households get a wrong `Region`.** `RGN` still varies within 3 of
  the 168 `(ED, SN)` clusters, so `cluster_features`' `.first()` collapse
  mis-assigns Region for 13 households. Options: accept (documented in
  `Guyana/_/CONTENTS.org`), or promote Region to a household-level column. Not
  fixed here — it needs a schema decision, and `cluster_features` is `(t, v)` by
  canonical declaration.
- **23 COVERN rows (2 EDs) have no weight** — those EDs are absent from
  `WEIGHT.dta`. Pre-existing, unchanged by this task.
- **43 DRBLS households are out-of-sample** (their triple is not in COVERN) and
  are dropped, as before.
- **Cross-country lead (not investigated here):** any country whose `i` is built
  from a *subset* of the source's own composite household id has this exact
  failure mode. Guyana was only caught because `COVERN` carries an explicit
  `NEWID`. The generic detector already exists and is *already firing*: the
  GH#323 RuntimeWarning from `_normalize_dataframe_index`. Nobody is reading it.
