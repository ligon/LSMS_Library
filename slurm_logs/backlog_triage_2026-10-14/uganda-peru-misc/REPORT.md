# Backlog triage `uganda-peru-misc` -- 13 issues, read-only (2026-10-14, development @ a7cf04f2d)

Records: `records.json` beside this file (one object per issue, the 09-24 schema). Nothing was committed, pushed or written outside `$STAGE`; no `gh` write was issued. One test file was run (`tests/test_gh772_uganda_earnings_shape.py`, 9 passed, no data). No `Country()` build was run; evidence is static plus the live warm cache's absence of a Uganda `earnings` parquet.

## 1. Table

| # | tier | type | tractability | dedup_status | prio | root_cluster | approach (one line) |
|---|---|---|---|---|---|---|---|
| 772 | B | bug | auto | resolved (a4bbcb08e + 296d79183, PR #807 v0.11.0) | 5 | pandas3-stack-semantics | close |
| 750 | D | infra | auto | resolved (PR #754 CI sweep; #767, #773; cf30c8aa3 retired the last 4 sidecars) | 5 | acquisition-path | close |
| 894 | C | framework | auto | partial (cd5172557 / PR #917: Design A chosen + test; Malawi/Nigeria still Design B) | 4 | panel-crosswalk-persistence | add `_/panel_ids.py` + committed JSON for Malawi and Nigeria, JSON never on a Make LHS, drop from `DESIGN_B_PENDING`, assert dict equality |
| 793 | C | design | human | partial (Uganda exemplar + two screens landed in v0.11.0; no loader, no vocabulary freeze) | 4 | sampling-design-record | human freezes the vocabulary; then `sampling.py` mirroring `population.py` |
| 942 | C | design | human | open (DESIGN.org complete, nothing in code) | 2 | cluster-identity-across-waves | answer DESIGN.org Q1-Q7; then per-boundary `panel:` block in `sampling.yml` + Country-level warning; file Uganda `v` rendering as its own Tier-B issue |
| 939 | C | design | auto | open; sequencing blocker cleared (PR #937 closed unmerged, PR #940 merged on the #935 crosswalk) | 4 | cfe-aggregation-evaluation | one-parameter `min_goods` sweep (1,3,5,9), ranking stability into `validation.org` |
| 880 | C | docs | auto | open (GhanaSPS CONTENTS.org untouched since 09-02) | 3 | contents-org-stale-vs-config | rewrite GhanaSPS CONTENTS.org §2 (:594-650) to the live NaN->0 decision; re-measure the wave-1 count |
| 849 | B | data-gap | diagnose-then-auto | partial (core screen landed; 2018-19 wired, slot 2 in PR #972; 2019-20 + four waves unwired by design) | 3 | uganda-wide-colmap-slot1-only | after #972/#967 merge: add `kg_factor` to CROP_COLMAPS for the five waves, measure per wave (a)-(e), wire |
| 700 | B | data-gap | auto | open | 3 | peru-unwired-waves | add `sample` from REG01 (v<-segmento, Rural<-a06), pad v to width 3 in sample AND cluster_features via a mapping.py function calling `format_id(value, zeropadding=3)`, omit weight with reason |
| 701 | C | data-gap | diagnose-then-auto | open | 4 | peru-unwired-waves | wire 1991 non-monetary tables from N00A/N01 with their own code lists; no expenditure until `_i/_d` is documented |
| 702 | D | data-gap | human | open | 4 | acquisition-path | human: bootstrap (A) or mark unstarted + delete the Panama-copied Makefile/module (B) |
| 791 | D | infra | blocked | open | 4 | acquisition-path | wait for the Benin grant; human browser download; then local-path ingestion + .rar/bare-.dta handling |
| 960 | C | docs | auto | partial (item 2 now true: poetry.lock tracked; items 1,3,4,5 stand) | 4 | venv-per-clone-state-in-shared-location | CLAUDE.md addenda 3+4 and savio_venv.md:92; the README is untracked (mirror owner), sucoder half elsewhere |

## 2. Reconcile

**Close now (verified by reading the diff):**

- **#772** -- `a4bbcb08e fix(#772)` rewrote `Uganda/_/earnings.py` to concat the wave `Earnings` series under `(t, i)`; `296d79183 verify(#772)` moved `tests/fixtures/uganda_baseline.json` from 129,160 to **22,447** rows (the dense-panel fabrication was the larger bug, 82.6% phantom rows). Both are ancestors of HEAD via the v0.11.0 release PR #807 (merged 2026-09-08). `tests/test_gh772_uganda_earnings_shape.py` passes here (9/9); coverage grades all eight cells `sane`; `Uganda/_/CONTENTS.org:1657` records it DONE.
- **#750** -- the thread's last open question ("retire the four Crop_encrypted sidecars?") was answered by `cf30c8aa3` (ancestor of HEAD; the directory is gone). PR #754 (merged 2026-09-03) runs `scripts/dvc_remote_reachability.py` weekly in `ci.yml:141-168`, and the in-flight CI slimming PR #973 keeps that job. Recoveries: PR #767 (KenyaLPS originals), PR #773 (Togo nonfood from EHCVM s09).

**Partial comment (what landed / what remains):**

- **#894** -- `cd5172557` (PR #917, 2026-09-14): Design A is the convention (CLAUDE.md, panel-ids skill, `tests/test_panel_ids_persistence.py` with `DESIGN_B_PENDING = {'Malawi','Nigeria'}`). Remaining: the two conversions (full Malawi + Nigeria re-warm, see §3).
- **#793** -- v0.11.0 (`8ec0bab4e`): `Uganda/_/sampling.yml` exemplar, `scripts/sampling_fingerprint.py`, `scripts/extract_sampling_evidence.py`, `.coder/sampling/fingerprint.csv`. Remaining: vocabulary freeze (human), loader, `Country.sampling`, per-country curation.
- **#849** -- core `harvest_kg` prefers a screened reported factor (per the thread); PR #972 (open) wires 2018-19 slot 2 and explicitly leaves 2019-20 as this issue's scope. Remaining: five waves + the per-wave measurement.
- **#939** -- the blocker in @ligon's comment (wait for the #937 crosswalk reconciliation) is cleared: #937 closed unmerged; PR #940 (`3c23299b5`) re-curated on the merged #935 crosswalk and carries the `min_goods` threading; `validation.org:68-70` says the sweep "is GH #939 and was not attempted".
- **#960** -- item 2 is resolved by the facts changing, not the doc: `poetry.lock` is tracked (`69eb3319e` per #959; last `fa9c8d605` 2026-10-14). Items 1, 3, 4, 5 stand.

## 3. Dispatchable now (tier A/B, auto, no decision needed)

None of the 13 is both Tier A/B and decision-free today. Two Tier-C items are nonetheless dispatchable because the human ruling is already recorded in-tree (#880: `SkunkWorks/derived_values.org:408-411` adjudicates CONTENTS.org as the stale side; #894: CLAUDE.md says Design A is the convention). Ordered by leverage, each naming its gate and cache cost (fingerprint rule: `Wave._input_hash` globs the wave dir and the country `_/` dir for `.py/.csv/.json/.txt/.tab/.tsv` + `*.org`; the country module and the wave `data_info.yml` are in every table of that country's hash; `CONTENTS.org` and `.md`/`.yml` siblings are excluded).

1. **#880** (docs, Tier C but zero-risk): rewrite GhanaSPS `CONTENTS.org` §2. Cache cost **zero**. Needs one cold GhanaSPS `household_roster` build in a private root to re-measure the wave-1 `household_characteristics` count (5,008 was measured under the fallback path; 5,007 expected).
2. **#700** (Tier B, one A/B decision with a recommendation -- pad `v` to width 3): new `sample` for Peru 1994 + a `mapping.py` function `format_id(value, zeropadding=3)` for `v` in both tables (`v` is a myvar, so `format_id` is not auto-applied). Cache cost: **full Peru re-warm** (wave `data_info.yml` edit), one wave, cheap. Note `_join_v_from_sample` merges on `['i','t']` (`country.py:2542`), so the framework join was never at risk; the hazard is the two served keys disagreeing.
3. **#849** (Tier B, diagnose-then-auto): gated on PRs **#972 and #967** merging, both of which edit `uganda.py`. Cache cost: **full Uganda re-warm** (`uganda.py` is in all 24-ish Uganda table hashes) -- the same re-warm each of those PRs forces, so ride immediately after them: one re-warm, not three.
4. **#894** (Tier C, auto): Malawi + Nigeria `_/panel_ids.py` + JSON. Cache cost: **full Malawi AND Nigeria re-warm** (new `.py` + `.json` in `_/` hit the `binp:` glob for every table; Nigeria has 22 script-path PP/PH tables). Schedule with another Malawi/Nigeria re-warm.
5. **#939** (Tier C, auto compute job, no library edit): `min_goods` sweep. Cache cost zero (warm GhanaLSS food reads).
6. **#960** (docs): CLAUDE.md addenda 3 and 4, `savio_venv.md:92`. Cache cost zero. The README half is untracked and the sucoder half is another repo.
7. **#701** Phase 1 (after #700): Peru 1991 non-monetary tables. Cache cost: Peru only.

## 4. Escalations (one line each)

- **#942**: approve DESIGN.org's design -- per wave boundary, two fields (`household_continuity`, `cluster_id_continuity`) in `sampling.yml`'s `panel:` block, warn at `Country`, no crosswalk -- yes/no? **And file Uganda's `v` rendering defect (272 of 322 pairs differ by one inserted digit; deterministic pair-table fix) as its own Tier-B issue** -- it is not among the 13 and is the only auto-fixable piece.
- **#793**: freeze the Uganda exemplar's vocabulary as-is (yes/no); land only the `panel:` block now for #942 (A) or wait for full curation (B)?
- **#700**: padding -- pad `v` to width 3 to match `i` (A, changes `cluster_features`' served `v`) or strip `i`'s padding (B, changes the household key)? Recommend A.
- **#701**: wire 1991's non-monetary tables while leaving every expenditure column unwired until `_i/_d` semantics are documented -- yes/no?
- **#702**: bootstrap Afghanistan (A) or mark deliberately unstarted and delete the Panama-copied Makefile and `from_dta` module (B)?
- **#791**: has the Benin EHCVM-V2.0 grant landed (yes/no)? Also: `.rar` handling needs `rarfile`/`unrar`, a dependency pin an auto agent may not touch.
- **#939**: is a sweep with the ranking written into `validation.org` sufficient to close (yes), or must the partition be re-selected if the ranking moves?
- **#960**: promote the squashfs mount to the documented default and demote adopt-or-build to a fallback in CLAUDE.md addendum 4 -- yes/no?

## 5. Contradictions with `CONTENTS.org` / `CLAUDE.md`

- **GhanaSPS `_/CONTENTS.org:594-598`** -- "Wave 1 has NO usable MonthsAway ... It is nonetheless *not wired*" and **:645** "It is moot while the column is undeclared" -- versus `2009-10/_/data_info.yml:142` `MonthsAway: s1d_27` with the NaN->0 fill and 15->12 clip in `2009-10/_/mapping.py`. The file CLAUDE.md addendum 5 tells every GhanaSPS agent to read first argues the opposite of what the country serves (#880).
- **GhanaLSS `_/CONTENTS.org:390`** -- "GLSS2 | 2xxx | 200 EAs, fresh draw" -- versus the same file's **:1109-1121**: "The documentation says GLSS1 used master-sample replicates 1 and 5 ... GLSS2 was *half* retained GLSS1 workloads plus *half* from replicate 6. 'Fresh draw' is therefore accurate for at most half of GLSS2." Recorded as unresolved in-file; #942's DESIGN.org Q4 asks whether the two-line correction rides with #942.
- **CLAUDE.md:70** -- "`assert 'worktrees' in lsms_library.__file__`" -- and **:92** "Follow the README's **adopt-or-build recipe** ... **Opt-in fast path**: `bin/savio_venv.sh`" -- versus #960's measured facts: `/local/jobNNN` is reaped per job (the adopt step probes emptied dirs), the working clone is now a `/local` clone whose `.pth` names the mirror (the assert cannot fire), and the README it points at is **not git-tracked** (lives only in the mirror checkout; `:18` and `:44` there still claim `/local` is persistent).
- **Afghanistan `_/Makefile`** -- its pattern rule is `%.parquet: %.py panama.py` and line 15 invokes `food_prices_quantities_and_expenditures.py`, a script CLAUDE.md records as removed; `afghanistan.py` imports `ligonlibrary.dataframes.from_dta`, a CLAUDE.md anti-pattern. The config is a Panama copy that never ran, which reframes #702's "abandoned mid-way".
- **Peru `_/CONTENTS.org:734-767`** does not contradict anything but is the authoritative statement for #700: `v` unpadded vs `i` padded, 19.9% of households non-joining -- "decide the padding once and apply it to both".
- **Uganda `_/CONTENTS.org:2098`** -- "TODO `KgFactor` is wired for 2018-19 only; every other wave ships one too" -- consistent with #849 and with PR #972's explicit decision to leave 2019-20 for this issue.

## Harness note

Read-only task: nothing to commit or push. `git status --short` is empty on the clone.
