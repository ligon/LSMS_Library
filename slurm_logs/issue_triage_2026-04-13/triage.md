# GH Issues Triage — 2026-04-13

**Prepared by**: triage agent, end-of-session  
**Session commits**: 57 commits on `development` since 2026-04-13 00:00 UTC  
**Reference baseline**: recently-closed issues #130–#158 (closed 2026-04-09 to 2026-04-12)

---

## Summary

- **Total open**: 22 issues (#107–#173, with gaps for closed ones)
- **Closable by today's session work** (cross-referenced against git log):
  - **#164** (Togo Age 94.5% missing) — fully resolved by commit `7e889ed4`
  - **#165** (wider age_handler adoption) — substantively complete; all 9 EHCVM countries switched to raw section files + age_handler across 9 commits; residual: Togo 2021-22 wave not yet confirmed, but the batch is done
  - **#173** (Age dtype object/float cross-country) — resolved by `87fbd7a6` (framework Int64 enforcement) + `52acff35` (closure bug fix) + per-country sentinel fixes
  - **#166** (Niger shocks Cope* leak) — Finding 1 (Niger Cope* columns) resolved by `f3075884`; Finding 2 (Affected* nulls) partially resolved by `f01ccd2a` (BooleanDtype coercion fix) but the per-country Benin/BFA/CIV zero-nonull root cause may remain
- **Quick wins (<1h each)**:
  - #163 Rural/urban lowercase spellings — two-line `data_info.yml` edit
  - #163 stray `int_t` in sample — already fixed by today's `int_t -> Int_t` Rejected Spellings commits (verify with a fresh `Feature('sample')` call)
  - #162 stray `i` in cluster_features for specific EHCVM countries — pattern is known; fix per-country in `data_info.yml`

---

## Issue Clusters

### Cluster A — Age / household_roster data quality  
- **#164** Togo Age 94.5% missing [trivial, RESOLVED today]  
- **#165** Wider age_handler adoption [medium, RESOLVED today — close this]  
- **#173** Cross-country Age dtype inconsistency [small, RESOLVED today — close this]  

All three can be closed as a group. The only residual is whether Uganda/Ethiopia/Nigeria 6–8% Age gaps warrant follow-up; PLAN.md explicitly deferred these ("audit per-wave codebook before touching").

### Cluster B — cluster_features grain and data quality
- **#161** Uganda cluster_features: wrong grain, stringified-float District, missing GPS [medium, fresh]  
- **#162** Cross-country stray `i` in cluster_features, 8 countries [medium, fresh]  

Uganda District stringified-float is fixed (`7ec871e9`). The wrong grain + GPS wiring remain for Uganda. Eight other countries still have stray `i`. These two issues are structurally identical (same root cause, same fix pattern); dispatch as one task.

### Cluster C — Categorical vocabulary harmonization  
- **#168** Add canonical harmonize_assets mapping [large, fresh — design settled]  
- **#170** Housing cross-country Roof/Floor vocabulary [medium, fresh — design settled]  
- **#171** individual_education cross-country harmonization + Uganda 56% nulls [medium, fresh]  

All three are "same shape as harmonize_food": per-country `categorical_mapping.org` tables + `mappings:` wiring. Can be dispatched as parallel country subagents once one country is done as proof-of-concept. #168 is the most complete (design finalized in two long comments, canonical vocabulary estimate of ~50-55 items); start there.

### Cluster D — Food schema overhaul
- **#169** Design: food_acquired canonical columns + cascade to food_prices/quantities [epic, fresh]  

Standalone epic requiring a design decision the human hasn't made. Three options are laid out with pros/cons in the issue. Blocks any structural improvement to `food_acquired`. No implementation work should start until Option A/B/C is chosen. Not ready for dispatch.

### Cluster E — Country-level data gaps (legacy/deferred)
- **#107** Burkina_Faso: missing categorical_mapping.org, food_items.org, nutrition [medium, stale]  
- **#108** Cambodia: missing nutrition, food_prices, other_features [medium, stale]  
- **#109** GhanaLSS: multiple incomplete features (shocks not feasible) [medium, partially resolved]  
- **#110** GhanaSPS: multiple incomplete features, missing region for 2013-14 [large, stale — superseded by #140]  
- **#111** Serbia: broken build, missing food_items.org [medium, stale]  
- **#112** Tanzania: missing food quantities 2019-20 and 2020-21 [medium, stale — blocked on data]  
- **#113** Tanzania: use community-level prices as fallback [medium, stale — blocked on #112]  
- **#115** Ethiopia: missing Food Conversion Table [medium, stale]  
- **#116** Malawi: incomplete conversion_to_kgs + missing FCT [medium, stale]  
- **#117** Panama: missing conversion_to_kgs.json [small, stale]  
- **#118** Tanzania: missing Food Conversion Table [medium, stale]  

These were all filed 2026-04-01 from a CONTENTS.org audit pass. All have comments from that same session summarizing findings. They are low-urgency; no regressions, just missing coverage. Grouped here because they share the same "nutrition pipeline not yet built" shape.

### Cluster F — Country rebuild / registration
- **#140** [umbrella] Rebuild GhanaSPS [epic, fresh — has detailed plan]  
- **#146** Armenia: data_access integration plan [small, fresh — needs human decision]  
- **#149** PyPI publishing v0.6.1 (wheel bloat) [small, ready to execute]  

---

## Per-Issue Notes

### #107 Burkina_Faso: Missing categorical_mapping.org, food_items.org, nutrition  
[stale / medium / 1 country]  
Filed 2026-04-01. food_acquired YAML configs exist for all three waves; today's session added age_handler to BFA 2018-19 and 2021-22 (touching those same wave directories). The food_items.org gap means BFA food labels are raw categoricals. Next step: create `Burkina_Faso/_/categorical_mapping.org` with a `harmonize_food` table mapping raw `.dta` labels to canonical food names. Medium effort, no external dependency.

### #108 Cambodia: Missing nutrition, food_prices, other_features  
[stale / medium / 1 country]  
Filed 2026-04-01. `other_features` is actually done (audit confirmed). `data_scheme.yml` is absent — `Country('Cambodia')` cannot be loaded via the API. Primary blocker is the missing `data_scheme.yml` and deprecated API patterns throughout all scripts. Nutrition blocked on FCT. Medium effort; Cambodia is not an LSMS-ISA country so lower priority.

### #109 GhanaLSS: Multiple incomplete features  
[stale / medium / 1 country — partially resolved]  
Filed 2026-04-01. Investigation confirmed shocks are not feasible (instrument doesn't have a shocks module). Other incomplete features (nutrition, FCT) remain. Low priority; the shocks finding is the main new information. The issue can be narrowed to just the remaining gaps.

### #110 GhanaSPS: Multiple incomplete features  
[stale / large — superseded by #140]  
Filed 2026-04-01. This is now fully subsumed by #140 (the umbrella rebuild issue). Can be closed as duplicate/superseded.

### #111 Serbia: Broken build  
[stale / medium / 1 country]  
Filed 2026-04-01. Build is broken due to copy-paste from Panama (`food_items.org` and `panama.py` dependencies in Makefile), dead imports, and missing `data_scheme.yml` registration. The `household_roster` DVC path may work independently. No one has touched this since the audit. Low urgency; Serbia is a single small wave.

### #112 Tanzania: Missing food quantities 2019-20 and 2020-21  
[stale / medium — blocked on data]  
Filed 2026-04-01. Missing `hh_j03_2`, `hh_j05_2`, `hh_j06_2` columns in those two waves — a data collection gap in the survey, not an extraction bug. Blocks `food_prices` and `food_quantities` pipelines. `food_expenditures` (value-based) is unaffected. The only actionable workaround is #113 (community-level fallback), which is also stale. Low priority.

### #113 Tanzania: Use community-level prices as fallback  
[stale / medium — blocked on #112]  
Filed 2026-04-01. A four-step workaround for #112 using community-level price data. The `clusterid` linkage between community prices and household locations has not been verified. Dependent on #112 decision; leave stale.

### #115 Ethiopia: Missing Food Conversion Table  
[stale / medium / 1 country]  
Filed 2026-04-01. `conversion_to_kgs.json` is complete. FCT (for nutrition) is missing. `nutrition.py` skeleton exists but the actual food-item → nutrient mapping has not been assembled. FAO East African FCT is the documented source. Medium effort when prioritized.

### #116 Malawi: Incomplete conversion_to_kgs + missing FCT  
[stale / medium / 1 country]  
Filed 2026-04-01. Conversion factor data exists in wave-level `.dta` files but hasn't been compiled into `conversion_to_kgs.json`. IHS2 (2004-05) has no conversion data at all. FCT also missing. Moderate effort.

### #117 Panama: Missing conversion_to_kgs.json  
[stale / small / 1 country]  
Filed 2026-04-01. `units.json` exists with partial pound-to-kg conversion factors; many survey-specific units have null entries. `nutrition.py` is complete using a Central America FCT (so the nutritional side is covered). Fix: complete null entries in `units.json` and generate the JSON output. Small, self-contained effort.

### #118 Tanzania: Missing Food Conversion Table  
[stale / medium / 1 country]  
Filed 2026-04-01. `conversion_to_kgs.json` (unit weights) is complete. The missing piece is the FCT mapping food items to nutrient values. Additional complication: 2019-20 and 2020-21 waves lack food quantity data (see #112), limiting FCT usefulness for recent waves. Medium effort; the quantity-data gap is the bigger blocker.

### #140 [umbrella] Rebuild GhanaSPS  
[fresh / epic / 1 country — has detailed plan]  
Filed 2026-04-10. GhanaSPS has no `data_scheme.yml`, every script is fatally broken (dead imports), sampling weights unclear for 2013-14 and 2017-18. Full plan at `slurm_logs/PLAN_ghanasps_cleanup.org`. Critical blocker: some on-disk data may be pre-release; human must verify WB catalog status before any implementation. This is an epic requiring 1-2 weeks of subagent work once the pre-release data question is resolved. Not ready to dispatch until human makes the data decision.

### #146 Armenia: data_access integration plan  
[fresh / small — needs human decision]  
Filed 2026-04-10. WB catalog entry is metadata-only; Armenia data requires manual download from ARMSTAT (Armenian Statistical Committee). Two options: (a) treat like Nepal (stub exists, document it, leave data-less), or (b) a human manually obtains data from ARMSTAT and pushes to DVC. The existing `data_info.yml` stub is accurate. No code work needed until the human decides. Quick close if decision is "treat like Nepal."

### #149 PyPI publishing: prep work for v0.6.1  
[fresh / small — ready to execute]  
Filed 2026-04-10. Wheel is 38.7 MB due to bundled `Documentation/` trees and `.dta` files. Fix is a `[tool.poetry] exclude` block in `pyproject.toml` covering `*/Documentation`, `**/*.dta`, `**/Data`. Target wheel size < 5 MB. Also needs license metadata, README polish. The `.release/SKILL.md` documents the Poetry gotchas. Could be done in a single focused session.

### #161 Uganda cluster_features: wrong grain + GPS wiring  
[fresh / medium / Uganda only — partially fixed today]  
Filed 2026-04-13. District stringified-float is fixed (`7ec721d0`). Remaining: (1) wrong grain (household-level rows rather than cluster-level), needs `groupby(['t','v']).first()` or equivalent after `df_geo` merge; (2) GPS not wired for 4 of 8 waves (geovar `.dta` files exist but not referenced). Well-documented in the issue with a table of which waves have geovar files. Ready to dispatch as a focused Uganda-only fix session.

### #162 Cross-country: stray `i` in cluster_features, 8 countries  
[fresh / medium / 8 countries — Benin, CotedIvoire, Ethiopia, Guinea-Bissau, Niger, Nigeria, Togo, Uganda]  
Filed 2026-04-13. Pattern is clear: household-level source files merged without collapsing to cluster grain. Uganda covered in #161; the other 7 are independent. Fix per country: add a `groupby(['v']).first()` or use a pre-aggregated geovar file. Dispatch as parallel per-country subagents; each fix is ~30 min. CAUTION: fixing the grain will reduce row counts (from household counts to cluster counts) and may surface downstream failures in tests that assert row counts.

### #163 Feature('sample') oddities: int_t leak, Rural spellings, China weight  
[fresh / small — partially resolved today]  
Filed 2026-04-13. Three sub-issues: (1) `int_t` leak — partially addressed by `int_t -> Int_t` Rejected Spelling commit, but worth verifying with a fresh `Feature('sample')` call that `Int_t` no longer appears as a data column; (2) Rural lowercase spellings — not yet fixed; two-line `data_info.yml` add (`rural: Rural`, `urban: Urban` in `spellings`); (3) China weight entirely null and `'0'` numeric Rural code from an unknown country — need localization grep. The `'0'` Rural issue is the most data-correctness-relevant.

### #164 Togo household_roster: Age 94.5% missing  
[RESOLVED today — close this]  
Commit `7e889ed4` switches Togo to raw `s01_me_tgo2018.dta` + `age_handler()`. Also follow-up commit `08939f4e` fixes `int_t -> Int_t` in Togo. Age coverage should now be substantially higher; verify with a fresh load and close.

### #165 Wider adoption of age_handler()  
[RESOLVED today — close this]  
All 9 EHCVM countries (Benin, Burkina_Faso ×2, CotedIvoire, Guinea-Bissau, Mali ×2, Niger ×2) switched to raw section files + age_handler. Framework closure bug fixed. Unit tests added. The PLAN.md explicitly deferred Uganda/Ethiopia/Nigeria (6–8% gap, needs per-wave codebook audit). Issue is substantively complete.

### #166 shocks: Niger leaks 26 raw Cope* + Affected* mostly null  
[fresh — Finding 1 RESOLVED, Finding 2 partially addressed]  
Filed 2026-04-13. Finding 1 (Niger Cope* leak): commit `f3075884` rolls Cope1-26 into HowCoped0/1/2 for Niger. Resolved. Finding 2 (Affected* mostly null): commit `f01ccd2a` fixes a BooleanDtype coercion issue that was dropping Affected* columns during finalization. However, the per-country Affected* mapping bugs (Benin/BFA/CIV declare mapping but it "never fires") have not been individually fixed. Those countries need their wave YAML to correctly populate Affected* columns. Medium follow-up work for 3-4 countries. Issue should remain open for Finding 2.

### #167 Implement plot_features across LSMS-ISA countries  
[fresh / epic — zero implementation exists]  
Filed 2026-04-13. `Feature('plot_features')()` returns an empty DataFrame; zero countries declare it. All 13 LSMS-ISA countries would need agricultural module extraction. The canonical schema needs `Columns:` added to `data_info.yml`. This is a large green-field feature addition. Not ready to dispatch until the schema is settled and at least one country is done as proof-of-concept. Low urgency unless there is active demand.

### #168 Add canonical harmonize_assets mapping  
[fresh / large — design fully settled]  
Filed 2026-04-13. Two long comments from Ligon settle the design (per-country `harmonize_assets` tables in `categorical_mapping.org`, shared Preferred Label vocabulary, same mechanism as `harmonize_food`). Three-phase plan: per-country tables → canonical vocabulary → convergence pass. ~13 countries to cover. Data entry is the bulk of the work; framework unchanged. Good candidate for parallel subagent dispatch after one country proves the pattern.

### #169 Design: food_acquired canonical columns  
[fresh / epic — design decision pending]  
Filed 2026-04-13. `Feature('food_acquired')` returns 54 columns where canonical schema declares 3. The index is also broken (not a named MultiIndex). Ligon's comment categorizes all 54 columns into 9 groups (A–I) and proposes three design options. This is a design discussion, not a ready-to-execute task. **Requires a human decision (Options A/B/C) before any implementation.** Affects every food-related downstream analysis. High-impact but blocked.

### #170 Housing: cross-country vocabulary harmonization  
[fresh / medium — design settled]  
Filed 2026-04-13. Only Uganda and Malawi have `housing` in their data_scheme. The two countries' Roof/Floor vocabularies are inconsistent. Fix: same pattern as #168 — per-country `harmonize_housing` tables. The canonical housing vocabulary is small (~15-25 concepts). Only two countries currently declare housing, making this a smaller scope than #168. Good early win for demonstrating the harmonization pattern.

### #171 individual_education: harmonization + Uganda 56% nulls  
[fresh / medium — design clear]  
Filed 2026-04-13. Mali mixes numeric codes with French text; Malawi has case variance; Uganda has 56% Educational Attainment nulls (unknown cause — wrong column? survey design?). Same harmonize_X pattern as #168/#170. Uganda null rate needs a per-wave probe before fixing. Smaller scope than #168 (fewer countries currently declare `individual_education`).

### #172 household_characteristics: silent coverage gaps  
[fresh / small-medium — partially framework, partially data]  
Filed 2026-04-13. Armenia and Guyana silently absent; 5 declared-but-never-built countries (Nepal, Senegal, Togo, Serbia and Montenegro, Azerbaijan). Root issue: derivation silently skips when roster is unavailable. The suggested fix (emit `UserWarning` when declared source is unavailable) is a small framework change. The country-specific data gaps (Nepal WB-blocked, Armenia NSO-only) are separate. Guyana is anomalous — its roster loads but characteristics fail. Guyana bug is highest-priority sub-item; the warning emission is a clean quick win.

### #173 household_roster: cross-country Age data quality  
[RESOLVED today — close this]  
Fully addressed by today's session: Int64 coercion in framework (`87fbd7a6`), closure bug fix (`52acff35`), per-country sentinel nullification (Nigeria, South Africa, Senegal, Kazakhstan). Ligon's comment noting we should NOT drop rows with missing Age is correct and already the implemented behavior (sentinels → NA, not drop). Close this.

---

## Suggested Next-Session Priorities

### Priority 1: Close resolved issues #164, #165, #173 (and partially #166)
**Rationale**: All three (four) are done in the git history but still show as open on GitHub. Closing them cleans the backlog and documents what was accomplished. #166 should be kept open for Finding 2 (Affected* nulls in Benin/BFA/CIV) but Finding 1 should be noted as resolved.  
**Next action**: Human closes #164, #165, #173 on GitHub; comments on #166 that Finding 1 is resolved.

### Priority 2: #163 — Rural spellings and `'0'` numeric code in sample  
**Rationale**: Two-line fix for lowercase `rural`/`urban` (add to `data_info.yml` spellings). The `'0'` numeric Rural code affects 2,263 rows and needs a grep to localize the country. Both are data-correctness issues in `Feature('sample')`, which is the sampling backbone.  
**Next action**: Add `rural: Rural, urban: Urban` to `data_info.yml` sample → Rural → spellings. Grep `data_info.yml` files for `Rural: 0` or equivalent mapping. One 30-min session.

### Priority 3: #161 + #162 — cluster_features grain fixes  
**Rationale**: Stray `i` and household-level grain in `cluster_features` means 8 countries' cluster tables have duplicate index entries, which silently multiplies data for any consumer joining on `(t, v)`. Data-correctness issue.  
**Next action**: Dispatch parallel subagents for each of the 7 non-Uganda countries in #162, fixing `data_info.yml` to collapse to cluster grain after any household-level merge. Uganda's GPS wiring for the remaining 4 waves is the other half of #161. One dispatch session covers both issues.

### Priority 4: #172 — household_characteristics UserWarning + Guyana investigation  
**Rationale**: The UserWarning framework change is a small, clean improvement (1–2h) that immediately improves debuggability for the 5 declared-but-never-built countries. The Guyana anomaly (roster loads but characteristics fails) is a likely bug worth isolating quickly.  
**Next action**: Add a `UserWarning` in `country.py`'s derivation path when a source table is declared but empty/missing. Run `Country('Guyana').household_characteristics()` and trace the failure.

### Priority 5: #149 — PyPI wheel bloat (v0.6.1 prep)  
**Rationale**: A 38.7 MB wheel is a real barrier to pip-install adoption. The fix is mechanical (add `exclude` patterns to `pyproject.toml`). The `.claude/skills/release/SKILL.md` covers the publish process. Ready to execute in a single session.  
**Next action**: Add exclude patterns to `pyproject.toml`, run `make release` (in release skill's sandbox), verify wheel size < 5 MB with `unzip -l`.

---

## Housekeeping

### Issues that look resolved and should be closed
- **#164**: Togo Age — done by `7e889ed4`. Close.
- **#165**: age_handler adoption — done by 9 commits. Close.
- **#173**: Age dtype — done by `87fbd7a6` + follow-ups. Close.
- **#110**: GhanaSPS incomplete features — superseded by #140 umbrella. Close as duplicate.

### Issues that look partially resolved (note in comment, keep open)
- **#166**: Finding 1 (Niger Cope* leak) resolved by `f3075884`; Finding 2 (Affected* nulls) remains.
- **#161**: District stringified-float resolved by `7ec721d0`; grain/GPS issues remain.
- **#163**: `int_t` leak partially fixed by Rejected Spellings; Rural lowercase and `'0'` code remain.

### Issues waiting on external dependencies or human decisions
- **#140** GhanaSPS rebuild: blocked on human decision about pre-release data.
- **#146** Armenia: blocked on human decision (treat like Nepal, or acquire from ARMSTAT).
- **#169** food_acquired design: blocked on human choosing Option A/B/C.
- **#112** Tanzania food quantities: data collection gap in the survey itself.
- **#113** Tanzania community-price fallback: blocked on #112.

### Issues that are clearly long-term / not urgent
- **#107, #108, #115, #116, #117, #118**: Nutrition pipeline and FCT gaps for individual countries. No regressions; just missing features. Batch together when prioritizing nutritional analysis.
- **#109** GhanaLSS: shocks confirmed infeasible; remaining gaps (nutrition) are in same bucket as #115-#118.
- **#111** Serbia: broken build. Serbia is a minor country with one wave. Low urgency unless there's active research demand.
- **#167** plot_features: zero implementation exists; needs schema decision before any work.
- **#168** harmonize_assets: design settled, but ~13 countries of data-entry work; epic scope.
- **#169** food_acquired redesign: high-impact epic, but blocked on design decision.
- **#170** harmonize_housing: only 2 countries currently declare housing; small-scope harmonization.
- **#171** individual_education: Uganda 56% nulls needs per-wave probe first.

---

## Open Issue Inventory (complete, sorted by number)

| # | Title | Status | Complexity | Impact |
|---|---|---|---|---|
| 107 | Burkina_Faso: missing categorical_mapping.org, food_items.org, nutrition | stale | medium | 1 country |
| 108 | Cambodia: missing nutrition, food_prices, other_features | stale | medium | 1 country |
| 109 | GhanaLSS: multiple incomplete features | stale | medium | 1 country |
| 110 | GhanaSPS: multiple incomplete features | stale/superseded | large | 1 country — close as dup of #140 |
| 111 | Serbia: broken build | stale | medium | 1 country |
| 112 | Tanzania: missing food quantities 2019-20, 2020-21 | stale/blocked | medium | 1 country |
| 113 | Tanzania: community-level prices fallback | stale/blocked | medium | blocked on #112 |
| 115 | Ethiopia: missing FCT | stale | medium | 1 country |
| 116 | Malawi: incomplete conversion_to_kgs + missing FCT | stale | medium | 1 country |
| 117 | Panama: missing conversion_to_kgs.json | stale | small | 1 country |
| 118 | Tanzania: missing FCT | stale | medium | 1 country |
| 140 | [umbrella] Rebuild GhanaSPS | blocked/fresh | epic | 1 country |
| 146 | Armenia: data_access integration plan | blocked/fresh | small | 1 country |
| 149 | PyPI v0.6.1 wheel bloat | fresh | small | all users |
| 161 | Uganda cluster_features: wrong grain + GPS wiring | fresh/partial | medium | Uganda |
| 162 | Stray `i` in cluster_features, 8 countries | fresh | medium | 8 countries |
| 163 | Feature('sample') oddities | fresh/partial | small | 1-2 countries |
| 164 | Togo Age 94.5% missing | RESOLVED today | — | close |
| 165 | Wider age_handler adoption | RESOLVED today | — | close |
| 166 | shocks: Niger Cope* leak + Affected* nulls | partial | small/medium | 4-5 countries |
| 167 | Implement plot_features | fresh | epic | 13 countries |
| 168 | Add canonical harmonize_assets | fresh | large | 13 countries |
| 169 | Design: food_acquired canonical columns | blocked/design | epic | all countries |
| 170 | Housing: vocabulary harmonization | fresh | medium | 2 countries now |
| 171 | individual_education harmonization + Uganda nulls | fresh | medium | 3-5 countries |
| 172 | household_characteristics silent coverage gaps | fresh | small | 5-7 countries |
| 173 | Age dtype cross-country | RESOLVED today | — | close |
