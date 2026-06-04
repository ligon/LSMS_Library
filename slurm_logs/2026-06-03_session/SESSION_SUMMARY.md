# Session Summary — (Country × Feature) matrix-fill (GH #167 + coverage expansion)
# 2026-06-03/04. Scrum-master: Sue (Claude Opus 4.8).

## Outcome: 4 features filled, ~34 new (country×feature) cells, ~1.5M new rows.
All work on per-feature integration branches; NOTHING merged to development/master. Ready for review.

| Feature | Branch | Result (cold Feature(), real build) |
|---|---|---|
| plot_features | feature/plot-features-167 | 12 countries, 376,266 rows (was 0). Pilot Tenure fix + 11 countries + build-rule fixes. |
| interview_date | feature/interview-date-coverage | +7 new (Iraq,India,Pakistan,Cambodia,Kosovo,Guyana,Tajikistan) -> 20 countries, 242,934 rows. |
| assets | feature/assets-coverage | +6 new (Iraq,Kosovo,Serbia,Liberia,Albania,Guyana) -> 12 countries, ~2.98M rows. Silent-dedup fixed. |
| individual_education | feature/individual-education-coverage | +9 new (Liberia,India,Kosovo,Ethiopia,Kazakhstan,Albania,SouthAfrica,GhanaLSS,Tajikistan) -> 16 countries, 863,603 rows. |

Per-country PRs: plot_features #280 + 11 country PRs; interview_date #296-301,305; assets #292-295,307,310 + dedup #315,316; individual_education #302-304,306,309,311-314. All merged into their feature branch + cross-country Feature()-verified on a COLD cache.

## Method (the workflow, validated)
recon (read-only) -> ADVERSARIAL REFUTE (caught real errors on every plot_features recipe) -> implement in worktree -> REAL-BUILD verify -> build-verifier red-team -> merge + Feature() verify.
KEY CORRECTION mid-session: implementation agents initially verified via direct script-run from the worktree CWD, which FALSELY passes (sys.path[0]='' imports worktree code even via the main venv) and hid make-path bugs. Fixed with the worktree-pinned-venv protocol (REALBUILD_PROTOCOL.md): build through the real framework, from /tmp, cold cache. Every later agent caught its own make-rule issues up front.

## Canonical decisions made this session
- plot_features Tenure: wave-keyed harmonize_acquire (Wave,File,Code); NaN not silent default; use_right/squatted spellings added; area clamp >2500 acres.
- TenureSystem spellings extended: granted_right_of_occupancy, state, community, cooperative.
- assets dedup rule: within (t,i,j) Quantity=sum, Value=sum, Purchase Price=sum, Age=min (count where no source qty).
- individual_education: free-text Educational Attainment (Phase-2 cross-country harmonization deferred).

## Deferred (not implementable this session)
- GhanaSPS: no data_scheme.yml (structural) — plot_features + assets deferred. Maintainer decision needed.
- China: interview_date — no date variable in microdata.
- Guyana: individual_education — HS column has no value labels (needs GLSS codebook).
- plot_features: pre-EHCVM waves (Niger 2011/2014, Mali 2014/2017, Burkina 2014), Tanzania 2008-15 (no ag source), Malawi 2004-05 (no plot roster).
- Uganda plot_features 2019-20: false-alarm resolved (verified all 8 waves).

## Found issues (PRE-EXISTING, out of scope — for follow-up)
- EHCVM assets (Benin/Burkina/CotedIvoire/Guinea-Bissau/Mali/Niger/Togo) error in Feature('assets'): "'float' object is not iterable".
- individual_education pre-existing declarer failures: Nepal (no data), Burkina_Faso, CotedIvoire (DVC), Mali ("Series ambiguous" build bug).
- Feature() returns an unnamed index (index.names==[None]) for some features — cosmetic aggregation quirk.
- interview_date: old declarers leak v/date as columns (schema inconsistency).
- Albania household_roster v-join is 100% NaN for 2005/2008 (roster i != sample PSU-HH i).
- South Africa individual_education post-secondary labels (codes 13-19) best-effort — confirm vs SALDRU codebook in Phase 2.
- EthiopiaRHS sample missing 3 waves (test_covers_all_waves fail) — tracked with PR #273.

## Incidents (recovered)
- Two worktree-agent git leaks (Burkina, Albania) switched the MAIN checkout's branch + (Burkina) stranded one docs commit. Both fully recovered (cherry-pick / branch restore); no data or PR lost. Lesson: worktree agents can leak git ops to the main checkout — re-check `git branch --show-current` before coordinator commits.

## Next steps for the human
1. Review the 4 feature integration branches -> development.
2. GhanaSPS structural decision (how it surfaces tables without data_scheme.yml).
3. Optionally fix the pre-existing EHCVM assets + individual_education declarer bugs.
4. Phase-2 cross-country label harmonization for individual_education attainment.
5. Next coverage targets per COVERAGE_MATRIX: shocks has only ~2-3 implementable (low value); most other gaps now filled.
