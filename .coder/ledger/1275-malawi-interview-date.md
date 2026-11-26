# Prior-Art Ledger — #1275 Malawi 2024–25 interview dates

Base `47df62d49e614c9a56e319def7324145d138c3d9`. Search tier: git + ripgrep. Inherit STANDING.md §§2–4 and AGENTS.md Data Access / Cache Behavior.

## §1 Task, restated

Make Malawi 2024–25 supply the date of each unique recorded household interview. Use only the two covers, the existing `interview_date` vocabulary and visit grain, and the existing Malawi identity convention. The date table does not require the roster branch, add an IHPS-only interview visit from consumption metadata, or change `sample`, weights or selection.

## §2 Existing machinery

| Contract | Anchor | Decision |
| --- | --- | --- |
| IHS6 and IHPS refresh are the same interviews | Malawi/_/CONTENTS.org, “2024-25 held and registered, not wired”; #1275 ruling 6643393923 | Reuse the 815-copy rule. An IHPS blank y4_hhid identifies refresh; its case_id points to the matching IHS6 interview. |
| Current household namespace | Malawi/2024-25/_/mapping.py:14, Malawi/2024-25/_/data_info.yml:9 | Existing cs_i for IHS6; exact y5_hhid for retained IHPS, walked by normal framework. |
| Classify refresh copies | Malawi/_/malawi.py:6015–6045 | Reuse ihps24_refreshed for the established identity map. Use it to check both rows and their calendar dates before removing just the panel copy. |
| Visit and declared grain | Malawi/_/malawi.py:466, Malawi/_/data_scheme.yml:91, local_tools.py melt_visit_intervals, canonical data_info.yml | Reuse the existing hook. Documented date-less visits are not served; only the recorded date column is supplied in 2024. Native (t,i,visit) with Int_t; public v is joined from sample. |
| Calendar parsing | datetime_parse.py parse_datetime; country.py Wave formatting / datetime_specs, GH #1045 | Explicit Gregorian ISO day at the existing build-time syntax, not format inference or date imputation. |
| Output scope | Country._join_v_from_sample | Reuse. A normal cold Country call requires nine all-wave covers; this author scope admits just two and verifies wave output and metadata fingerprints. |

## §3 Definitions and exact authority

- IHS6 (8507) and IHPS (8520) both document `interviewDate` in own Module A. The separate IHPS `consumption_date` is not visit 2. See https://microdata.worldbank.org/catalog/8507/variable/F1/V7?name=interviewDate and https://microdata.worldbank.org/catalog/8520/variable/F2/V115?name=interviewDate.
- Both held covers store ISO YYYY-MM-DD strings with Stata display %-9s and storage length 10. The DDI calls all 2,763 panel values valid character data, but one retained cover row contains an empty string. No date is invented. Native calendar dates parse on 12,888 IHS6 and 2,762 of 2,763 IHPS rows, including 815 out of 815 exact matching refresh dates.
- SOURCE/CONTENTS retain the 815 same interviews, 1,948 retained interviews and the established namespace. Expected dated rows are 12,888 + 1,947 = 14,835, not a new sample count.

## §4 Invariants and bounded inputs

- Only the existing date hook's one made visit for each dated interview; nulls do not become zero dates or fake visits. Compare equal copies after parse, preserving ordinary missing behavior.
- Guard duplicate/missing IDs, missing overlaps, invalid link identity and disagreement before dropping copies. No changes to sample, panel IDs, earlier visit counts, other tables, country schema, or weights.
- The two private sources are 2024-25/Data/Cross_Sectional/hh_mod_a_filt.dta (`77291c794d64fb001df3266e9d7b417a`, 2,751,262 B) and 2024-25/Data/Panel/hh_mod_a_filt_24.dta (`c4c79be4babe53ffc52d4df01723e9f7`, 892,953 B). Exact MD5/length, physically independent 0400 copies, offline one CPU, no raw IDs/output or other source rights in this lane.

## §5 Reuse decision

Extend only 2024 YAML and its local formatter. Reuse the existing refresh map, strict date parser and the existing Malawi visit hook. Keep shared functions untouched. Use source-free actual YAML-path tests plus confined native verification, and compare base/candidate metadata fingerprints explicitly.

## §6 Open questions for the human

None for the measured date-only change. A complete public/default/Make and Feature/equality check requiring all nine sources is a separate coordinator verification.

### Phase 3 — verification

- **OK (§§2–4), source and guard:** two exact held, physically independent private covers. The finite projected Stata fields carry no value-label pointers. All 815 duplicate dates match; exactly one retained date is blank. In strict calendar mode the new wave YAML dispatcher emits all 14,835 independent oracle dates and no others, unique (t,i,visit), visit=1, Int_t datetime64[us]. No identifiers or raw rows were emitted.
- **OK (§4), regression:** 90 passed, 2 pre-existing real warm-cache cases skipped in empty private state, across the six new source-free actual YAML-route tests, date conversion tests and GH #1045 tests. The new tests cover calendar leap validity, invalid day refusal, duplicate IDs, a missing IHS6 counterpart, contrary dates and both copies blank.
- **OK (§§4–5), scope and fingerprints:** 26/26 Malawi country and 26/26 2024-25 wave hash checks change, including runtime-derived surfaces. All 130 earlier-wave hash checks and all build-transform fingerprints stay equal. No country/shared code, earlier files or schema changes.
- **Independent verification pending in author scope.** The coordinator has the separate nine-cover grant for public Country/Feature and old-frame tests. The existing cold/warm visit index dtype difference remains outside this change.
