# Prior-art ledger — #1275, Malawi 2024-25 individual education

Inherits [STANDING.md](STANDING.md) §0. Evidence uses git, ripgrep, and the
separately approved projected audit against 47df62d49.

## §1 Task

Wire C09 from the two published Module C sources into the established Malawi
individual_education table. Keep one representation of each interview.
Only the IHS6 copy of the shared IHPS refresh interview is served.

## §2 Existing machinery

- `2019-20/_/data_info.yml:individual_education`: two-source C09 serving,
  the file's panel interview-local person field, and the existing
  `harmonize_education` Original Label -> Preferred Label mapping.
- `2024-25/_/mapping.py:cs_i`, `_/malawi.py:CS_24`, and `format_id`:
  shared household namespaces and no newly constructed keys.
- `Wave.grab_data`, `_merge_subframes`, and `_collect_file_paths_from_block`:
  join to the declared authoritative panel cover and fingerprint it.
- `Country._finalize_result` and `_join_v_from_sample`: existing committed
  household ID walk, sample cluster join, schema, and hollow-row removal.

## §3 Definitions in force

- Malawi `_/CONTENTS.org` (2024 registration) and
  GH #1275 comment 6643393923 establish that 815 refresh records denote
  the same IHS6 interviews. Prior housing and roster gates use the same bridge.
- Paired instrument MD5 `2ac9a99b1410c9669df9aa603eb114dd`, printed
  pages 9–10. Module C targets age 3+, C06 is ever attended school, and
  C09 is highest qualification. Its numeric 1 is explicit NONE. A blank
  is not NONE, and No education in the canonical table does not tell
  whether the person ever attended.
- Existing written schema `(t,i,pid)` with one Educational Attainment
  string column. Preserve all approved aliases including GH #645.

## §4 Invariants and exact evidence

- IHS6 C uses case_id/PID and IHPS C uses y5_hhid/id_code (not the
  released longitudinal PID). C keys match B and their own A covers
  exactly: 55,911 IHS6 and 12,050 total IHPS. Zero lost, duplicated or
  orphan keys. All 3,575 refresh persons across 815 interviews agree
  on both person keys and C09 answers, including 758 paired blanks.
  The C09 books are identical and all observed codes are already mapped.
- After removing refresh copies there are 64,386 raw C records. 49,996
  have an answer (43,389 IHS6 and 6,607 retained IHPS). Of 14,390 C09 blanks,
  4,911 are under age three, two have unresolved age and 9,477 are age
  three or older. In these releases all C06 YES respondents have C09,
  and all C06 NO/blank responses lack C09. Neither age nor C06 is a
  runtime filter. Never assign qualifications from these fields.
- Exact runtime modern source paths are `Cross_Sectional/hh_mod_c.dta`
  (`ed5222f8685bc12b9f3b7e962bea69c5`, 88,092,727 B),
  `Panel/hh_mod_c_24.dta` (`0a6e1f58385d70972dc4bf0d84c6fb41`, 17,344,413 B)
  and `Panel/hh_mod_a_filt_24.dta`
  (`c4c79be4babe53ffc52d4df01723e9f7`, 892,953 B). The B fragments and
  IHS6 A cover were diagnosis only. No roster PR dependency in production.
- Keep source-free tests, patch and audit entirely wave-local. No school
  recoding, age rule, panel/roster/weight edit or changed earlier wave.
- Private aggregate receipt and reproducer:
  `runtime/mw-1275-education-a.kMPiKOeV/results-attendance.json`.
  No raw reads occurred during implementation.

## §5 Reuse decision

Declare the raw C09 witness alongside the normal mapped value and compare
the released answer and exact within-interview grain before removing
copies. A temporary panel-cover-only row must never escape as a person.
The framework's existing hollow-row behavior removes missing C09 only
after the overlap guard, so reported NONE survives.

## §6 Questions

No pending policy decisions. Independent public default/Make parity
remains a separate data check.

### Phase 3 — verification

- OK (§§2-5): 32 source-free tests passed, six skipped, in the real YAML
  house/person guard, GH #645 null-label checks and category merge selection.
  Controls cover local vs longitudinal person identity, paired and one-sided
  blanks, same-normalized but different native labels, coverage and duplicates,
  no B access, and panel cover source hash binding.
- OK (§4): 22 country and 22 modern wave fingerprints changed. None of 110
  older wave fingerprints changed; only 2024-25 education became declared.
- OK (§4): read-only merge-tree previews passed against #1758 (6569df4f86),
  #1759 (ee935a3a34), #1774 (171c043888) and #1778
  (2566a35d4c8542292fbdfe1a5f51f446fda6fbaa). A placement-only change
  preserved the Python AST and parsed YAML exactly relative to 9bbc075504.
- Public Country/Feature data comparisons remain separate from this patch.
