# Prior-Art Ledger: #1733 Tanzania R1/R2 shock coping, original instruments

**Search tier used:** git + rg/grep floor (gitnexus unavailable). Inherits STANDING.md; cites CLAUDE.md, data_info.yml.

## §1 Task
Settle from the official public NPS Y1 (2008-09) and Y2 (2010-11) household instruments whether shock coping was asked. Evidence only: no verdict, config, code, test or CONTENTS.org edit. Pinned 47df62d49e614c9a56e319def7324145d138c3d9.

## §2 Existing machinery
- `lsms_library/coverage_matrix.py:169,284-347`: verdicts apply to undeclared or empty cells only. reuse (read).
- `lsms_library/coping.py`, `Tanzania/_/coping.yml`: read-time label rules; no wave-level "not asked". read.
- Sidecars `2008-15/Documentation/NPS-R{1,2}/`: exact identity of both target forms (GH #1441). reuse as hash oracle.

## §3 Definitions in force
`shocks` HowCoped0..2 are ranked positions (data_info.yml); `food_coping` is a different feature. The UPD union panel is not a fielded round (Tanzania/_/CONTENTS.org). Not-asked and asked-not-distributed differ, and C4 is mandatory (docs/guide/coverage.md).

## §4 Invariants
One folder 2008-15 holds four rounds. 2008-09 and 2010-11 `shocks` grade `sane`, so a verdict row would be inert, and false at (country, feature, wave) grain. `optional: true` is country-grain (CLAUDE.md "Known granularity mismatch").

## §5 Reuse decision
No quantity computed; no new library symbol. Throwaway stdlib readers live in the dispatch evidence directory.

## §6 Findings
Section R of the R1 workbook and the R2 form (English, Swahili) has questions 1-5 only; manuals, the UPQ R1/R2 panels and DDI 76 / 1050 agree; R3 is the first round with a coping question (positive control). Not asked in the published forms inspected. This does not establish the exact fielded Y1 revision or show that a different field pack or deployed form could not resolve the question. Report, hashes and checks (20 of 20) are in the dispatch evidence directory, not in the repository.
Open: authenticate the fielded Y1 authority specified in #1733 before any fielded-scope or column-and-wave verdict. Do not reinterpret Year 2 food coping or record a whole-shocks verdict.

---
### Phase 3
No symbol changed. Verification anchored on §2 and §3: OK.
