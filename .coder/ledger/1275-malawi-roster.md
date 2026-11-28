# Prior-Art Ledger - #1275 Malawi 2024-25 household roster

**Search tier:** ripgrep + Git plus the two released World Bank Module B
dictionaries and the live issue/PR history. Read against development
`47df62d49e614c9a56e319def7324145d138c3d9`. Inherits `STANDING.md`.

## §1 Task, restated

Wire the held IHS6 cross-sectional and IHPS 2024 retained-panel Module B
rosters as one country wave with one representation of each interview.
The 815 IHPS refresh interviews are the same interviews as their IHS6
counterparts, not additional households. Keep the IHS6 roster representation
of those interviews, like the sample/food paths. Preserve retained-panel
household linkage, the released within-household person grain, existing
rosters and weights. Partial #1275 only, after #1506 / #1604 / #1711.

## §2 Existing machinery

| Path/symbol | Contract and targeted tests | Decision |
|---|---|---|
| `Malawi/2019-20/_/data_info.yml:216` and `2016-17/_/data_info.yml:222` | Two files, cross-sectional `case_id` prefixed by `cs_i`, panel wave-native `i` and `id_code` as `pid`; `Sex/Age/Relationship/MonthsAway` from Module B. `tests/test_gh1278_malawi_cs19_namespace.py` checks namespace parity. | Extend the wave-local YAML pattern only. |
| `Malawi/2024-25/_/mapping.py:14` and sample/panel declarations in `data_info.yml:9` | Keep IHS6 IDs under `cs-24-` and identify refresh copies by missing normalized `y4_hhid` in the panel cover. Persist existing household link and weight fields on the IHS6 sample row. `test_malawi_panel_id_guard.py` and `test_malawi_food_borrowing.py`. | Reuse the same namespace and cover-based rule; no new linkage map. |
| `Malawi/_/malawi.py:6012` (`ihps24_refreshed`, `drop_ihps24_refreshed`) | Sample and food read the same cover's `(y5_hhid, y4_hhid, case_id)` to remove refresh copies. A hidden call from a roster YAML hook would not hash the cover's raw sidecar for that table. | Reuse the cover as a declared roster input, and compare the resulting set to the existing helper in verification. |
| `Wave.grab_data` and `Wave._input_hash` (`country.py:2032,1450`) | Sanctioned YAML extraction and source fingerprints include all table-declared files before pre-finalize L2 write. Existing tests cover `dfs:` left join, source drop handling, subset/read parity. | Reuse. The roster covers only the two Module B files and panel cover; a missing panel cover must not quietly authorize copies. |
| `Country._finalize_result` and `_join_v_from_sample` (`country.py:4267,3377`) | Walks committed household IDs before attaching sample `v`. Kinship/schema normalization and `household_characteristics` downstream already exist. | Reuse; do not mutate published maps or emit `v` from the roster producer. |
| `diagnostics.py:1411,1585,1781` | `check_panel_consistency` expects a full roster spine and checks links after the ID walk; the current reported #1604 misses are 1,948/7,634 and a 2019-20 -> 2024-25 zero-overlap warning. | Reuse the actual checks, not a parallel definition. |

## §3 Definitions & conventions in force

- `STANDING.md` §§3-4; `CLAUDE.md` on grain and months away;
  `lsms_library/data_info.yml:435` and `Malawi/_/data_scheme.yml:38`.
- The World Bank file dictionaries identify `case_id, PID` in
  [IHS6 Module B](https://microdata.worldbank.org/catalog/8507/data-dictionary/F2?file_name=hh_mod_b)
  (55,911 released rows) and `y5_hhid, id_code` in
  [IHPS Module B](https://microdata.worldbank.org/catalog/8520/data-dictionary/F3?file_name=hh_mod_b_24)
  (12,050). IHPS `PID` is a different assigned person serial; do not use it
  in place of roster `id_code` by name similarity. Both source dictionaries
  name `hh_b03`, `hh_b04`, `hh_b05a` and `hh_b07`.
  The shared [household questionnaire](https://microdata.worldbank.org/catalog/8507/download/359615),
  printed Module B pages 5-6, gives years at B05a and cumulative months away
  in the previous 12 months at B07. PDF MD5 matches the committed
  documentation sidecar (2ac9a99b1410c9669df9aa603eb114dd).
- `Malawi/_/CONTENTS.org:324,376,5637` documents the two-entry source
  layout, same-interview Q2 refresh, 12,888 XS and 1,948 retained IHPS
  households, and expected diagnostics. The historical "No table of this wave
  is served" at line 5641 is explicitly superseded by #1604; the sample
  exists, the roster does not.
- The current maintainer ruling says "one row per interview" (issue
  [comment](https://github.com/ligon/LSMS_Library/issues/1275#issuecomment-6643393923));
  on the person roster this means each native person row of an interview is
  represented at most once, not one summary row per household.

## §4 Invariants & assumptions

- Do not stack refresh copies or deduplicate person keys speculatively.
  Check unshifted IDs and compare refresh roster people under IHS6 household
  `case_id` plus the correct local ID correspondence; count discrepancies.
  No guessing if source identifiers or cell values differ.
- No changes to the public sample, weights, panel links, earlier-wave
  configs, other country schemas or source sidecars. Keep household and
  person namespace joins alike across the roster and sample. Do not treat
  availability of a source as proof that a needed key is populated.
- Report only aggregate counts, checks and hashes. No source rows, names,
  IDs or instrument free text in logs or tracked artifacts.
- The current pending real-data verification is limited to the exact four
  2024-25 A/B sources, then the seven historical Module A and seven
  historical Module B sources for all-wave parity and panel consistency.
  Use only private pinned provider/cache roots and an offline actor.
- Verify source/config/package identity, entirely cold and warm history on
  default, isolated Make parity, unchanged earlier-wave values and schema,
  unique grain, sample join coverage, full panel diagnostics before/after,
  and a selective source-fingerprint mutation check on copied sidecars.
  Quantify actual fingerprint movement rather than requiring zero.

## §5 Reuse decision

Extend the ordinary YAML roster with an explicit left join against the
panel cover and a wave-local validation/filter hook. This avoids creating a
new estimator, new caching path or a country-wide Make rule. Do not use
the YAML secondary-source drop fallback as authority for keeping panel rows.
A missing or duplicate cover identity for a roster row, or a missing refresh counterpart, must surface
as a specific error, not as a fabricated identity or silent row drop.
Use the existing two-entry household rule, library format_id, kinship,
presence semantics and finalized v join.

## §6 Open questions before implementation

- Prove the retained and refresh roster keys, local person IDs, native
  relationship/age/residence labels, completeness and refresh correspondence
  against the exact held files, then record fallback behavior on discrepancy.
  Public metadata does not prove row-level correspondence.
- A stable IHPS serial, translated names and non-roster modules are outside
  this slice. Do not introduce a new synthetic pid or infer longitudinal
  person linkage.

### Phase 3 - verification

- Roster keys and payload — OK (anchored on §§3-4). On the four current-wave
  Module A/B source files, every Module B household joins its own cover and
  every cover household has people. Cross-section: 55,911 rows / 12,888
  households. Retained panel: 8,475 rows / 1,948 households. Refreshment:
  3,575 rows / 815 households. Exact IHS6 PID to IHPS id_code correspondence
  conserves all 3,575 duplicates including their four answers. Using IHPS
  PID matches zero of those keys. No duplicate or null person keys and no
  unrepresented sample households.
- Wave YAML and hook — OK (§§2, 4-5). Synthetic actual-wave path checks raw
  columns and local IDs, refresh payload and person-key refusal, namespace
  separation, duplicate refusal, missing cover cases and dependent hashes.
  pytest -q tests/test_gh1275_malawi_roster.py tests/test_malawi_panel_id_guard.py:
  19 passed in the pinned extracted image, in a network namespace with
  private config/data/cache/state/temp roots and one worker.
- Standard Country and Feature APIs — OK (§§2-4). In independent held-byte
  projections (18 MD5-checked cover/roster files, 108,938,388 bytes per
  root), the default and Make backends agree on exact sorted row/type
  fingerprints. Fresh one-wave requests equal their all-wave-warmed
  counterparts (exact in-process frame comparison) and both give 64,386
  rows, 14,836 distinct households and 1,948 retained panel endpoints.
  Feature agrees after aligning level order to Country. No null keys,
  including v, and source Age/MonthsAway values and their missingness are
  conserved. Against base, the unchanged 260,612 older roster rows and full
  71,954-row sample match exact sorted row/type fingerprints.
- Panel diagnostics — OK (§2). Base 5 pass / 2 warn / 1 fail. Both candidate
  backends 7 pass / 1 warn / 0 fail. panel_ids_targets_exist and
  attrition_monotonic turn green. Prior updated_ids_cover_waves warning
  stays. No crosswalk or weights changed.
- Cache scope — OK (§§2, 4). 22/22 registered Malawi country hashes change
  under the country hash's full-wave union. Older wave hashes remain
  identical (0 of 110 computed pairs); 22/22 calculated 2024-25 pairs
  change, of which sample, panel_ids and food_acquired were previously
  declared. No old value change claimed from the hash census alone.
  The cover fingerprint is an explicit input at both roster tiers.
- Validation limits: measured only the roster/sample/panel slice and its
  exact admitted held sources, with no full data suite or shared cache
  refresh. The first validation harness attempt was stopped by my incorrect
  Country index-order assertion; the actual order is (i,t,v,pid), which the
  corrected harness checks. An intermediate check also caught an Age and
  Relationship column-order difference across backends; declaring in the
  older waves' order removed it. Final tests use the corrected declaration.

