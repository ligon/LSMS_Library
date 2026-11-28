# Prior-Art Ledger — Malawi 2024–25 shocks (#1275, #1556)

Search tier: ripgrep and git. Pinned base: `47df62d49e614c9a56e319def7324145d138c3d9`.

## §1 Task, restated

Add a 2024–25 producer to the existing Malawi `shocks` table and therefore to
`Feature('shocks')`. Serve all IHS6 and retained IHPS, keeping the IHS6
version of shared interviews per the component ruling. Keep the public
`(t, i, Shock)` grain and its recorded effects and ranked coping payload.

## §2 Existing machinery

| Path | Behavior | Reuse / test |
|---|---|---|
| Malawi `_/malawi.py:6013` and 2024–25 `_/mapping.py:15` | `CS_24`, household key formatting, panel cover's blank previous ID selects refresh | Reuse same convention and declared sidecar, not the helper's hidden read |
| Malawi `2019-20/_/data_info.yml:272` | Native shock labels, stock d / purchases e with separate booleans, U04 ranks | Reuse native labels and exact mapping semantics |
| `country.py:4267,4466,4482` and Malawi `_/coping.yml` | Canonical panel walk, drop wholly blank payload, then ranked-slot normalization and set | Reuse; `tests/test_shocks_malawi_iraq_food_effects.py` and coping regressions |
| `Wave._input_hash`, `Country._table_cache_hash` | Config and declared applicable sidecar fingerprints | Reuse; assert isolated sidecar changes affect shocks |

## §3 Definitions and conventions in force

- `STANDING.md` §§3–4, `CLAUDE.md` and `lsms_library/data_info.yml` define the existing index, nullable values, paths and cache rules.
- Malawi `_/CONTENTS.org`, notes around lines 5649–5656, distinguish 815 shared
  refresh interviews from 1,948 retained. Retained Panel cover `case_id` is not
  a current IHS6 identity.
- The #1556 food effect semantics are in the 2026-11-06 Malawi note. The
  #1562 ranked positions and read-time set/filler rules are in the 2026-11-13
  note. These new files carry the same five effects and three coping slots.

## §4 Invariants and assumptions

Source census: 12,888 IHS6 and 2,763 IHPS covers; all 815 shared dates match.
At native household and shock type there are 26,080 aligned shared keys,
and 91 copying differences in ranked codes (Panel Other versus named IHS6).
Keep IHS6 rather than asserting equal payload. Phase A verified the shared
form and both exports without reading open text. U01's five-year form and
three-year export labels conflict; this table does not consume U01/U02, dates,
counts, any recall estimate, or any specify text.

## §5 Reuse decision

Use the declared build pipeline for both Module U files plus the IHPS cover,
with one wave hook to check the shared shock grids and choose IHS6 whole rows.
The panel-identity provenance is explicit and hashed. Reuse the older payload
contract, leaving exactly one wholly blank ranked retained source row out of
the public result without changing older waves.

## §6 Open questions for the human

None for the ruled effects and coping coverage. U01 recall and any
all-experience grid are outside the table, independent of this change.

## Phase A exact inputs

Paths are under `Malawi/2024-25/`. The first three are declared in the build.
The other two were checked only for independent identity and routing evidence.

| Input | DVC MD5 | Bytes |
|---|---|---:|
| Data/Cross_Sectional/hh_mod_u.dta | `7c5cafbcd767f1e43dba4ecf37d61b4e` | 141,068,408 |
| Data/Panel/hh_mod_u_24.dta | `fc81c8a3581cd5a851f693120f8070b3` | 27,607,072 |
| Data/Panel/hh_mod_a_filt_24.dta | `c4c79be4babe53ffc52d4df01723e9f7` | 892,953 |
| Data/Cross_Sectional/hh_mod_a_filt.dta | `77291c794d64fb001df3266e9d7b417a` | 2,751,262 |
| Documentation/ihps_ihs6_household_questionnaire.pdf | `2ac9a99b1410c9669df9aa603eb114dd` | 2,345,011 |

## Phase 3 verification

- **OK (§§2–5)**. Ten source-free checks cover separate food effects, nulls,
  ranked slots and the set, differing shared answers, malformed input grains,
  cover mismatches, warm reuse, and the three explicit source fingerprints.
- **OK (§§2–5)**. Related source-free subset passed 67 tests with 35
  data-required cases deselected. No survey content was used for these tests.
  Run `tests/test_gh1275_malawi_shocks.py`,
  `tests/test_shocks_malawi_iraq_food_effects.py` and `tests/test_coping.py`.
- **OK (§§2–4)**. Direct base comparison moved 26/26 country hashes and
  26/26 2024–25 wave hashes mechanically via the wave YAML/module change.
  All 130 earlier-wave hash checks and 26 build-transform fingerprints
  remained exactly equal. No shared runtime or older table descriptor changed.
- The hook placement in `739fc8258` keeps the same function AST, imports,
  and parsed YAML as the initial source-free-tested version. Read-only merge
  previews against the separately open roster, component, date, housing,
  and education changes are clean pairwise. These are not dependencies.
- The separate independent Country/Feature tests are the coordinator's
  verification scope. The release evidence predicts 35,080 public rows in
  14,003 households for the 2024–25 addition alone.
