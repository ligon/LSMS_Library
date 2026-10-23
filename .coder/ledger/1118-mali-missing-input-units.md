# Prior-Art Ledger -- GH #1118 Mali missing plot-input units

Search tier: ripgrep + git, with source-only stdlib text/AST planned.
Standalone base: `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Inherits `.coder/ledger/STANDING.md`; no new framework machinery.

## §1 Task, restated

Resolve only Mali's portion of GH #1118 under the settled GH #1041
missing-unit rule. Both EAC-I native `plot_inputs` scripts share
`mali.plot_inputs_finalize`. A retained record with genuinely absent unit
must eventually carry existing `U_UNKNOWN`, without changing rows, grain,
eligibility, quantities, flags, labels, valid units or derived mass.
Phase0 is source/metadata only: no production fill until guarded source
evidence separates raw absence from substantive labels lost by mapping.
Pending PRs #1180/#1186/#1199 are not implementation prerequisites and are
not merged or cherry-picked into this branch.

## §2 Existing machinery

| Symbol | Source | Existing coverage | Decision |
|--------|--------|-------------------|----------|
| `_unit_labels` | `lsms_library/countries/Mali/_/mali.py:313` | No direct Mali helper test found by exact-symbol search in tests | Reuse unchanged; strips decoded labels and maps country `u` table, so mapping loss must be diagnosed separately |
| `plot_inputs_finalize` | `lsms_library/countries/Mali/_/mali.py:428` | No direct helper test found; add meaningful synthetic tests after release | Extend only final missing-unit representation after existing duplicate reduction, conditionally on source authority |
| Native input drivers | `lsms_library/countries/Mali/{2014-15,2017-18}/_/plot_inputs.py` | Existing country build surface; genuine execution held | Reuse source slots, own dictionaries, quantities, household-purchase annotation and exact writer paths |
| Country input builder | `lsms_library/countries/Mali/_/plot_inputs.py` and `_/Makefile` | Existing normal Default/Make routes; no routing repair in this task | Reuse; retain any actual-parent unsupported-wave behavior as a separate finding |
| `U_UNKNOWN`, `_U_SENTINELS`, `_unit_sentinel_mask` | `lsms_library/transformations.py:3744`, `:3748`, `:3773` | `tests/test_962_unit_sentinel.py`, `tests/test_crop_kg_factor.py` | Reuse canonical sentinel and missing-unit refusal, no new spelling |
| `seed_kg`, `nitrogen_kg` | `lsms_library/transformations.py:5569`, `:5446` | `tests/test_gh1093_declared_unit_consumers.py`, `tests/test_867_nitrogen_content.py` | Reuse unchanged; prove no fabricated mass |
| Existing minted-unit AST guard | `tests/test_962_unit_sentinel.py:761` | Existing regression | Reuse after guarded runtime release |
| Source IO, cache hashes, finalization, sample-v join | `STANDING.md` §2/§4 | Existing framework tests | Reuse sanctioned machinery and exact package/config identity; no bypass or cache stamping |

Frozen reconnaissance: `C/mali1118-recon/FINAL-PINS.json`
SHA `20bf1fd587edb5074c43ecc7d5e06aceb81ce025b1239cd720da2b3ca567a5b7`;
`RECON.md` SHA `18d651aca5158e0cd40d62a86a7e0138ff1f1aff3f8e9965b4dfd73b3da2fd82`.
Here `C=/local/job40255479/control/isa-backlog-20261021`.
The historical 42,089-null issue count is not fresh runtime evidence.

## §3 Definitions and conventions in force

- Canonical sentinel: `lsms_library/data_info.yml:18` says a recorded amount
  with no unit must put `Unknown` on `u`, not a null. Its consumer rule at
  line36 says `Unknown` is not a unit. Reuse `transformations.U_UNKNOWN`.
- Inherit canonical schema and sanctioned IO from `STANDING.md` §3/§4 and
  `CLAUDE.md` Data Access / Grain Collapse; no shared schema or unit-map edit.
- `Mali/_/data_scheme.yml:182` declares item-level plot inputs only in EAC-I
  2014-15/2017-18. Fertilizer/pesticide crop can be missing; seed crop is
  recorded when available. Household purchases annotate plot applications.
- Full own 1,246-line `CONTENTS.org` read first, including TODO/WAITING
  declarations and all historical qualifications; no task-state headings
  or LOGBOOK drawers occur. Actual idiosyncrasies: rural2014 observations
  are agricultural holdings, urban2014 observations households; rural2017
  is one-third Lourd/two-thirds Leger, with exploitation rather than menage
  in the household key. Four configured waves are not one uniform survey.
- Preserve the explicit contradiction: Overview says "Mali EHCVM ... Four
  waves are configured"; the correction says "Only *2018-19 and 2021-22*
  are EHCVM; *2014-15 and 2017-18 are EAC-I*". The presence section says
  binary filters "do not give a continuous duration measure", whereas the
  dated caveat records the existing2018 Oui->12/Non->0 proxy. Neither is
  permission to change unrelated survey meaning.

## §4 Invariants and assumptions

- `_unit_labels` currently maps both raw absence and substantive unmatched
  decoded labels to missing. Source authority must resolve this distinction
  before any final fill; unknown meaningful labels are a stop/report gate.
- Preserve the existing `has_content` predicate: quantity or purchased
  quantity present, Purchased true, or Improved nonmissing. Purchased false
  alone does not retain an otherwise empty row; zero/negative reported
  quantities are not new eligibility decisions.
- Preserve groupby keys, `dropna=False`, sum `min_count=1`, first valid unit,
  max flag reductions, sort order and missing plot/crop keys. Fill only
  after `g['u'].first()`: a pre-collapse sentinel can displace a later real
  unit. Include both duplicate orders and all-missing duplicate controls.
- Keep source dictionaries, quantities, flags, valid units, IDs, grain,
  labels, factors, reducers and every non-u value unchanged. No Malawi or
  Nigeria eligibility work; no framework/schema/routing repair.
- `mali.py` is a whole-country build input: full source-bound
  549/39/1,362 predata maps must measure actual exposure, not copy old bills.
- No runtime, project/numerical imports, tests, fingerprint census,
  provider payload opens/hashes/copies/decoding, normal cache root,
  acquisition or shared writes during phase0. Runtime needs root release
  of a concrete source-authority/guard/budget plan. Existing mounted venv
  only, private DVC directories, exact controller/worker/Make identities.

## §5 Reuse decision

| Quantity or behavior | Decision | Reason |
|----------------------|----------|--------|
| Missing-unit spelling | Reuse | Existing `U_UNKNOWN` and consumer refusal are authoritative |
| Native output collapse | Reuse | Existing reducers and first-valid-unit semantics must survive |
| Final unit null representation | Conditional extend | Smallest shared-country seam, only after raw-vs-unmapped diagnosis |
| Unit mapping and scientific conversions | Reuse unchanged | A representation repair does not authorize new semantics |
| Source readers/writers and normal caching | Reuse | Existing sanctioned APIs and real writer/storage stages |
| Targeted helper tests | New test coverage | No direct helper tests found; actual-parent failures must discriminate early-fill/no-fill/eligibility mutations |
| Genuine source-first diagnostic | New bounded private evidence | Needed to establish authority of current nulls; no existing fresh counts claimed |

## §6 Open questions and gates

- Which current raw unit codes/own labels are truly absent, and which
  substantive labels are lost by the country map? This blocks production.
- Authenticate exact sidecars/SOURCE declarations, held endpoints and
  complete sample/weight dependency spine. Six core input sources may need
  eight additional files; do not silently expand the budget.
- Root reviews the phase0 source plan and finite source/copy/byte budget,
  then authorizes guarded synthetic/source-first runtime separately.
- A genuine newly established survey idiosyncrasy goes in this checkout's
  CONTENTS only after source evidence. No human decision is requested for
  routine choices already covered by root's bounded task authorization.

### Phase3 verification

Pending: phase0 ledger only; no product edit or runtime evidence exists.

### Phase 2 -- source authority measured and fix applied (2026-10-22, Sue / Claude Opus 5.5)

Branch now merges development `60aa81e9e`.  `diagnose_1118.py` runs both real wave
scripts in-process with `plot_inputs_finalize` wrapped (nothing written):

| wave | retained null `u` (pre-collapse) | blank raw | non-blank unmapped | served null `u` |
|---|---|---|---|---|
| 2014-15 | 14,096 | 13,628 | 468 (all `Manquant`) | 14,053 |
| 2017-18 | 28,036 | 28,036 | 0 | 28,036 |

`Manquant` is the survey's missing label, so §6's gate is answered: no substantive
label is lost by `_unit_labels`.  The fill (`out['u'].fillna(U_UNKNOWN)` after the
collapse) is applied.  The #1041 ruling covers flag/purchase-only rows too, so the
archived test asserting `U_UNKNOWN` there is kept and renamed to say so.

- Tests: new file 11 fail / 14 pass on 60aa source; 25 pass on the candidate, with
  `test_962_unit_sentinel`, `test_867_nitrogen_content`, `test_gh1093_declared_unit_consumers`
  (156 passed, 2 skipped in total).
- Cold `Country('Mali').plot_inputs()`: 113,735 rows both sides, index identical,
  every non-`u` column exact; `u` changes on exactly 42,089 rows, NA -> `Unknown`.
  `Feature('plot_inputs')(['Mali'])` now matches `Country()`.
- Hashes: 24/549 country and 96/2,109 wave hashes move, all Mali; 0/40 fingerprints.

Finding for a separate decision: 41,559 of the 42,089 rows have `Quantity == 0` from
the raw source (unused slots, unit skipped).  Eligibility is out of scope here.
Evidence: `slurm_logs/isa_backlog_2026-10-21/1118-mali-units/`.

Use-flag check (`flags_1118.py`, `flags.json`): on the zero-quantity / blank-unit
fertilizer and pesticide slots the plot-level GROUP use flag reads `Oui`
(2014-15 11,577 + 587 `Manquant`; 2017-18 27,786 of 27,786).  The flag is per
group, the quantity per type, so these rows are type-level non-use inside a
positive group answer.  The hash census above predates the CONTENTS.org and test
edits; neither is a build input, so 24/0/96 stands.

### Phase 3 -- group use answers served (2026-10-22, Sue / Claude Opus 5.5)

Maintainer decision: serve the plot-level group use answers explicitly, in
`plot_inputs` (not `plot_features`, which covers only the EHCVM waves).  New
input codes `inorganic_fertilizer` -> `Inorganic Fertilizer` (Uganda's
spelling) and `phytosanitary` -> `Phytosanitary products`; new column
`Applied: bool`.  `plot_inputs_finalize` counts a non-null `Applied` as content
and reduces it with `max`.  Label choice: a distinct label naming a fertilizer
would raise `NutrientCoverageWarning` on every Mali `nitrogen_kg` call
(`_FERTILIZER_LABEL_HINTS`), while Uganda's spelling matches its 0.20 share
with NaN quantity, so mass is unchanged and only the match tally grows.

- Tests: `tests/test_mali_input_use_flags.py` 7 fail / 1 pass on 6ffeb3caf
  (the pass is the missing-answer control); all pass on the branch, 164 passed /
  2 skipped with the #1118 and unit/nitrogen suites.
- Cold build: 113,735 -> 179,817 rows; all existing rows exact; 66,082 new group
  rows; nitrogen_kg / seed_kg identical.
- Hashes: 24/549 country, 96/2,109 wave (Mali only), 0/40 fingerprints.

Amendment (same day, maintainer decision): the group rows are labelled
`Inorganic Fertilizer?` / `Phytosanitary products?`, and a trailing `?` is a
library convention for a use ANSWER (`transformations.is_use_answer_label`).
`nitrogen_kg` drops such rows before label matching, so the Uganda-spelling
trade-off above (shared label, inflated `matched_rows`) no longer applies.
`seed_kg` selects the exact `Seed` label and needs no change.

### Phase 4 -- review follow-ups (2026-10-23, Sue / Claude Opus 5.5)

Branch `fix/mali-plot-inputs-followup-20261023` on development `9a0ed5f3c`.
Evidence: `slurm_logs/isa_backlog_2026-10-21/mali-followup/`.

| fix | change | precedent |
|---|---|---|
| marker | `plot_inputs_finalize` fills `crop` with `mali.CROP_NA = '(not crop-specific)'` before the collapse | `Niger/_/niger.py:658-668`, `:693`; the six other EHCVM siblings' `fillna(CROP_NA)` (Benin `:92`, Burkina_Faso `:91`, CotedIvoire `:84`, Guinea-Bissau `:125`, Senegal `:103`, Togo `:105` of `2018*/_/plot_inputs.py`) |
| unresolved seed crops | the 45 seed rows whose recorded crop `harmonize_food` does not map get the marker | Niger's CROP_NA comment, "any seed row whose crop could not be resolved" (`niger.py:659-660`) |
| tally | `nitrogen_kg` adds `attrs['nitrogen_input_match']['use_answer_rows']` (always present) | -- (new key; existing keys unchanged) |
| docs | comment block beside `plot_inputs` in `data_info.yml`; NO `Columns: plot_inputs` entry | no Columns block for plot_inputs exists; `Applied` is declared only per country (Ethiopia, Malawi); a `type: bool` entry would feed `_enforce_canonical_dtypes` (`country.py:7220`) for every country |
| `Applied` optional | `Applied: {type: bool, optional: true}` | `Malawi/_/data_scheme.yml:306-308`, `Ethiopia/_/data_scheme.yml:234-236` |

Rejected: leaving the 45 seed rows NaN (the brief's fallback when no precedent
exists -- Niger's comment is one).  Not done: two `harmonize_food` rows for
`Echalotte` / `Poireaux` (a Mali categorical-mapping change, outside this scope).

- Tests: new/updated 29 fail / 28 pass on base source (the passes are the
  sweep's non-vacuity check and the `Applied`-declared guard, which base
  already satisfies; a mutation removing `Applied` from Mali's scheme makes the
  guard fail); 605 passed / 5 skipped on the branch with the #1118, unit,
  nitrogen, build-hash, schema and canonical-index suites; the 5 warm-gated
  skips rerun on a warm private root: 107 passed / 1 skipped (`plot_labor`).
- Cold `Country('Mali').plot_inputs()`: 179,817 rows both sides; identical
  after base `crop` NaN -> marker, on exactly 145,530 rows; Site I silent;
  full-index groupby on `Feature('plot_inputs')(['Mali'])` 34,287 -> 179,817.
  `nitrogen_kg` 906,361.83 kg / `seed_kg` 1,455,210.96 kg unchanged;
  `use_answer_rows` 66,082.
- Cross-country `Feature('plot_inputs')()`, 14 countries: per-country and
  pooled `nitrogen_kg` / `seed_kg` and tallies identical but for the new key
  (0 everywhere except Mali); pooled frames exactly equal.
- Hashes: 24/549 country and 96/2,109 wave (Mali only), 0/40 fingerprints;
  the `data_info.yml` comments alone move 0.
- Re-measured for `feature.py`: Nigeria's `plot_inputs.u` holds both nulls
  (5,546) and literal `Unknown` (178) -- the promotion-collision state -- with
  no collision (96,620 rows in and out).
