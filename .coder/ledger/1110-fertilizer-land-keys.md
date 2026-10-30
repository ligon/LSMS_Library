# Prior-Art Ledger — GH #1110 fertilizer land keys

**Search tier:** ripgrep + git floor. Fixed public development baseline
`bff799496510d7e7d2470ddbc14e335d96a0b6d8`; no GitNexus index used.

## §1 Task, restated

Repair `fertilizer_rate(on='parcel')` on Malawi and Ethiopia without changing
fertilizer eligibility, units, nutrient shares, area, or source observation grain.
The existing feature-side underscore split destroys already shared native keys.

## §2 Existing machinery

| symbol | source | decision |
|---|---|---|
| `nitrogen_kg` | `transformations.py` | reuse both nutrient/product numerators and unit factors |
| `fertilizer_rate` | `transformations.py:6830` | extend only its land-key selection |
| `_parcel_from_crop_plot`, `_parcel_from_feature_plot` | `transformations.py:4860` | retain existing Uganda/anonymous bridge; do not change `yield_kg` |
| country validation and base keys | `.coder/ledger/1160-pooled-rate.md` | reuse index-before-column country identity, one-sided/null refusals |
| Uganda native key proof | `.coder/ledger/1205-uganda-parcel-area.md` | do not broaden that yield-only policy in this task |

## §3 Definitions & conventions in force

- Inherit `STANDING.md` §§3–4, `CLAUDE.md` data access, and canonical
  `data_info.yml` plot area in hectares.
- Ethiopia `CONTENTS.org:941–987`: field key is holder–parcel–field;
  parcel attributes join on holder + household + parcel, never holder alone.
- Malawi `CONTENTS.org` plot-feature identity and GH #1110 entries: garden–plot
  keys are shared literally with plot inputs. Splitting only area keys is wrong.
- Both countries' GH #1110 entries explicitly contradict the transform's
  measured-no-op claim. Their historical rate counts are not fresh acceptance pins.
- Preserve Malawi G `input_obs`/H ownership and Ethiopia unique-plot input rules.

## §4 Invariants & assumptions

- Country axes/columns remain identity; scalar attrs never create an output
  country axis or override labelled rows (GH #1160).
- Country metadata may identify the native key vocabulary only when both
  country-less operands agree; mismatched/missing metadata retains legacy policy.
- For labelled Malawi/Ethiopia, default parcel mode preserves literal native
  keys on BOTH sides. No area grouping at garden/holder grain, match heuristics,
  imputation, or fallback to broader keys.
- All numerator, positive-area filtering/grouping, warning and finite-rate
  arithmetic remain unchanged. Explicit `on='plot'` remains literal everywhere.

## §5 Reuse decision

Extend land-key dispatch only for the two documented vocabularies; reuse existing
parcel helpers otherwise. Keep `yield_kg` and country producers unchanged.
Tests must independently expect distinct per-plot rates, cover pooled identities
and metadata precedence, preserve anonymous legacy bridges and wrong-key failure.

## §6 Open questions for the human

None required for these documented literal identities. Broader country parcel
policies and prior scientific/unit/eligibility issues remain outside this fix.

### Phase 3 — verification

- `fertilizer_rate` — OK (§§3–5): country-bound literal keys fix the documented
  asymmetry; numerator, unit/area arithmetic, existing group grain and warnings
  remain unchanged. Legacy output level `parcel` does not assert coarser
  geographical parcel aggregation. `yield_kg` is unchanged.
- New literal regressions — OK (§4): first 17 cases on public baseline gave
  8 expected failures/9 passes; final 19 cases pass. A 169-case focused suite
  passes (includes 17 of the new cases, not 169 additional cases). The first
  candidate's two empty-key dtype regressions are preserved and repaired.
- Actual normal Country operands — OK (§§3–5), fixed product `1db0001ed`:
  Ethiopia 2021–22 PI 17,638/PF 14,878 yields 2,941 rates; Malawi 2019–20
  PI 64,835/PF 23,259 yields 11,346. Both nutrient/product bases agree exactly
  with explicit plot mode and a separate complete-key area lookup. Original
  baseline yields zero rates for these selected waves. Fresh-process warm
  payloads and private cache bytes/mtimes agree exactly. These are two selected
  waves, not all-wave acceptance or a new coarser parcel estimator.
- Failed arms retained: Ethiopia 2013–14 fails before the rate in an unstaged
  area-conversion dependency; its final DVC fallback path is duplicated, but a
  general cached-reader path defect is not proven. Malawi's first independent
  oracle rejected a StringDtype missing-sentinel representation difference;
  v2 explicitly matches the transform's str-key representation. No numeric
  value cast or product change was used to pass that comparison. Malawi v2
  is readback from the partially built private cache, not an empty-cache claim.
- Independent Ethiopia review: eight additional adversarial checks pass;
  scalar attrs choose vocabulary only and never establish country identity.
  Partial unmatched keys retain existing silent omission; Uganda's walked-ID
  fallback limitation remains outside this Ethiopia/Malawi repair.
- `make api-audit` passes; approved image interpreter adapter avoids Poetry
  installation. Full receipts and exact pins are in the isolated operator
  packet `fertilizer1110-public-v1`, exported under recovery/Uganda.
