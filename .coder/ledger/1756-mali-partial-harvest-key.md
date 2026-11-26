# Prior-Art Ledger — GH #1756, Mali 2017-18 partial harvests carry harvest-to-date

Inherits `.coder/ledger/STANDING.md` (§0). Base `47df62d49`. Search tier: ripgrep + git
(gitnexus did not connect in this session; substitution declared per CLAUDE.md).

## §1 Task
Ruling @ligon 2026-11-25, "Add the key and correct the documentation": reuse the existing
`Mali::crop_production::harvest-to-date` key on eligible 2017-18 seasonal (s7f) rows whose
`s7fq10` is `Non` and whose `Quantity` is recorded; correct the wave docstring (`s7fq10`, not
`s7fq06`); support exact-key inputs `s7fq10` / `s7fq13a` / `s7fq13c`. No quantity, factor, sale,
tree row, sale-only row, panel key or other-wave behaviour may move. Source-free in this step.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| `DERIVATION_HARVEST_TO_DATE`, `derive_harvest_to_date` | `Mali/_/mali.py` | the registered identity and its key | `tests/test_gh1437_mali_harvest_total.py` | **reuse** (no second key) |
| `stamp_harvest_to_date` | `Mali/_/mali.py` | EHCVM stamp: `finished == 'Non'` and `Quantity.notna()` | same | **reuse the predicate**; not called, because it needs `finished` as a column of the frame and the EAC-I finalizer returns a fixed column list |
| `_with_key` / `derivations.union_keys` | `Mali/_/mali.py`, `lsms_library/derivations.py` | sorted `MULTI_SEP` union of keys on a row | #1437, #1457 tests | **reuse** (the non-destructive union) |
| `stamp_hh_crop_sale`, `stamp_tree_derivations` | `Mali/_/mali.py` | the post-finalize `(derivation, df)` stamps of this wave | #1457, #1267 tests | **pattern reused**; chained before the new stamp |
| `crop_production_finalize` | `Mali/_/mali.py` | map crop/unit, mask 99/9999, collapse on (t,i,plot_id,crop) | #1749, #824 tests | **unchanged** (shared with 2014-15) |
| `_is_quantity_sentinel` | `Mali/_/mali.py` | the "Manquant" codes | yes | **reuse** in the inputs callable |
| `_eaci17_keyed` | `Mali/_/mali.py` | raw 2017-18 file with served `t`, `i`, `crop` | via `inputs_hh_crop_sale_single_row` | **reuse** |
| `inputs_harvest_to_date`, `_HARVEST_WINDOW_RAW` | `Mali/_/mali.py` | registry `inputs` for the key, EHCVM waves | #1437 | **extend** with a 2017-18 branch; the EHCVM table is untouched |
| `_plot` | `Mali/2017-18/_/crop_production.py` | `"{field}_{parcel}"` | none directly | **moved** to `mali.eaci17_plot_ids`, same body, so the inputs callable and the build spell one plot id |
| `derivations.call_inputs`, `Country.derivation_inputs` | `lsms_library/` | the registry/access boundary | `tests/test_derivations.py` | **reuse, unchanged** |

## §3 Definitions & conventions in force
- A key names an OPERATION; an identity under a name implying another window fits, "the registry
  is the point" (CLAUDE.md, "Derived Values", fit rule).
- Stamp in the wave script AFTER any collapse (same section, registering step 3).
- Wave scope lives in the entry's `waves:`; the wave is not in the key (same section).
- `harvest-to-date`: `Mali/_/CONTENTS.org`, "the 2021-22 harvest total, and the window key".
- 7F.13 / `s7fq10` / `s7fq06`: `Mali/_/CONTENTS.org`, GH #1749 section, "Found on the way".

## §4 Invariants & assumptions
- `crop_production_finalize` collapses on the HARMONIZED crop and sums `Quantity`; two s7f lines can
  land on one served row. Measured in #1749: none do after the crop map. Not re-measured here.
- Tree rows and Farm-level sale rows sit on `plot_id = 'Farm-level'` and have no s7f line.
- `derive_*` names in a country module must be registered (drift test); the new helpers are not
  named `derive_*`.
- `.yml` siblings and `CONTENTS.org` are not build inputs; the wave script, `mali.py` and
  `data_scheme.yml` are (one Mali re-warm).
- ASSUMED, not verified source-free: `s7fq10` decodes to the labels `Oui` / `Non`, as the #1749
  red team reported it ("`Non` on 72 rows").

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the key and its function | reuse | ruling: "Reuse the existing ... key" |
| eligibility predicate | reuse (`Non` and served `Quantity` non-NA) | the EHCVM rule; "existing missingness rules" |
| key union | reuse `_with_key` | existing sorted `MULTI_SEP` union |
| association of `s7fq10` to served rows | new: `stamp_eaci17_harvest_to_date` | the shared finalizer drops unknown columns; the answer is matched afterwards on the collapse key and ambiguity is refused |
| 2017-18 inputs | extend `inputs_harvest_to_date` | same callable, same boundary |

## §6 Open questions for the human
- None blocking. Whether a refusal should fail the wave build or only withhold the key was decided
  here as "fail" (the brief: "report/refuse it rather than guessing"); say so if the softer form is
  wanted.

---
### Phase 3 — verification
- `stamp_eaci17_harvest_to_date` — OK (anchored on §2, §5): reuses the key, the predicate and
  `_with_key`; returns only `Derivation`.
- `eaci17_plot_ids` — OK (§2): the wave script's `_plot` body, moved; the script imports it.
- `inputs_harvest_to_date` 2017-18 branch — OK (§2): `_eaci17_keyed` + `_is_quantity_sentinel`.
- `_unfinished`, `_line_keys` — OK (§5): private helpers of the association; no existing
  equivalent found (`rg "eq\('Non'\)"` finds inline uses only).
- `crop_production_finalize`, `stamp_harvest_to_date`, `_HARVEST_WINDOW_RAW` — unchanged.
- No REINVENTION or CONTRADICTION found. Not verified: anything requiring the survey file.
