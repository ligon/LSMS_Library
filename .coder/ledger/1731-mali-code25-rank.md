# Prior-art ledger — #1731, Mali 2021-22 coping option 25

Inherits [STANDING.md](STANDING.md) §0. Search tier was ripgrep and git,
with the live issue and reviewed 2026-11-13 ruling. Base 47df62d49.

## §1 Task

Implement the Mali-only share of the “Keep slot, registered” ruling on
2021-22 shocks. No change to which coping choices/positions are served on
the held release, and no change to the other country hooks in #1731.

## §2 Existing machinery

| Symbol | Location | Reuse |
|---|---|---|
| `derive_coping_autre_rank`, `coping_autre_rank_key` | `lsms_library/ehcvm.py` | Compute and identify the least free rank under the ruling. |
| `inputs_coping_autre_rank`, `COPING_RANK_LABELS` | `lsms_library/ehcvm.py` | Read original operands through the wave's formatter. |
| `inputs_shocks_code25_rank` | Benin and Niger 2021-22 `_/mapping.py` | Wave-local registry wrapper pattern, without changing shared code. |
| `Country.derivation_inputs`, `normalise_coping` | `lsms_library/country.py`, `lsms_library/coping.py` | Existing panel re-key and read-time set semantics. |

## §3 Definitions in force

- `Mali/_/CONTENTS.org`, GH #1562 section and November 4 coping notes:
  W1 grid establishes the set only; W2 code 25 has Oui/Non where the other
  options have ranks. All 72 code-25 answers were lone in the audited release.
- `isa-decisions.org`, 2026-11-12 and 2026-11-13, positions use reported
  order and a genuinely inferred option-25 position is registered.
- `lsms_library/data_info.yml`, shocks index is `(t,i,Shock)` and
  `Derivation` is optional. Purchases and food stocks stay separate (#1556).

## §4 Invariants

- Keep all physical positions, even gaps. The synthetic fourth-choice
  regression in `test_gh1556_ehcvm_shocks_2021.py` drops the overflow without
  throwing. It changes no served slot and therefore has no stamp or trace.
- Lone Other remains first and unkeyed. Only a served inferred position gets
  the key. Any existing no-stamp release needs no new all-null table column.
- Raw trace matches stamped rows at served grain with original columns and
  preserves the existing ID walk. No sample or panel rebuild.
- Keep the diff wave-local and append the registry entry away from #1772's
  partial-harvest work. Do not change Mali CONTENTS, the common runtime,
  non-Mali waves, or the other #1731 countries.

## §5 Reuse decision

Use the shared ranking helper and raw mixed-row selector. Keep the old Mali
physical-slot array instead of copying the compacting sort from other waves.
Exclude a hypothetical fourth choice from the local registry trace to agree
with the established output behavior. No new representation or policy.

## §6 Questions

None for this patch. The remaining #1731 country hooks are out of scope.

### Phase 3 — verification

- OK (§§2-5): 78 source-free tests passed, with 39 data-marked tests
  deselected, in the GH #1562 and GH #1556 files. Positive controls cover
  all eight occupied-rank subsets, actual trace resolution, one walked ID,
  no-cross-wave reads, exact source-column names, and distinct no-stamp cases.
- OK (§4): static census changed 24 of 24 Mali country-table fingerprints
  and 24 of 96 wave-table fingerprints, exclusively W2; one W2 import
  closure, zero shared build-transform fingerprints. No sampled Benin or
  Niger country, wave, helper-import or build-transform fingerprints changed.
- The independent W2 gate is limited to `s14b_me_mli2021.dta` at
  `5cd57bd816d551b914feb45a69afdae4`, 6,928,409 bytes, verified private
  copies under a hard-offline single-core runtime. The released metadata
  confirms the code-25-only Oui/Non vocabulary. The native response census
  finds 72 lone choices, 0 mixed, 0 fourth, 0 Oui elsewhere.
- OK (§§3-5): all 4,884 published W2 rows, nine columns, index order and
  dtypes, values and non-task attrs match base for cold and warm Country
  and Feature, in default and Make. No duplicate served keys; Country
  keeps (i,t,Shock) and Feature keeps (country,t,i,Shock). The only
  metadata delta is the expected 0-row code-25 registry summary; the
  trace is empty at (t,i,Shock) with all 26 named native operands.
  No raw IDs or free text were emitted. All-wave verification is separate.
