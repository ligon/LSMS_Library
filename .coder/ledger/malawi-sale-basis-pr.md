# Prior-Art Ledger - Malawi sale units and basis PR

**Search tier:** git and ripgrep. No GitNexus or memory MCP was available.
Inherits [STANDING.md](STANDING.md) and
[854-malawi-shipped-factors.md](854-malawi-shipped-factors.md).

## §1 Task, restated

Prepare the existing Malawi unit and sale-basis work as one PR against
GitHub `development` at `3cf124ca3`. Carry sales' own units and condition,
separate raw and boiled vendor groundnuts, and disclose each inferred
sale attachment through the existing derivation registry. Preserve the
agreed price thresholds and the harvest rows. Include the two native-unit
aliases already committed on the shared mirror. Re-review the combined
stack, repair demonstrated regressions, and verify from cold sources.

## §2 Existing machinery

| Symbol or record | Source | Reuse and verification |
|---|---|---|
| `_crop_conditions`, `_sale_block` | `lsms_library/countries/Malawi/_/malawi.py` | Reuse the harvest vocabulary when reading the sale's own answer; `tests/test_malawi_shipped_factors.py`. |
| `_attach_sales`, `derive_sale_basis` | same country module | Review the existing staged attachment rule; `tests/test_malawi_sale_basis.py`. |
| `_su_reference`, `_lookup_reference` | same country module | Reuse within-wave reference construction and lookup; repair only the all-null-unit grouping regression. |
| `sale_basis_decisions`, `inputs_sale_basis_*` | same country module | Reuse the input-grain replay and raw-source accessors; delivered tests check keys, amounts and joins. |
| Derivation registry, summaries and input API | `lsms_library/derivations.py`, `docs/guide/derivations.md` | Reuse; `tests/test_derivations.py`. |
| Country and Feature post-read processing | `country.py`, `feature.py` | Reuse; schema/index tests and scoped Feature audit. |
| `harmonize_food`, `u` mappings | `Malawi/_/categorical_mapping.org` | Reuse declared mapping tables for raw/vendor split and `Kilogram`/`Kkilogram` aliases. The country-level aliases also affect `plot_inputs`. |

## §3 Definitions and conventions in force

- The API harmonizes the interface while preserving survey detail:
  `CLAUDE.md`, Design Philosophy, and `STANDING.md` §§3-4.
- `condition` is Malawi's shelled/unshelled/not-applicable answer, with
  `unknown_condition` for missing or unasked questions:
  `Malawi/_/CONTENTS.org`, "the SALE has its OWN condition too".
- A key identifies an operation; each alternative branch has its own key;
  a deletion has `rows: 0` and recoverable inputs:
  `CLAUDE.md`, Derived Values, and `docs/guide/derivations.md`.
- The sale's quantity and money remain survey-reported. The derived
  operation is its attachment to a harvest row; the row's recorded
  harvest condition remains unchanged: `Malawi/_/derivations.yml`.

## §4 Invariants and assumptions

- Module P never asks harvest S/U; Module Q does not ask sale S/U before
  2016-17. These unknowns reflect the questionnaire, not omitted wiring.
- Reference prices use agreeing sales from the same wave. Units are
  retained for fine references and centered before pooling at crop level.
- The agreed tolerances remain 10 references per basis, separation
  probability outside (0.25, 0.75), and margin greater than half the gap.
  The outer quarters between medians qualify; the middle half does not.
- Exact/wildcard attachments take priority. Later branches attach only
  where both the sale and the row are unclaimed and the pairing is unique.
- Missing native units must survive existing attachment semantics; an
  all-null unit group must not fail a dtype-changing join.
- The 2026-09-25 rulings require separate per-row keys for price overruling
  a recorded sale answer and for absent reference cells. They are already
  implemented in the stack; no new policy decision is needed.
- Validation on agreeing cases does not establish accuracy on the selected
  group whose sale answer is overruled. The registry must distinguish them.
- The existing variety collapse and its discarded sales are a separate,
  recorded defect. This PR does not change the declared crop grain.

## §5 Reuse decision

| Work | Decision | Reason |
|---|---|---|
| Sale-basis rules and derivation disclosure | Reuse/review | Existing implementation follows the recorded user rulings. |
| All-null-unit failure | Extend regression coverage and repair grouping | A groupwise median transform avoids the join dtype failure without changing the statistic or excluding rows. |
| Kg aliases and raw/vendor labels | Reuse | Existing country mappings express both changes. |
| Validation | Reuse tests and audit harness | Private cold baseline and candidate roots isolate source changes from cached output. |

## §6 Open questions for the human

None blocks this PR. The recorded unresolved-sale and variety-collapse
limitations remain disclosed rather than silently repaired with a new rule.

### Phase 3 - verification

PASS against code commit `2515d1668` and GitHub base `3cf124ca3`.
The review found and repaired the all-null-unit join regression and two stale
or unsupported documentation claims. The final checks passed 517 tests with
no skips, 675 independent attachment cases, and exact reference-statistic
equality on ten seeded 5,000-row samples (seed 20260925).

Both revisions cold-built all 26 Malawi API entries. Harvest rows and fields
are unchanged; raw food quantities and expenditures are unchanged after the
intended label mappings. Changes in 35 inferred food quantities and 31 prices
are exactly explained by the groundnut split and kilogram alias. All other
tables agree, except the intended 93,162 plot-input unit relabelings. The scoped
Feature audit adds no findings. All 32 derivation-input calls pass their
join/deletion checks. There is no new estimator or departure from the stated
thresholds.

Cache scope is 21 materialized source tables (24 scheme keys); control hashes
and framework fingerprints do not change. Full results and reproduction
commands are in `slurm_logs/malawi_units_pr_2026-09-25/VALIDATION.org`.

Reviewed and verified by Sue (GPT-6), 2026-09-25.
