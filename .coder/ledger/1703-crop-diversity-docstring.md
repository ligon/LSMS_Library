# Prior-art ledger — #1703 crop-diversity Farm-level docstring

**Base:** 47df62d49e614c9a56e319def7324145d138c3d9. **Search:** ripgrep/git,
live GitHub issue and merged PR text, source inspection. Inherits [STANDING](STANDING.md).

## §1 Task

Refresh only the `crop_diversity` docstring. Use the issue's authorized
historical-labeling option, name the current producing countries verified in
source at the base, and flag the Ethiopia W1 coverage caveat (#1570). Do not
remeasure data or change the occurrence rule.

## §2 Existing machinery

| Anchor | Contract / receipt |
|---|---|
| `transformations.py:8733,8754` | `_without_redundant_farm_level` uses the sentinel and household/wave/crop/(country) key, not quantity or sale contents. `crop_diversity` excludes `unknown_plot` first and deduplicates by land/season for count. Value weights admit positive values without testing plot. |
| `data_info.yml:9–38`, merged #1644 / #1495 ledger | Household-grain record not allocated to an individual plot, may carry a harvest, sale, or both. Mali's off-grid EAC-I trees and household harvest records are established examples. |
| Merged #1567 / #1568, #1457, and `b90ec6e40` | Existing Nigeria/Mali example extended to Malawi, Mali 2017–18, Niger, Tanzania, Ethiopia. `f18c8e5e8` identifies the older Nigeria comparison as pre-#1433. |
| Current country producers | Scan of literal sentinel, constants, and construction calls finds Benin, Burkina Faso, Cote d'Ivoire, Ethiopia, GhanaLSS, Malawi, Mali, Niger, Nigeria, Tanzania and Togo. Later EHCVM routes confirmed in 2021–22 `_/crop_production.py` and CONTENTS: Benin and Cote d'Ivoire can hold harvest reconstructed from dispositions as well as sales; Togo's row is for sales; Burkina Faso uses an `s16d` household-level record both for unallocated sales and qualified harvest construction, separately from field `s16c` lines. |
| `GhanaLSS/_/ghanalss.py:1064,1113` | GLSS1/GLSS2 crops use household-grain Section 9B, so the builder sets every plot key to `Farm-level`. Their cited count remains historical. |
| #1570 / merged #1568 | Ethiopia W1 section 9 has documented coverage limits; certain section 11 sales and section 12 tree sales are the only served evidence of a crop. This is not a claim that the entire W1 table contains only sales. |
| `tests/test_gh1469_farm_level_occurrence.py`, `test_gh1460_unknown_plot.py`, `test_863_transforms.py`, `test_gh1475_country_crop_transforms.py` | Existing source-free controls for exact first-pass filter, retained farm-only keys, weight and multi-country behavior. |

## §3 Definitions

STANDING §§2–4 and the canonical schema govern. A structural household-level
row is different from an unrecorded plot. A sale-only record can be redundant
for counts even though value remains in its own column. A harvest-bearing row
can count when it stands alone, but the helper never branches on whether
harvest exists. A surviving null plot ID is not the structural sentinel.

## §4 Invariants

Only the named docstring may change in production. All executable AST and
all tokens outside that string remain exact. No country files, tests, data,
config, implementation, S3, or release edits. No global build fingerprint or
country/wave hash moves. Include GhanaLSS nutrition's imported framework
closure and table hashes in the metadata control.

## §5 Reuse

Use the existing transform tests and repository metadata census. Use merged
country rulings and sources for the wording. Do not introduce a new scientific
comparison or infer dates/counts beyond the documentary receipts.

## §6 Open scope

The older Nigeria and Mali examples are historically measured, not verified
at the present checkout. Ethiopia W1 source questions stay with #1570.
No source, calculation, or eligibility decision belongs in this issue.

## Verification against the ledger

- OK (§§2–4): replacing only the `crop_diversity` docstring in the complete
  production AST makes the trees identical. Source bytes on both sides of
  that exact string span are identical too. No other production file changed.
- OK (§5): 158 passed, 12 data-gated deselections. Scope was GH863 pure
  transforms, GH1469 occurrence, GH1460 unknown plot, GH1475 country-aware
  reducers, and the existing GhanaLSS framework-closure checks. Runtime
  used the pinned image interpreter, package realpath validation, one CPU,
  one build worker, and private/no-network configuration and state.
- OK (§4): code-only census before commit equals the exact-base census.
  Zero changes among 556 country hashes, 2,323 wave hashes, 42 build
  transform entries, and 38 framework-import entries, with zero errors.
  This includes the GhanaLSS framework imports and nutrition table hash.
  Reproduce with `slurm_logs/isa_backlog_2026-11-13/tools/hash_census.py`
  on both trees with isolated roots. No source bytes were read or obtained.
