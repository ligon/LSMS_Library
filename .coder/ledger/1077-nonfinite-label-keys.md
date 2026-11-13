# Prior-Art Ledger - #1077 nonfinite categorical keys

Search tier: ripgrep and git; no available GitNexus MCP. Baseline
`f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## Section 1. Task

Prevent nonfinite numeric keys in existing categorical tables from crashing
query-time label selection for ISA countries. Preserve the helper's additive
integer-code spelling expansion and original dictionary. At the mapping
caller, omit rows whose resolved source key is actually missing, so missing
metadata cannot turn a null input into an unrelated code or label.
This is a shared helper fix, isolated from the country worktrees. The original
zero-cache-cost premise was contradicted by its Uganda build consumer (below).

## Section 2. Existing machinery

| Machinery | Location | Existing coverage | Decision |
|---|---|---|---|
| Numeric spelling augmentation | `lsms_library/country.py:407` | `tests/test_u_code_leak.py` | Extend finite-value guard only |
| Categorical key resolver | `lsms_library/country.py:481` | `tests/test_categorical_key_resolver.py`, label-selection tests | Reuse unchanged |
| Query-time mappings | `lsms_library/country.py:3449,3535` | `tests/test_label_selection.py`, `tests/test_gh1077_nonfinite_label_keys.py` | Filter actual missing resolved source keys; reuse mapping path |
| Food label contract | `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md` | Label selection and passthrough tests | Preserve |
| Uganda food build caller | `lsms_library/countries/Uganda/_/uganda.py:361,386`, `food_acquired` imports/calls `_augment_numeric_code_keys(harmonized_unit_labels())` | Private held-L1 before/after rebuild required | Preserve genuine build fingerprint dependency |

## Section 3. Definitions and conventions

The helper's docstring defines a purely additive operation: original keys win
collisions, integer-valued numeric keys gain integer/float-string spellings,
and nonnumeric labels remain untouched. See `STANDING.md` sections 3-4 for
canonical schema, sanctioned IO and pre-finalize caches. `country.py`'s
`_categorical_key_column` remains the sole key-column resolver.

## Section 4. Invariants

- Preserve every original key/value pair, including nonfinite keys.
- Do not manufacture numeric variants for NaN or positive/negative infinity.
- Preserve finite integer expansion, fractional-key behavior and collisions.
- The mapping caller filters actual missing source keys before dictionary
  construction. Preserve null targets, all nonmissing source keys including
  infinities and literal strings 'nan'/'inf', and sentinel protections.
  Actual null input must remain null in both column and index mappings.
- Do not change curated tables, missing-label errors, data, build routing,
  cache schema or fingerprint policy. The finite guard's genuine Uganda
  build dependency moves all 28 Uganda hashes; the other 521 of 549 hashes
  and all 39 tagged framework fingerprints must remain unchanged.
- Verify pure helper edge cases and actual existing categorical table/target
  cells; test a public Country request on a private warm input copy.
- Keep all raw data and detailed diagnostic logs private.

## Section 5. Reuse decision

Extend the existing helper with a finite check before integer conversion.
Reuse numpy already imported in the module, the categorical parser/key
resolver and the actual query-time mapping path. No new abstraction or
curation policy is needed.

## Section 6. Open questions

The original additive helper contract is unchanged. Root approved a bounded
caller fix after the adversarial probe: keeping a NaN source-key row allowed
pandas to turn missing input into an unrelated nonmissing target in 21/34
real column cells and 8/34 index cells (one measured process). Uganda u/Code
changed np.nan/None/pd.NA into 807089 via the missing 2005-06 dictionary key.
The caller must therefore drop only actual missing resolved source keys
before `set_index`, while preserving all nonmissing keys and null targets.
This is the issue's skip-NaN-key scope; it is not new label curation. The
intermediate probe remains private failure evidence. The caller fix is now
implemented and verified with zero fabricated null labels in both paths.

Measured correction to the root-authored initial premise: the helper is also
called while building Uganda food, so a guard edit changes its country-level
framework-import closure and every Uganda table hash. Root approved this
28-hash cost and a physically private held-L1-only rebuild, with the existing
offline guard; excluding the helper from fingerprints would hide actual build
code and is forbidden. The historical issue's 36 cells are not current scope:
34 current cells reproduce, and current metadata under the old positional rule
reproduces 35 with one additional Niger Original Label target. The historical
remaining count discrepancy stays qualified rather than invented.

## Verification

Implementation: `e7ea575fd7d8de1cb353badc20a7e4d1dceabf67`. Safe report and
aggregates: `slurm_logs/isa_backlog_2026-10-21/1077/REPORT.org` and
`validation.json`. Independent review remains the publication gate.

- **OK (sections 2-5):** repeat symbol/call-site search confirms reuse of the
  original numeric augmentation and mapping caller. Numpy was already
  imported. The key resolver, curation, schema and fingerprint policy are
  unchanged; no parallel resolver or replacement estimator was introduced.
- **OK (sections 3-4):** the helper preserves every supplied original key/value,
  including nonfinite keys, while skipping their numeric aliases. Finite,
  fractional, collision, literal-string and nonmissing infinity behavior is
  covered. The caller drops only actual missing resolved SOURCE keys and
  preserves null targets, declared-key precedence and kg/Value sentinels.
- **OK (section 4):** 230 targeted tests pass, zero fail/skip, with four
  loadfile workers. The final 119 new regressions give 118 failures and one
  already-correct Niger control on immutable f8. Isolating the original caller
  with the finite guard retained gives 52 failures and 67 passes, proving the
  caller filter is necessary beyond the exception fix.
- **OK (sections 2-4):** all 34 current real table/target paths succeed, and
  all 34 column plus 34 index null-input probes preserve nulls. The
  finite-only counterfactual's 21/8 fabricated outputs and Uganda metadata
  code 807089 example are retained as safe failure evidence. Nonmissing
  literal nan/inf controls and target-null rows remain distinct.
- **OK (sections 2-4):** held-L1-only offline cold Uganda builds preserve the
  complete 355,571-row raw country and each of eight raw wave frames,
  values/index/logical attrs exactly. Default/Preferred public results match
  exactly across revisions. Code selection succeeds without changing any
  payload, non-u key or attr, including a separate fresh process's default,
  Code, default-after-Code, Preferred and new-Country Code requests. Warm
  replays write no parquet. The pickle-byte oracle was corrected to logical
  attrs equality on the preserved first-cold snapshots, not by dropping attrs.
- **OK (sections 2, 4):** measured cache scope is exactly 28 Uganda table
  hashes changed, 521 other-country hashes exact and all 39 tagged framework
  fingerprints exact. The genuine `uganda.food_acquired` build import is
  preserved, never excluded. No country/source file was edited.
- **OK (sections 3-4):** scoped Feature before/after grids have 21 EXACT
  unchanged default/Preferred/Aggregate finding records. The Code request
  removes its load-failure warning and empty-assembly failure and now emits
  five existing quantity/null-key/cluster diagnostics. Candidate has zero
  fail/error/skip; all raw warnings/logs/snapshots remain private.

The initial zero-cache-cost premise was a **CONTRADICTION** to the existing
Uganda build caller and was corrected with root approval before rebuilding.
The finite-only proposal's missing-input relabeling was a **CONTRADICTION**
to source preservation and was fixed at dictionary construction with root
approval. No remaining runtime contradiction or reinvention was found against
this final ledger. The historical 36-cell total remains qualified; Niger's
intentional self-key exclusion and null-target curation are separate contracts,
not unimplemented #1077 exception fixes.

Sue
