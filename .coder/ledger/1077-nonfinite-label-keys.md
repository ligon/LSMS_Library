# Prior-Art Ledger - #1077 nonfinite categorical keys

Search tier: ripgrep and git; no available GitNexus MCP. Baseline
`f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## Section 1. Task

Prevent nonfinite numeric keys in existing categorical tables from crashing
query-time label selection for ISA countries. Preserve the existing additive
integer-code spelling expansion and the original replacement dictionary.
This is a shared helper fix, isolated from the country worktrees. The original
zero-cache-cost premise was contradicted by its Uganda build consumer (below).

## Section 2. Existing machinery

| Machinery | Location | Existing coverage | Decision |
|---|---|---|---|
| Numeric spelling augmentation | `lsms_library/country.py:407` | `tests/test_u_code_leak.py` | Extend finite-value guard only |
| Categorical key resolver | `lsms_library/country.py:481` | `tests/test_categorical_key_resolver.py`, label-selection tests | Reuse unchanged |
| Query-time mappings | `lsms_library/country.py:3510` | `tests/test_label_selection.py` | Reuse actual mapping path |
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

None for this bounded guard. A separate issue would be needed to change how
original nonfinite replacement keys themselves are interpreted by pandas.

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

Pending implementation, targeted tests, metadata census, public replay,
fingerprint comparison and independent review.

Sue
