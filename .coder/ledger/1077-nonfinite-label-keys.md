# Prior-Art Ledger - #1077 nonfinite categorical keys

Search tier: ripgrep and git; no available GitNexus MCP. Baseline
`f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## Section 1. Task

Prevent nonfinite numeric keys in existing categorical tables from crashing
query-time label selection for ISA countries. Preserve the existing additive
integer-code spelling expansion and the original replacement dictionary.
This is a shared read-path fix, isolated from the country worktrees.

## Section 2. Existing machinery

| Machinery | Location | Existing coverage | Decision |
|---|---|---|---|
| Numeric spelling augmentation | `lsms_library/country.py:407` | `tests/test_u_code_leak.py` | Extend finite-value guard only |
| Categorical key resolver | `lsms_library/country.py:481` | `tests/test_categorical_key.py`, label-selection tests | Reuse unchanged |
| Query-time mappings | `lsms_library/country.py:3510` | `tests/test_label_selection.py` | Reuse actual mapping path |
| Food label contract | `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md` | Label selection and passthrough tests | Preserve |

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
- Do not change curated tables, missing-label errors, data, build functions,
  cache schema or fingerprints.
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

## Verification

Pending implementation, targeted tests, metadata census, public replay,
fingerprint comparison and independent review.

Sue
