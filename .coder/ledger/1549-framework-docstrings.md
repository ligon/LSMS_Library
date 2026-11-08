# Prior-Art Ledger — GH #1549 / dated framework docstring measurements

**Search tier used:** ripgrep + git + Python AST (floor). No GitNexus tool
is available in this dispatch; use the substitutes named in `CLAUDE.md`.
Inherits `.coder/ledger/STANDING.md`. Initial source is exactly
`b8346cf1f953ac82517b1b7473c715a5655ca950`.

## §1 Task, restated

Assignment A only: correct historical/current-state ambiguity in the module
docstring of `lsms_library/quantity_audit.py` and the `crop_diversity`
docstring in `lsms_library/transformations.py`. Preserve the scientific
examples and eligibility distinctions, executable code, configuration, tests,
registry and defaults. Do not replace dated measurements with new unsupported
counts. ROOT owns scientific decisions, metadata-census approval, source/data
verification, independent review and publication.

## §2 Existing machinery (this task's area)

| symbol / record | base path:line | relevance | existing checks / decision |
|---|---|---|---|
| `crop_diversity` | `lsms_library/transformations.py:8135`, passage at 8222 | occurrence key and count/value distinction; 2,303 Nigeria Farm-level rows is a GH #843-era example | `tests/test_863_transforms.py`; reuse behavior unchanged |
| Site Q dated corpus table | `lsms_library/quantity_audit.py:134-161` | explicitly cold commit `9a0ed5f3c`, 2026-10-23; 62,844 Nigeria rows is historical, as are firing counts and cost | ledgers `857-crop-quantity-screen.md`, `1259-site-q-positive-reference.md`; preserve benchmark |
| harvest-line record | `lsms_library/countries/Nigeria/_/CONTENTS.org:2702-2801` | #1433 dated 2026-11-04; retained labelled lines, `production_obs`, sale reattachment and consumer effects | commit `655948bb3f4f9c6ddb74b111194f2370f40c5dbe` plus record/review commits `223b197f`, `b23ef74e`, `8688f755`, `51255948`; no new source check |
| `_strip_docstrings`, `_norm` | `lsms_library/_build_registry.py:375-395` | callable AST normalization removes docstrings | `tests/test_build_transform_hash.py`; preserve implementation |
| `framework_imports_fingerprint` | `_build_registry.py:605-650` | script imports resolve callable closures through `_norm`, distinct from file hashes | inventory statically; actual enumeration requires ROOT grant |
| `Wave._input_hash`, `Country._table_cache_hash` | `lsms_library/country.py:1353,4506` | hash country/wave `_/*.py` bytes and callable fingerprints; library docstrings are not country/wave build-module files | no exclusion/schema change; actual cost pending census |
| `check_quantities` | `quantity_audit.py:1050`; caller `country.py:4447` | pure read-path report; excluded at `_build_registry.py:258` | no behavior/test change |
| API surface audit | `scripts/api_surface_audit.py` | stdlib AST/static inventory, no package import or Country constructor | run directly under prepared interpreter, never Make |

## §3 Definitions & conventions in force

- Per `STANDING.md` §3/§4 and `CLAUDE.md`: harmonize the interface without
  discarding native detail; do not add a derivation or change a unit here.
- Nigeria `CONTENTS.org:2704-2708`: "That key is not a harvest line" and
  "The index is now =(t, i, plot_id, crop, production_obs)=."
- Nigeria `CONTENTS.org:2484-2486`: "The rulings were made when only the first
  line of a =(plot, crop)= was served"; the section's counts retain that frame.
- Nigeria `CONTENTS.org:2767-2773`: "two lines of ONE plot state a harvest"
  and "the rule does not pick a line." This qualifies the transform's old
  "sale no plot can own" wording: no unique harvest-row attachment is the
  accurate description, without changing the consumer rule.
- `crop_diversity` preserves a Farm-level occurrence where no plot row of the
  crop exists and keeps all such rows for `weight='value'`; `unknown_plot`
  exclusion is a separate rule. Keep the Mali and GhanaLSS examples.
- Site Q is "counts and NAMES", never clips/rescales/drops. Its dated table
  and positive-only-reference rationale remain authoritative for that build.

## §4 Invariants & assumptions

- Full Nigeria `CONTENTS.org` (4,057 lines) read first, including WAITING at
  3505: pooled nonfood recall windows remain unresolved. No count campaign,
  recall/unit ruling, native eligibility change or prior accepted work replay.
- AGENTS.md and CLAUDE.md are byte-identical at base (SHA256
  `b5ea7b495528b61727e52df306fcd481ab8120b3f6d937c0c18c867aea47993a`).
- Only the two named docstring spans may differ in library source. Prove
  exact surrounding bytes and AST equality after removing ONLY those spans;
  all other library/config/test bytes must match base. No new mirrored tests.
- Before any verification: prepared PROJECT_PYTHON, exact cwd/PYTHONPATH and
  LSMS_COUNTRIES_ROOT, private data/config roots, BLAS/OMP/MKL/build workers 1,
  network namespace disabled. Initial checks use stdlib only; no library import,
  Country/Feature constructor, providers, source getters, Make or DVC.
- Static expectation: `_norm` discards the edited function docstring; the
  module docstring is outside callable source. This is a boundary inventory,
  NOT a measured no-invalidation claim. No fingerprint exclusions may change.
- Reuse external `hash-census.py` SHA256
  `dd7667b509464788fe8d06cb79a673c7ea6d36c8e16c5de592e2ce1bc7301e36`.
  Prepare an exact namespace/head/config-only wrapper after freezing product
  bytes and request ROOT review through the absolute dispatch channel. Do not
  run it or real Country constructors without that separate grant.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| historical row/firing/timing examples | reuse | qualify the recorded measurement instead of inserting an unmeasured count |
| scientific consumer rule and Site Q predicate | reuse unchanged | this assignment authorizes documentation only |
| behavior-preservation verification | static proof | byte/AST comparison is stronger and narrower than replaying data or the suite |
| actual cache impact | reuse existing census, pending grant | static normalization alone is not full hash enumeration |

## §6 Open questions for the human

- After code/docs freeze, ROOT must review the exact metadata-only wrapper
  before its Country(preload_panel_ids=False) enumeration. Until then actual
  hash impact remains unexecuted. No product wording decision blocks edits.

---
### Phase 3 — verification

- `quantity_audit` module docstring — **OK (§2/§3/§4)**: retain the entire
  dated table, rule, firing counts, timings and scientific examples; rename
  its heading to a historical benchmark and identify subsequent Nigeria work.
- `crop_diversity` docstring — **OK (§2/§3/§4)**: preserve every numerical
  example, count/value and Farm-level/unknown_plot distinction. Correct the
  attachment description and date the Nigeria counts/effects before #1433.
- **No REINVENTION or CONTRADICTION**: there is no new implementation. The
  verification removes ONLY the allowed module/function docstrings, not all
  docstrings, and obtains equal ASTs and equal remaining source bytes.
- `evidence/static-validation.py` passed once under the prepared Python,
  `unshare -rn`, explicit private roots and thread pins. It executes the
  existing pure source-credential gate (False), blocks library/provider
  imports and raw/harmonized opens, and permits no writes outside evidence.
  Package resolution and runtime identities pass without importing the package.
- Strict direct `scripts/api_surface_audit.py --strict` passes: ten kwargs
  documented, all fifteen hand-authored skills listed, zero untracked hashed
  inputs. Five existing unused public helpers are informational and untouched.
- Exact proof and complete logs are in the absolute dispatch evidence directory
  named by this assignment (`static-proof.json`, `validation.log`, `api-audit.log`).
  No data files were produced and no guard denials occurred.
- Full CI and data checks remain outside this assignment. Actual cache-hash
  enumeration remains a separate ROOT-approved stage; no numerical cache-cost
  or no-invalidation claim follows from these static checks.
