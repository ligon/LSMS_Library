# Prior-Art Ledger - GH1218 constructed-empty explicit types

**Search tier used:** ripgrep + Git + stdlib AST floor; no GitNexus mutation or runtime audit.
**Author:** Sue (GPT-6). Phase0 static diagnosis only, 2026-10-22.

## §1 Task, restated

Constructed zero-row Country tables must honor existing explicit country-local
payload type declarations while retaining the GH1215 original declaration order,
required/optional membership, and existing string index shape. Populated frames,
optional absence, science, row eligibility, canonical precedence, and country
extraction stay unchanged. The independent Burkina Faso observation is supplied
by root, not reproduced here. No production edits or runtime are released.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_required_scheme_columns`, `_optional_scheme_columns` | `lsms_library/country.py:1020`, `:1038` | authoritative payload membership | GH1039/optional guard controls | reuse |
| `_empty_declared_frame` | `lsms_library/country.py:1050` | constructed empty shape, original declaration order, object payloads | `tests/test_gh1039_make_backend_no_country_rule.py:431` | extend only this empty path |
| `_enforce_declared_dtypes` | `lsms_library/country.py:7104` | existing scalar/list coercion; dict declarations currently ignored | datetime/boolean/canonical controls | reuse through an empty-only declaration projection |
| `_SCHEME_DTYPE_MAP` | `lsms_library/country.py:6636` | nullable bool/int/float/string and calendar targets | dtype controls | reuse; no second map |
| `_enforce_canonical_dtypes` | `lsms_library/country.py:7164` | global canonical dtype wins later in finalize | existing canonical tests | unchanged |
| `_finalize_result` dtype calls | `lsms_library/country.py:3952` | declared then canonical coercion on every API read | existing API tests | unchanged |
| constructed-empty callers | `lsms_library/country.py:5028`, `:5404` | absent-subset and no-wave results | GH1039/1099/1204/1215 | cover both callers |
| interleaved GH1215 order control | `tests/test_gh1039_make_backend_no_country_rule.py:443` | order/index plus old object-payload expectation | source control, not run here | intentionally update explicit-type expectation |

## §3 Definitions & conventions in force

- Inherit `.coder/ledger/STANDING.md` §§3-4, `CLAUDE.md` (symlink to `AGENTS.md`),
  and `lsms_library/data_info.yml`; do not create global fake column definitions.
- GH1215 OptionA: constructed-empty declaration order; no universal cold/warm
  complete-axis or dtype equality promise. Baseline is f7c33dd64d4b2d6e9a62838c33cc30d07863d706.
- Frozen BF source is 657fed11a63354886772d0f978cec8adb2ad647e; its complete
  `countries/Burkina_Faso/_/CONTENTS.org` was read including tasks and dated caveats.
- BF plot inputs are household x input-slot grain, no parcel axis; used, purchased,
  and gifted quantities/units are distinct. Code21 physical classification stays
  unresolved. Source quantities and unit normalization are outside GH1218.
- The BF notes explicitly say "Default retains" the four nullable types while
  Make leaves dict-declared fields "as object". Historical purchase bindings
  q05/q07a versus the later q08/q09a correction are dated evidence, not permission
  to change extraction again. No new undocumented country fact was discovered.

## §4 Invariants & assumptions

- Only constructed-empty payload coercion may change. Preserve declaration order,
  membership, row count zero, index names/order/level dtype/storage/codes, attrs,
  and inputs. Do not broaden nonempty dict coercion.
- Unknown/no-type declarations remain best effort; list declarations reuse the
  existing constrained-string reading. Required dict and optional dict explicit
  types need equivalent empty-only treatment; optionality does not imply type.
- Canonical types still override country declarations in `_finalize_result`.
- Actual-parent discrimination must use identical supported test declarations;
  BF's original parent lacks the four newly declared fields and is not a direct
  four-field dtype control.
- Separate literal package identity from configuration identity for any planned
  BF composition; never label it exact-parent combined-package evidence.
- Preserve frozen1215/BF roots. No imports/tests/census/provider hashes/copies or
  raw decode in phase0. Future broad549/39/1362 fingerprints precede any rebuild.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| payload membership/order | reuse | existing required/optional helpers plus GH1215 ordered source projection |
| nullable dtype enforcement | reuse | empty-only unwrap of explicit dict type can feed existing enforcer/map |
| canonical override | reuse unchanged | finalize already applies canonical last |
| fixture/API machinery | extend | existing GH1039 fake-country no-builder routes are meaningful old-framework controls |
| real BF check | proposed composition | use fixed authenticated BF657 declaration/scripts with both framework heads; no BF country-chain merge |

## §6 Open questions for the human

No new human policy question: literal explicit types are already authorized in
scope. Root decisions still required before production/runtime: final PR base
and GH1215 prerequisite; precise source-only config composition/private DVC tmp;
bounded provider/copy plan; guard/census/test/genuine releases. The full GH1218 body was supplied by root and read locally (SHA256
6828890f5826a491901780cb0dd40a3ddba1c94472c4569aa4db1a2ed97ed9e2).

### Phase3 - verification

Static diagnosis complete; implementation/runtime remain held for root review.

- `_empty_declared_frame` proposal - OK (sections2,4,5): replace its final
  expression return at1068 with the same index assignment, then payload-only
  type projection and the unchanged enforcer call, then return. Only dict values
  carrying a string `type` are unwrapped; unknown/missing/non-string types remain
  ignored, as the existing enforcer dictates. Required and optional dict forms
  get identical type treatment, different existing membership semantics.
- Index collision - OK (section4): payload projection iterates `columns` after
  excluding `levels`, so a numeric declaration on an index name cannot recast
  existing string index storage/codes/names/order. Existing structural keys and
  malformed scheme behavior stay unchanged; no new validation policy.
- Canonical precedence - OK (sections2-4): country-local coercion stays before
  global canonical coercion in finalize. Current global `household_roster.Age`
  is float (`data_info.yml:363-369`), notwithstanding older AGENTS Int64 prose.
  Test that actual canonical declaration, not a fake Columns registration.
- GH1215 combined controls - OK (sections2-4): intentionally replace its direct
  helper all-object assertion with exact six declared nullable payload types;
  keep every order/index assertion. Existing Uganda API types remain unchanged.
- Base - recommended GH1215 prerequisite/f7 foundation; root to select a final
  clean publication base and actual old-framework control without reset/force.
  Do not merge BF657 country ancestry into the framework PR.
- Genuine proposal - six manifest-bound provider metadata records total
  6,335,825 declared bytes; four new private roots require24 distinct binary
  copies /25,343,300 bytes. No provider was opened/hashed/copied/decoded here.
  Root should prepare a new BF657 detached config checkout/private DVC tmp;
  same literal config with old/new package heads is explicit supported
  composition, not exact historical BF-parent combined-package evidence.
- Future verification - corrected pre-import guards and meaningful old-framework
  dtype controls first; full549/39/1362 native-config hash measurement before
  rebuild. Broad movement is predicted because helper reaches ALL-scoped
  `_aggregate_wave_data`; actual counts and cost must gate genuine work.
- Prior guidance contradictions - old skill no-auto-hash/.pth/core-first claims
  conflict with current source/current AGENTS. Current canonical Age float also
  conflicts with AGENTS' older Int64 example. Preserve/source-cite these, do not
  redesign types or grain to reconcile historical prose.

No production edit, package/numerical import, build fingerprint census, test,
provider hash/copy/decode, API, network or acquisition was performed. Source/Git/
AST and frozen JSON/source metadata only. No runtime reproduction is claimed.

### Approved production source epoch (runtime still held)

Root accepted the frozen phase0 proposal and approved only country.py, the existing
GH1039 tests, and this ledger. Base is exact GH1215 f7c prerequisite, stacked until
human merge. Root prepared the exact f7 framework base and a NEW detached BF657
config checkout, each with private DVC tmp; frozen BF/1215 roots remain unchanged.

- Implemented the existing set_index assignment followed by a fresh payload-only
  projection and unchanged enforcer. Dict.type strings unwrap; missing/unknown/
  malformed type declarations stay ignored. Original input scheme is untouched.
- Intentionally replaced GH1215's direct-helper all-object assertion with the six
  existing declared nullable targets; kept original declaration order and index
  assertions. Added direct scalar/required/implicit-required/optional recognized
  types, unknown/missing/malformed types, bare lists, index collision, canonical
  priority, populated dict/absence, normal absent subset and separate declared
  wave/no-results controls. These are unexecuted regression sources, not passes.
- stdlib AST parsing and Git diff whitespace check pass. AST equality against f7
  confirms the shared dtype enforcer/canonical enforcer and both membership/index
  parsers are unchanged. No project/numerical imports, tests, fingerprint census,
  provider operations or genuine actors have run. Guard/calibration/test plan and
  exact source pins must be reviewed before any runtime release.

Frozen phase0 READY e48a938be5fd064fc79646680a81e99e8a107340dca738821d701efc157b637e
binds the pre-edit9aa source epoch. Original metadata postexit v1 stopped on an
incorrect optional-exclusion set assertion; its source/failure was preserved.
A separate external corrected metadata postexit completed0 before production
edits; the old frozen packet was never rewritten. No data/runtime replay.
