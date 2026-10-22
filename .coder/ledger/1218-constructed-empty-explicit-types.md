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
bounded provider/copy plan; guard/census/test/genuine releases. Current full GH1218
body is requested from root because the local issue listing contains title only.

### Phase3 - verification

Pending implementation/runtime release. All source mechanisms above are static
observations and proposals; no runtime success or reproduction is claimed.
