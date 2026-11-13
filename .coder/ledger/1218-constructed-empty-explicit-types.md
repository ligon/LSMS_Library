# Prior-Art Ledger - GH1218 constructed-empty explicit types

**Search tier used:** ripgrep + Git + stdlib AST floor; no GitNexus mutation or runtime audit.
**Author:** Sue (GPT-6). Phase0 static diagnosis only, 2026-10-22.

## §1 Task, restated

Constructed zero-row Country tables must honor existing explicit country-local
payload type declarations while retaining the GH1215 original declaration order,
required/optional membership, and existing string index shape. Populated frames,
optional absence, science, row eligibility, canonical precedence, and country
extraction stay unchanged. The independent Burkina Faso observation is supplied
by root, not reproduced here. Root approved the original empty-only edit and
conditional guarded runtime. The actual candidate census exposed a required
fingerprint-serialization prerequisite, now explicitly authorized in scope.
The current five-path scope is country.py, its existing GH1039 tests,
_build_registry.py, existing test_build_transform_hash.py, and this ledger;
the original production authorization covered only the first two and this ledger.
At source2f, metadata controls and the full native fingerprint census completed,
but the candidate empty-frame selection has45 passes and12 calendar-type
failures. A new bounded synthetic diagnostic measured the empty-MultiIndex
alignment error below. Root authorized a minimum constructed-empty-only calendar
amendment; new-source runtime and all genuine data work remain held.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_required_scheme_columns`, `_optional_scheme_columns` | `lsms_library/country.py:1020`, `:1038` | authoritative payload membership | GH1039/optional guard controls | reuse |
| `_empty_declared_frame` | `lsms_library/country.py:1050` | constructed empty shape/order; source2f honors basic explicit types but leaves calendar payloads object | GH1039 candidate45PASS/12calendarFAIL | extend only this empty path |
| `_enforce_declared_dtypes` | `lsms_library/country.py:7104` | existing scalar/list coercion; dict declarations currently ignored | datetime/boolean/canonical controls | reuse through an empty-only declaration projection |
| `_SCHEME_DTYPE_MAP` | `lsms_library/country.py:6636` | nullable bool/int/float/string and calendar targets | dtype controls | reuse; no second map |
| `_enforce_canonical_dtypes` | `lsms_library/country.py:7164` | global canonical dtype wins later in finalize | existing canonical tests | unchanged |
| `_finalize_result` dtype calls | `lsms_library/country.py:3952` | declared then canonical coercion on every API read | existing API tests | unchanged |
| constructed-empty callers | `lsms_library/country.py:5028`, `:5404` | absent-subset and no-wave results | GH1039/1099/1204/1215 | cover both callers |
| interleaved GH1215 order control | `tests/test_gh1039_make_backend_no_country_rule.py:443` | order/index plus old object-payload expectation | source control, not run here | intentionally update explicit-type expectation |
| `_ser`, `_CONST_TYPES`, `_closure_parts` | `lsms_library/_build_registry.py:406`, `:443`, `:492` | deterministic constants and transitive build dependency discovery | `tests/test_build_transform_hash.py` unknown/pattern/closure controls | extend explicit dtype support without excluding the coercer or map |
| nullable dtype state | mounted pandas `core/arrays/string_.py:194-232`; `boolean.py:44`; `integer.py:249`; `floating.py:183` | Boolean/Int64/Float64 are fixed nullable families; String has storage and NA-versus-NaN state | proposed serializer controls, unexecuted | serialize actual supported state, not arbitrary repr |
| `_to_datetime_mixed` | source2f `country.py:6981-6999` | existing pandas mixed-format conversion and mixed-timezone warning wrapper | GH798; new tiny ISO control returns datetime64[us] | reuse for zero-row calendar initialization |
| `_coerce_datetime_column` | source2f `country.py:7002-7112` | populated calendar inference; datetime64 input passes through | existing GH798; new empty MultiIndex diagnostic raises at7065 | unchanged; do not run value inference on a constructed zero-row payload |

Older line pointers above refer to the initial framework baseline. The explicit
source2f ranges identify the measured calendar failure epoch.

## §3 Definitions & conventions in force

- Inherit `.coder/ledger/STANDING.md` §§3-4, `CLAUDE.md` (symlink to `AGENTS.md`),
  and `lsms_library/data_info.yml`; do not create global fake column definitions.
- Reuse `.coder/ledger/780-ghanalss-fingerprint.md` sections2-5: extend both
  deterministic serialization and standalone constant recognition; retain the
  else-reject rule and complete dependency closure. Existing pattern handling
  and all previously supported constant encodings remain unchanged.
- Reuse `.coder/ledger/1045-date-parser.md` sections2-5: the declared aliases
  datetime/date/timestamp already map to the existing calendar machinery;
  datetime64 input passes through without re-parsing or changing its resolution.
  Do not change calendar inference, reports, warnings, populated values or the
  build-time parser for a zero-row constructor correction.
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
- For a constructed zero-row payload only, recognize calendar targets through
  the existing map with safe string checks and initialize through the existing
  `_to_datetime_mixed` wrapper before the unchanged enforcer. There are no date
  values to infer; preserve the existing index and the converter's native empty
  resolution. Do not fix or bypass the populated calendar parser globally.
- Actual-parent discrimination must use identical supported test declarations;
  BF's original parent lacks the four newly declared fields and is not a direct
  four-field dtype control.
- Separate literal package identity from configuration identity for any planned
  BF composition; never label it exact-parent combined-package evidence.
- Preserve frozen1215/BF roots. No imports/tests/census/provider hashes/copies or
  raw decode in phase0. Future broad549/39/1362 fingerprints precede any rebuild.
- The actual nullable dtype objects in `_SCHEME_DTYPE_MAP` must remain build
  dependencies. Support only BooleanDtype, Int64Dtype, Float64Dtype and
  StringDtype explicitly; String storage and missing-value semantics are distinct
  inputs. Unknown dtype families, subclasses and arbitrary objects remain
  rejected. Do not rewrite the dtype map, broaden populated coercion, omit the
  coercer, use a name-only callable fallback, or stringify arbitrary objects.
- Candidate62105/50be60 is an original census failure, not an expected success.
  No complete candidate map JSON was saved; earlier local maps are unavailable.
  Completed base census/calibrations/controls must not be replayed. A failed
  candidate stage may resume only under new source/runtime authorization.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| payload membership/order | reuse | existing required/optional helpers plus GH1215 ordered source projection |
| nullable dtype enforcement | reuse | empty-only unwrap of explicit dict type can feed existing enforcer/map |
| canonical override | reuse unchanged | finalize already applies canonical last |
| fixture/API machinery | extend | existing GH1039 fake-country no-builder routes are meaningful old-framework controls |
| real BF check | proposed composition | use fixed authenticated BF657 declaration/scripts with both framework heads; no BF country-chain merge |
| nullable dtype build constants | extend `_ser` and `_CONST_TYPES` | first actual candidate census reaches the existing map through the new empty-only enforcer call and exposes unsupported dtype objects |
| hash tests | extend existing `test_build_transform_hash.py` | reuse its stable/different/nested/standalone/real-closure/hash-seed and unknown-rejection patterns |
| constructed-empty calendar target | reuse existing map and `_to_datetime_mixed` | initialize only declared zero-row calendar payloads; the unchanged enforcer then sees datetime64 and passes it through |

## §6 Open questions for the human

No new human policy question: literal explicit types and the narrow serializer
prerequisite are already authorized. Root selected exact GH1215 f7c as prerequisite
and BF657 as a deliberate supported configuration override. Root still reviews
new calendar-amended source pins and guarded tests before any further runtime; genuine work
has a separate release. The full GH1218 body was supplied by root and read locally (SHA256
6828890f5826a491901780cb0dd40a3ddba1c94472c4569aa4db1a2ed97ed9e2).

### Current verification and calendar amendment, before source edits

Source2f83bb2e909437f83c97629a6c949148897c1e07 is preserved as five exact
committed-source copies before this ledger amendment. Snapshot READY SHA256
7074ebcb711d32de6fd1428970a65b1db43ad50e729c949c418a5b865d060534
binds manifestd710a96bdd9d44e521ae32e266fef663bbf86ccc21dcc17af0d42da5ef3a2e7a.
Original physical receipts and distinct private copy receipts are recorded;
the copies are private0600, not claimed to share original physical metadata.

- Metadata serializer controls: actual f7 parent16 cases =13 meaningful
  failures/3 passes; candidate25 cases all pass, no errors/skips. The original
  host owner-receipt gate stop is preserved; its additive correction maps only
  UID/GID through the original boot's literal namespace maps.
- Complete saved f7 versus2f census: all549 country,39 framework and1362
  declared-wave hashes move; keys and ordered declarations stay equal. This is
  broad invalidation, not zero cost. No genuine cache rebuild was run. Exact
  source/config and unchanged Country AST outside the helper bind materialization
  policy; this is not a new runtime per-table policy-value census.
- Original parent57 actor12236/cfe183 exits1:39 dtype assertions fail,
  seven Make children stop at guard startup,11 pass. The seven original child
  assertion lines/PIDs/environments were not recorded and stay unknown.
- A new stdlib actual-Make toy measures inputMAKEFLAGS=-j1 becoming
  recipeMAKEFLAGS="s -j1", MAKELEVEL=1. Its later measurement does not recover
  the old unknown child history. An additive guard accepts only that measured
  serial spelling and records every new startup; old guards remain untouched.
- A separate continuation runs only the seven blocked parent cells:
  actual4 dtype failures/3 passes, no errors/skips. The initial5/2 prediction
  and failed semantic gate6800d3 remain preserved. The unexpected all-dvc pass
  uses the source-bound warm-cache row-filter path before empty construction.
  Recipe/test association is source/order/time/path evidence, not an invented
  original branch trace or PYTEST_CURRENT_TEST record. Seven pandas-only Make
  recipes need no library-import probe; only actual package importers do.
- Candidate57 actor62249/952ae8 exits1:45 pass and12 fail, no errors/skips.
  Every failure is datetime/date/timestamp times scalar/required/implicit-required/
  optional, with object output instead of the unchanged datetime64[s] oracle.
  All10 startups pass. The original swallowed inner exception was not captured;
  it is not reconstructed from the later diagnostic.
- One new synthetic diagnostic44550/ae8c19 exits0 and immediately persists
  each of four observations. Direct empty-MultiIndex parsing raises
  `ValueError: cannot join with no overlapping index names` at source2f
  country.py7065, `year_last = was_str & parts[0].notna()`. The existing enforcer
  catches that exception and leaves object storage; the constructor does too.
  A tiny populated ISO control returns2001-01-31T00:00:00 with datetime64[us]
  and no traced Country exception. This newly measured cause does not turn the
  original unrecorded inner traceback into saved historical evidence.

The diagnostic handoff SHA256 is
917506e2476e49f51369b2c8c1f2a29ec7e62107f2533de1d93641eb8b65eb59.
It binds the four immediate full observations, actual source/boot/Popen identities,
one source-bound denied import probe, zero data files and one harmless empty
dvc-cache directory. One verbose tool read was truncated; the original complete
observation files are authoritative. No whole-task zero-attempt/network or
kernel-trace claim is made.

Root now authorizes the minimum helper-only calendar initialization through the
existing map/wrapper, followed by the unchanged enforcer. Keep all12 test oracles
and all other tests byte-identical, including populated/index/unknown/list and
canonical-priority controls. Same five tracked paths; no shared parser change.
A genuine new source revision can justify all57 cases and a fresh candidate-only
census against savedf7 after review. No parent/base census or packaging-driven
replay. Supporting and all genuine provider/data work remain held.

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

### Approved fingerprint prerequisite amendment (source-only)

The original candidate7eb596c223055d45c4f45cffdba4f7eb6aee76e4 census,
tool session62105 terminal50be60, exited1 at build_transforms_fingerprint ->
_closure_parts -> _ser with `TypeError: build constant of un-serialisable type
'BooleanDtype'`. The new `_empty_declared_frame` calls the unchanged enforcer,
which references the existing nullable dtype map. This is a genuine unsupported
build constant, not evidence to remove that dependency or to weaken the census.

No fingerprints-private.json was written. Earlier country/wave locals were not
saved and are unavailable; their counts/values are not recovered or claimed.
The separate candidate failure metadata report is pinned SHA256
88d2e65f4ff4290705a36a79565e19d12805d389df39a453d5f27cd176894910.
It authenticates original PID/time/source and1459 guard rows, including1458
denied missing-reference attempts and one denied import probe. Endpoint absence
is current metadata, not invented historical absence or a zero-attempt claim.

Before this amendment, five literal7eb source files were copied into the private
source-only snapshot. Its READY is SHA256
621fc331611e0ae58358479a8f72354c3510d2fa96d3e449051b282114ae8a72;
manifest9900cecac2802f539d08ee8cce84b8a129d0eb4e116c42ff63716cf93bd5e2b3.
Original7eb remains in Git, and prior runtime sources/streams/results are intact.

Planned controls cover each map dtype, nested/standalone constants, String's
four storage/missing-value combinations, fresh-instance/hash-seed determinism,
real map mutation reaching the build closure, unsupported families/custom
subclasses and the existing arbitrary-object rejection. These controls and
the amended source are unexecuted; runtime/genuine work remains held.
