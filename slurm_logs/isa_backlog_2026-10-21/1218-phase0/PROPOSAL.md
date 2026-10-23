# GH1218 phase0 proposal - existing explicit types on constructed empties

Sue, 2026-10-22. STATIC ONLY. No project/numerical imports, tests, build-hash census,
provider opens/hashes/copies, payload decode, API, network, or production edit.
The sole source delta so far is the prior-art ledger, first committed/published
as d60275ada. Root supplied the full current issue locally after that commit.

## Decision and source mechanism

Extend only `lsms_library/country.py:_empty_declared_frame` (1050). Its two
production call sites are `_aggregate_wave_data.absent_subset_frame` (5028) and
`_aggregate_wave_data.load_from_waves` (5404). Membership remains the union of
`_required_scheme_columns` (1020) and `_optional_scheme_columns` (1038), ordered
by the original entry, as GH1215 requires. Construct the existing string index
first, then feed the existing `_enforce_declared_dtypes` (7104) a payload-only
projection. A dict carrying a string `type` becomes that string in this local
projection; other declarations retain their existing representation. Do not
mutate the supplied scheme. The existing enforcer/map owns nullable bool/int/
float/string/list/calendar handling. No new dtype map or nonempty dict coercion.
A dict without type, an unknown type, or a malformed non-string type remains
best effort and object, rather than being guessed. In particular a malformed
dict.type list must not accidentally acquire the valid bare-list semantics.

Finalize's existing order stays declared dtypes (3954), canonical dtypes (3960).
Canonical override therefore still wins; no canonical schema edits are needed.
Indices are excluded from the projection: even an index dict saying type:int
must not change the existing empty string index storage, names, codes or order.
Populated results never enter this constructor; the shared enforcer is unchanged.
Scalar and bare-list declarations also use the existing enforcer in the empty
constructor, instead of waiting for finalize. Their public API coercion already
exists. This is an intentional change to the helper's direct intermediate dtype
contract and must be stated in its docstring and tests.

## Clean base and minimal paths

Recommended PR prerequisite is GH1215. Implement on the present f7c33dd64
foundation for a reviewable diff, then root should publish against a development
base that already contains GH1215 (or explicitly stack on that exact prerequisite
until merged). Preserve this branch and ledger; no reset/force/cherry-pick is
proposed now. Root will choose/prepare the final base and actual control before
production. Do not merge BF657's entire country/framework chain into this PR.

Proposed changed paths: `lsms_library/country.py`, the existing
`tests/test_gh1039_make_backend_no_country_rule.py`, and this task's ledger.
The helper docstring is the needed documentation. No country config, global
Columns, transformation, row eligibility, or optional membership changes.
Keep GH1215's Uganda fixture module unchanged unless a concrete test interaction
requires a separately scoped amendment. Its current nullable API expectations
already agree with the intended explicit types.

The interleaved GH1215 direct-helper test at GH1039 test line443 currently says
all six payload dtypes are object. Replace that assertion intentionally with
explicit nullable types for Unit/Quantity/Flag/Count/Factor/Label; keep its exact
original declaration-order and string-index checks. The old assertion is valid
history for GH1215, not a rule to silently preserve in the combined suite.

## Meaningful synthetic controls before genuine data

Use the existing GH1039 fake-country no-builder fixtures, private countries/data
roots and literal package identity. Run identical new test bytes against the
actual old framework f7c33dd64 and the new candidate, before/after the product
edit as separately logged controls. Tests must import the selected Country
implementation and cannot patch the helper under test. Save actual failures,
JUnit and full logs; a missing four-field schema in BF's old3dc parent is not
four-field regression discrimination.

- Direct empty helper, each existing recognized map spelling (bool,int,float,
  str,string,datetime,date,timestamp), for scalar, optional dict and required
  dict forms; assert exact dtype class/storage/NA policy and zero length. Use
  pandas dtype oracle objects, never only str(dtype). Bare-list declarations
  retain constrained StringDtype semantics. Cover empty and populated label lists.
- Unknown scalar/dict types, dict without type, None, malformed non-string type,
  ordinary non-column metadata and non-string keys retain membership/type rules;
  no crashes, new columns, guessed defaults or dtype narrowing. Dict.type list
  is a malformed-type negative control distinct from a supported bare list.
- No index, index-only, MultiIndex, interleaved payload/index declarations, and
  an explicitly numeric index spec: exact old index names/order/levels/codes,
  code dtypes, storage/NA value and empty shape; no required/optional reshuffle.
- Both actual production routes: cold unsupported subset and the nested
  no-builder/no-results return; Default and Make, before any supported request
  and after supported/all coverage. Preserve full-request failure versus proper
  unsupported-subset empty behavior, missing-Make executable and genuine-builder
  failures. No-country-rule detection must not be monkeypatched into the result.
- Canonical override through normal `_finalize_result` using an existing canonical
  name with conflicting country type; canonical wins exactly as before. No fake
  global Columns entry. Unknown country-local fields stay local.
- Explicit typed optional payload absence on a populated frame remains allowed:
  neither constructor nor untouched enforcer synthesizes it. Populated dict,
  scalar/list, flags and values/masks/attrs/index are compared to actual old code;
  no broad nonempty dict policy is introduced. Assert supplied scheme unchanged.
- Retain the entire GH1039 module plus existing GH1099 and GH1204/1215 bounded
  no-builder/coverage selection; add the relevant dtype/date/schema tests after
  static collection review, not a broad data-gated suite. Maximum two loadfile
  workers total; LSMS_BUILD_WORKERS=1 and BLAS/OMP/MKL threads=1. No skips accepted
  as positive coverage without stating the exact unavailable route.

The first directly typed dict controls should fail old f7 solely on exact dtype/
storage (object versus existing nullable targets), with axes/order/membership
passing. Required-dict and optional-dict controls distinguish type from membership.
A test that only checks empty rows or equality with another object frame is weak.

## Source/config composition for the genuine grid

Ask root to prepare a NEW read-only detached BF657 config checkout (or equivalently
an exact manifest-bound source-only snapshot) with its OWN private `.dvc/tmp`.
Never repoint the frozen author's link. For both package heads set
`LSMS_COUNTRIES_ROOT` to this same literal BF config root, and separately set
`PYTHONPATH`/cwd to the selected framework checkout. This is a deliberate supported
config-override composition, not the exact historical BF parent combined package.
Both package roots and config root, Country class/source, module __files__, every
worker and Make child identity must be literal and independently asserted. BF
scripts/formatters come from the fixed config root; global canonical metadata and
framework code come from the selected package. All four new fields are present
on both sides. Root must bind exact prepared paths before runtime release.

`paths.countries_root` (38), `Country.file_path/resources/formatting_functions`
(2451 onward), and config.countries_dir implement this supported separation.
The redirecting writer must see each script beneath that same countries_root.
No in-tree parquet/raw materialization is allowed. Pooled Feature work is scoped
to Burkina_Faso only, with its existing attrs carriers preserved.

## Predata hash and safety gates

After the final source/base choice and guard calibration, measure the entire
549 Country hashes /39 build fingerprints /1362 declared-wave hashes on both
heads using normal native configurations, metadata only. `_aggregate_wave_data`
is ALL-scoped @build_transform (4979) and calls the helper; predict broad
549/39/1362 movement, not zero. Record actual key sets, missing/None and exact
counts; stop unexpected scope before data. Do not run a corpus rebuild. Hash
changes are genuine content-invalidation costs even though populated values
should not change. Separately record fixed-BF-config hashes for the real actors.

Reuse the corrected pre-import filesystem/network guard and calibration; pin the
controller and actor identities. Guard every subprocess via inherited PYTHONPATH/
sitecustomize with exact package/config allowlists. Network namespace isolates
transport; Python audit denies real acquisition, external/DVC/Slurm commands and
all writes outside own runtime roots. Resolve dir_fd endpoints accurately. Bind
seeded event attribution to source/controller PID/terminal evidence, not event
class. Log all attempts without assuming zero; classify any known import/no-op
behavior with exact physical unchanged controls, never a blanket zero-network
claim. Root-owned metadata exceptions must be explicit and narrow. Full source,
provider, copy, cache and operand physical controls precede imports/reads as
appropriate. Freeze checkers and preserve every stopped version and log.

## Exact bounded genuine plan (not released)

Metadata-only authority: root-authenticated BF author READY aa9fb425..., full967
manifest5c01b158..., and manifest-bound providers-authenticated-before-copy.json.
Current contents of that metadata file were gated, not any raw provider bytes.
The six existing logical sources are one2018s16b and five all-wave sample
sources: 2014welfare,2018s00+ponderations,2021s00+panelponderations. Country's
`_join_v_from_sample` calls unfiltered `sample()`, so one s16b blob is insufficient
for an ordinary Country/Feature result. Total declared bytes per root6,335,825.

Four fresh L1-only data roots: old-default,old-make,new-default,new-make.
Six genuine sources ×four roots =24 nonsymlink/singlelink distinct0600 copies,
25,343,300 bytes total. No extra quarantine, staged raw oracle copy, get_data_file
materialization, fifth root, old harmonized cache, or source payload overlay.
Immediately before copy: reauthenticate each original binary MD5+SHA256+Stata
magic+fullphysical receipts against exact current sidecar bytes and SOURCE;
then verify all24 complete copied bytes/MD5/SHA/magic/physical distinctness before
any decode. One read-only `_dvc_cache_path` layout preflight per root may inspect
resolution only. Guard unexpected misses; never let a fallback fetch or rewiring
repair an absent provider. Preserve historical authority/current receipt split.

Eight normal actors: each role cold, then a separate fresh-process warm read.
Per actor normal Country calls: supported2018-19,all,unsupported2014,unsupported2021.
Each warm actor also BF-only Feature supported/all/two unsupported requests,
plus seed_kg(all_frame) and nitrogen_kg(all_frame). Retain full native2018 wave,
actual persisted country data, API/Feature and derived operands, footers and byte
receipts at named producer stages; no sample of rows. There are56 normal returns
(4cold×4 +4warm×10), not a claim of56 independent oracle groups. No known label
error case is added merely to inflate coverage. Optional two unchanged scoped
Feature scanner actors may reuse the already complete Default roots; never warm
unrelated cells. Exact selection/controller pins require root review first.

Expected decisive four empty comparisons on NEW framework: Default vsMake for
2014 and2021, cold and freshwarm, full strict dtype/storage/NA/axes/typedattrs.
Run corresponding four on OLD framework and preserve actual failures. Also
candidate-versus-old matched route controls: all unchanged populated/native/
Feature/seed/nitrogen operands full strict; old-Make empty type differences are
intentional repaired dtype deltas, never full equality. Complete original unused
MultiIndex domains/codes/code-array widths/order/names/EA storage remain operands,
not normalized away. Exact typed attrs include dict/immutable-record class and
_FIELDS presence. Correct GH1206 strict comparator (typed-object/EA aware) needs
positive and seeded fault calibration for nullable and empty frames before use.
No casting, trimming levels, attrs transplant or numeric-tolerance weakening.

Producer-byte checks remain separate from full frames: native/footer hashes can
differ because framework fingerprints changed even with equal data. Compare
producer schemas excluding only individually declared expected hash-footers in a
separate diagnostic, while retaining complete footer bytes and strict outcomes.
Record every raw L1 footprint, all newly created sample/wave/country paths, exact
coverage manifest, and warm unchanged receipts. The real four empty cases occur
after a normal supported build; synthetic controls separately cover truly barren
cold construction, without promising all-cache-zero on sample-dependent calls.

## Existing evidence, qualifications, and next release

Root, not this static task, supplies the independent BF observation:151 matrix
111PASS (40writer-stage+71other),40prewriterFAIL; new same-parent reference matrix
2DefaultPASS/2MakeFAIL solely four optional dtype/storage differences, with axes/
typedattrs passing. Preserve original author historical states/failures. The
original BF3dc parent lacked new fields. No inference that code21 is physically
Seed or that this repair changes quantities/units/yields is authorized.

Documentation specifics: BF CONTENTS1382 records household x input grain and
1384 the1,329/4,509 formatter difference;1400-1412 leave code21 unresolved;
1438-1448 old purchase labels contradict later1492-1493 current q08/q09a
correction;1509 explicitly records Default nullable versus Make object empty
fields. These are chronological source distinctions, not permission to rewrite
country semantics. All1519 lines, including TODO/caveats, were read.

Guidance contradiction retained: backlog-workflow and add-feature verification
prose still says framework/config edits are not auto-hashed and speaks of the old
.pth pin; current AGENTS cache description and live `_build_registry` say otherwise.
Cross-country-features still describes core `.first()` where current AGENTS pins
most_complete_row. Use the current source and explicit import/fingerprint gates,
not those older simplifications. Static skill inventory replaces make api-audit
in this phase because execution is held; runtime audit can run after release.

Retained exploratory stops: default `python` missing (no command executed beyond
shell lookup; python3-S used afterward); a nonexistent optional-test filename;
overlong doc/locator outputs truncated and were repaired by bounded reads. One
over-broad filename-only locator enumerated unrelated payload paths without
opening/hashing/copying/decoding them. No numerical/product success is claimed.

Next: root approves clean base, exact minimal paths, composed config preparation
and bounded runtime plan. Only then implementation plus calibrated guards,
predata complete fingerprint measurement, meaningful old/new synthetic controls,
and separately released genuine grid. Independent review remains required.

## Exact proposed insertion and canonical/index examples

At current country.py1068, retain the same `set_index` decision as an assignment.
Immediately afterward (before returning), form a fresh projection over `columns`:
for each original value, unwrap `value.get("type")` only if it is a dict and that
value is a string, otherwise retain the original value. Pass that projection and
the constructed frame to `_enforce_declared_dtypes`, then return. Index names are
absent from `columns`; no value on an index declaration reaches the enforcer.
No edits to `_enforce_declared_dtypes` or `_SCHEME_DTYPE_MAP` are proposed.

Current canonical `household_roster.Age` is float at data_info.yml363-369; test a
conflicting country bool/int declaration against Float64 through finalize. Older
AGENTS prose using Int64 for Age is a source-proven documentation contradiction,
not today's canonical oracle. A no-type dict remains object: diagnostic/schema
readers default such a dict to str, but GH1218 only promises existing EXPLICIT
types and must not introduce that implicit runtime policy. A bare list remains
a valid constrained-string declaration; a list nested inside dict.type is not
a string type and stays ignored.
