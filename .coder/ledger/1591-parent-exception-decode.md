# GH1591 — decode wave-worker exceptions in the parent relay

Search tier: git + ripgrep + AST floor; no gitnexus/cq tools exposed. Base is
current authoritative b8346cf1f953ac82517b1b7473c715a5655ca950. Inherits
STANDING.md §§3–4, CLAUDE.md cache/fork/import discipline and data_info.yml;
no estimator, canonical schema, country source or scientific definition changes.
Fresh issue1591 is OPEN with0comments; synthetic report, no live instance found.
The independent1578/fullCI actors and all their old failures remain frozen.

## §1 Task, restated

_parallel_waves._pack_exception currently loads its pickle in the child, then
returns the original object. That cannot prove the pool result thread can load
it in the parent. A class installed/imported only in a fork child can poison
result decoding, become BrokenProcessPool and lose its real class/traceback.
Carry bytes in the existing error tuple and decode in parent relay instead.
Root approved this narrow direction and parent BaseException validation.

## §2 Existing machinery and reuse

- _parallel_waves.py:447 _pack_exception formats original type/text/traceback,
  catches serialization failure and returns four error fields. Extend only its
  first field to serialized bytes orNone; eliminate the child loads probe.
- :569 relay emits warnings, files grain/null/datetime/drop ledgers BEFORE
  raising; reuse same order, same context note and WaveWorkerError fallback.
  Decode first field here and validate BaseException; a custom reduce yielding
  another BaseException remains supported, while a nonexception falls back.
- :598 _is_fork_unsafe reads existing text/traceback only. :611 prebuild groups
  shared targets, joins all workers in input order, converts actual dead-worker
  BrokenProcessPool into explicit dead-worker payloads, and performs the existing
  parent fork-unsafe capture. Preserve all these functions byte/AST exact.
- country.py:5835/:5849 outer (has_table,result) protocol remains opaque;
  GH1292 successful-empty and GH1590 ordered parent fallback/proven completion
  machinery is reused unchanged. No dataframe/fallback/cache producer edits.
- errors.WaveWorkerError:106 is Exception, deliberately not RuntimeError or any
  generic build-absence exception. Preserve class/signature/attributes; update
  only its serialization description. Actual failures never become absence.
- tests/test_parallel_waves.py covers CPU/memory/serial/forkunsafe, actual fork
  scheduling, frame parity, warning origin/order and grain/null ledger relay.
- tests/test_failure_is_not_absence.py::TestWorkerRelay pins by-path refusal,
  bad constructor, strict exception and genuine dead-worker distinctions. Its
  two direct first-field identity/None assertions must change to bytes plus
  parent relay assertions, preserving every public safety protection.
- GH689's named OriginEaError intentionally reduces to builtin Exception with
  class-prefixed text. Its private pack-object identity assertion likewise
  needs bytes adaptation, while its exact decoded type/text assertions remain.
  Synthetic equivalent now; no actual Country API invocation in author controls.
- _build_registry exclusions prebuild/relay/visible_cpus and existing closure
  controls keep all pool machinery outside build fingerprints. No new helper,
  exclusion, cache version or theoretical invalidation policy is required.
- Existing ledgers1281,1292 and1590 preserve original traceback notes, the outer
  opaque pair, positive empty proof, and parallel-primary/serial-parent placement.

## §3 Definitions and conventions

Per STANDING§§3–4 and CLAUDE: cached frames are pre-finalize data and pool code
only transports results/diagnostics. Native source identity and table content
are outside this task. Failure is not absence (GH1524): unknown worker errors
use WaveWorkerError; only actual BrokenProcessPool is labelled worker death.
The four-field internal carrier remains (serialized_or_None,type_name,text,tb).
Pickle-compatible custom reduction is existing behavior, not proof that the
unpickled object's concrete class equals the original class.

No country operands are admitted. For the necessary existing GH689 test oracle
only, personal FULL5969-line Uganda CONTENTS read is authenticated unchanged
as a378120-byte prefix (SHA295e6fda1b3302b30642d0a567187d43c455c2506c520fd64940cf6812956fae),
and all48added lines read; current6017lines SHA5a49a4f63af660d05b97ad2a350693aa9ec43bfe3368af3db6225e9cea896011.
Relevant idiosyncrasy: parish v and design-origin ea are distinct from2013-14,
and ea follows sampled origin on moves. The documented rotation intention vs
“This same sample was used ...2015/16” remains a qualified contradiction;
no inference about that design is made. New demand-adapter notes explicitly
separate source aggregation from EA/parish and weight caveats. No country
source/config/CONTENTS change or original producer execution is authorized.

## §4 Invariants

- Preserve original type where supported by native pickle, message, attributes,
  existing notes, and contextual original worker traceback text after transfer.
- Serialization or parent-load failure retains original type/text/tb in existing
  WaveWorkerError; do not misclassify it as a dead worker or catchable absence.
- A decoded non-BaseException gets that same fallback, not a late TypeError.
  Actual type ancestry is required: isinstance may consult an instance-provided
  __class__ property. ROOT selects issubclass(type(exc),BaseException), preserving
  the existing exceptException/interrupt boundary and false-valued real errors.
- Warn/file ledgers before refusal, retain dedup and requested wave order;
  forkunsafe detection/fallback, actual death, grouping, serial/no-fork/daemon
  and all CPU/memory/thread policies remain unchanged.
- No new Country constructor/API/source/getters/Make/warm/shared-cache campaign.
  Pure invented fork controls use private falsecred/unshare MAIN-import roots,
  threads/build1, pytest2loadfile and named source/write refusals.
- Existing test, fixture, baseline, skip and tolerance protection is retained;
  only private bytes-carrier expectations are updated where necessary.

## §5 Implementation and verification decision

Reuse the four fields and pickle.dumps; move pickle.loads to relay under its
own Exception fallback. Check BaseException after decoding. Reuse existing
WaveWorkerError + add_note, no new exception type or carrier framework.
Only two functions change behavior; errors docstring and necessary private
carrier test assertions follow their new representation.

Before product edits, preserve actual oldhead negatives for two separate
real-pool cases: module only in child, and parent module exists but lacks the
child-only class. New controls require original class/text/tb/context instead
of BrokenProcessPool, healthy sibling and actual child PIDs. Other controls
cover normal strict exceptions, constructor-load failure, unpicklable values,
intentional BaseException remapping, nonexception reduction, corrupt bytes,
real owned-child death, warning/ledger/serial/forkunsafe boundaries. Reuse the
existing pure parallel suite and WorkerRelay tests; do not invoke GH689's
actual Country config entrypoint in this author scope. Compare all unaffected
module function ASTs and protected country/config bytes. Metadata/fullCI and
independent review remain separate gates after the source freeze.

## §6 Decisions / scope boundary

Root accepted the plan and necessary private-carrier oracle adaptations; no
new scientific or source policy decision is identified. If actual contract
conflict appears, report exact evidence before broadening. Parent-side decode
catches Exception, preserving the existing interrupt/BaseException boundary.
1578 fullCI41022082 is separately monitored at exacta711; no duplicate suite.


### Phase 3 — executed pure verification

Runtime737d89b307b0e7edf0f766101c33ce60a470403b changes only packing and
relay behavior. All other parallel-module AST is exact, including warning
emission, all four ledgers, target grouping, forkunsafe/serial/real-death paths
and CPU/memory policies. errors.py changes documentation only; its normalized
behavior AST, Country/config/schema/build-registry objects are exact b834.

Before product edits, exact oldhead b834 ran both actual fork regressions and
failed as expected: parent missing module and parent module missing class both
misreported BrokenProcessPool instead of WorkerOnlyError. Original probe,
logs/receipts/JUnit remain unchanged (2FAIL,7.13s pytest/13.384s wrapper).

Final runtime pure selection passed64,0skip/deselected in11.93s pytest
(18.505s wrapper):15new controls,39existing parallel controls,9existing
WorkerRelay controls plus the existing all-build-closure exclusion pin. Actual
fork cases include child module state unavailable in parent, a real module
imported from invented Python code only after fork, a healthy sibling, full
warning/grain/null/datetime/drop relay before refusal, and real owned-child
exit kept as BrokenProcessPool. Direct controls preserve strict exception
types/attributes/notes, bad constructor/dump/corrupt-pickle fallbacks,
intentional BaseException reduction/remapping and nonexception rejection.

The only existing test-method edits are the two WorkerRelay private-carrier
assertions and GH689's private carrier/decode assertion. All other method ASTs
are exact; its decoded generic Exception/type-prefixed text assertions remain.
The actual GH689 Country/config entrypoint was NOT executed; an invented
custom-reducer equivalent passed instead. No fixture, baseline, skip or
numerical tolerance was weakened. Six controller/xdist guard receipts stayed
active with0denials; private roots retained zero data files/symlinks.

Anchored verdict: OK (§§2–4) for reuse of the four-field error tuple, fallback,
notes, warning/ledger ordering and parent reconstruction. OK (§4) for actual
worker-death and forkunsafe distinctions. No CONTRADICTION or REINVENTION
identified. Structural closure controls show no parallel callable enters a
build fingerprint; full450/1895/35/186 metadata census, strictAPI, fullCI and
independent review are UNRUN for1591 and remain separate gates. The unrelated
1578 fullCI success does not validate this task. No source/getter/actualCountry
API/scientificMake/warm/shared-cache/GitHub campaign occurred.


### Independent-review P2 and bounded revision selection

Exact89aba independent12case run:9PASS3FAIL7.33s. Two distinct proxy shapes
expose the same validation defect: an advertised ValueError __class__ passes
isinstance then causes TypeError at raise; a __class__ getter raising RuntimeError
escapes validation. The third failure repeats the advertised proxy after side
effects. All original64PASS and independent failure receipts remain immutable;
no own old-cohort replay is needed to establish this confirmed finding.
ROOT authorizes a one-predicate genuine-ancestry repair, canonical regressions for
both proxy shapes plus false-valued genuine exceptions, and bounded affected
relay controls. No pool/forkunsafe/ledger/warning/source/default/metadata policy
changes, fullCI or metadata campaign is included. Ethiopia preparation is held
without a running actor; resume it only after the new1591 freeze is handed off.
