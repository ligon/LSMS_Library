# Nigeria draft1318 — fixture portability and failure cleanup

Ledger first; source-only read of exact3ed84b3d4d57a44831f5f2e10419d4074f91c3df,
CI run40982203327/job123824685895 and source1292-nigeria-ph-education.md.
Inherit STANDING.md/CLAUDE.md; preserve all five YAML corrections, original35
assertion bodies and measured P3source1d117831 evidence. No source science change.

Nigeria CONTENTS read first incl WAITING: one directory, two PP/PH logical rounds;
logical Q1 need not equal calendar interview quarter. W2 catalog agricultural
households vs BID re-interview all W1 is explicitly unresolved. PH selected
series does not mean every education instrument is PH-only. Recall windows
WAITING and early W1/W2 skip-routing uncertainty remain unadjudicated.

CI ordinary Python3.11.17 allows bytecode. Fixture copies executable nigeria.py
and mapping.py, snapshots all metadata, then actual Country.waves dynamically
imports nigeria.py. Log adds Nigeria/_/__pycache__/nigeria.cpython-311.pyc;
all35 carrier teardowns fail the strict snapshot assertion. Since cleanup follows
that assertion, countries_root/data_root caches remain bound to the private
last-candidate root even after monkeypatch restores env. Later failures explicitly
name that fixture prefix for GhanaLSS/Malawi, or say only1country available.
Original P3 used bytecodeoff; its35passingphase is distinct and not relabelled.

Reuse pytest monkeypatch.context: scope sys.dont_write_bytecode=True and only
fixture env overrides, retaining exact snapshots/no-data-file assertions. Restore
that child context before outer finally clears both path caches, even on setup,
case or invariant-assertion failure. No broad pycache ignore or snapshot exemption.
Other test monkeypatches remain owned by their ordinary fixture teardown.

Propose2disjoint regression cases (prior bytecodeTrue/False): drive real metadata
fixture, populate both private path caches, corrupt a private copied schema after
yield, require the strict invariant refusal, then verify prior env/bytecode and
empty path caches/restored path resolution. Exercise failure boundary, not a
mocked cleanup helper. Do not run/collect/import these or original35 now. Any
future validation is new scoped root-reviewed authority, not a P3replay budget.

No products/framework/config/dependency/CI policy edits. No actual sources,
cache/provider/native/Make/API/build/data streams, public writes or actors. Root
owns full diff review and any future guard/command/testing/publication gate.
