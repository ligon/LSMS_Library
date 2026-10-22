# Prior-Art Ledger - GH1196 parquet index string policy

**Search tier used:** ripgrep + git + installed-source inspection; no memory or
knowledge-graph tool. This ledger precedes the concrete private patch proposal.
Stage 1 permits this ledger commit and private design only: no product, test or
configuration edit, package import, payload decode, test, API or source staging.

## §1 Task, restated

Propose a generic `local_tools.get_dataframe` parquet-read repair that restores
the pandas string missing-value policy recorded for each physical index field.
Niger `food_acquired` and Uganda `cluster_features` independently retain
`numpy_type="string"` in Arrow pandas metadata but are reconstructed as the
NaN-policy `str` dtype. Both complete public indexes are MultiIndex; the `u`
and `v` diagnostics describe their extracted Index levels. Preserve values,
missingness, complete index structure and typed attrs. Root must inspect the
actual proposed patch before any product/test/configuration edit or data work.

## §2 Existing machinery

Source anchors are release f8d236694c26fe9e709c5548f02c70f74ea72c33.

| Symbol / evidence | Anchor | Existing role | Decision |
|-------------------|--------|---------------|----------|
| `get_dataframe` | `lsms_library/local_tools.py:1686`, parquet dispatch near1837 | sanctioned IO, pandas/pyarrow parser and fallback errors | extend only the successful parquet-read boundary |
| `to_parquet` | `lsms_library/local_tools.py:3225` | GH645 null coercion, index preparation, JSON attrs, two atomic writer branches | reuse unchanged |
| `map_index` | `lsms_library/local_tools.py:3840` | legacy index mapping after decode | reuse unchanged |
| Country country-cache gate | `lsms_library/country.py:4434` | freshness/coverage before finalization | reuse unchanged; insufficient as the only repair boundary |
| Country route selection | `lsms_library/country.py:5800` | Default country-cache path versus Make wave path | preserve both paths |
| Wave cache gates / finalization | `lsms_library/wave.py`, `lsms_library/country.py` | native-wave reads and canonical post-read processing | reuse unchanged |
| GH645 regressions | `tests/test_gh645_to_parquet_null_coercion.py` | real nulls versus literal null spellings and mixed objects | retain meaningful controls |
| cache metadata/atomicity regressions | `tests/test_cache_hash_invalidation.py` | writer metadata and failed-write safety | retain controls; do not redesign writer |

Prior tracked diagnosis ledger:
`isa-campaign/.coder/ledger/1196-index-roundtrip.md`, initial
468d9733701e1b1402dcc4d428b980830e0ce298 and clarification
859846b0a85627b67fca64841d1dfa563ca949ab. The original initial wording is
preserved; the latter corrects full-frame versus extracted-level classes.

Frozen diagnosis root:
`/local/job40255479/control/isa-backlog-20261021/shared1196-diagnosis`.
`READY-v4.json` SHA256
08a2aa8392cd21b4b6d4308523334303ed7b43d42cf767ab1e1bf56b1958832a;
281-entry `final-private-manifest-v4.json` SHA256
368e4997fd99d0982c6db54992fa19e8f6bea82271b28a1e2ec91e2d1afcd24c.
Use `safe-exports/final/{DIAGNOSIS.org,PROPOSAL.md,ROUTE-COVERAGE.md}` and the
separate release-v4 qualifications; never mutate or execute into that root.

## §3 Definitions and conventions in force

- Inherit `STANDING.md` §3/§4, `CLAUDE.md` and canonical `data_info.yml`.
- Follow the sanctioned generic reader/writer and current-hash/full-coverage
  cache gates; no stamp, schema lever, exclusion or assumed-fresh override.
- Current Niger CONTENTS SHA256
  b1bf5794b242c794437995eb76742db763951397d8dfdb691d064af569ef67ee
  and Uganda CONTENTS SHA256
  774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c
  match the complete diagnosis-stage readings, including TODO/WAITING/LOGBOOK.
- Niger uses `grappe` and the 2014 `(GRAPPE,MENAGE,EXTENSION)` convention;
  split-offs and `OriginalLabel` unit dictionaries retain their documented
  meanings. Quote the tension: CONTENTS:12 says "each grappe is visited in
  exactly one =vague=", while:160 says both seasonal visits cover "half of
  every EA". This repair does not resolve sampling documentation.
- Uganda retains native EA/GPS-bin/parish eras, district/parish identity and
  missing Region caveats. CONTENTS:253 says sample Region "is complete for
  every household";:270 records three 2019-20 households with missing Region.
  The settled CONTENTS:2317 ruling says the 2018 label is wrong and "it IS a
  condition slot"; do not reopen that season/condition decision.

## §4 Invariants and assumptions

- Restore only recognized recorded `string` versus `str` missing policy on
  already-string reconstructed index fields. Preserve the storage already
  selected by the reader; metadata does not justify inventing storage.
- Bind physical `index_columns` descriptors to index positions, including
  unnamed or duplicate logical names. Preserve names, level values/dtypes,
  unused reconstructed levels, codes/code dtypes, masks, order and data columns.
- Preserve exact recursively typed full attrs at matching stages. Existing
  writer JSON record-to-dict conversion is separate and remains unchanged.
- Preserve RangeIndex, absent/malformed/unknown metadata and parser-failure
  behavior; no blanket types_mapper, broad cast or new exception swallowing.
- The writer's reset/set preparation already prunes unused source levels;
  do not promise to recover information lost before Arrow in this fix.
- Default observed calls reach the Country read gate. Both observed tables
  lack `materialize: make`; Make takes the wave route and bypasses that gate.
  Four saved Uganda Make endpoints establish the actual symptom there.
  Niger Make runtime remains unmeasured; route coverage is source-only so far.
- No country decode, finalizer, reducer, scientific factor, identifier, unit,
  dependency, writer or atomicity change. GH645 distinctions remain exact.

## §5 Reuse decision

| Behavior | Decision | Reason |
|----------|----------|--------|
| generic IO/parser | extend reader only | per-field metadata exists before the index reconstruction loses its policy |
| Arrow/pandas dtype metadata | reuse | distinguishes `string`/`str` fields without country or value guesses |
| strict comparator | reuse with source pins | check_exact=True, rtol=atol=0, complete axes and recursively typed attrs |
| freshness/build routing | reuse | repair must cover generic direct/wave/country reads, including Uganda Make |
| fingerprint inventory | measure later | diagnosis simulation predicts549 API/39 framework/1362 wave changes; not an actual patch census |
| real source acceptance | stage later after approval | Default+Make Uganda and Default Niger; Niger Make if held prerequisites permit |

## §6 Open questions and authorization boundary

Root authorized stage1 on2026-10-22: this ledger first, followed by PRIVATE
`proposed.patch` and `DESIGN.md`, without applying the patch. No new survey or
missingness policy is proposed. Concrete helper/malformed-metadata behavior
and regression matrix are for root review before implementation. Future actual
hash inventories must precede any approved scoped source build; no corpus rebuild.

### Phase3 verification

Stage2 is explicitly authorized by private ROOT-STAGE2-APPROVAL.json. Applied
the exact reviewed patch SHA256
6fa0329189b79af399be0bcd89e2367b71041ebe39b648a1c88145906d879482.
Actual prebuild census, at local_tools SHA256
0a5f4ce56d471f7db7b925b3bebabd85c6d5de19ffe46579bbd39b9566db52f6,
moves549/549 API,39/39 framework and1362/1362 declared-wave hashes with exact
key sets and zero data endpoint changes. No source/API output claim follows.

Focused v1:87cases,85PASS/2FAIL,0skip; original XML/source retained. The two
failures were an incorrect test expectation for Arrow's unknown-dtype error
and a fault fixture treating a string temp path as Path. Narrow v2:4PASS,
only those two corrections plus two new nullable-Float64 exact-comparator
negatives; no replay of the85passing nodes. Actual-parent public-IO cohort:
24cases,23 dtype-assertion failures/1already-NaN positive,0errors/skips.

Pandas frame equality with check_exact/zero tolerances accepts tiny nullable
Float64 changes in this image. New checks add per-column ExtensionArray and
NumPy exact operands/masks; private strict comparator also covers nested
Series attrs. Original comparator/results remain preserved and qualified.

Held-source gate:17declared sources (Niger2/Uganda15),68physical copies across
four parent/candidate Default/Make empty-L2 roots; declared MD5 and full copy
receipts terminally passed before any decode/API. Actual Niger Make is now
required, replacing the historical source-only limitation for this new cohort.
Normal source-cold/warm, full frames/attrs and scoped scanner proof remain
pending; no author PASS. Frozen diagnosis source-memory/JSON/guard/sealing
qualifications remain at their original pins. The stage1 manifest includes
the root's approval file; do not claim it predates that file's creation.

### Approved wave-reader boundary correction

At clean bd80d86c26786ca92680160abd790260bc924c3f, the actual Make cohort
exposed a remaining read route: Wave._read_l2_wave_cache directly called
pd.read_parquet, as did the script-path Wave.grab_data branch. The generic
get_dataframe repair therefore did not cover either direct Wave read.
Twelve saved strict failures concern that missing-policy boundary. Two more
concern an existing Uganda singleton Default/Make column-universe and unused
index-level difference; they remain separate, with no padding or pruning.
All14 full typed attrs comparisons were exact. The original bd80 source,
operands and stops are frozen by the795-file manifest SHA256
06ffefbf446c763b4e61a3497160ac2654758be16f13cc3c338ee0a2c782c83d.

Root explicitly approved the exact private expansion patch SHA256
942527406d9abaa3a58e853b11255ef5740de4d59a982ca028dd72dce7b26c31 and design
3661423d205757cd044538d9e792df322d8a5515d0a36262cc48cf8763799be5, with all
conditions terminally satisfied in ROOT-EXPANSION-GATES-SATISFIED.json.
This ledger update precedes applying that expansion. Add a shared parquet-only
already-open-stream reader; preserve caller-specific payload errors and keep
footer/restoration errors outside that catch. Wire get_dataframe and BOTH
direct Wave paths without generic source lookup or new parser fallback.
The field-directed restore helper and both writers remain unchanged.

Measure actual549/39/1362 hash inventories against bd80 AND f8 before new
data builds. Use meaningful actual Wave/parser/error/race controls and a
separate clean bd80 counterfactual checkout. Then use new candidate private
source-cold/fresh-warm roots for both backends and both targets, with strict
EA/axes/full typed attrs, warning, scanner and matched CLI stage evidence.
No old completed prefix is replayed to overwrite evidence. Existing singleton,
JSON, guard, original XML and caller-stop qualifications remain explicit.
No science/schema/exclusion/acquisition change, author PASS or public write.

The exact two-path expansion is now applied. Actual guarded prebuild censuses
on candidate, clean bd80 and f8 each contain549/39/1362 entries. Candidate
differs on every entry against BOTH controls, with exact keysets and zero
data-endpoint changes. The new private wave-expansion root uses the original
denial policy with only its new private root and three exact prepared metadata
targets; no prior launcher/evidence root is edited.

Expansion candidate v1 retained72 existing selected PASS nodes and one
collection ERROR in the new file (incorrect sibling-module import). Its source
and XML remain intact. Correcting that test-only import, v2 ran ONLY the47 new
nodes:47PASS,0skip. Actual clean-bd80 counterfactual:47nodes,21PASS/26FAIL,
0error/skip. Twelve failures are recorded-index-policy assertions; six are
absent-private-helper API-contract negatives; the remaining eight concern
missing footer/restoration boundaries or same-open-stream contracts. They are
not all described as source-value or row-loss evidence. Existing disappearance,
corrupt-payload and payload-error-disposition controls pass on both versions.

New held staging terminally verified17 immutable declared sources and34 physical
copies into two candidate backend roots, empty L2 and nlink1/distinct inodes,
with all MD5/SHA/size/mode/mtime and sidecar gates before decode/API. Source-cold,
fresh-warm/scanner/CLI evidence for the new runtime remains pending.

### Completed runtime evidence and author release boundary

Tested runtime is7f4b222c2d3cb936e1b93f4fa4baa9d98fc732a1.
Both new candidate source-cold backends and fresh-process/new-object warm
backends completed normally for Nigerfood239877/Ugandacluster4805. Default
has12current caches with full4/8-wave manifests; Make has10current native
caches and no country cache. Both warm runs have0writer calls and0byte/mode/
mtime changes. No old API/source/test prefix was replayed to replace evidence.

The strengthened saved329-pair assessment records256full exact,72intended
recorded-policy-only changes and1explicit existing singleton qualification,
with0unexpected differences. All44within-route cold/warm pairs are full
exact, including independent EA values and recursively typed object elements.
All22cache footers reconstruct by expected-hash-only replacement, including
nested ARROW:schema. Existing writers and map_index remain AST-exact.

The Uganda singleton qualification includes native-str versus country-union-
string v policy, as well as824x3versus824x5columns and unused levels.
Actual2019writer input/prepared/footer/read/Make subset agree onstr; full
country footer/raw/Default subset agree onstring. Seven native waves record
str and2009-10 recordsstring. No cross-route dtype/column padding or pruning
was used to claim full equality. The stronger failed singleton assertion is
retained separately from this source-bound qualification.

Scanner27raw occurrences are multiset-exact to bothf8/bd80:26WARN and1known
FAIL, not a zero-finding or full-suite PASS claim. Two scanner baselines and
six CLI stage frames are full exact tobd80. Scanner/CLI cache endpoints have
zero changes. CLI JSON attrs and pre-Arrow unused-level preparation remain
explicit serialization boundaries. All176matched warning category/message
multisets agree;136full raw multisets agree and40retain location differences.

The earlier comparator's object-EA element-type gaps are retained and bounded
by8negative controls. NEW private exact_deep.py recurseskind-O objects,
skipping onlyStringDtype, and reassesses SAVED operands only. Original checks
are not retroactively called strict. Original87-case XML, new collection
error, comparator key/path/receipt-schema stops and the initial attempted-v4
stream-basename reuse are explicit in the safe report; its unique failure
trace survives though those initial stream bytes lack a separate receipt.

Final notes and safe reports are documentation descendants only. Independent
review/root acceptance, not an author PASS or public closure, is required.

Sue
