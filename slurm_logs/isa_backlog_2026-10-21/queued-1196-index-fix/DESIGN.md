# GH1196 stage1: proposed generic parquet index repair

Proposal only, not applied. Source parent is
f8d236694c26fe9e709c5548f02c70f74ea72c33. Ledger-first head is
ee533e09af4b29408985a0c7a1abf815c5c385d1, durably published by the normal
SuCoder hook. Its only tracked change is
`.coder/ledger/1196-parquet-index-policy.md`. `proposed.patch` changes only
`lsms_library/local_tools.py` in memory; no product/test/configuration file was
changed. No package import, test, API, decode/unpickle or source staging ran.
The generator used stdlib text IO, difflib and `ast.parse` only.

## Grounding and decision

The accepted frozen diagnosis is pinned by `READY-v4.json` SHA256
08a2aa8392cd21b4b6d4308523334303ed7b43d42cf767ab1e1bf56b1958832a and
its281-entry manifest SHA256
368e4997fd99d0982c6db54992fa19e8f6bea82271b28a1e2ec91e2d1afcd24c in
`/local/job40255479/control/isa-backlog-20261021/shared1196-diagnosis`.
This proposal does not change those reports or retroactively improve their
source-memory, JSON-attrs, guard-event or sealing qualifications.

Both complete real frames are MultiIndex: Niger food_acquired has239877 rows
and extracted `u`; Uganda cluster_features has4805 rows and extracted `v`, with
zero null v. The original Uganda plain-Index wording described that extracted
level, not the full frame. Diagnosis separately established the same first
observed transition for both cases: Arrow25 `_extract_index_level` calls
`ChunkedArray.to_pandas(types_mapper=None)`, which selects pandas3 NaN-policy
`str` even when the field's pandas metadata still says `numpy_type="string"`.
Data-column extension reconstruction happens after those index fields have
been removed. The generic writer retained the metadata; map_index and the
observed finalization paths did not introduce this difference.

Current Niger and Uganda CONTENTS are byte-identical to the complete f8 reads
(including TODO/WAITING/LOGBOOK), at the two SHA256s in ledger §3. Relevant
idiosyncrasies remain Niger's grappe/EXTENSION and seasonal halves, OriginalLabel
unit dictionary and Uganda's EA/GPS-bin/parish eras and missing Region caveat.
The Niger opening says "each grappe is visited in exactly one =vague=" while
its sampling account says visits cover "half of every EA". Uganda's statement
that sample Region "is complete for every household" is qualified by its
recorded three missing2019-20 Region values. The2018 second harvest slot is
settled as a condition slot, despite the wrong source season label; no part
of this reader proposal reopens that ruling or changes country data policy.

## Exact product boundary

1. Add one private helper immediately before `get_dataframe`:
   `_restore_parquet_index_string_policy(df, pandas_metadata)`.
2. Keep the existing `pd.read_parquet(f, engine='pyarrow')` call, arguments,
   sniffing and ArrowInvalid handler. On success only, read
   `pyarrow.parquet.read_schema(f).pandas_metadata` from the SAME already-open
   file and pass the successful pandas result and metadata to the helper.
   This is a footer-only second read, not another payload decode or write.
   It does not reopen a path that an atomic writer might have replaced.
3. Return through the same `check_read` path. No other parser, gate, writer,
   Country/Wave/Feature method or build registry entry changes.

The same-file footer read belongs in the try/else success branch. Existing
ArrowInvalid-to-ParquetFormatError handling and nonparquet fallback remain
around the original reader only. No helper error is converted into a format
fallback. No new exception handler is introduced. Existing pandas metadata
parsing errors propagate as before; the helper does not parse or repair JSON.
Any new footer IO failure also stays loud rather than becoming fabricated data.

### Field matching and the only allowed conversion

The helper requires a dictionary with list `index_columns` and list `columns`.
Every descriptor must be a distinct physical-field string and the descriptor
count must equal the reconstructed index's number of levels. A Range descriptor
(a dictionary), an empty/absent/malformed descriptor collection or an ambiguous
mapping returns the original frame unchanged. Each descriptor must match
exactly one `columns[*].field_name`; a missing or repeated mapping likewise
returns unchanged. Legacy records lacking physical field_name are not guessed
from logical names.

Descriptor POSITION selects the corresponding index level. Logical `name`
is never used to look up or select a level. This supports an unnamed index,
duplicate logical level names, and a logical name that is also a data-column
name: Arrow assigns distinct physical descriptors in these cases. Test actual
Arrow metadata rather than assuming how it spells generated field names.

For a matched field only these recorded values are actionable:

| Recorded numpy_type | Requested na_value | Storage |
|---------------------|--------------------|---------|
| `string` | `pd.NA` | the already-read level's StringDtype.storage |
| `str` | `np.nan` | the already-read level's StringDtype.storage |
| anything else or absent | unchanged | unchanged |

The current level must already have `pd.StringDtype`; there is no conversion
of object, numeric, datetime, categorical, ArrowDtype or unknown indexes into
strings. Equal dtypes produce no assignment. Only a mismatching recognized
string policy calls `level.astype(pd.StringDtype(storage=..., na_value=...))`.
No label mapping, stringification, value inference or type-wide mapper runs.

For a plain Index, assign the converted Index with its existing name. For a
MultiIndex, use `set_levels(replacements, level=integer_positions,
verify_integrity=False)`. Positional selection avoids duplicate-name ambiguity.
Only the missing policy changes; level labels, their order and lengths remain
the same, so there is no reason to refactor codes or infer them again. The
false integrity flag preserves the existing code arrays/dtypes, including
unused reconstructed levels. It is not permission to replace labels, insert
levels, prune unused values or hide an integrity failure. Tests must compare
all levels, codes, code dtypes, masks, order, axis classes/names and sortorder.

The frame's data columns and attrs are untouched. No `types_mapper` is supplied
because one physical Arrow string type can represent index fields with distinct
recorded missing policies. Storage is taken from the successful read rather
than invented from metadata that did not retain it.

### Boundaries expressly retained

- Both writer branches, GH645 mixed-object/null masking and literal `None`,
  `nan`, `<NA>` labels, JSON attrs conversion and atomic replacement stay exact.
- Writer reset/set preparation can prune unused SOURCE MultiIndex levels
  before Arrow. That pre-existing boundary remains, and must be tested separately
  from the helper's preservation of its reconstructed input levels.
- RangeIndex (including nondefault start/step), missing/unknown metadata,
  non-string levels, parser failure types/causes and nonparquet sniff routes
  keep their existing behavior. No blanket cast or exception swallowing.
- No new attrs contract, dependency/schema bump, hash exclusion, cache stamp,
  assumed-fresh read, country decoder, scientific factor, reducer, ID or unit
  rule. Changing the Country read gate alone is excluded as an incomplete fix.

## Proposed discriminating regression matrix

No test code has been added or run in stage1. A later new focused file should
exercise the public sanctioned IO, plus small direct helper controls where the
writer or Arrow cannot preserve the necessary structure. All expected-policy
operands are independently constructed from the recorded policy, never copied
from the observed result dtype merely to pass.

The existing writer's reset_index can reject duplicate logical index names or
an index/data-column name collision before Arrow is reached. Do not change that
writer behavior to make a reader test possible. For those names, use an actual
Arrow pandas-metadata synthetic fixture and the public get_dataframe reader,
plus direct helper controls; retain the original writer error as an unchanged
boundary. Do not report such a fixture as a successful sanctioned-writer
roundtrip. Both ordinary writer branches remain separately exercised by the
valid noncolliding cases.

| Cohort | Required assertions / actual-parent control |
|--------|---------------------------------------------|
| Existing diagnosis eight negatives | both writer branches x plain/MultiIndex x missing/no-missing; retain actual f8 eight dtype failures as prior evidence and rerun the approved new cohort on the new actual-parent root later |
| Content distinction | duplicate labels, genuine missing values and literal `None`/`nan`/`<NA>`; a same-policy data-column mirror; exact payload/masks and full attrs at matching stages |
| Mixed policy | one pd.NA-policy and one NaN-policy index level with the same Arrow physical type; reverse their positions; each retains its own policy and storage |
| Names | plain unnamed, multiple unnamed levels, duplicate logical names, and level/data-name collision; actual physical descriptors drive positional matching |
| Empty/all-null | empty and all-null plain/MultIndex string cases; exact classes, dtype policies, masks and levels/codes; any newly observed non-string reconstruction is a stop before broadening the conversion |
| Non-default structure | deliberately nonlexicographic level order, duplicate rows, null codes and unused reconstructed levels; helper preserves every code/value/dtype/sortorder; end-to-end writer expectation begins at its prepared frame |
| Existing NaN policy | recognized `str` remains exact; direct helper also corrects a pd.NA-policy level when actual field metadata says `str` |
| Storage | helper tests with existing python/pyarrow storage both preserve that storage; end-to-end tests assert actual reader-selected storage, not unsupported historical-storage recovery |
| Non-string / Range | numeric, datetime, categorical, mixed non-string/string levels; default and stepped/named RangeIndex stay exact |
| Metadata no-op | None/non-dict; absent/non-list/empty fields or columns; Range/mixed descriptors; physical-count mismatch; repeated physical descriptor; missing/duplicate field mapping; malformed column record; unknown/missing numpy_type; no field_name. Preserve the original frame and attrs, with no inferred repair |
| Parser failures | actual corrupted parquet/missing magic/truncation; malformed pandas JSON/index kind that already fails; original error class/message/cause chain; reader programming errors still loud |
| Valid parser controls | actual parquet, parquet content with another suffix, CSV/Stata/SPSS sniff controls from the existing suite, no changed source interpretation |
| Attrs/metadata | recursively typed full same-stage attrs, footer hash/coverage/audits and ordinary serialized JSON attrs; do not equate JSON-converted attrs to the original API record classes |
| Writer safety | unchanged GH645 controls and both partial-write atomicity controls, preserving existing target bytes and cleaned sibling temps |

Retain meaningful parent failures by cause: the old roundtrip cohort should
fail exact dtype assertions; a new direct helper import/signature negative is
an API-contract negative, not evidence of row loss. No failure-to-skip or
implementation-mirroring expected dtype. Reuse the frozen exact comparator's
source-pinned function AST, including dtype.na_value/storage, all MultiIndex
levels/codes and recursively typed immutable record fields. Retain meaningful
nextafter/type/null/name/order/unused-level comparator negatives.

Existing narrowly relevant paths found on f8:
`tests/test_gh645_to_parquet_null_coercion.py`,
`tests/test_cache_hash_invalidation.py`, `tests/test_reader_sniff.py`,
`tests/test_gh891_wave_subset_cache.py`, and relevant index/finalization tests
selected by actual source lookup. The initial lookup also named two paths
absent from f8 (`test_gh1077_nonfinite_label_keys.py` and
`test_parquet_format_error.py`); its exit2 is a source-search stop, not a test
failure. Do not invent those files or import unrelated integration tests.
`test_reader_sniff.py:244+` owns the GH1070 parquet format controls here.

## Later source-backed acceptance, after separate authorization

Before any build, independently measure actual parent versus patched549 API,
39 framework and1362 declared-wave inventories. The diagnosis's synthetic
marker predicts all of them will change for a generic reader edit; that is
expected cost, not an actual patch result. An unexpected footprint is a stop.
No exclusions/schema lever and no rebuilt-output claim for any other country.

Use separate physical private roots per head/backend/consumer with exact mounted
interpreter/cwd/PYTHONPATH/countries/config/data identities and guards before
any import, unpickle, preparation or API. Network namespace/socket denial;
all numerical/build/Make threads1 and at most2 explicit loadfile workers.
Terminal prerequisite gates precede dependents. Held declared sources only,
with canonical DVC MD5 plus raw SHA/size/mode/mtime/nlink1/distinct-inode copy
receipts and actual current hash/full-request coverage gates. No acquisition,
DVC CLI, shared writes, NO_CACHE, manual stamps or in-tree payloads.

Minimum actual routes are Uganda cluster_features DEFAULT AND MAKE, and Niger
food_acquired DEFAULT. Both tables lack `materialize: make`; actual Make calls
take `load_from_waves`/Wave gates rather than the Country read gate. Four saved
Uganda Make endpoints prove that route's existing manifestation. Niger Make is
source-route-only in the diagnosis: later independently inventory held declared
prerequisites, measure it if held, or stop and disclose the exact source gap
without silently shrinking the claim or acquiring anything. Preserve native
hashless script-cache legitimacy rather than claiming all such companions
contain current hashes.

Actual normal source-cold and fresh-process/new-object warm must compare complete
source-native/raw-cache/public frames, valid request subsets/full coverage and
matched arguments, data columns, keys/duplicates, all index internals, masks,
dtypes, order and FULL typed attrs with check_exact=True and rtol=atol=0. Distinguish
prepared API attrs from post-writer JSON attrs; all stage comparisons must match
their serialization stage. Retain whole warning records and reconcile scoped
Feature/scanner findings as full multisets. Prove both originally affected
extracted levels and complete MultiIndexes, no hidden dtype normalization.
Measure warm bytes/modes/mtimes at endpoints; Make writes remain a measured
route fact, never a reason to weaken a failed zero-write check. Save outputs
before assertions and preserve every strict stop.

Stage1 release is the ledger head plus this design and unapplied patch. No
runtime verdict, measured post-patch fingerprint, repaired cache, source-cold
output or review PASS is claimed. Root inspection and a separate authorization
are required before applying or testing the patch.

Sue
