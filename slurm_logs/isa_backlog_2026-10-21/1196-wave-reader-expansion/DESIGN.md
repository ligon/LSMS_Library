# GH1196: shared parquet-only reader proposal after the Make stop

This is an unapplied private proposal against clean runtime/test head
`bd80d86c26786ca92680160abd790260bc924c3f`. No production, test, configuration
or tracked ledger file has been edited for this proposal. The generator uses
stdlib source-text operations and AST parsing only. The original stage1 design,
patch, all bd80 operands,14 strict stops and their checker sources remain intact.

## Measured reason for the expansion

The existing bd80 patch repairs `get_dataframe` reads. Its actual prebuild
census moved549/549 API,39/39 framework and1362/1362 declared-wave hashes
against f8; all keys matched, with no data-endpoint change. That inventory is
pinned to bd80/local_tools SHA256
`0a5f4ce56d471f7db7b925b3bebabd85c6d5de19ffe46579bbd39b9566db52f6`.
It is not the inventory of this proposal.

Both candidate Default targets have strict fresh-process warm equality. Both
candidate Make targets still change from the recorded pd.NA string policy on
their first cold API return to the NaN string policy on the fresh-process warm
return. The measured sources are Niger food_acquired239877 rows and Uganda
cluster_features4805 rows. Direct native-cache reads through get_dataframe
are repaired. Normal Make cached-wave calls are not.

The source trace is exact:

1. Both tables have a non-null country declaration and neither is
   `materialize: make`. The Make backend takes Country.load_from_waves in
   country.py:5823, not the country-cache gate.
2. Wave.grab_data calls `_read_l2_wave_cache` at country.py:1805 on a YAML
   cache hit and immediately returns its frame, without map_index/finalization
   inside that wave method.
3. `_read_l2_wave_cache` directly calls `pd.read_parquet(cache_path)` at1722.
   It never reaches get_dataframe or `_restore_parquet_index_string_policy`.
4. A second direct serving read is in the script-path Wave.grab_data branch
   at2178. It is not the route of these two actual source-backed targets,
   but leaving it unchanged would leave the same generic reader defect in
   another normal wave-serving route. Its coverage here will be synthetic
   actual Wave.grab_data, not a claim of another country source build.

The saved146-pair assessment retains124 full exact pairs,8 explicitly
recognized policy-only pairs and14 unaccepted pairs. Artifact-only v2
classification finds12 of those14 fail only on the recognized index missing
policy. All14 full typed attrs comparisons are exact. The other2 are matched
Uganda2019-20 singleton requests: both parent and candidate Default have
824x5 while Make has824x3. Default carries Latitude/Longitude from its all-wave
country union; Make serves the requested native-wave columns. Their full
MultiIndexes also retain different unused-level structures (Default t8/v2876,
Make t1/v824). Common columns and expanded row-label values/masks are exact;
this does not make the complete frames or axes equal. This existing backend
structure difference is outside1196 and will remain an explicit qualification.

The14 original strict failures are not relabelled as successful full-frame
comparisons. No operand is cast, pruned, padded or normalized.

## Exact proposed production change

Only two files change, as shown in `proposed.patch`:

- `lsms_library/local_tools.py`: add a private parquet-only stream reader
  `_read_parquet_with_index_policy` immediately before get_dataframe and route
  get_dataframe's existing parquet attempt through it. Reuse the already
  reviewed field-directed restore helper unchanged.
- `lsms_library/country.py`: import that reader and replace exactly the two
  direct serving reads above. Open their known local cache paths in binary
  mode once. No get_dataframe call is introduced into either wave route.

The helper accepts an ALREADY-OPEN stream and passes the caller's unchanged
reader kwargs to pandas. It reads pandas metadata from that SAME open stream
with pyarrow.parquet.read_schema and then calls the existing restore helper.
It does not open paths, resolve sources, sniff formats, call DVC, acquire data,
run the null-read audit, stamp caches, or change the writer. The original
get_dataframe parser, lookup, local/DVC/World Bank branches and final audit
remain in place.

The helper's return is `(frame, None)` or `(None, original_error)`. The latter
is possible only for the caller's explicitly supplied PAYLOAD-read exception
tuple. This separation is necessary: an outer catch around the complete helper
would accidentally convert a new footer/metadata failure into a parser fallback
or cache miss. The helper catches only around the existing pd.read_parquet
operation. Footer, metadata and restore failures stay outside that catch.
There is no new exception class, wrapping, catch-all or parser fallback.

| Caller | pandas arguments | Payload errors returned | Existing disposition |
|---|---|---|---|
| get_dataframe | engine='pyarrow' | ArrowInvalid only | suffix .parquet raises existing ParquetFormatError from the SAME original ArrowInvalid; other suffix retains the existing fallback cause |
| YAML wave gate | original omitted engine | OSError and ArrowInvalid only | failed local open or payload read is a cache miss, exactly as the old gate; freshness/drop grading and replay/stamp policy stay in the gate |
| script-path wave read | original omitted engine | none | missing/open/payload errors remain loud as before; no source fallback is added |

The local YAML open is caught separately for OSError, covering a disappearance
between the existing existence/freshness checks and open. The two wave reads
never re-enter generic source lookup if a cache is missing or corrupt. The
same open descriptor binds payload and footer if an atomic writer replaces
or unlinks the cache path after it has been opened.

The reader does not force an engine onto the wave routes, so their existing
pandas engine selection remains. Existing parser programmer errors stay loud.
New footer/metadata/restore errors stay loud. The ordinary binary stream
context owns closing; no new close-error swallowing is proposed.

The restore helper's field matching, no-op rules and conversion are byte-exact
to bd80: physical descriptor positions, recognized string/str policy only,
already-read StringDtype storage, no change to values/names/codes/unused levels/
sortorder/columns/full typed attrs. Both writer branches, GH645, JSON attrs,
atomicity, units, IDs, eligibility, science, reducers, schema and registry
exclusions remain unchanged. No diagnostics.py read is added to this bounded
Country/Wave serving-path fix.

## Hash gates after separate approval

No new hash result is claimed for this unapplied patch. After approval, record
new actual549 API/39 framework/1362 wave inventories BEFORE any new candidate
data build. Compare exact keys/values against BOTH the existing bd80 inventory
and actual f8 inventory. All moves are predicted for each comparison because
the build-path generic reader now calls the new helper. The actual counts must
be measured; no source-text simulation replaces that census. Stop on unexpected
scope. Do not add an exclusion, schema lever, stamp or corpus rebuild.

## Discriminating focused controls

Keep the original87-node85PASS/2FAIL XML, narrow4PASS correction/comparator
cohort and actual-parent24-node23FAIL/1PASS XML unchanged. Existing85 passed
nodes do not become a retroactive green87. The new patch changes the shared
reader, so affected public-reader and gate tests are new justified evidence at
a new source pin, not a replay to erase a stop.

1. Exercise actual synthetic Wave.grab_data YAML cache hits with ordinary and
   MultiIndex `string`/`str` policy, including mixed policy and genuine nulls.
   Use full axes/levels/codes/dtypes/masks/order and recursive typed attrs, with
   explicit EA exact/NumPy value checks. A direct helper-only pass is not enough.
2. Exercise actual script-path Wave.grab_data selected existing hashless and
   current-hash parquets. Assert recorded policy restoration, unchanged replay
   behavior and no hash stamping/Make/source lookup on the hit.
3. Keep meaningful original GH1040 site1/site2 absent/fresh/stale/hashless,
   degraded/replay and error controls as applicable. Add actual disappearance
   after gate selection and corrupted payload controls: YAML must miss;
   script route must preserve original OSError/ArrowInvalid. No network fallback.
4. Inject payload OSError/ArrowInvalid separately from footer OSError/
   ArrowInvalid and restore programmer errors. Only the original caller's
   payload errors are caught. Verify the generic reader's suffix-dependent
   original ParquetFormatError cause and nonparquet fallback stay unchanged.
5. An actual same-open-file race control replaces the path AFTER the real
   pandas payload read but BEFORE footer read. The helper must use the original
   open descriptor, retain original metadata and never reopen the replacement.
   Existing supported local and mocked DVC stream reader tests remain relevant.
6. Reuse actual unnamed/duplicate-name/mixed-policy physical-metadata controls,
   Range/nonstring/empty/allnull/no-op/parser controls and GH645/atomic controls
   affected by the new helper. Their expected values remain independent of
   observed results. Keep explicit nextafter/tiny nullable Float64 comparator
   negatives; do not weaken the comparator or broad-cast test operands.
7. Run the genuinely new wave-route regression nodes on clean bd80 from a
   physically copied private test file before a writer commit. Label dtype
   assertion failures separately from absent-helper/API-contract failures.
   Existing f8 counterfactuals remain at their original source pins.

Use unique JUnits/logs for every new run and at most2 total loadfile workers,
all numerical/build/Make threads1. No full suite or broad corpus test.

## New runtime cohort after separate approval

Keep all four old bd80/f8 source-built roots immutable. After the actual new
hash census, stage17 declared held sources into four NEW physical private roots:
new-runtime Default/Make plus matching control roots as approved. Verify declared
MD5, raw SHA/size/mode/mtime, nlink1/distinct inode and immutable provider/copy
receipts terminally BEFORE decode/API. Reuse original source-reference artifacts
only with matching source/argument/stage provenance; do not decode a source
prefix again merely to reconstruct an old output.

Required actual source-cold and fresh-process/new-object warm routes remain
NigerDefault/NigerMake/UgandaDefault/UgandaMake. Save endpoints and complete
warnings BEFORE assertions. Capture native writer inputs/prepared frames,
native caches, Default raw country cache, public full/supported singleton,
explicit currencyNone and valid Niger currencyindex, and Feature default/None
separately. Compare each route cold/warm exactly at its own request/column
universe; compare controls at the same stage. Preserve the existing Uganda
singleton Default-vs-Make column/unused-level qualification separately.

Every served expected hash/request coverage must be current for its checkout.
Make's ten native caches and absent country cache remain an intended route
fact. Measure fresh warm bytes/modes/mtimes without a second attempt to obtain
zeros. No NO_CACHE, assumed-fresh option, stamp, acquisition or DVC CLI.

The runtime acceptance should include scoped unmodified scanner phases1a/1b/2
for Nigerfood and Ugandacluster, their actual declared kwargs only, with full raw
finding multisets and captured API warning records. The existing bd80/f8 scan
is27 records/head,26WARN/1knownFAIL, exact raw multiset. A new read-route patch
does not authorize hiding that existing FAIL or inventing zero warnings.
The harness persists message strings but not warning category/stack for every
internal warning; distinguish that from complete outer API warning records.

Default CLI matched-input and post-serialization-stage artifacts already passed
at bd80. A new runtime CLI check, if approved, should be limited to the same
two real commands/arguments and matched-stage full-frame/footer comparisons,
with existing JSON attrs conversion explicit. No generic writer change.

After all new actors finish, freeze runtime/tests before docs-only descendants;
verify blob equivalence, exact source/provider/old-root receipts and zero active
consumers. Separate review remains required. No author PASS, GitHub write, PR,
merge or issue closure is authorized by this private proposal.

Sue
