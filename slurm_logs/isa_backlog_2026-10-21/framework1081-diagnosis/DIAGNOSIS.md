# #1081 read-only diagnosis

Confirmed on clean frozen f8 `f8d236694c26fe9e709c5548f02c70f74ea72c33`, branch `fix/1081-derived-categorical-errors-20261021`, at `/local/job40255479/worktrees/isa-framework-1081`. No implementation, tracked edit, git/ref mutation or GitHub write was made.

The queued issue body/comments were read from root's frozen context JSON; no network/API refresh was attempted. Its claim that the categorical failure always remains loud needs qualification: the generated handlers can also return a successful legacy result after masking it. This is proved with a mocked successful legacy result, not an observation of an existing healthy Ethiopia legacy cache.

## Grounding

Read the complete 2,350-line Ethiopia CONTENTS, its live TODO/WAITING/LOGBOOK material, project guidance, standing ledger, categorical and Aggregate-label contracts, and the named tests plus key-resolver tests. The sole live TODO is the Food Composition Table (#115); there is no live WAITING/LOGBOOK entry. Food composition is distinct from kg conversion.

Relevant idiosyncrasies remain untouched: W2/W3 use household_id2 and wave-scoped reidentified EAs; Panel I and Panel II do not link; W1's urban category denotes small towns while later waves include larger towns; Tigray is excluded from W5's represented population; API-time unit aliases and build-time unit cleaning are different operations. No source data was acquired or built for this diagnosis.

CONTENTS explicitly corrects “No u table is wired” and “the mapping is identity”: both are false for the live Ethiopia unit mapping. It also flags the sampling-overview phrase “rural and small-town areas in waves 1-3” against the documented restriction to wave1 alone, and the cover-weight statement against the release's distinct longitudinal-weight claim. These contradictions are retained as evidence and are not resolved by this framework task.

## Actual call paths and findings

`Country.__getattr__` builds the real public handlers from `_FOOD_DERIVED` (`country.py:6069`) and `_ROSTER_DERIVED` (`:6076`). Fault injection preserves that generator and intercepts only source aggregation or the derived finalizer, following the existing Country-level units test machinery.

Food path: public handler -> `_aggregate_wave_data(waves, 'food_acquired')` -> transform -> `_finalize_result` -> relabel/poststeps. Source and derived-finalizer calls are inside the try block at `country.py:6267`; scalar relabel is outside it. The catch at `country.py:6292` includes RuntimeError and takes a legacy aggregation route unless explicit units, mapping-label dict or valuation was requested.

Roster path: public household_characteristics -> `_aggregate_wave_data(..., 'household_roster')` -> roster transform, caught by `country.py:6362`. There is no corresponding strict-map-label exception branch here. Market attachment and target checks occur after this try block.

| Actual origin/path | Existing f8 outcome |
|---|---|
| Food source or derived finalizer; bare/default | Categorical error masked; legacy route called |
| Same; scalar labels='Aggregate' | Also masked: scalar label selection leaves map_labels empty |
| Same; explicit nonempty mapping-label dict | Same exception instance, args, cause, notes and custom context preserved |
| Prices/quantities; explicit units | Same original categorical exception preserved |
| Expenditures; explicit valid valuation | Same original categorical exception preserved |
| Roster source; default, scalar Aggregate or mapping dict | Categorical error masked; legacy route called |
| LSMS_LABELS_STRICT environment alone | Does not make either derive catch strict |
| Prices/quantities `_unit_kg_factors` before the try | Typed categorical error already escapes unchanged |
| Healthy Ethiopia scalar Aggregate relabel | Existing LabelUnavailableError for missing curation; unchanged |

The parser deliberately uses distinct RuntimeError subclasses to prevent fallback. `EmptyCategoricalTableError` (`local_tools.py:2239`) retains the actionable fill/delete declaration message and parser cause; `CategoricalMappingConflictError` (`:2183`) names conflicting paths; `CategoricalKeyDeclarationError` (`:2225`) names a malformed explicit key declaration. All three are masked by these two generated catch blocks when they reach the guarded source/finalizer sites.

For masked failures followed by exhausted mocked fallback, the visible error is a new plain RuntimeError from the actual `_rebuild_failure_error` (`country.py:909`), with no original exception as cause/context. The actionable parser message is only logged at INFO. With successful mocked fallback, all four derived methods return the legacy sentinel. Neither observation warrants saying every real source error must stay loud, nor that Ethiopia currently has such a usable legacy cache.

`CategoricalKeyDeclarationError` has the same explicit non-fallthrough contract and is a concrete additional finding. I recommend including it alongside the two named errors in the same two narrow re-raise clauses. This recommendation is explicit; no code or scope extension has been applied.

## Evidence and counts

- 162 existing tests pass, zero skips/errors/failures: the four context-named files plus `tests/test_categorical_key_resolver.py`.
- 166 generated-handler probes verify current behavior. Categorical source probes include 51 masked/15 preserved outcomes (including12 mocked successful legacy returns); finalizer probes include18 masked/15 preserved; pre-try helper probes preserve all24.
- Five additional explicit-valuation controls pass: all three categorical errors retain identity, generic RuntimeError stays strict, and UnitsUnavailableError retains intentional country decoration/cause.
- The concrete private proposed regression file has73 cases. On f8 it must produce42 failures and31 passes, zero errors/skips. Typed-contract failures are28 for the issue's two errors plus14 for the proposed key-declaration extension; the15 strict typed controls and16 generic legacy success/failure controls already pass.

All parser errors are produced by actual categorical readers over private synthetic Org files, rather than fabricated message strings. Private per-case records preserve full messages/context; exported summaries contain no survey records or IDs.

Two initial compressed private probe drafts had syntax errors before any probe executed; their copies/logs are preserved. A later aggregate-report snippet had a variable-name typo after reading the completed JUnit results and was corrected without rerunning tests or changing a result. These are harness failures, not passing product evidence.

## Preserved boundaries

UnitsUnavailableError intentionally falls through for default and scalar-Aggregate calls; explicit units/mapping/valuation names the country and raises with the original error as cause. Do not convert this policy into unconditional propagation. Unrelated FileNotFoundError/KeyError/ValueError/RuntimeError legacy success and exhausted-fallback behavior remains deliberate.

Do not change LabelUnavailableError/Feature degrade, scalar relabel semantics, strict env policies, table parsing/resolution precedence, empty read-time merge behavior, nonempty tables missing a requested value column, country configs, schemas, scientific quantities or persistence. The read-time empty-table merge and missing-value-column fall-through remain separately pinned report-only residuals.

No candidate exists, so actual hash/fingerprint cost is unmeasured. Measure immutable f8 versus the approved candidate in fresh processes; do not infer zero from “read path,” change a hash lever, or use stash. This diagnosis used an empty private data root, no real source/cache access and no acquisition.

Concrete proposed boundary and checks: `PROPOSAL.md`; prior art: `ledger-draft.md`; safe counters: `probe-summary.json`, `valuation-controls.json`, `baseline-contract.json`; final immutability receipt: `proof.json`. Full logs/frames and synthetic parser traces remain private.

Sue (GPT-6).
