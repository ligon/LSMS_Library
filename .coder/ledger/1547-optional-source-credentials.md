# Prior-Art Ledger — GH #1547 optional-source credentials

**Base:** `b8346cf1f953ac82517b1b7473c715a5655ca950`.
**Search tier:** ripgrep + git floor; no GitNexus tool is available. Inherits
`STANDING.md`. Full byte-identical `AGENTS.md` / `CLAUDE.md`, shared
prior-art-ledger and scrum-master-hpc skills, `323-site4-dfs-merge.md` and its
four-commit history, the supplied static diagnosis / runtime boundary / scope /
issue / dedup context, and both full #675 / #323 site-4 test modules were read.
Their historical country measurements are prior art, not new observations.

## §1 Task, restated

Extend only the existing secondary `dfs:` fallback in `Wave.grab_data` to
recognize the real `botocore.exceptions.NoCredentialsError`. Reuse #675's
classification, declared-column loss records and degraded-cache behavior.
Primary and single-source errors keep propagating; config-required and
structural/schema failures retain their existing behavior. No country or
scientific decisions belong to this generic invented-Alpha task.

## §2 Existing machinery

| Symbol | Base location | Existing test / decision |
|---|---|---|
| `Wave.grab_data` | `country.py:1920–2399` | Extend only tuple at 2101–2102; nested candidate handler at 2004 stays narrow; `column_mapping` precedes the try |
| primary rethrow | `country.py:2103–2105` | Bare `raise`, original identity; do not move |
| post-hook config check | `country.py:2233–2275` | #323 `test_required_column_supplied_by_the_wave_hook_is_not_reported_absent`; preserve |
| drop filing | `country.py:2277–2286` | #675 optional-loss test at 632–663; preserve |
| `_drop_record` | `country.py:8728–8756` | #675 `test_drop_record_classifies_by_cause`; reuse |
| `_file_wave_drops` | `country.py:8759–8775` | #675 `test_file_wave_drops_counts_declared_columns_only`; reuse |
| `_degraded_drops`, `_degraded_cache_verdict` | `country.py:8783–8854` | #675 config, still-down, heal-once, written-key controls; reuse |
| `_probe_source` | `country.py:8801–8831` | #675 `test_probe_is_memoised_and_uses_only_the_fast_tiers`; already absorbs failed probes; preserve |
| `source_tier`, `source_fetchable_fast` | `local_tools.py:548–589` | #675 tier tests; stat / fast-tier distinction, preserve |
| wave / country read gates | `country.py:1825–1889,4773–4980` | Consume existing drop metadata; no changes |
| source provider / writer seams | `country.df_data_grabber`, `country.to_parquet` | New strict invented-only fixture required; old CSV/parquet/Country fixtures are not runtime authority |

Impact sweep: `grab_data` is reached by generated wave getters (1263–1264),
`cluster_features` (2413–2415), `food_acquired` (2485), and aggregation plumbing
(5787). It is an ALL-table `@build_transform()` root. A body change can
invalidate both harmonized tiers corpus-wide. Actual metadata footprint is
**UNMEASURED**; no hash exclusions or schema levers will be changed to avoid it.

## §3 Definitions & conventions in force

Per `STANDING.md §3–§4` / `CLAUDE.md` "Gotchas with Teeth" and
`323-site4-dfs-merge.md §11.2–§11.3`, required-vs-optional comes from the declared
scheme and presence is judged after sibling merge, `derived:`, `drop:` and the
request hook. This is not a new optional-source flag or schema rule.

Exact base `country.py:2233–2235`:

```python
# a false alarm.  Only the config-bug drops (a KeyError, or --
# GH #675 -- a `file:` naming no source at all) can escalate;
# an unavailable file stays soft.  Prose is not enforcement.
```

Exact base `country.py:2277–2280`:

```python
# GH #675: file each drop with the declared columns it cost,
# judged at the same moment as the guard above.  An
# UNAVAILABLE-source drop that costs one (required or
# optional) is a DEGRADED build: stamped into the parquet so a
```

`test_gh323_site4_dfs_merge.py:423–451`,
`test_missing_file_stays_soft_even_for_a_required_column`, asserts a
sidecar-declared unavailable secondary loses `Latitude` but retains `Region`:

```python
assert "Latitude" not in df.columns
assert "Region" in df.columns
```

## §4 Invariants & assumptions

- Reuse `STANDING.md §4` and the runtime restrictions in this dispatch. No real
  constructor, source provider, getter, writer, cache, acquisition or network
  operation is authorized by this implementation task.
- No generic `OSError`, `BotoCoreError`, `PartialCredentialsError`, `Exception`
  or `BaseException` catch is added. Current tuple contains `FileNotFoundError`,
  `PathMissingError`, `DvcException`, `KeyError`.
- An unavailable required column **already** degrades softly. Do not add a
  required-unavailable hard error. A config-required loss still raises before
  filing/writing, with the original cause. A config-optional drop is recorded
  and never graded; no-source is a config cause regardless of credential error.
- No change to #675 probe/read gates/exclusions, #1591 serialization, #1592
  market handling, dependency/auth/GPG behavior, country configuration, #1520
  wave-map work, or scientific policy.
- Runtime is separately gated: accepted 13-binding guard refuses actual
  `Wave.grab_data`. Submit frozen actual-method / strict receiver / provider /
  writer proposal, operator and guard diff + hashes through the absolute
  `QUESTIONS_FROM_AGENT.txt` before any LSMS import or tests. No capture behind
  the guard or AST handler replacement. Until approval, runtime is unexecuted.

## §5 Reuse decision

| Behavior | Decision | Reason |
|---|---|---|
| secondary no-credentials loss | extend existing tuple | Real exception import + membership provides selected existing policy |
| classification, missing-column metadata, warning, probe, heal-once | reuse | #675 already owns the exact taxonomy |
| primary / single-source / unrelated exception behavior | preserve | Existing try boundaries and bare rethrow suffice |
| verification boundary | new strict invented fixture | Existing tests exercise prohibited real IO and constructors; actual production method must still be tested after exact ROOT review |

## §6 Open questions / remaining gates

No policy question remains: ROOT explicitly selected narrow consistency with
#675. ROOT must review the exact runtime boundary before it executes. Full CI,
configured data checks, metadata footprint and science acceptance belong to ROOT.

### Phase 3 verification

Pending implementation and separately approved source-free runtime. No test or
timing claim is made by this planning commit.
