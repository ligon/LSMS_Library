# Prior-Art Ledger — GH #1281, declared-wave configuration failures

Search tier: ripgrep + git. Inherits `STANDING.md`.

## §1 Task, restated

Prevent a source-column KeyError in a declared YAML wave from silently
removing that wave and stamping a partial L2-country parquet as current.
Baseline development: `57d2555855f23739a4beca86297c6d5fa702fcf3`.
The change is shared loading behavior, exercised with synthetic countries;
it changes no country-specific survey policy.

## §2 Existing machinery

| Machinery | Source | Decision / tests |
|---|---|---|
| Per-wave extraction | `country.py:1828`, `Wave.grab_data` | Reuse real YAML and sanctioned CSV reader; GH323 Site4 tests exercise config errors versus unavailable optional sources. |
| Build/concat/cache descent | `country.py:5391`, nested `load_from_waves` / `build_wave` | Extend the KeyError branch only, before legacy fallback, concat and country writes. `tests/test_dvc_caching.py` covers cache routes. |
| Worker exception relay | `_parallel_waves.py`, `prebuild` / `relay` | Reuse original exception propagation; existing parallel-wave tests. |
| Hash invalidation | `Wave._input_hash`, `Country._table_cache_hash`, `_build_registry` | Reuse the fingerprinted orchestrator; no exclusion or manual schema bump. Measure parent/candidate hashes. |
| Metadata census | `slurm_logs/isa_backlog_2026-10-21/1221-followup/hash_census.py` | Reuse against empty data roots with raw-source/network guard; counts are hash coverage, not successful build counts. |

## §3 Definitions and conventions in force

`build_wave` calls a method only when the wave declares it in data_scheme
(`country.py:5419`). An undeclared table is legitimate absence, not a
configuration failure. The GH323/GH675 distinction in `Wave.grab_data`
(`country.py:2010–2192`) already treats malformed configured columns
differently from an unavailable optional source. Preserve that distinction.
Use canonical schema and I/O per `STANDING.md` sections 3–4.

## §4 Invariants and assumptions

- A failing declared-wave KeyError must propagate before country concat and
  either L2-country write route. Preserve the original error as the cause.
- Keep the existing AttributeError legacy fallback in this narrow change.
- Undeclared waves, healthy waves, empty answers, optional-source degradation,
  subset selection and existing warm reads retain their established meaning.
- A failed rebuild must not overwrite/restamp a previously stale cache.
- The attempted-wave coverage manifest does not prove every wave succeeded;
  metadata alone cannot count historical swallowed KeyErrors.
- This orchestrator is fingerprinted. Hash changes are expected to invalidate
  possibly poisoned old country caches; do not hide them with exclusions.
- Tests use synthetic CSV sources only, private roots and scoped opt-in for
  in-tree fixture sources. No real data acquisition or cache clearing.

## §5 Reuse decision

Extend the existing per-wave exception branch; use contextual RuntimeError
with a chained KeyError rather than a parallel cache metadata mechanism or a
new exception taxonomy. Reuse the existing hash, writer and worker relay.
Tests must demonstrate the failure on the actual baseline and distinguish
healthy/undeclared/empty cases.

## §6 Open questions for the human

None blocks the documented configuration-error fix. A full source-build
census is separate from hash coverage and must be reported with its actual
scope; no corpus-clean claim follows from synthetic verification.

## Phase 3 verification

The new synthetic suite has 10 passing candidate cases. The actual baseline
fails all four malformed-column/index cases; the corrected stale-cache
control also fails on the baseline. The first baseline run included one
test-fixture missing-directory failure, retained in its original log.
Healthy, absent, empty, selected-subset and AttributeError controls pass.

The selected regression run passes 308 tests. It included the two unmarked
CotedIvoire real-data degradation tests in `test_gh675_degraded_build.py`:
`-m 'not requires_s3'` does not exclude them. Their fixture uses private L2
roots and the shared L1/probe; do not describe that run as data-free or as
proof of zero remote/cache activity. Future offline selections use an empty
private data root as well as explicit test selection.

Independent source critique accepts the narrow branch with qualifications:
worker transport preserves the chained KeyError as traceback text rather
than `__cause__`, and derived API fallback safety is not proved here.
Guarded parent metadata census: 549 country hashes / 40 fingerprints /
2,109 wave hashes / zero errors. The existing PR queue was integrated first
under the user's steering; see the current verification below.

### Integrated-baseline verification, 2026-10-25

The fix cleanly incorporates development763bd90a. Candidate06edcea36 also
promotes the strict GH1281 Malawi regression to an ordinary data-gated test,
preserving its assertions and requires_s3 marker. That real-data test was not
executed in this offline review.

Independent final gate:83passed,0failed/errors/skips,21data-gated cases
deselected,0guard violations under private empty roots and exact candidate
imports. It covers the ten new synthetic cases,31Malawi offline hook/config
cases, six worker-relay cases,25id_walk cases,nine v-join cases and two
independent controls. The latter verify an escaping hook-style KeyError at
the Wave.grab_data boundary and a real two-worker YAML/CSV failure. Serial
transport retains __cause__; the parallel transport preserves the original
KeyError in worker traceback notes, with no country cache written. No full
formatting-hook dispatch or all-country source build is claimed.

Fresh baseline and candidate metadata censuses both complete at549country/
40fingerprint/2109wave hashes,0errors. All549/40/2109hashes change, so this
fix requires a corpus-wide re-warm, including countries outside the eight
campaign countries. This intentionally invalidates potentially poisoned old
caches. Each census used the same unmodified helper and guard, allowed no
existing raw-source/network/build attempt, and denied/counts expected absent
source probes. The measurement establishes hash impact, not corpus health.

Evidence: private control/fix1281/review-final, including the final gate and
both guarded census receipts. The shared package file, inherited id_walk
changes and the promoted regression were reviewed against the integrated
baseline. No global cache clear or acquisition was performed.

`build_wave` — OK (anchored on §§2–4): aborts before concat/cache writes;
reuses established extraction, writer and exception relay. No new cache
metadata mechanism, schema rule or country-specific survey interpretation.

### Recovery against reviewed development - 2026-10-25

The recovered PR1300 head is a348bda6e6d690efece158b2e34d936406923b89.
The coordinator merged reviewed development6a86604615e28a8e4f6767de8889b71d18db9828
into an isolated recovery branch, preserving both histories. Product/test gate
pin76ac157f32a5bd570cda92cc3d0709f0930534d1 differs from development only in
the original four PR paths. Production and tests are not amended here.
The later merge base includes the separately reviewed Arrow test reduction
repair; that diagnostic identified no causation by the declared-wave guard.

The old private review-final receipts named above are unavailable in this
recovery. Their83-pass/21-deselected and metadata results remain attributed
historical evidence, not freshly authenticated acceptance. No missing result
is reconstructed as successful. Other recovery metadata runs demonstrated
that errors={} alone is insufficient when a guard denies real config inputs;
fresh cache-impact proof must verify actual inputs and every denial.

Independent source review found no blocker in this narrow handler, with the
serial-cause, worker traceback-note and derived RuntimeError fallback limits
explicit. It also corrects a historical shorthand: Malawi2013cluster_features
has a dfs block; the escaping KeyError concerns its PRIMARY sub-df, as current
issue1281comment6376116241 states. One cover file does not make the whole
table a single-file declaration. The promoted requires_s3 regression is no
longer xfail in this candidate; the country note records the earlier state.
No coordinate, acquisition or household-link policy changes here.

The NEW finite offline proposal has84 current-source cases plus21 data-gated
deselections: ten synthetic YAML/CSV cases,31Malawi hook/config cases, six
worker-relay cases,25id_walk cases, all10declarative v-join cases and two new
independent controls. The last two are recreated recovery tests, not recovered
historical receipts: an escaping hook-style boundary error, and actual
two-worker malformed CSV extraction with a barrier and distinct worker PIDs.
Actual collection/counts and complete failure causes must be checked after
root release; no test has run at this authoring stage.

The control uses current development plus the identical external ten-case
test carrier and new two-case module, leaving tracked product bytes unchanged.
Predictions are seven meaningful failures and five healthy passes. Candidate
tests and all provider/country builds remain unexecuted here. Root supplies
the guarded receiving-node setup and owns acceptance. A separately verified
metadata census and the requested final-source corpus catch/build inventory
remain required; no full corpus-health or whole-issue completion is claimed.
