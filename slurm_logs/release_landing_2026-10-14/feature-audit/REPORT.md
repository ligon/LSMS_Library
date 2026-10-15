SCOPE DEVIATIONS: No source, test, fixture, baseline, dependency, lockfile or
DVC sidecar was changed for the Feature audit. No shared L2 writes, DVC CLI,
cache clearing, NO_CACHE or assume_cache_fresh bypass was used. The audit used
the existing scanner unchanged. Its sequential jobs=1 path does not enforce
the printed 600-second timeout; this limitation is recorded, not concealed.

The complete Feature audit finished at frozen source
`7fa05179c8519f44ea329ba6387f5d6d6441a8ea` with findings. It is not a clean
semantic audit and is not authority to bless the held Uganda nutrition fixture.

## Exact scope and result

The unchanged `bench/feature_audit/scan.py --features all --phases 1,2 --jobs 1
--include-no-microdata` completed **39 features across 38 countries**, exercising
all **549 declared country/table pairs**, **38 baseline Feature calls** and all
**17 configured Feature variants** across seven features. There were **zero
scope skips**, exclusions or country caps. The non-DataFrame panel_ids property
is loaded for its 11 declaring countries; the existing harness deliberately
does not assemble that property as a cross-country DataFrame.

| Phase | Diagnostic records |
|---|---:|
| 1a: Country sanity | 1,234 |
| 1b: baseline Feature assembly | 808 |
| 2: configured variants | 1,502 |
| Total | 3,544 |

| Emitted status | Records |
|---|---:|
| WARN | 3,504 |
| ERROR | 19 |
| FAIL | 5 |
| PASS | 16 |

These are emitted diagnostic counts, not test-pass totals: the scanner omits
most passing sanity/assembly checks. Its process exit code is zero even when
findings exist. The 3,528 non-pass records cluster into **266 patterns**: five
FAIL clusters, two ERROR clusters and 259 WARN clusters. Clustering is grouping,
not adjudication or proof of shared scientific cause.

Wall time was approximately **3,731.36 seconds (62.19 minutes)**, derived from
the process-start observation and completed log mtime; rounded per-feature
durations sum to 3,720 seconds. Durations cover each feature's whole Country,
assembly and variant pipeline, not an individual table build or a measured
phase duration. Food prices took 971 seconds across 22 declaring countries,
baseline and six variants; food quantities took 902 seconds across 22 countries,
baseline and four variants. No timeout fired; the sequential path has no
enforced per-build timeout. The audit ran to completion without restart.

## Findings kept separate

- **19 Country build errors:** exactly the census's documented gaps: Armenia 2,
  Nepal 11 and Peru 6. They remain errors. There are no other build-error cells.
- **Five real tenure-vocabulary failures:** plot_features for Albania, Cambodia,
  Kosovo, Tajikistan and Timor-Leste. They match the pre-existing known-defect
  enumeration and ledger602 section7. Source/check/schema history and exact
  old-shared versus cold-private tenure-column equality establish continuity;
  they are not new mappings introduced by this release. The scanner retains
  all five FAIL statuses. Closed #602 fixed enforcement, not these mappings.
- **60 caught Feature country exceptions:** 52 missing-source/unwired warnings
  and eight missing-cluster-dependency warnings under market='Region'. GhanaSPS
  fails that variant for three food features, household_characteristics and
  household_roster; Panama for the three food features. These warnings exclude
  the affected countries from returned variants and are not equivalent to
  successful coverage. The Region dependency gaps predate the release; sample
  provides cluster identities but no functioning Region geography fallback.
  Independent review is in control/countries/market-dependency-review/REVIEW.md;
  GhanaSPS remains under open#140 and Panama is now tracked as#1009.
- **GhanaLSS true food-row loss:** an independent read-only replay confirms
  13,472 null-key rows deleted during Feature collapse, separate from legitimate
  visit aggregation. The mechanism is identical at v0.14.0, pre-train and frozen
  source. It is now tracked in #1008. Per-wave lost Quantity/Expenditure sums in
  TRIAGE.md are raw-column audit checksums, not comparable kilograms or an
  economic total across units, waves and currency regimes.
- **Aggregate-label country selection:** the assets and derived-food total
  drift warnings reconcile to the countries retained by Contract B, within
  stated floating-summation precision. This does not erase Ghana's null-key
  loss, unmatched labels, Nigeria asset-grain warnings or other semantic flags.
- **Remaining diagnostic warnings are retained for triage.** Index/schema
  mismatches, null content, scale warnings, noncanonical values, grain-loss and
  conversion warnings were not downgraded on the basis of expected=True or a
  successful build. This run completed deterministic phases1/2 and selected
  material investigations; it did not scientifically adjudicate all259 warning
  clusters.

TRIAGE.md records the source evidence, per-wave Ghana decomposition, independent
replay, exact tenure failure denominators, named country idiosyncrasies and
quoted documentation contradictions. No guessed vocabulary mapping, Region
fallback, null-key sentinel or nutrition baseline edit was made.

## Isolation, identity and authentication

Worktree: `/local/job39919092/release-work/runtime/corpus`, clean at completion.
Interpreter: `/local/job39919092/venv_img/bin/python` (Python3.11.6).
The launcher asserted imported package, country-config and data-root identity,
with matching cwd/PYTHONPATH/LSMS_COUNTRIES_ROOT. It pinned CPU0-7, one scanner
process, BLAS/OMP/MKL=1, LSMS_BUILD_WORKERS=1 and LSMS_MAKE_JOBS=1.

The Feature data root is
`/local/job39919092/release-work/runtime/data-feature-audit`, a physical copy of
the completed cold corpus: 1,519 files /340,885,785 bytes, all SHA-256 equal and
all regular-file inodes distinct when copied. Only the content-addressed L1
dvc-cache symlink is shared. The original
`/local/job39919092/release-work/runtime/data-corpus` remains quiescent and
unchanged: all1,519 file digests, sizes, mtimes, device/inode identities and the
file set verified after the scan; no in-tree country parquets exist.

The targeted Ghana replay used a third separate Ghana-only physical cache copy;
the independent reviewer used pure in-memory functions on read-only raw files.
The tenure comparison was read-only and made no Country/Feature API calls.
The private Feature copy may have changed its own caches during ordinary API
builds; it is not the preserved cold-corpus snapshot or a proposed shared-cache
promotion. In particular, the corpus's two hashless Uganda sibling outputs
remain preserved as evidence of #788/#809, with no manual restamping.

**LSMS_SKIP_AUTH=1 suppresses import-time setup/unlock/prompt only.** Existing
credentials and shared L1 were available, and ordinary lazy source access was
enabled. This was not a credential-free audit; see the known #1001 distinction.

## Aggregate archival package

`publishable/summary.json` records scope, totals, exact variant grid, timing,
authentication and timeout limits. `feature-coverage.tsv` records each feature's
declaring-country count, emitted records, call coverage and duration.
`check-counts.tsv` records feature+phase+check+status counts.
`caught-error-counts.tsv` records feature, phase, country, exception type,
category and kwargs without raw messages. `hard-finding-counts.tsv` preserves
the19 errors and5 failures. `cluster-counts.tsv` contains cluster IDs and counts
without raw patterns or examples. `provenance.json` records SHA-256 and sizes
of private raw artifacts, frozen harness/source files, launcher, aggregate
generator and preservation/input manifests.

The archived publish_summary.py accepts `--evidence-root
/local/job39919092/release-work/control/runtime/feature-audit` (also its default),
so its private input paths do not depend on the script's archival location.
Reproduction requires those private raw inputs and the frozen worktree; the
aggregate package alone does not contain survey-warning payloads.

Raw results.jsonl, scan.log, clusters.json, summary.json and replay logs stay
private because warnings can contain survey keys or example rows. This report,
TRIAGE.md, tenure-history.json, market-dependency-history.json, snapshot.json,
original-preservation.json, completion.json and the aggregate generator are
aggregate-only evidence suitable for review/archiving. No GitHub publication
or shared-cache promotion was performed by this lane.

The coordinator retains the release decision, remaining issue/documentation
follow-ups and Uganda's historical-nutrition scientific hold.
