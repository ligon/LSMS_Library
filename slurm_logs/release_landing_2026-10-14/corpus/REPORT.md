SCOPE DEVIATIONS

One early read-only CotedIvoire comparison omitted taskset/BLAS pins; it was
repeated with the required pins and gave the same exact result. Details are in
SCOPE_DEVIATIONS.md. No source, test, fixture, dependency, DVC sidecar, or shared
L2 edits were made. No exceptions were hidden or assertions weakened.

The full declared corpus completed at source
`7fa05179c8519f44ea329ba6387f5d6d6441a8ea`: **38 countries, exactly 549 unique
Country.data_scheme pairs; 530 success, 0 empty, 19 error**. This includes
noncanonical, JSON, derived and unavailable declarations. Wall time was
4,527.97 seconds (75.47 minutes), with at most two country subprocesses and
serial tables within each. Every worker asserted package, config, data-root
and source identity. The source worktree is clean and contains no parquets.

The 19 errors remain in the denominator, with tracebacks: Armenia 2 (catalog
documentation without microdata), Nepal 11 (licensed sources not acquired),
and Peru 6 (declared but unwired tables). No other table errors occurred. The
country notes in FINDINGS.md name survey-specific idiosyncrasies and quote
contradictions rather than assuming documentation is stale. In particular,
Nepal's claim that all features return empty frames contradicts today's loud
missing-source errors; Peru intentionally preserves declarations for its
unwired tables.

The original private root is **quiescent and preserved** at
`/local/job39919092/release-work/runtime/data-corpus`. The worker completion
record is completion.json. Snapshot-manifest.json records SHA-256, bytes,
mtime and inode for all 1,519 files (340,885,785 bytes). Only dvc-cache is a
symlink, pointing to the shared content-addressed L1. There are no hardlinked
or symlinked writable parquets. The separate Feature audit root is
`/local/job39919092/release-work/runtime/data-feature-audit`; its initial copy
was verified byte-for-byte with distinct inodes. The coordinator is authorized
to copy the preserved original for independent cache-presence tests.

At each completed call, all 424 materialized country parquets had their
expected hash and full declared-wave manifest; 125 calls had no country
parquet (including derived/JSON/error cases). Final snapshots, after all
sibling builds, contain **422 fresh country parquets and two hashless Uganda
siblings: earnings and fct**. Their final manifests are also absent. The generic
footer helper calls hashless files legacy; the actual L2-country read policy
is stale and rebuilds them under #809. The income/nutrition scripts rewrite
those sibling outputs, the mechanism documented under #788. We preserved this
evidence without restamping. All other final country manifests cover the
declared waves. The per-call logical-wave checks recorded 582 fresh and 601
hashless script-cache observations, with zero stale wave hashes. Those are
observations, not a count of unique physical wave files.

Shared cache snapshots were read-only comparison evidence, not assumed truth.
Of the 1,365 shared files, 1,295 had unchanged recorded content and 70 changed;
154 new files have no shared counterpart. No shared files were omitted from
the rebuilt snapshot. FINDINGS.md and the named comparison JSON files explain
the differences. Material examples include the already-implemented Tajikistan
sotka-to-hectares conversion (old country total 88,238, rebuilt 882.38), the
reviewed Niger identifier/unit repairs, Nigeria W5 clusters and sold-unit
column, Burkina zero-quantity repair, Panama value-unit repair, Peru keys,
and Uganda's reviewed food-chain candidate. Some apparent changes are only
row order or pre-finalization wave transforms; exact comparisons distinguish
those. Existing source defects and scientific caveats are not declared fixed
merely because execution succeeded.

Uganda completed all 28 declarations. Its eight changed shared-cache entries
are five food entries and three crop entries. The food/FCT/nutrition chain
exactly matches the independently reviewed candidate manifest; nutrition is
23,777 x 15 with raw fingerprint
`12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a`.
The historical nutrition scientific decision remains pending and no fixture
patch was applied. Crop production is 133,331 rows, matching the documented
#848 second-condition-slot wiring, with +1,422 rows in 2018-19 and +1,303 in
2019-20 versus shared state. Quantity, Quantity_sold and Value_sold totals
match CONTENTS's recorded after values. Named idiosyncrasy: the second slot
usually reports the dried form of a crop whose first slot is fresh, while
identical unit/condition collisions still aggregate; the open grain question
is retained. Ninety-one Uganda cache files had no shared counterpart.

Reproducibility and evidence:

- Frozen worktree: `/local/job39919092/release-work/runtime/corpus`.
- Runner and environment: run_corpus.py and run.sh; ordinary API from empty
  L2, no NO_CACHE or assume_cache_fresh, CPU 0-7, BLAS/OMP/MKL 1,
  LSMS_BUILD_WORKERS=1 and LSMS_MAKE_JOBS=1.
- Scope and identities: inventory.json and each country's identity.json.
- Per-table results: each country's results.jsonl (shape, columns, dtypes,
  served digest/sums, null counts, wave coverage, warnings, elapsed time,
  expected/stored hashes and footers); errors retain full diagnostics.
- Shared/final cache evidence: each country's shared-baseline.json and
  private-final-cache.json, plus cache-comparison.json and progress.json.
- Human interpretation: FINDINGS.md; concise totals: country-summary.tsv
  and table-summary.tsv; precise original-file identity: snapshot-manifest.json.
- Feature audit is a separate gate using the existing scan.py harness with
  --features all --phases 1,2 --include-no-microdata --jobs 1. Its results and
  report live in the sibling feature-audit control directory.

Remaining follow-up drafts: preserve the #788/#809 sibling-rewrite cache
finding; record Tajikistan's tenure-tagged plot IDs and unit provenance in
CONTENTS and reconcile its contradictory data_scheme comment; reconcile the
quoted country documentation discrepancies in FINDINGS.md; retain existing
country scientific/source residuals and Uganda historical baseline hold.
