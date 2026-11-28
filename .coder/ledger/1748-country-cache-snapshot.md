# Prior-art ledger — #1748 verified country-cache snapshot race

**Base:** 47df62d49e614c9a56e319def7324145d138c3d9. **Search:** git/ripgrep and call-site review.

## §1 Task

A country L2 cache can be replaced atomically between path-based coverage grading
and opening the payload. Source-free reproduction: a full-wave footer admits
the request, but a same-hash subset payload is served. Repair the verified
L2-country read race, not the whole intermittent hosted test issue.

## §2 Existing machinery

| Symbol | Anchor | Contract / existing tests |
|--------|--------|---------------------------|
| `_read_l2_country_cache`, `_read_assumed_fresh_cache` | `country.py:4952,5178` | normal freshness/coverage, assumed freshness skips hash and degraded checks; `test_gh891_wave_subset_cache.py`, `test_gh1071_managed_cache_recovery.py` |
| `_cache_covers_waves`, `_restrict_to_waves` | `country.py:4812,4858` | manifest is build evidence, not observed row labels; full request passes through NULL t; `test_gh891_wave_subset_cache.py` |
| `_read_parquet_with_index_policy`, `_read_managed_parquet_cache`, `_reject_unreadable_cache` | `local_tools.py:2151,2167`, `country.py:320` | same-stream index policy, only payload IO/decode errors recover, preserve observed identity and null audit; `test_gh1071_managed_cache_recovery.py` |
| Hash / wave / audit footer readers and `cache_freshness` | `local_tools.py:3613–3772` | existing metadata keys and absent/malformed/exact-hash grading set parity expectations |
| Build exclusions | `_build_registry.py:119` | two country API gates do not determine stored bytes; `test_build_transform_hash.py` and metadata-only census |

## §3 Definitions

Standing ledger §0 and CLAUDE.md Cache Behavior are authoritative. Valid
coverage does not imply one observed row for every covered wave (#891). The
#1071 recovery ledger preserves ownership and error boundaries. Assume-cache
is not a freshness or degradation check. Healthy old parquet bytes remain
usable without metadata migration or a build fingerprint change.

## §4 Invariants

The admitted descriptor supplies hash, coverage, any build/audit witnesses
and decoded payload even if its path is replaced or unlinked. Ordinary warm
hits still avoid source/Make/DVC and misses avoid decoding. Never delete a
replacement or a borrowed live input when a different identity is observed.
Keep #1071's best-effort check, not an atomic compare-and-unlink guarantee.
Preserve miss reasons/handoffs, legacy coverage rules, footer/strict-warning
errors, null audit, parquet engine,
and the pre-finalize row/index/string policy. No changes to wave writers,
DVC stage, Other-copy promotion, raw readers, or output schema.

## §5 Reuse/extension

Use one parsed footer and the existing metadata keys, stream index-policy
decoder, null audit and exact-entry rejection on the ordinary,
borrowed-input, and assume-cache country gates. A dedicated country footer and
managed payload adapter leave hashed path-based readers byte-identical.
The build registry resolves possible methods by bare name, so the country's
new footer method names deliberately do not collide with existing build
symbols such as `waves`. No registry exclusions were added.

Do not change the explicit strict nonborrowed `_recover_unreadable=False`
fallback, which keeps the old path-based `get_dataframe` and its raw/format
error policy. No production call site uses that combination. The private
Make prerequisite admission call passes `_borrowed_input=True` as well; its
validation reads a snapshot, but the separate path-based copy/promotion
transaction is unchanged. No inode polling or locks on the warm-hit path.

## §6 Open scope

Real hosted #1748 test parameter and original traceback are not public. A
verified cache defect is not attribution of that failure. No survey source,
cache/credential access, or live build is required.


## Verification against the ledger

- OK (§2–§4): synthetic same-hash replacement reproduces 2 → 1 waves on the
  base. The regression keeps both admitted waves, a NULL key, and the
  corresponding grain/date/drop metadata across ordinary, assumed and
  borrowed routes. The short-manifest miss decodes no rows and closes its
  stream. Full missing manifests, absent/bypassed caches and exact old-inode
  rejection retain the scoped contracts. Existing #1040 and #1071 tests cover
  post-read error policy, real corrupt payloads, and fresh rebuild handoff.
- Source-free verification: 163 passed, 3 deselected. Selected
  `test_gh1748_country_cache_snapshot.py`, `test_gh1040_wave_read_gates.py`,
  `test_gh1071_managed_cache_recovery.py`, `test_dvc_caching.py`,
  `test_gh891_wave_subset_cache.py` and `test_build_transform_hash.py::
  test_l2_country_read_gate_is_not_folded_into_any_build_fingerprint`. Run on
  the extracted repository Python 3.11 image with package identity asserted,
  a network namespace, one CPU, private state and `-m 'not requires_s3'`.
  Expanded verification after compatibility companion: **406 passed, 2
  skipped, 3 deselected** in 221.12 s. Added GH1066, GH1071 private Make,
  GH809, GH920, GH1051 b/c/e and the full build-transform hash module.
  No held-source tests ran.
- An independent prebuilt-cache witness on the same previously written Ethiopia
  cache changes 26,130 → 4,999 rows on the base, versus 26,130 → 26,130 after
  the fix. This uses previously prepared L2 snapshots, with no raw source
  or build in the witness. It does not identify the hosted failure.
- OK (§2–§4): metadata-only census from the exact base to tested code
  `2130510ddf4e7e745a46232c0d80455a7413e528` found **zero** changes across
  556 country hashes, 2,323 wave hashes, 42 build-transform entries (including
  the global fingerprint), and 38 framework-import entries. Both runs had
  zero errors. Reproduce with the repository's
  `slurm_logs/isa_backlog_2026-11-13/tools/hash_census.py` in isolated roots.
