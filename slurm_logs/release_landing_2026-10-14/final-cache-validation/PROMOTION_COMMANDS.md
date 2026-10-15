These commands are prepared for parent review. No real cache operation has
run. `promotion-manifest.json` does not yet exist: the final validation must
produce the exact reviewed file/retirement allowlist first.

Manifest contract
-----------------

The JSON manifest has `version: 1`, the exact `final_sha`, the fixed
`source_root` and `shared_root` shown in PLAN.md, and
`validation_status: reviewed_for_promotion`. Its `files` mapping is keyed by
relative parquet path; every entry includes `sha256`, `bytes`,
`disposition: promote_verified`, and one of these classifications:

- `country_fresh_full_coverage`: also requires matching non-null
  `stored_hash`/`expected_hash`, `declared_waves`, and `footer_waves` covering
  those waves. This is the only permitted `var/` classification.
- `wave_fresh`: retain stored/expected hash and logical/physical wave evidence.
- `script_wave_legacy_verified_provenance`: retain the cold/final-build input
  provenance and the explicit hashless-wave policy; never use for `var/`.
- `companion_verified_provenance`: an explicitly reviewed country `_/*.parquet`
  companion with source/build/value provenance.

Additional row/column/fingerprint/coverage/provenance fields remain in each
entry for review. The optional `retire` mapping explicitly lists omitted old
files with `disposition: backup_then_retire` and a reason. Nothing else is
deleted. Every candidate and exclusion must have a recorded disposition;
the manifest is not inferred from a broad filesystem glob at apply time.

Preparation and read-only snapshot
---------------------------------

After the parent reviews the final manifest and tool, fix the exact digest
and the final source SHA below. A shell variable is not an approval: use the
digest recorded in the parent's review, not an unreviewed freshly changed file.
Record verified session facts: the known session builders have finished or
use private roots, and the shared inventory is unchanged before/during staging.
Do not claim a global lock on external builders; this tool has none. Any known
shared writer must be stopped/coordinated before the operation.

```bash
promotion_control=/local/job39919092/release-work/control/runtime/final-promotion
promotion_manifest="$promotion_control/promotion-manifest.json"
promotion_digest=REVIEWED_MANIFEST_SHA256
promotion_final_sha=FINAL_DEVELOPMENT_SHA
promotion_transaction=/global/scratch/fsa/fc_jevons/ligon/cache/.lsms-promotion-FINAL_SHA_PREFIX-UNIQUE_SUFFIX
promotion_note='VERIFIED_SESSION_BUILDER_STATUS_AND_UNCHANGED_SHARED_INVENTORY_FACTS; NO_GLOBAL_LOCK_CLAIM'

taskset -c 0-7 /local/job39919092/venv_img/bin/python "$promotion_control/promote_cache.py" snapshot \
  --manifest "$promotion_manifest" --manifest-sha256 "$promotion_digest" \
  --final-sha "$promotion_final_sha" \
  --snapshot "$promotion_control/shared-prepromotion.json" \
  --quiescence-note "$promotion_note"
```

Snapshot only reads shared L2 and writes its inventory into local control.
It skips L1 entirely. The next command writes only the new transaction
directory alongside the shared root, staging physical copies and backups
metadata; active L2 is still unchanged. Stage refuses a pre-existing transaction
directory or any shared parquet change since the snapshot.

```bash
taskset -c 0-7 /local/job39919092/venv_img/bin/python "$promotion_control/promote_cache.py" stage \
  --manifest "$promotion_manifest" --manifest-sha256 "$promotion_digest" \
  --final-sha "$promotion_final_sha" \
  --snapshot "$promotion_control/shared-prepromotion.json" \
  --transaction "$promotion_transaction"
```

Actual promotion (only after the parent reviews the exact allowlist)
------------------------------------------------------------------

```bash
taskset -c 0-7 /local/job39919092/venv_img/bin/python "$promotion_control/promote_cache.py" apply \
  --manifest "$promotion_manifest" --manifest-sha256 "$promotion_digest" \
  --final-sha "$promotion_final_sha" --transaction "$promotion_transaction" \
  --quiescence-note "$promotion_note"

taskset -c 0-7 /local/job39919092/venv_img/bin/python "$promotion_control/promote_cache.py" verify \
  --manifest "$promotion_manifest" --manifest-sha256 "$promotion_digest" \
  --final-sha "$promotion_final_sha" --transaction "$promotion_transaction"
```

Apply checks the complete live parquet inventory against the fresh snapshot,
verifies every staged file, and records/fsyncs an intent before each operation.
Existing targets receive verified recoverable backups. Replacements use
same-filesystem `os.replace`; explicitly retired paths are backed up then
unlinked. Already byte-identical files are left alone. Final verification
checks exact resulting paths/bytes and full metadata identity of unlisted
parquets. No library call occurs, so the reviewed candidate cannot rebuild
under the promotion tool.

An interruption leaves `state.json`, `journal.jsonl`, staged files and backups.
The tool refuses to resume a partial apply blindly. Inspect the journal and
preserve the quiescent window. A journalled rollback is available:

```bash
taskset -c 0-7 /local/job39919092/venv_img/bin/python "$promotion_control/promote_cache.py" rollback \
  --manifest "$promotion_manifest" --manifest-sha256 "$promotion_digest" \
  --final-sha "$promotion_final_sha" --transaction "$promotion_transaction" \
  --quiescence-note "$promotion_note"
```

Rollback works from intents, including a crash after replacement but before
the completion journal entry. It refuses to overwrite a file changed by
another writer; it restores verified old bytes, retains backups, and verifies
the original fileset and contents. Original inodes are not restored and are
not claimed to be. The transaction never swaps the root, touches dvc-cache,
clears a country globally, or deletes its evidence automatically. Directory
or journal leftovers after a crash require inspection, not blind cleanup.

Synthetic validation only: promotion-synthetic-checks.json records six passing
scenarios and the exact tested tool hash. No real source/shared/candidate
cache paths were accessed by those scenarios.
