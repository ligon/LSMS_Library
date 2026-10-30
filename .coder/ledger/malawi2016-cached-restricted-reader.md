# Malawi 2016 plot-input restricted reader

## §1 Scope

Extract only the reviewed held-L1 reader repair from
fa1485d04c78337cbbfac936ee2dd7ad7271ef02 onto public
123fef6aff119c71402ab9ae6e9a72790ad52969. Search tier: Git/ripgrep.
No new quantity, observation, unit, ownership, eligibility or namespace policy.

## §2 Existing machinery

`local_tools._dvc_cache_path` (503–545 at the tested base) already resolves
sidecar-addressed held blobs for `get_dataframe`, including both DVC layouts.
`_ensure_dvc_pulled` remains the existing best-effort population step.
`Malawi/2016-17/_/plot_inputs.py:_read_usecols` is the restricted reader for
the selected Module D columns; full decoding encounters an unused bad text byte.
Its one caller and its pyreadstat/temporary-file path remain unchanged.
The four new actual-helper AST-isolated tests cover cache/fallback and
decoder success/failure without importing a top-level data driver.

## §3 Definitions

Inherit STANDING.md, CLAUDE.md and data_info.yml. This remains the documented
restricted-reader exception, not a new ordinary raw-reader API. Read only the
same source bytes/usecols with apply_value_formats=False. Source-qualified G
input_obs and H ownership remain #1323; #1174/#1190/#1278 stay separate.

## §4 Invariants

No shared helper/cache policy, DVC metadata, unit decoding, producer reducer,
source declaration, plot key or emitted payload changes. Retain DVCFS.open
fallback when no held path resolves; propagate open/decoder failures and
delete the temporary file in finally. No native cohort replay or cache clear.

## §5 Reuse and verification

Reuse `_dvc_cache_path` after the existing ensure step rather than constructing
DVC's repository-wide storage index for a held blob. Exact product/test/docs
bytes come from reviewed2aed82f1135ae7cd61ec064e521604b61c22a87c.
Four invented tests pass; patched normal Country/Feature/warm run is bounded
evidence. Independent review SHA256
dc097984f07c06218384f84b790e684335e69a2d71e624cbd7a75f013447bb29.
Latest public Malawi/framework/source-helper bytes match the tested base.

## §6 Remaining work

The original direct Make arm stopped after two completed natives and partial
2016 work; country compiler execution remains missing. Raw-concat/finalizer
composition remains non-exact on index/attrs. Actual compiler verification
with authenticated sealed native prerequisites is a separate next unit, not
acceptance supplied by this extraction. Other restricted readers and nine
other targets are unchanged. Preserve all original failed receipts.
