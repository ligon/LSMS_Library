# Draft release notes: approved framework increment

This is draft text for the approved framework increment at `532d58700fa46e52a0b390198604eca1dc4de00e`. It is not a complete release history since v0.15.0. The listed PRs remain open, except the merged Windows expectation repair; this draft does not announce or authorize a 0.16.0 release.

## Returned data and derived quantities

- Explicit metric labels now parse the entire decimal-comma token, preserving fractions, multipliers and unit boundaries while refusing malformed suffix matches. Measured outputs change for 945 quantity rows and 783 price rows across Senegal, Benin and CotedIvoire. ([PR #1156](https://github.com/ligon/LSMS_Library/pull/1156))
- Seed mass, nitrogen mass and median-price valuation reuse declared unit-mass factors, including country-specific factors carried by pooled input metadata. Ethiopia gains 706 usable Quintal price observations in the measured valuation pool; its native input frames and seed/nitrogen totals remain unchanged. This does not change price-pooling policy. ([PR #1154](https://github.com/ligon/LSMS_Library/pull/1154))
- `seed_kg`, `nitrogen_kg` and `fertilizer_rate` preserve an actual `country` index or column as reduction identity. Country-less outputs retain their existing shape. See the migration note for pooled, empty and invalid-input cases. ([PR #1164](https://github.com/ligon/LSMS_Library/pull/1164), [PR #1165](https://github.com/ligon/LSMS_Library/pull/1165))
- A strict subset of known waves with no declaration of the requested table returns a typed, declared empty frame where no genuine country builder exists. Cold, warm and partially cached requests agree, without replacing a usable cache for other waves. Genuine source/build failures still fail. ([PR #1155](https://github.com/ligon/LSMS_Library/pull/1155))

## Provenance, cache reads and verification

- Provenance parse/render retains unknown fields and repeated or empty metadata occurrences. Held-release facts remain separate from latest catalog observations; conflicting rewrites and incomplete acquisition/push receipts cannot certify a new held release. This is metadata protection, not raw-file staging or rollback. ([PR #1163](https://github.com/ligon/LSMS_Library/pull/1163))
- An assumed-fresh parquet disappearing at its data read falls through to the ordinary cache/build path. Permission, corruption, mapping and source errors remain visible. Forced-cold regression tests use isolated physical cache copies. ([PR #1162](https://github.com/ligon/LSMS_Library/pull/1162))
- The canonical farm audit now includes crop production, plot labor and plot inputs, and reports loss of a whole positive-built country separately from row conservation within retained countries. ([PR #1157](https://github.com/ligon/LSMS_Library/pull/1157))
- The Windows shocks digest expectation now reflects the existing cp1252 decoding correction; this increment does not introduce a new decoder change. ([PR #1153](https://github.com/ligon/LSMS_Library/pull/1153))
- Uganda nutrition invariance accepts one additional measured exact raw fingerprint while preserving its primary approved fingerprint, structural checks and all other baseline entries. This test-only allowance does not broaden the data fixture or introduce numerical tolerance; future unmeasured fingerprints still fail. The failing CI kernel itself was not observed. ([PR #1161](https://github.com/ligon/LSMS_Library/pull/1161))

## Validation and remaining limits

At the exact source above, the local HPC suite completed with **8,851 passed, 276 skipped, 9 xfailed and 1 xpassed**, with no failures or errors (736.35 s). Native Windows build, unit and smoke jobs passed. Credentialed Linux [run 39533863014](https://github.com/ligon/LSMS_Library/actions/runs/39533863014) remains pending as of 2026-10-21 03:40 UTC. Installed-package validation covers packaging contents and a configuration smoke check, not every installed-wheel microdata path.

The earlier paired corpus/audits retain 19 source/wiring gaps and five tenure findings (#602); this is not an all-green corpus claim. EthiopiaRHS coarse crop grain remains a visible audit exclusion. Existing #1110 includes Malawi's 33,402 plot versus 11,622 parcel rate rows, contradicting an older "deliberate no-op" docstring; parcel science and source policy are not resolved here. Acquisition staging (#1149) and the proposed CI enforcement change (#932) remain unapplied. Issue-closure reconciliation and release approval are separate work.

Sue
