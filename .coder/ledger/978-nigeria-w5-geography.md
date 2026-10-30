# Nigeria #978: acquired W5 household geography wiring

## §1 Task
Finish the remaining served wiring over public188adf03 (1311/1318/1328 merged).
Reuse the completed1285 acquisition; never replay or transplant the old
fix/978 branch, which contains acquisition sidecars only. Diagnose W5 cached
sources and preserve own-round covers and labelled PH same-location identities.
No publication, acquisition or new coordinate representative policy.

## §2 Existing machinery (Git/ripgrep/AST floor and connected GitHub)
- `ROUND_COVERS` and `cluster_features_for_wave`, Nigeria/_/nigeria.py:
  existing geo tuple `(file,latitude,longitude)`, household-unique extraction
  through df_data_grabber and LEFT joins onto actual round_covers. Extend W5
  metadata only; no parallel extractor. Covered by gh1120 helper tests.
- `round_covers` / `derive_ph_same_location_v`: own cover > supplement > own
  roster, W5 labelled hh_moved=2 plus PH ea=0 permits COMPLETE known PP key.
  Reuse unchanged. Existing tests/test_gh1120_nigeria_{round_covers,same_location}.
- `Wave.cluster_features` / `_collapse_to_cluster_grain` /
  `most_complete_row`, country.py: audited Site2 projection selects one
  observed household row. Reuse unchanged; never average coordinates.
- `tests/nigeria_round_oracle.py`: recorded_round / recorded_geo and explicit
  nullable + NumPy exact numeric assertions independently check recorded keys.
- Existing978 acquisition ledger and1149 staged-acquisition ledger bind the
  held v02 release; no acquisition code belongs in this task.
- `get_dataframe` / `_dvc_cache_path`: sanctioned cached reader and cache
  availability probe. Five exact W5 blobs (geo, PP/PH covers/rosters) exist at
  their declared L1 paths with matching sizes; this is stat evidence, not yet
  independent byte-integrity or source-value verification.

## §3 Definitions and country caveats
Inherit STANDING.md, CLAUDE.md and canonical data_info.yml. Nigeria CONTENTS
read completely, then its public188adf03 updates: one directory/two logical
rounds; numeric state/LGA/EA v, moved-/unlocated- singletons, own-round source
membership/weights, only labelled rule-qualified complete own-PP identity reuse.
Source coordinates are household fixes; a cluster-selected fix is not an EA
centroid or independently verified original sampling-location coordinate.
CONTENTS says v02 HELD and NOT YET WIRED, while obsolete tests still describe
v01 as held. Record this contradiction explicitly rather than infer acquisition
failure. Historical112/708 versus34/510 multi-fix counts used different keys;
diagnose PP, PH raw and PH refined keys separately, not an averaging choice.
The nonfood RecallWindow WAITING caveat remains unrelated and unchanged.

## §4 Invariants
Only wire verified `hhid`, `lat_mod`, `lon_mod` from root Data/v02 file.
Preserve sample rows/weights/strata, physical/logical visit mapping, all
household-grain non-coordinate fields, round membership and identity/provenance
rules. Cluster projection may select a different observed household when
coordinates improve completeness; measure every selected non-coordinate and
Derivation change rather than assume invariance. LEFT
join may not introduce geo-only households. Keep Site2 disagreement reporting
and the selected observed row; do not average, round or silently discard fixes.
No optional:true bypass or broad cache clear/rebuild. New W5-only verification
is not a replay or extension of previous accepted cohorts. No other country or
framework/scientific selector change. Existing held-source budget is not reused
as permission for broad acquisition. All reads go through sanctioned helpers.

## §5 Reuse decision
Extend only W5 geo tuple and relevant prose/tests if actual-source diagnosis
confirms the declared key/coordinates. Reuse existing compiler, reducer,
oracle and reader; report within-key disagreement and source displacement
metadata. One bounded local1CPU/8GiB verification in the existing normal-QoS
Slurm40561868 is sufficient unless evidence requires a separate normal job.
Use private data/config/L1 pointers and thread limits1; no shared cache writes.

## §6 Verification and remaining gates
W5-only source diagnosis verifies4883 uniquehhid/55columns, all paired coords;
PP112/708 raw groups decompose into real34/510 plus ea0pseudo78/198. PH raw
real24multifix becomes31under existing approved refined identities; all31
repairedHH have fixes. No key or coordinate policy changes.

One genuinely new candidate comparison against exact public188adf03 preserves
sample/HH payload/weights. Existing Site2 produces PP911/886pairs, PH900/900;
selectedHH changes14/0 and selected Region/District/Rural/Derivation changes0.
Every complete selected pair equals its actual selected sourceHH. The current
guard refused one import-time urllib3._has_ipv6 socket constructor and exits1
after scientific assertions/results; keep this distinct from clean acceptance
and earlier unknown-caller diagnosis. No replay to clear a guard badge.

Eight scoped invented-source tests pass (zero raw/source-cache reads); peer's
five independent boundaries pass on productd07. The obsolete W5 no-geo routing
case is corrected by test-onlyee7013, without product changes or peer replay.
An additive SOURCE-IDENTITY receipt hashes the same five blobs after comparison:
all explicit hash:md5 rawMD5s match, with actualSHA256/size; original path/size
qualifications remain historically true. No decoding/build rerun for hashing.

Root/peer final ordinary review and publication remain. No external writes or
merge, API/Make/Feature/warm-cache/all-wave replay, acquisition, country-specific
stayer filter, averaging or claim of visit-specific physical location. Physical
EA-location interpretation remains an explicit #161/#841/#978 limit, not a new
unapproved representative policy. Source arrays/IDs are not exported publicly. Module/schema whole-file hashes
can invalidate other Nigeria table caches; no fingerprint census, cache clear
or rewarm is performed for that implementation cost.
