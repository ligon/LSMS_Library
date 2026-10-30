# Uganda #1120 locating covers and missing plots

Search tier: ripgrep + git; GitNexus unavailable. Prior-art-ledger skill read
from /global/home/users/ligon/.sucoder/skills/prior-art-ledger/SKILL.md.

## §1 Task
Implement approved D1/D2 Uganda source-cover membership and own-cover weights,
and source-supported D3 without replaying #1350 or changing known-plot reducers.
## §2 Existing machinery
Reuse Uganda `v`, `comm_frame_ea`, `_format_agsec_hhid`, country df_edit hooks,
`reduce_to_agreed`, YAML dfs joins, and `Country._join_v_from_sample`.
Tests: test_uganda_323_grain, test_uganda_comm_frame_ea, test_uganda_crop_markers,
test_gh1198_uganda_production_observations. Reconcile retained 7631c13 / 8f75a4d,
never restore their whole module. Crop production already owns production_obs.
## §3 Definitions
Inherit STANDING.md §§3/4 and canonical lsms_library/data_info.yml.
Issue1120 comments 6367256988 and 6380186741: AGSEC1 hwgt_W5 is panel-only;
unlocated-{hhid} is epistemic singleton, moved-{hhid} denotes actual movers;
unknown_plot is epistemic, distinct from structural Farm-level.
## §4 Invariants
Preserve HH-primary values including nulls. Validate duplicates before merging;
no first/sum to conceal cover conflicts. Keep 2009 GPS-bin rule, early comm
crosswalk, later parish composites, and panel maps. No household coordinates
newly attached to clusters. CONTENTS historical "keep v=<NA> and are dropped"
is superseded by D2, not silently dismissed. Geography is not inferred.
## §5 Reuse
Extend country sample/cluster hooks and declare source union in YAML. Register
identity derivations in existing registry. Reuse crop production observation
identity for missing keys; do not alter current marker or reducer policy.
## §6 Root review
Plot labor's max reducer conflates distinct missing-plot rows and has no source
observation axis. A sentinel alone cannot repair this. Explicit residual review
required if source observation identity cannot be safely represented here.

## Phase 3 verification
- `locating_cover`, `locating_frame`, `_adopt_alternate_cover`, `sample`,
  `cluster_features`: OK (§§2–4), explicit membership and primary precedence;
  source duplicates checked before joins; registered derivation operands.
- `_plot_key`, `_missing_plot_observations`: OK (§§3–5), use canonical
  unknown_plot and existing input_obs/production_obs; release row coordinates
  only on missing plots, known-key eligibility/reducers unchanged.
- D3 labor: explicit residual (§6), 141 source observations / 95 current keys;
  coordinator assigned independent review and requested existing behavior.
  Prototype retained in dispatch evidence only, restored baseline source.
- Four real 2010-11 cluster rows are deleted by the existing all-null API gate
  on both baseline and candidate. CONTENTS.org "cluster_features is a PROJECTION"
  explicitly calls this the honest outcome for 42 households; preserve it as
  the documented located-keyspace exception, not an unresolved D2 decision.
- No shared library/schema implementation changed. All original sample values
  and known-plot payloads checked with masks/ExtensionArrays/nonmissing NumPy
  equality. 135 focused tests passed before the discarded labor experiment;
  final scope rerun recorded in dispatch evidence. Country/Feature all-wave
  sample/cluster and three plot waves exercised. Strict limitations documented
  in CONTENTS; no snapshot repinned and no umbrella closure claimed.
