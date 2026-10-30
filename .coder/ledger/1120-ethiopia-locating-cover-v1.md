# Prior-Art Ledger — GH #1120 Ethiopia locating covers

**Search tier used:** ripgrep + git floor. No GitNexus tool is exposed.
Baseline: `373af66f0390b59a919b32db1b42cd8086ee6beb`, tree
`5bea654a41bdeb855a90cb34137785ab7be2a987`.

## §1 Task, restated
Extend Ethiopia's five sample drivers and household-grain cluster hook under
approved D1/D2/D3: locate agriculture-only households from recorded covers,
preserve primary household-cover rows, distinguish unlocated singleton clusters
from recorded moves, and inventory source-missing plot keys before any repair.
Panel closure is a dependency, not a new linkage policy.

## §2 Existing machinery
| symbol | path | purpose / tests | decision |
|---|---|---|---|
| sample drivers | `lsms_library/countries/Ethiopia/<wave>/_/sample.py` | HH weights, strata and order; `tests/test_sample.py` | extend |
| retained D1 six nodes | authenticated context `prior/retained-D1-helper-reference.py` | recorded W2/W3 EA relation, exact holder agreement, HH-priority append, new-EA hook; historical tests reference only | reconcile narrowly |
| panel generator | `lsms_library/countries/Ethiopia/_/panel_ids.py` | W2 baseline and W3 identity chain | extend only after oracle closure |
| independent oracle | authenticated context `prior/panel_cover_oracle_v2.py` | reviewed `_rows`, `_select`, `analyze_covers`, `compare_mappings` | reuse scientific functions |
| `_normalise_sample_weights` | `lsms_library/country.py:7595` | mean-one on nonmissing wave values; `tests/test_weight_normalisation.py` | reuse unchanged |
| `_join_v_from_sample`, `_finalize_result` | `lsms_library/country.py` | framework ID walk, v joins, dtype/attrs and missing-row policy | reuse unchanged |
| cluster projection | `Wave.cluster_features`, `_collapse_to_cluster_grain` | observed-row audit; `tests/test_grain_collapse_cluster_features.py`, `tests/test_gh323_ethiopia_config.py` | reuse unchanged |
| `_plot_id_from`, plot constructors | `Ethiopia/_/ethiopia.py:2630` and siblings | holder/parcel/field keys; existing plot tests | inventory first |
| `unlocated_id` | `Nigeria/_/nigeria.py:402` | absence distinct from recorded mover | precedent, not Ethiopia source authority |
| geovariable attachment | `Ethiopia/_/ethiopia.py:936` | full native field key; `tests/test_ethiopia_plot_geovariables_1150.py` | preserve native percent |

## §3 Definitions & conventions in force
Inherits `.coder/ledger/STANDING.md` §§3–4 and `CLAUDE.md` IO, sample,
grain and derived-value contracts. Canonical `lsms_library/data_info.yml`
`Single Index.plot_id`: `unknown_plot` is epistemic; `Farm-level` is structural.
Country `CONTENTS.org` sections Sampling Design, plot_features, GH #837,
GH #1150, and all dated corrections control grain and provenance.
Existing `Ethiopia/_/derivations.yml` window registrations remain unchanged.

## §4 Invariants & assumptions
- Panel I W1–W3 and Panel II W4–W5 are separate. W2/W3 emit household_id2.
- W3 PP ea_id needs recorded relations from BOTH W2/W3 HH covers; prefix only
  checks a recorded mapping. Ambiguous or absent relations refuse.
- Holder payload/EA/weight/codes, including missing-versus-present, must agree
  before stable reduction. No approximate equality or imputed weights.
- HH precedence and first-seen order survive. New cluster rows are only for
  EAs missing from HH results; geo joins use households, never EA cartesian joins.
- Historical YAML centroid prose conflicts with later CONTENTS/core observed-row
  selection; preserve the latter. Missing GPS is not permission to fabricate it.
- Rural definitions change; the catalog/cover panel-weight tension is unresolved.
- Sanctioned IO, owned node-local caches, approved interpreter, one CPU, network
  fallback denied, finite selected inputs and bounded stages. No shared writes.
- Retained broad branch and historical counts are not acceptance oracles.

## §5 Reuse decision
Extend the existing drivers with narrowly reconciled D1 helpers. Reuse the
reviewed scientific panel oracle with current runtime/custody only. Do not
change committed maps without exact source-proved deltas. D2 requires finite
HH/PP/necessary PH inventory before absence; D3 requires raw missing-key census.
No speculative plot sentinel or unrelated scientific-helper edits.

## §6 Open questions for the coordinator
Independent review requested for any newly discovered cross-visit cover conflict;
no new D1/D2/D3 policy is requested. Actual source closure and final independent
review are pending. Historical oracle design approval is not product acceptance.

### Phase 3 — verification
Pending. No product edits or actual survey reads at ledger creation.

### Phase 3 — executed result (2026-10-30)
Product checkpoint 9f3ff012 extends only six retained helper nodes, five sample
drivers, and the panel generator/maps. Existing helper function ASTs, including
#1352 native-percent covariates, are unchanged. Country notes record findings.
55 targeted tests pass (new D1 composed controls, #1150, normalization).
64/67 finite source roles were read; three W1–W3 plot geovars unavailable.
All five HH/PP/PH sets and household geos were read. Source D3 census found no
missing holder/parcel/field. Two W3 PH rows lack both household identifiers;
no invented singleton household or speculative plot repair was committed.
The six-cover independent scientific oracle and private generator agree in both
directions. W2 links +25/3776 unchanged; W3 +27/12 corrected/4942 unchanged.
Native sample HH payload/order/masks/attrs and raw weights are exact. Composed
Country/Feature/full/subset/strict checks pass for sample26130 and cluster2179;
all located sample keys covered. Warm5.15s preserves12cache artifacts. W3 crop
25999/labor70312 survive Feature/Region/strict reads without missing keys.

Failures remain distinct: staging duplicate-MD5 alias assertion before reads;
failed raw-geovar retrieval; failed cold actor whose observer altered parquet
caller resolution; provisional D2 null-ID failures removed after raw identity
inspection. Corrected composition reuses verified, explicitly hash-stamped
native samples and is NOT a clean cold acceptance run. Final independent product
review is pending; author tests are not that review. Root evidence REPORT.md,
source-plan, receipts, per-actor counters and closed private stage witnesses
carry the finite custody/command details. Remaining scope: independent review,
clean cold acceptance under an approved future budget if required, and missing
plot-geovar source acceptance. No broad historical suite or branch was replayed.

### Current independent review and settled anonymous-row ruling

Independent Burkina review supports the retained D1/panel delta and qualified
composed evidence (review SHA256
012d165fea7b9f5ae835de30e33c7f14b35ecb60e522c2ae04e881d97685ed06).
The earlier pending-review statements above describe author-stage history.
Country attrs were checked by value equality, not a recursive typed comparator.
Runtime Feature checks are named-axis/payload/mask/count checks; separate closed
earlier/warm Feature pickle byte equality is not a stronger executed typed
Country-to-Feature metadata assertion. Preserve the failed cold run, composed
native cache stamps and three unavailable current-run plot-geovar objects.

The maintainer settled GH #1357, choosing option 1: continue excluding anonymous
records from household-keyed tables. Ruling:
https://github.com/ligon/LSMS_Library/issues/1357#issuecomment-6526192658
The two W3 source-row facts/private audit remain; no new household identity,
orphan representation, holder-prefix inference or constructor change is made.
This decision is no longer an outstanding human choice. It does not establish
positive or otherwise-eligible payload loss. D2 singleton and D3 unknown-plot
implementations were not justified by this finite inventory. Refs #1120/#660;
no umbrella closure or clean-cold acceptance is claimed.
