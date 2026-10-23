# Prior-Art Ledger DRAFT - GH #1135 cluster warning sizes

Private diagnosis draft only; no Git edits or implementation authorization. Inherits .coder/ledger/STANDING.md. Search tier: ripgrep + git + explicit Python metadata/synthetic probes; no gitnexus/delegation.

## §1 Task, restated

Carry provenance-backed per-boundary cluster-renumbering and linked-household-change sizes into ClusterIdentityWarning's first line without changing existing SPAN/no-threshold trigger or served data. All12curated boundaries need honest measurement availability; historical moved-prefix exclusion is not net of all movers.

## §2 Existing machinery

| Symbol | Source | Existing proof | Decision |
|---|---|---|---|
| SamplingRecord/from_config/_freeze | sampling.py:300,431 | test_sampling schema/immutability/JSON/parquet cases;97pure cases pass | extend one optional panel block; reuse storage |
| _check_panel/_PANEL_FIELDS | sampling.py:189,565 | TestThePanelDeclaration | extend validated field vocabulary; do not bypass freeze |
| sampling_records/_sampling_path | sampling.py:627 | shipped config/override tests | reuse; no runtime sample fetch |
| audit_cluster_identity | sampling.py:912 | TestTheAudit;119synthetic historical request pairs independently exact | keep selection; transport count metadata |
| _format_cluster_report | sampling.py:961 | TestTheWarning; private4fail/1passcounterfactual | extend first line for every boundary |
| outermost_call/check_cluster_identity | sampling.py:882,994 | strict/nested/dedup/dataidentity controls | reuse unchanged |
| attach/merge_attrs/records_from_attrs | sampling.py:739,769,792;feature.py:151 | tests+private exact logical parquet roundtrip | reuse unchanged |
| source sample/id_walk | STANDING§2/4, country.py | no current data calls in this diagnosis | only reuse under a future prepared data scope |
| prior provenance placement | .coder/ledger/603-population-record.md§3-5 | sibling YAML/read-time attach avoids build inputs | reuse; no data_scheme rewrite |
| historical boundary measurement | copied h2h3.py/h2h3.out/h2h3_pairs.csv |5copies receipt-reverified,119pair policy replay | reuse historical exact N/P as provenance, not current R/K |

## §3 Definitions & conventions

STANDING§3 defines v as sample-owned cluster identity. The #1135solehumanruling keeps SPAN/no threshold and requires first-line boundary sizes net of movers. `_PANEL_FIELDS`/Uganda sampling.yml say derived quantities excluded; QUOTE: "DERIVED QUANTITIES DO NOT BELONG IN THIS FILE." The newer ruling specifically authorizes measured boundary counts here. Document that limited exception; do not widen unrelated PSU/weight/design metrics.

N is unique served post-id_walk household links with both v nonmissing. R is changed-v households attributable to the boundary event after actual movement exclusion, over the full N denominator. Historical P excludes only moved-prefix IDs. K is distinct earlier real sampling clusters proved renumbered, not pairs, HH, synthetic bins or singleton sentinels. See PROPOSAL for non-bijective/kind-change unavailable handling; no physical interpretation may be invented.

## §4 Invariants & assumptions

STANDING§4 sanctions source IO and protects IDs/attrs/build placement. No datum, eligibility, v, grouping, table schema, fixture, tolerance or source changes. Preserve all12declarations and every existing warning firing decision. Unavailable is null with reason, not0. Metadata counts need separate measured provenance. Existing note source may be documentary while counts are derived. The historic first-wins(t,i)measure is not a new production key policy. Geographic agreement or clean ID bijection alone cannot rule out movement. No actual Country sample/cache read, source build or raw availability claim was made here.

## §5 Reuse decision

Reuse current immutable record, carrier, report ledger, wave ordering, strict/dedup and footer compatibility. Extend loader validation/reporttransport/firstline only after approval. New optional cluster_id_measurement block is justified because no tested current field carries typed counts. No new runtime count engine: it cannot infer physical movement from arbitrary served frames, or see skipped endpoints without fetching data. Numerical population remains separately gated on evidence. Anticipated hash0/0 derives from existing YAML suffix and excluded read closures; actual candidate549/39comparison is a later mandatory measurement, not yet claimed.

## §6 Decisions and evidence gaps

No open H2/H3policy decision. All12exact historicalN/P held; cluster counts and all-mover masks absent from these artifacts. root will choose whether to authorize a partial mechanism and a bounded Nigeria source-measurement followup. No numeric current-production backfill may be approved solely from prose. PROPOSAL names the required per-country carriers and residual closure limits.

## Phase3 diagnosis verification

OK(§2/4):97existingpure tests passed;7explicit data cases deselected. OK(§3):historicalscript proves narrower prefix exclusion; report does not call it actual net-of-movers. OK(§2):119synthetic request policy results match77SPAN/12ADJ. CONTRADICTION(§3,exposednotimplemented): frozen schema rejects new count field; first-line contract fails4private regressions while existingSPAN/dataidentity passes. OK(§2/4):nestedrepresentation synthetic JSON/parquet/fullattrs exact, no tracked change. No implementation verification or current-source measurement is claimed.

Sue
