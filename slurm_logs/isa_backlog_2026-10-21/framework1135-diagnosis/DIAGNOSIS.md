# GH #1135 diagnosis - read only

Pinned source: f8d236694c26fe9e709c5548f02c70f74ea72c33, diagnose/1135-cluster-warning-size-20261021.
Owner: Sue (GPT-6). No implementation, tracked edit, Git mutation, source build, current survey data call or GitHub write.

The mismatch is real and bounded: `sampling._format_cluster_report` gives a first-line header without sizes or individual boundaries; `audit_cluster_identity` drops any numerical measurement from its report; `_PANEL_FIELDS` rejects an added count block. SPAN/no-threshold are already correct. Completing the numeric promise for all twelve boundaries is blocked by evidence, not by the warning formatter.

The human ruling is settled: "H2: keep SPAN as built" and "H3: no threshold", with the warning's first line stating "clusters renumbered and the share of linked households whose `v` changes, net of movers" for every boundary. The ruling is in the complete root-provided live context, including its sole comment. No policy question about span or thresholds remains.

## Existing machinery and current behavior

`sampling.py:189` freezes panel field names. `_check_panel` (:565) validates continuity and its own provenance. `SamplingRecord.from_config` (:431) rejects unknown nested fields. `_freeze` (:300) already provides recursively immutable JSON-compatible dict/list subclasses. `sampling_records` (:634) honors countries_root and caches records. `attach` (:739), `merge_attrs` (:769), and `records_from_attrs` (:792) already handle Country/Feature metadata and plain parquet-roundtrip dictionaries. Reuse them.

`audit_cluster_identity` (:912) selects declared wave-scoped/redefined/mixed boundaries by country wave order between the first and last present wave; it does not compute sample statistics. It requires a nonempty DataFrame carrying v and at least two known waves. `_format_cluster_report` (:961) prints the first sentence of each note below the header, capped at 240 characters. Nigeria's first note sentence does not contain its linked-household denominator. Parsing that note is neither reliable nor authorized.

`check_cluster_identity` (:994) returns the identical frame object. Nested internal table fetches stay silent; once-per-outermost-call deduplication and the process report ledger already exist. Strict uses the dedicated `ClusterIdentityError`, which deliberately evades generic derived-table fallbacks. Feature can still drop a strict-refused country under its existing degrade behavior. None of these policies needs editing.

## Country records read in full

All five CONTENTS files, including TODO/WAITING/LOGBOOK entries, were read. Ethiopia (2,350 lines) and Uganda (3,604) reuse my immediately preceding complete immutable-f8 readings, with exact current SHA256 equality against review1081 receipts. Tanzania (2,111), Nigeria (2,166), GhanaLSS (5,838) were read completely here. `source-pins.json` records the exact blobs.

- Ethiopia: Panel I W1-W3 is independent of new Panel II W4-W5; W2/W3 use ea_id2 rather than ea_id. A linked-household id embedding an EA can hide movers. W5 Tigray is outside the represented population. The overview's "rural and small-town areas in waves 1-3" conflicts with the dated correction limiting that restriction to W1. That is a quoted documentation tension, not permission to reinterpret sampling counts.
- Tanzania: UPHI tracks a Round-4 descendant line backwards; r_hhid identifies the interviewed household in its own round. The four 2008-15 rounds share one physical folder. Extended 2019-20 and Refresh 2020-21 are disjoint. WAITING #114 remains live despite the issue's closure. The script says "the NPS TRACKS movers" and "1,607 of the 8,359 panel lines ... change clusterid" (`2008-15/_/sample.py:68`); CONTENTS' round-3 entry instead establishes whole-cluster renumbering. Its quoted old DESIGN reading "panel-stable; 20% are movers" conflicts with that later evidence. Raw UPHI lines and served linked-household counts are different layers. The round-4 linkage caveat (reused r_hhid strings) remains unresolved.
- Uganda: 2005/2011 comm are 8-digit integers, 2009/2010 comm are 7-character strings; later v changes from EA to parish code to district/parish names. 2009 v mixes frame EAs with synthetic coordinate bins, with 24 missing-tail households. Original movers and split-offs are distinct. The quoted "one third ... refreshed each wave" is in tension with the 2015 BID's "This same sample ... 2015/16"; the 2019 panel-weight TODO remains open. A rendering mismatch is not proof of a physical move or redraw. The sampling note's 2,607 linked households at 2005->2009 and the held H3 output's 2,601 are not interchangeable: the latter explicitly requires v on both sides.
- Nigeria: ten PP/PH API labels map to five physical wave folders. v is state/lga/ea, not ea alone. ea=0 produces a per-household moved- singleton; a whole-EA LGA recode is a separate event. Refreshed and long-panel households have different linkage coverage. CONTENTS explicitly says "Every other changed v at a PH -> PP boundary is a mover's moved-{hhid} singleton" (:508); the older DESIGN table instead called these changes "sample expansion". W4 inaccessible-area coverage also qualifies national boilerplate. No ID or weight correction is proposed.
- GhanaLSS: GLSS1->2 is a designed rotating half-panel; later rounds are independent. Raw PANELC links 714 households; the historical served sample links 712. CONTENTS preserves the contradicted claim "there is no rotating panel or refreshment sample mechanism" opposite the GSS statement "Half the workloads ... were therefore retained for GLSS2". It also preserves the former "200 EAs, fresh draw" row with its dated correction. EA/workload counts must not be conflated. The #1008 cover-gap recovery does not license silently changing a denominator; its recovered households are explicitly absent from PANELC.

Proposed future note addition: distinguish historical pairwise sample count, raw tracking-line count, exact movement carrier/exclusion rule, linked denominator, and proved cluster renumbering count. Specifically record that h2h3's moved-prefix exclusion is not a general mover adjustment. Keep dated contradictions and live caveats.

## Held historical measurements and provenance

Root physically copied five original PR1137 artifacts from durable scratch into `root-historical-1137/`, after verifying SHA256, size, mode, mtime, distinct inodes/single links, and original-after equality. My diagnostic probe independently reverified all five destination receipts. Their historical scripts were read, never executed. Initial inspection looked for the receipt in the parent directory and got ENOENT; it is inside root-historical-1137. The root had already supplied terminal SUCCESS; the later probe used the correct receipt and preserved the failed command output.

Exact measurement code: `root-historical-1137/h2h3.py`. It calls normal `Country(C).sample()`, then selects t/i/v, drops nulls, and keeps the first record per (t,i). Its denominator is the intersection of served household IDs with non-null v at both endpoints. It does not prove duplicate-key unambiguity before first-wins selection. It turns v to string only after dropping nulls. The code records exact integral linked/keep values in h2h3_pairs.csv and exact integral all-changed/ex-moved-prefix values in h2h3.out; printed decimal shares are rounded and were not promoted. The original data/cache hashes and complete historical frame snapshots are not among these five artifacts, so the old counts are not independently current-source reproducible from this set alone.

The crucial line is `mover = va.str.startswith('moved-') | vw.str.startswith('moved-')`; `nm = ((va != vw) & ~mover).sum()`. This excludes a representation-level Nigeria sentinel. It does not consult any actual movement/status carrier in Tanzania or Uganda. Tanzania 2019's own record says tracked households genuinely moved, so calling all 849 historical changed households a net-of-movers numerator would contradict the recorded evidence. No clusters-renumbered column is computed anywhere in these H2/H3 artifacts.

All twelve currently curated boundaries correspond exactly to this historical table. These are historical served-prefix measurements, NOT fresh f8 sample results:

| Country / boundary | historical linked N | all changed D | changed after only moved- exclusion P | exact P/N | clusters in held H3 output |
|---|---:|---:|---:|---|---|
| GhanaLSS 1987-88 -> 1988-89 | 712 | 712 | 712 | 712/712 | not recorded |
| Ethiopia 2011-12 -> 2013-14 | 3776 | 3776 | 3776 | 3776/3776 | not recorded |
| Tanzania 2010-11 -> 2012-13 | 2904 | 591 | 591 | 591/2904 | not recorded |
| Tanzania 2012-13 -> 2014-15 | 521 | 108 | 108 | 108/521 | not recorded |
| Tanzania 2014-15 -> 2019-20 | 849 | 849 | 849 | 849/849 | not recorded |
| Uganda 2005-06 -> 2009-10 | 2601 | 2601 | 2601 | 2601/2601 | not recorded |
| Uganda 2009-10 -> 2010-11 | 2640 | 821 | 821 | 821/2640 | not recorded |
| Uganda 2010-11 -> 2011-12 | 2510 | 2510 | 2510 | 2510/2510 | not recorded |
| Uganda 2011-12 -> 2013-14 | 1717 | 1717 | 1717 | 1717/1717 | not recorded |
| Uganda 2015-16 -> 2018-19 | 2973 | 2973 | 2973 | 2973/2973 | not recorded |
| Nigeria 2016Q1 -> 2018Q3 | 1493 | 79 | 10 | 10/1493 | not recorded |
| Nigeria 2019Q1 -> 2023Q3 | 5067 | 343 | 112 | 112/5067 | not recorded |

## Twelve-boundary evidence requirements

| Boundary | Available corroboration and its layer | Additional movement/cluster evidence required | Held status here |
|---|---|---|---|
| GhanaLSS 1987->1988 | exact historical N/P; CONTENTS/YML say 79 paired retained clusters; PANELC says 714 raw links | served sample + verified panel walk, pair table and Appendix-I location agreement; rule out movement using evidence beyond note; count 79 as observed paired floor, not retained design total | historical aggregates/scripts held; numeric cluster pair table and movement classifications not in copied set |
| Ethiopia 2011->2013 | exact historical N/P; prose describes 333 complete bijective EA pairs | current walked sample pairs and verified Y2 ea_id/ea_id2/geovars authority; movement separation if claiming net-of-all-movers | aggregate/scripts held; no structured cluster/movement output copied |
| Tanzania 2010->2012 | exact historical 591/2904; raw prose 1565/7082 UPHI lines, 83 raw pairs versus 82 served changed pairs | upd4_hh_a.dta round2/3 UPHI + r_hhid + clusterid, exact finalizer lineage, prove which whole-cluster pairs renumber rather than relocate | aggregates/scripts held; no cluster-pair or movement-mask artifacts copied |
| Tanzania 2012->2014 | historical 108/521; prose separates 58 reversions and 10 new-format households; linkage ambiguity remains | same UPD4 carriers round3/4 plus Extended/Refresh membership ha_07_1; audit reused r_hhid linkage and classify every changed linked HH | aggregates held; final cluster count and mover exclusions unavailable |
| Tanzania 2014->2019 | historical 849/849 but known genuine movement and nonbijective pairs | UPD4 round4 bridge to HH_SEC_A.dta sdd_hhid; clusterid/sdd_cluster/t0_* and hh_tracking_status, current-only location answers; verify labels and interval semantics, distinguish split-offs | exact historical prefix numerator held; true net-of-movers numerator/cluster count unavailable |
| Uganda 2005->2009 | historical 2601/2601; comm rendering plus synthetic bins | GSEC1.dta hh_status (2009 mover=2, split-off=3, nonmover=1) plus comm and panel walk; verify status reference period, classify missing/unknown status and orig-nonmover anomalies; bins are not clusters | historical figures held; joined movement classification/true count unavailable |
| Uganda 2009->2010 | historical 821/2640; bins resolve to populated comm | both GSEC1 files, hh_status semantics per wave and origin/current reference; distinguish bin replacement from actual cluster renumbering | historical prefix count held; mover-adjusted numerator and physical cluster count unavailable |
| Uganda 2010->2011 | historical 2510/2510; prose 322 pairs, 272 digit insertion +50 district recodes | exact 322-pair table, source comm dtype/namespace and movement/status proof; no digit truncation | historical HH count held; pair table/movement output not copied |
| Uganda 2011->2013 | historical 1717/1717; EA->parish changes kind | GSEC1 comm/h1aq4a and panel bridge; interval-specific movement classification; report changed identity kinds without asserting an EA/parish bijection | historical counts held; true mover-adjusted numerator unavailable; literal physical renumbering count not established |
| Uganda 2015->2018 | historical 2973/2973; code->district/parish name composite | gsec1/GSEC1 parish_code/name/admin fields and panel bridge; verified movement labels/period; source-key kind change is not a bijective recode | historical counts held; net movement/renumbering count unavailable |
| Nigeria 2016Q1->2018Q3 | historical 10/1493 after69 moved- exclusions; prose one whole-EA recode | current served samples/panel identity; actual covers' state/lga/ea/hhid show ea=0 sentinels and whole-EA reassignments; exact earlier real-cluster pair set | exact old numerator/denominator held; cluster1 only prose, not copied structured measurement |
| Nigeria 2019Q1->2023Q3 | historical112/5067 after231 moved- exclusions; prose12 whole-EA recodes | same carriers, current pair table and whole-cluster coherence; no inference from repeated state/ea alone | exact old counts held; cluster12 only prose, not copied structured measurement |

"Unavailable" refers to the required structured measurement/proof in this diagnosis's prepared set. It does not assert the underlying raw blob is absent anywhere on the node. No shared L1/L2 was inspected, and no raw-source availability census or acquisition was attempted. Geography agreement, a bijection and a changed ID alone do not prove no household moved.

## Scope, compatibility and validation

97 existing sampling tests passed, 7 data-dependent cases deliberately deselected (`-m 'not requires_s3'`), not silently skipped. The private regression draft gives 4 expected baseline failures and 1 pass: schema acceptance, report count transport, first-line size, unavailable fallback fail; existing SPAN/no-threshold/data identity passes. All inputs are synthetic. An independent synthetic two-row replay matched all 119 historical SPAN/ADJ outcomes exactly (77 SPAN,12 ADJ endpoint matches); it remeasures policy selection, not household counts.

Existing nested record storage was tested with synthetic explicit-null measurement fields through JSON-compatible metadata, physical private parquet, sanctioned get_dataframe, records_from_attrs, exact logical attrs/frame equality and immutable nested mutation rejection. This proves the existing representation supports the proposed shape; from_config still rejects the new field, as the negative test shows. No legacy parquet migration/schema/hash stamp is needed. Future count metadata intentionally enriches sampling attrs; all other attrs and data remain exact. Feature's existing sampling carrier should carry the new nested block without modification.

Static hash assessment: sampling.yml is excluded by suffix from Wave._input_hash (:1326) and Country._table_cache_hash (:4261); only per-wave data_info.yml and country data_scheme.yml are named YAML inputs. The sampling reader/report paths are outside build closures by current tests and `_EXCLUDED_CALLABLES` (:235-236). A bounded sampling.py + five sibling sampling.yml change therefore has a grounded zero-change expectation. No implementation exists, so an actual candidate all549/39 delta is NOT claimed. Measure complete baseline/candidate inventories in separate exact-source processes once a boundary is approved; fail if any actual hash changes. Do not solve a mismatch by excluding new dependencies.

`bench/feature_audit/scan.py` still does not drain sampling_reports; that existing #942 residual is separate. A focused fake-Country/Feature carrier check and one prepared current-cache Country sample reproduction suffice after implementation; no full source/scanner sweep is justified for a reporting-only patch. Real cache prerequisites require a separate preparation decision before reads/builds.

Full outputs and failed commands remain private. Guarded probes ran in unshare -rn with connect guards and exact import/cwd/countries/data assertions, numerical/build/Make threads1. Only private roots were writable; no checkout .dvc/tmp was created. Three outside-root write attempts were denied during guarded imports/tests; no tracked changes resulted. Runtime logs and full failures are retained rather than suppressed. Final freeze proof records exact original tracked files/modes/index/HEAD/clean and process quiescence.

Sue
