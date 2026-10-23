# GH #1135 Nigeria boundary-count diagnosis - READ ONLY

Source: f8d236694c26fe9e709c5548f02c70f74ea72c33, diagnose/1135-nigeria-boundary-counts-20261021. Sue (GPT-6).

The current source-backed served counts reproduce the historical N/P controls, but the historical interpretation of P as "net of movers" does not survive the actual movement fields. W5 has 480 recorded movers with nonzero EA:461 nearby and 19 far. Its112 real-ID-change households include six recorded nearby movers and 20 unknown movement statuses. True boundary-interval movement-adjusted R/K remain unverified; neither112/5067 nor12clusters may be promoted as that complete result.

No warning/schema/runtime change is made. SPAN/no threshold remain settled. No source builds, acquisition, DVC CLI, cache bypass/stamp, shared-cache writes or GitHub writes occurred.

## Country notes and source path

The full2166-line Nigeria CONTENTS reading, including all TODO/WAITING/LOGBOOK, was carried only after verifying its exact current SHA25693bcd7230736ddfdf607f3b9a4a53dbc9cdbed297fca16489b79171d8d90a65f against the frozen prior full reading. The checkout is clean f8. Actual idiosyncrasies: five physical wave directories expose ten separate PP/PH API labels; sample uses the planting cover for BOTH labels in each wave; v is the state/lga/ea composite, not ea alone; refreshed and long-panel samples differ; ea0 produces a per-household moved- singleton. Thus2016Q1->2018Q3 and 2019Q1->2023Q3 are API boundaries, while their sample-v sources are plantingw3->plantingw4 and plantingw4->plantingw5.

`Nigeria/_/nigeria.py:15-34` declares waves/folder mapping; its cluster_id(:108) maps ea0 to moved-{hhid}. `2015-16/_/data_info.yml:6`,2018-19:6 and 2023-24:21declare the exact covers and composite. W2/W3 panel_ids specs declare previous_i=hhid, so these are identity edges rather than a new crosswave identity transformation. There is no Nigeria country sample.py or panel_ids.py. All relevant code/config remains unchanged.

A reportable tension is preserved rather than called stale. Country data_scheme says "the panel follows households that move while keeping their ORIGINAL ea"; the helper says a tracked household "that left its original EA is recorded with ea == 0". These may distinguish kinds/times of movement; neither proves completeness of ea0 for the human ruling. CONTENTS says "Every other changed v at a PH -> PP boundary is a mover's moved-{hhid} singleton". The source confirms the structural change partition, while its real-ID-change cohorts ALSO contain recorded dwelling movers. A real EA code and recorded dwelling movement are not mutually exclusive.

Proposed future country note: document the exact movement variable labels/codes below, explicit missing status, and the distinction between an EA sentinel, a current dwelling response and an interval-specific cluster identity event. Do not revise IDs or classify missing responses as nonmoving.

## Preparation before dependent probes

Root's ignored countries/.dvc/tmp symlink resolves to this task's owner-private dvc-tmp; root-metadata-preparation.json pins it. Every other checkout write is denied. Mounted image Python, exact cwd/PYTHONPATH/countries/data realpaths, private XDG/config/temp/DVC/matplotlib, unshare-rn and active connect/write guards were asserted. Numeric/build/Make thread counts are1; one consumer per data root.

The1519-parquet manifest is `/local/job40255479/control/release-v016/verification-prep-seed1519/copy-manifest-private.json`. Only six manifest-listed Nigeria parquets were physically copied: var/sample, the country _/panel_ids companion, and four W2/W3 panel wave inputs. The seed's sixteen additional unmanifested DTAs were excluded. Each copy matched manifest SHA256/size/mtime, source/destination mode, distinct inode/single link and original-after bytes. No seed symlink to shared L1 was copied.

The actual f8 sample hash is50a08899f967862e064284e829e3e0ae9dd9836d21f1a30d35e5e2243fa254f7 and equals the stored hash. Its manifest explicitly covers all ten Country.waves labels. All four panel wave parquets match their actual current Wave hashes. The country companion legitimately has no embedded hash/coverage; it was NOT restamped or described as hash-fresh. Before any normal sample call, its complete19110-row frame was checked exactly against the four current-hash wave frames; their unique keys and identity edges were independently checked against MD5-verified W2/W3 sources.

Static trace correction is retained: the enclosing _aggregate_wave_data initiates lazy updated_ids before it invokes its nested fresh-cache reader. The companion IS a dependency of these new-Country calls. Actual route tracing later observed exactly one lazy panel read and one L2 sample read per call; no wave or country builder ran.

Shared held L1 was read only. Physical private copies of four covers passed sidecar-MD5 and SHA/size/mode/mtime/source-after checks. Both supported cache layouts were considered; initial flat-layout-only failure is preserved, followed by successful files/md5 preparation. The successful terminal receipt precedes raw decoding or normal APIs.

| Declared source / identity dependency | sidecar MD5 |
|---|---|
| Nigeria/2015-16/Data/secta_plantingw3.dta | 55ab9db6cf41894fdf6b762621db5ebd |
| Nigeria/2018-19/Data/secta_plantingw4.dta | 939a0e15129478043a67749247b03c73 |
| Nigeria/2023-24/Data/Post Planting Wave 5/Household/secta_plantingw5.dta | 8b8c9968e887770e0cccd6cac83a1431 |
| Nigeria/2012-13/Data/Post Planting Wave 2/Household/secta_plantingw2.dta | 9600a3ab631fb06ba87ede371f03c0e9 |

Five relevant instrument PDFs were absent in BOTH shared held layouts at the exact sidecar keys below; no acquisition was attempted. This is a precise source gap, not a claim that the documents do not exist upstream or anywhere else on disk.

| Required instrument path | sidecar MD5 |
|---|---|
| Nigeria/2015-16/Documentation/PP_W3_Household_Quest.pdf | c40e97568cf203a6735e71f9dd16f9b2 |
| Nigeria/2018-19/Documentation/pp_w4_household_quest.pdf | da641e8c226502f32b80183b1de59a7b |
| Nigeria/2023-24/Documentation/ghs_panel_w5_pp_household_questionnaire.pdf | c096dce419806f6d0eecf91606c28a63 |
| Nigeria/2023-24/Documentation/ghs_panel_w5_pp_interviewer_manual.pdf | 4c0f81f1f720f4d84f3bc1328f57b542 |
| Nigeria/2023-24/Documentation/ghs_panel_wave_5_basic_information_document_v1.pdf | bd68d6b4bd1f6292113d80c507ce1f09 |

Raw observations were decoded only with get_dataframe; get_data_file supplied private materialized paths and MD5 was rechecked. A pyreadstat metadata-only read of that sanctioned private file supplied variable labels; no second raw observation reader was used. Native full frames remain private. `categories_only=True` emitted one RuntimeWarning about tracked_obs's categorical mapping; the unconverted observations were unaffected, and the complete warning log remains private.

## Actual source fields

- W4 hhsmlc: "DOES THE HOUSEHOLD LIVE AT THE SAME LOCATION?";1="1. YES",2="2. MOVED WITHIN LOCALITY". Among5263 source households:1354 code1,126 code2,3783 missing. The1493 linked cohort has 1354/126/13respectively. All ten real-ID-change households report1.
- W5 hh_moved: "HAS THE HOUSEHOLD MOVED FROM THE ORIGINAL LOCATION?";1="1. YES, MOVED CLOSE TO ORIGINAL DWELLING",2="2. NO, IN ORIGINAL DWELLING",3="3. YES, MOVED FAR FROM ORIGINAL DWELLING". Across5067 households:585 code1,4299 code2,20 code3,163 missing. All5067are served linked households at this API boundary.
- W5 trackedw4/ trackedw5pp are separate "WAS THIS HOUSEHOLD TRACKED..." yes/no fields; all 112 real-ID-change rows carry2 in both. This does not establish why20hh_moved responses are missing or their movement state. Do not treat a tracking response as a nonmovement response.
- W3 ea has the explicit value label0="0. Moved". W4/W5ea have no corresponding value-label map in the held covers. Code ea0 and the served moved-prefix mask agree on every examined source/served row; that validates the formatter representation, not an all-movement predicate.

The current cross-tabs directly disprove completeness for recorded movement. W4 has 83 code2 nearby-move households with ea!=0. W5 has 461 code1 nearby and 19 code3 far with ea!=0. Conversely254ea0W5 rows report code2original dwelling; ea0 cannot simply be relabelled "moved at this boundary" either. Full counts/status/null cross-tabs are in counts-safe.json; raw pair tables and IDs remain private.

## Current counts and definitions

N is the unique served household-i intersection with nonmissing v on both sides, after the normal API's panel finalization. We asserted one unambiguous i/t before counting; no first-wins or drop-duplicate policy was introduced. All served household sets and every served v match the direct native-code cover projection for the four requested endpoint labels.

D counts every changed v. P excludes changed rows if EITHER endpoint has the ea0/moved-prefix sentinel. P is the historical rule, not the ruling's all-mover R. The movement-response counts below are exact observations; their interpretation as the specified preceding-boundary movement interval is conditional because the instruments defining "same/original" reference location and skip logic are unheld.

| API boundary | N | D | sentinel-caused changes removed | P | reported same/original among P | recorded moved among P | unknown among P | coherent distinct earlier real-ID groups |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|2016Q1->2018Q3|1493|79|69|10|10|0|0|1|
|2019Q1->2023Q3|5067|343|231|112|86|6|20|12|

Historical1493/10 and 5067/112reproduce without forcing agreement. The first P share is10/1493=0.67%. The second is112/5067=2.21%, explicitly sentinel-only. If W5's response categories apply to the intended interval, the nonmover numerator is bounded86..106by its20 unknowns (1.70%..2.09% over the FULL5067denominator). Do not remove mover rows from N. Without that interval interpretation, these are status-conditional bounds, NOT established true-interval R bounds. No exact true R is claimed.

The ten W4 cohort households fill one old EA and one new EA,10 source/served households on each side; all report the same location, their state and EA serial remain fixed, only LGA changes, and neither old/new composite appears in the opposite wave. This is one coherent source-ID reassignment supported by reported nonmovement, not ten clusters.

The112W5 cohort households form12distinct earlier real-ID groups with an injective old/new relation, all same state/EA serial and changed LGA. No old composite appears later or new composite earlier. The earlier full groups contain114 households, the later groups 112; the linked sizes are eight groups of 10, two of 8, one of 9 and one of 7. Counting observed old groups gives 12; counting household changes gives 112 and is wrong for a cluster metric. Ten groups have at least one reported-original-dwelling anchor. The other two groups are exactly20 unknown-status households. Thus a flag-interpreted anchored-group count is10, with two additional coherent but movement-unclassified groups; a true physical/interval renumbering K is not fully established by this alone. Do not turn12 into an unqualified physical-cluster claim or10 into a complete total.

## Normal API and fresh-process warm preservation

Six ordinary new-Country calls ran, with exact arguments recorded: default sample(), sample(waves=['2016Q1','2018Q3']), sample(waves=['2019Q1','2023Q3']) in one initial process and again in a separate warm process. These are normal CURRENT-HASH L2 reads, not source-cold rebuilds.

Default serves 49770 rows across ten labels:5000/5000/4859/4859/4696/4696/5263/5263/5067/5067. The paired calls serve9959 and 10330 rows. All have unique i/t and zero missing v. The separate-process frames are exact on every value/null, row/column/index order/name/dtype and full logical attrs. All three warning category/message lists are exact; each call emits one existing ClusterIdentityWarning. No full warning example is exported.

All14 private data/cache files stayed byte/size/mode/mtime exact through both API stages. The six parquets, four physically copied L1blobs and foursanctioned private raw materializations were accounted for; reference pickles stay outside data_root. No builder ran and no new parquet or derived cache was manufactured.

## Preserved harness failures and limits

Private initial attempts remain with their scripts/logs: a setup syntax error before execution; flat-layout-only L1 lookup; an overly narrow assumption that get_data_file would return a dvc-cache path rather than its legitimate private materialization; NumPy integer dictionary keys in metadata JSON; and equality testing derived boolean masks with different pandas dtypes. Followups corrected only the diagnosis harness. No library, input or tolerance was edited. Full served-frame dtype equality remains strict; only the mask proof compares boolean values. There was no product failure hidden by these corrections.

No independent physical relocation truth or instrument-bound temporal definition is asserted. The cover observations support the bounded facts above without requiring a missing PDF for every fact. #1135's other ten boundaries retain the exact evidence gaps in the frozen original diagnosis. PROPOSAL.md gives a useful explicitly partial reporting option and a live-issue update draft. No implementation was approved or attempted.

Sue
