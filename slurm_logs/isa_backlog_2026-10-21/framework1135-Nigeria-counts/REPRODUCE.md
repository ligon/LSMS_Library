# Reproduce Nigeria #1135 diagnosis

Read only, source f8d236694c26fe9e709c5548f02c70f74ea72c33. All launcher/scripts below live in /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts; run.sh explicitly cdsto /local/job40255479/worktrees/isa-Nigeria-1135 and sets exact mounted interpreter/package/countries/data, private caches/config/temp, single threads, unshare-rn and active guards. No runtime implementation.

Preparation steps prepare.py, prepare_sources_v2.py and prepare_panel.py are single-use physical-copy operations and assert destination absence. Do not rerun them over frozen copies. Their JSONreceipts contain manifest/sourceSHA, sidecarMD5, bytes/modes/mtimes/inodes/currenthashes/fullcoverage. Terminal preparation plus panel-source-proof precedes all normal APIcalls. Five document gaps remain explicit.

Executed probe sequence:

```sh
/local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/run.sh /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/oracle_phase1_v3.py
/local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/run.sh /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/sample_probe.py initial
/local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/run.sh /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/sample_probe.py warm
/local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/run.sh /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/count_probe_v2.py
/local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/run.sh /local/job40255479/control/isa-backlog-20261021/framework1135-Nigeria-counts/status_refinement.py
```

Initial/warmare separate processes, three new Country objects each. Exact kwargs and per-wavecounts are in sample-initial-safe.json/sample-warm-safe.json; full frames and warnings stay private. count_probe uses immutable private source/reference frames, not additional normalAPI/sourcecalls. Fourdirect native-code projections match every served v and key. Full source fields and old/newpair IDs remain private.

Safe results: DIAGNOSIS.md, PROPOSAL.md, ledger-DRAFT.md, counts-safe.json, status-refinement-safe.json, validation.json, freeze-proof.json. Existing 97synthetic1135tests were not repeated; no source builds/fullscanner/faultcatalog or broadtests were needed. Preserved failed harness attempts and complete warnings remain in private logs; no productbehaviorwaspatched to obtain success.

Sue
