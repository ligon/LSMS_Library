# Reproduce frozen Malawi diagnosis

Use the existing image Python and de9 checkout. All raw inputs were already held; do not acquire missing documentation.
`run.sh` asserts exact import/config/data realpaths in the analysis scripts and activates a socket deny guard. The sole restricted raw read reuses the unchanged production IHS4 XS reader because the sanctioned full reader fails on its documented unused bad-byte string. Header inspection uses authorized metadata-only pyreadstat.

The original run used private copied prerequisite L2; plot_inputs wave and country L2 were absent. This is a normal target-cold API proof, not an all-table/all-prerequisite-cold claim. No assume_cache_fresh or hash stamping bypass was used.

Run in a **new** private root; the preparation script refuses an existing directory. These commands do not remove or mutate a cache. Set the new path to another unused private name when rerunning.

```bash
cd /local/job40255479/worktrees/isa-Malawi
export MALAWI1111_EVIDENCE_ROOT=/local/job40255479/control/isa-backlog-20261021/Malawi/1111-reproduction
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/prepare_from_held.py "$MALAWI1111_EVIDENCE_ROOT"
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/census_api.py > "$MALAWI1111_EVIDENCE_ROOT/api.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/source_census.py > "$MALAWI1111_EVIDENCE_ROOT/source.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/conservation.py > "$MALAWI1111_EVIDENCE_ROOT/conservation.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/analysis_matrices.py > "$MALAWI1111_EVIDENCE_ROOT/matrices.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/merge_source_checks.py > "$MALAWI1111_EVIDENCE_ROOT/merge-source.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/supplement.py > "$MALAWI1111_EVIDENCE_ROOT/supplement.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/final_aggregates.py > "$MALAWI1111_EVIDENCE_ROOT/final-aggregates.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider tests/test_1109_malawi_input_quantity.py > "$MALAWI1111_EVIDENCE_ROOT/tests.private.log" 2>&1
bash /local/job40255479/control/isa-backlog-20261021/Malawi/1111/run.sh /local/job40255479/control/isa-backlog-20261021/Malawi/1111/freeze_check.py > "$MALAWI1111_EVIDENCE_ROOT/freeze.private.log" 2>&1
```

Each Python command starts a fresh process. In particular, conservation.py verifies a warm ordinary Country call against the preceding cold frame and all attrs, and independently checks source organic answers and f8 nonorganic preservation.

Optional held-document extraction uses `instruments.py` and `extra_docs.py` through the same wrapper. PDF reader is the root-provisioned pypdf5.0.1 under `/local/job40255479/control/isa-backlog-20261021/pdf-tools/pypdf-5.0.1`, not a project dependency. Source manifest records unheld2013 and absent2016 authority; no fetch occurs.

Public issue snapshots were read with GitHub tools and saved in github-1111.json, github-1111-comments.json, github-1118.json and github-1174.json. They are dated evidence, not live authority for a later code task.

Safe aggregate outputs: api-census.json, source-census.json, conservation.json, analysis-matrices.json, merge-source-checks.json, supplement.json, final-aggregates.json, codebooks.safe.json and freeze-validation.json.
All `*.private.*`, held bytes, caches, row frames, source members and warning/log text stay private. No raw identity or frame belongs in a commit/GitHub message.

Sue.
