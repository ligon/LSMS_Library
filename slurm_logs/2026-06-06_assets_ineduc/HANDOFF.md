# Session handoff — 2026-06-06 — assets + individual_education coverage fill + DVC lock

Node n0055.savio3, job 34958831. Scrum-master session (Sue / Claude Opus 4.8).

## Shipped (PRs open into development)

- **PR #333** `feature/coverage-fill-2026-06-06` — 5 (country×feature) cells, all
  pass `is_this_feature_sane`:
  - Guatemala `assets` (ECV17E14; t,i,j; Qty=p14a02/Value=p14a05/Age=p14a03; 37,995)
  - Guatemala `individual_education` (ECV11P10; p10acp; 31,697)
  - China `individual_education` (S01B; s01b10; pid=s01bid join clean; 1,744)
  - Azerbaijan `individual_education` (A03; diploma; i=[ppid,hid]; 9,005)
  - Cambodia `individual_education` (hh_sec_3; s03q04; pid=children_out_of_H__id;
    1,973; ~26% out-of-household-children orphans — benign, documented)
- **PR #334** `fix/dvc-write-lock-retry` — `_run_dvc_with_lock_retry()` wraps the
  write-path `dvc add`/`dvc push` (backoff+jitter, lock-markers only); CLAUDE.md
  "use get_dataframe(), never `dvc pull` CLI concurrently" guidance + anti-pattern.
  test_data_access.py 38 passed.

## Key findings

- **interview_date is declared-but-broken** (20 declarers but the assembled
  `Feature()` collapses to a tuple-string object index, empty date/v columns).
  Live evidence added to issue **#325**; someone opened **PR #335** to fix it.
- **DVC reads are already lock-free** (v0.7.3 direct-S3 bypass in
  `_ensure_dvc_pulled`; no Repo.fetch). Our recurring "Unable to acquire lock"
  pain was self-inflicted by agents shelling out to `dvc pull` CLI. Prior art:
  `slurm_logs/dvc_lock_repro/`, `SkunkWorks/dvc_writer_distribution.org`. A real
  fetch/write **queue** is the deferred core change (write-path coordination).
- `sample` is at full coverage (33/34; only Armenia missing — blocked on data).

## Deferred (recon done, NOT implemented)

- **Pakistan** `individual_education` — only `primgr` (primary grade 0–5) in the
  F03 section; no secondary/highest-level column. Would misrepresent attainment.
  Needs a true attainment variable before wiring.
- **GhanaLSS** `assets` — script-path, multi-file (Section 10 fragmented across
  5–8 files/wave in 1998-99, 2012-13, 2016-17); 1991-92 & 2005-06 lack the module.
- **Tajikistan** `assets` — opaque unlabeled column encoding, ~3–6% coverage;
  needs WB TJSS codebook.

## Coverage matrix now (the 4 marginal features)

assets 20→21 (Guatemala). individual_education 20→24 (Guatemala, China,
Azerbaijan, Cambodia). plot_features 12, interview_date 23 (broken).
Remaining real gaps are the 3 deferred above + the interview_date fix (#325/#335).

## Artifacts (this dir, untracked)

- `verify_matrix_2026-06-06.py` — observed coverage of the 4 features.
- `verify_clean_cells.py` — per-cell is_this_feature_sane verification.
