# GhanaLSS Aggregate Label measurement, 2026-09-15

Read `FINDINGS.org` first.

Re-run any script from the repo root with the venv and PYTHONPATH set
(the `.pth` trap — `python <script>.py` puts the *script's* dir on
sys.path[0], not cwd, so the main checkout wins without PYTHONPATH):

    PYTHONPATH=$PWD .venv/bin/python slurm_logs/ghanalss_aggregate_labels/<script>.py

| script | what it measures |
|---|---|
| `gate.py` | served `j` from `food_expenditures()` vs country `Preferred Label` — the binding blocker |
| `size.py` | row / expenditure exposure of unmapped `j`, per wave, and recoverability |
| `crosswalk.py` | coverage + conflicts via the country table's per-wave columns |
| `build_candidate.py` | writes `aggregate_label_candidate.yml` + `conflicts.json` |
| `measure_agg.py` | the naive PL-to-PL join (kept: it is what produced the stale "9 orphans") |
| `fct.py` | `FCT Code` coverage and the orphan spelling hypothesis |

All read the warm cache at `data_root()`; none writes to the config tree.

Added 2026-09-16 for the transformation-ladder analysis:

| script | what it measures |
|---|---|
| `ladder.py` | Lc injectivity per wave, Lw→Lc totality, Lcp surjectivity |
| `ladder2.py` | the collapses the country table already encodes; the 13 unresolvable pairs; the effect of applying Lcp |
| `ladder3.py` | duplicate-index consequence of applying Lcp (Trap 9) |
| `audit.py` | country+wave table integrity sweep |
| `audit2.py` | dead rows, code collisions, Aggregate coverage |
| `audit3.py` | raw->PL plausibility sweep; produced-side check; split-removes-collisions proof |
