# GH #805: L1 census of the cp1252 reader re-decode

`get_dataframe` re-decodes the cp1252 strings of a pre-118 `.dta` (block
comment above `_C1_RE` in `lsms_library/local_tools.py`).  The decision is
per FILE and was measured on the blobs held in L1 on 2026-10-19 -- 4,911 of
6,813 `.dta` sidecars.  The other 1,902 were never looked at.  These two
scripts let the census be re-run over any blob store, in particular over the
staging store AFTER the bundle re-warm has fetched what is missing.

| file | what |
|---|---|
| `census_l1.py` | every held `.dta`: read the blob as the reader does, apply the LIVE predicate (imported from `local_tools`, so it judges with the code on `PYTHONPATH`), record a verdict (`clean` / `gated-118` / `no-candidate` / `decode` / `refuse` / `error`) and per-file counts |
| `frames_l1.py` | every C1-bearing file of a census, read through `get_dataframe` itself; hashes the served frame, so two code versions can be compared frame by frame |
| `census_2026-10-19.md` | summary of the run at `45692e06a`, with its comparison against `b7b6de11e` (before the truncated-double tightening). The path in its "Compared with `../work/census_head.jsonl`" heading is that `b7b6de11e` run, on the worker's node-local disk; it is not committed |
| `census_2026-10-19_c1_files.tsv` | one row per C1-bearing file (309): verdict, candidates, C1 cells, decoded cells and labels, refusal reasons, decoded characters |

## Re-running it over the staging store (after the re-warm)

```sh
cd <checkout at the bundle head>
export LSMS_DATA_DIR=<staging data root>       # frames_l1 reads <root>/dvc-cache
env PYTHONPATH=$PWD PYTHONSAFEPATH=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python slurm_logs/gh805_cp1252/census_l1.py \
    --blobs $LSMS_DATA_DIR/dvc-cache --nproc 6 \
    --out census_staging.jsonl --summary census_staging.md \
    --tsv census_staging_c1_files.tsv --strings c1_strings_staging.jsonl
```

Read, in this order:

1. **Coverage**: the "not held" line should have shrunk to the countries no
   build reads (Bulgaria, Bosnia-Herzegovina, Nicaragua, Brazil, Afghanistan,
   ...).
2. **REVIEW**: `decode` files whose decoded characters fall outside the set
   the r1 red team eyeballed (`’‘“”–—…œŒšžŠŽ€˜`).  The predicate refuses a
   file on a cp1252 hole byte or a symbol between two letters, but it cannot
   tell cp1252 from a code page that avoids both.  Mac Roman and cp852 are
   the known blind spots: cp852 `Kova\x9fevi\x86` would decode to
   `KovaŸevi†`.  A person must look at every file listed there.
3. **refuse**: every new refused file should be a DOS code page.  On this
   run they are Albania cp437/cp850, Panama cp850, and Kazakhstan / Kyrgyz
   cp866.
4. **Strings excluded as TRUNCATED double encodings**: each should be a UTF-8
   double encoding cut short.  On this run there are two: Nigeria 2018-19
   `S2AQ10` (the case the rule exists for) and one coincidental cp866 name in
   Kyrgyz 1996 `ANTHFL96.DTA`, whose file is refused anyway.

Pass `--compare census_2026-10-19.jsonl` to diff verdicts against a previous
run.  The full jsonl is not committed (1 MB); regenerate it from this commit
with the command above.  `--summary-only` re-summarises an existing `--out`
without reading any blob.

What the census cannot see:

- a caller passing `encoding=`, which disables the re-decode.  Three scripts
  do, and all three are dead (Burkina 2018-19 / 2021-22 and Senegal 2018-19
  `food_acquired.py`);
- the 12 `error` files.  pandas cannot read them, so `get_dataframe` falls
  back to pyreadstat, which the re-decode does not touch: 11 China 1995-97,
  plus Malawi 2016-17 `ag_mod_d.dta`.

## The 2026-10-19 run (L1 as copied at the base commit; predicate at `45692e06a`)

| verdict | files | C1 cells | decoded cells | decoded labels |
|---|---|---|---|---|
| decode | 168 | 1,229,213 | 1,229,213 | 1,168 |
| refuse | 58 | 599,321 | 0 | 0 |
| no-candidate | 47 | 31,633 | 0 | 0 |
| gated-118 | 36 | 16,252 | 0 | 0 |
| error | 12 | -- | -- | -- |
| clean | 4,590 | 0 | 0 | 0 |

**Before/after the truncated-double tightening.**  The predicate at
`b7b6de11e` is the before; the predicate at `45692e06a` is the after.

- **Census verdicts:** 2 files differ.
  - `Nigeria/2018-19/Data/sect2_harvestw4.dta` goes from `decode` to
    `no-candidate`.  Its 3,755 label cells are no longer turned into
    `â€˜O LEVELâ€`.
  - `Kyrgyz Republic/1996/Data/ANTHFL96.DTA` stays `refuse`; its candidates
    go 47 → 46.
- **Decoded cells:** 1,232,968 → 1,229,213 (−3,755).
- **Served frames:** `frames_l1.py` over all 309 C1 files, each code version
  run separately.
  - **308 / 309 hashes are identical.** The one that differs is the Nigeria
    file: same shape (26,557 × 60), with C1 cells served going 0 → 3,755.
    The label is now served as read, which is what development serves.
  - Nigeria `individual_education` reads `s2aq9`, not `s2aq10`, so no table
    moves.

The broader rule ("a UTF-8 lead followed by a C1 continuation byte,
anywhere") was measured first and rejected.  It dropped 40 of the 47
candidates of Kyrgyz 1996 `ANTHFL96.DTA` and flipped that file from
`refuse` to `decode`, on 8 cells.
