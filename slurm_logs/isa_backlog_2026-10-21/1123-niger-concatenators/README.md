# GH #1123 extension: Niger country concatenators refuse a missing wave

Burst jobs (co_carleton / savio4_htc), base `57d255585` (development) vs
branch `fix/niger-concatenator-andand-20261023` at `cbb52e0ae`.  Aggregates
only; row-level frames stayed in the job directory.

| script | what |
|---|---|
| `scripts/run_build.sh` | hash census; cold default-backend build of every Niger table; focused pytest selection |
| `scripts/run_make.sh` | cold `LSMS_BUILD_BACKEND=make` build of the five concatenated tables |
| `scripts/run_inject.sh` | scratch config tree, one wave script broken (`raise SystemExit`), fresh data root per case |
| `scripts/run_compare.sh` | base vs branch frames (exact), attrs, the recipes' own `var/` parquets, hash census |
| `scripts/run_oracle.sh` | each `var/<table>.parquet` == the four wave parquets read directly with pyarrow |

Inject case names: `<backend>-<table>-<broken wave>`; `andandonly-*` is the
branch with `niger.concat_wave_parquets` mutated to skip (the `&&`-alone
premise of the ruling).
