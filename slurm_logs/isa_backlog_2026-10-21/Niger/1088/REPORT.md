# Niger #1088: exact quantity missing-code recognition

Sue, 2026-10-21. Baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`;
ledger/design published before runtime edits as `1e8aeaf88`.

The change recognizes exactly nominal `9999.9` and its promoted float32
representation `9999.900390625` at the existing CS07 quantity-read mask.
Existing nonpositive, `9999` and `99999` quantity handling remains; Price uses
its original sentinel tuple. No rounding, tolerance, range clipping, grain
change, second-record decision or framework change is included.

## Source evidence and its limits

All three wired raw grids were already held. Their declared MD5s and copied
bytes were checked before sanctioned reads. Source rows are 23,409 / 34,380 /
21,960 for 2011 passage 1, 2011 passage 2 and 2014 respectively. The 2014
quantity columns `cs07q05`, `cs07q07`, `cs07q09` contain 43 / 32 / 1 exact
`9999.900390625` values, all served on 21 clusters and five varieties.

The held 2014 columns are **Stata double**, already holding that exact
float32-representable value. Their earlier precision history is unproven.
`float32(9999.0)` is exactly `9999.0`; the issue's earlier wording conflated
two nominal codes. All three files label the quantity fields "poids du
produit" and supply no numeric missing-code value labels. Four relevant
questionnaire PDF blobs are absent from held L1 and were not acquired. The
held World Bank harmonization scripts have no CS07/comprix handling. The
nominal missing-code interpretation therefore follows the existing issue /
CONTENTS ruling, rather than new independent instrument confirmation.

Across the nine wired Price fields, neither nominal `9999.9` nor its promoted
value occurs; neither `99999.9` nor `99999.8984375` occurs in any wired Price
or quantity field. Existing exact `99999` Price counts are 0 / 7,361 / 5 by
source file. Existing exact `9999` quantity counts are 5,981 / 7,377 / 5.
The held quantities include 9997 and 9998, so broad all-nines/range filtering
would change other source values. Full aggregate form counts: `summary.json`.

## Verification

Actual default-backend API builds used two separate initially absent private
L2 roots with physical copies of only the three held L1 blobs. Each cold run
was followed by a fresh-process warm replay. `unshare -rn` disabled network
access for the process and its children; imports and country-config realpaths
were asserted against this worktree. Numerical/build threads were one.

| Check | Baseline | Changed |
|---|---:|---:|
| Served rows | 46,043 | 46,043 |
| 2011-12 / 2014-15 rows | 32,352 / 13,691 | 32,352 / 13,691 |
| `(t,v,j,u)` cells by wave | 8,923 / 6,500 | 8,923 / 6,500 |
| Encoded missing-code quantities served | 76 | 0 |
| Content-repeat drops by source | 0 / 17 / 117 | 0 / 17 / 117 |
| Second-record `obs > 3` rows by wave | 15 / 15 | 15 / 15 |

Every index key, Price and other column is identical. Exactly 76 quantities
become NA; the other 45,967 quantities are identical, including missingness.
Each version's cold/warm frames are equal. The 13,478 wide exact duplicates
in 2011 passage 2 and all 134 existing content-repeat drops are unchanged.
The four second-record quantities of 1 against a first-record missing code
remain distinct; all 30 nonrepeat second-record rows retain their current
representation, whose policy decision remains open.

The new actual-helper/finalizer regressions fail before the runtime patch
(8 failed, 27 passed before adding four further double-neighbor controls).
Final focused checks: **55 unit/assembly cases passed**, **12 real Niger/Mali
cases passed**, zero skips or failures in those selections. Controls cover
both exact representations, all three slots, old missing codes, ordinary
weights, adjacent float32/double values, Price retention and second-record
equality.

A Mali comparator from the held release seed was verified against its current
hash and all-wave manifest, then physically copied privately. Direct Feature
assembly retains 86,261 unique rows: Niger 46,043 plus Mali 40,218. The scoped
audit emits zero failures/errors and three existing contract warnings: the
two cluster-level tables have no household `i`, and Mali's instrument lacks
`obs`, so assembly adds `obs=1`. `feature-audit.jsonl` records them explicitly.
There is no `community_prices` variant in the audit's phase-2 kwarg grid.

Country-module hash coupling causes a one-time Niger re-warm: **23/23**
declared Niger table hashes change. Healthy controls: **0/24 Mali**, **0/25
Tanzania** change. No shared cache was cleared, rewritten, restamped or
promoted. The older shared Mali/Tanzania parquets were stale and were not
used as comparators.

## Reproduce

From the checkout, with its mounted image interpreter (set `py` accordingly):

```sh
repo=$(git rev-parse --show-toplevel)
py=/local/job40255479/venv_img/bin/python
export PYTHONPATH="$repo" LSMS_COUNTRIES_ROOT="$repo/lsms_library/countries"
export LSMS_SKIP_AUTH=1 LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONDONTWRITEBYTECODE=1
unshare -rn "$py" slurm_logs/isa_backlog_2026-10-21/Niger/1088/replay.py \
  --out /local/job40255479/control/isa-backlog-20261021/Niger/1088/replay-new
```

Use a new private output directory on rerun; the replay refuses a populated
data root and refuses missing held blobs. It reads baseline code with
`git show`, not a checkout/ref mutation. It emits only aggregate comparisons.
The focused tests are `tests/test_community_prices_variety.py`; select
`-m 'not requires_s3'` for unit/assembly controls and `TestBuilt -k 'niger or
mali'` for the real tables in the prepared private cache.

Allocation-local API scripts, full logs and returned frames remain in
`/local/job40255479/control/isa-backlog-20261021/Niger/1088/`. The tracked
`summary.json` contains safe aggregate source/form, dedup, cold/warm, hash and
assembly evidence. Raw microdata and identifiers are not committed.
