# RED-TEAM review of branch `fix/nigeria-w5-cluster` @ df3b211f6 (element `nigeria-w5-cluster`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/nigeria-w5-cluster` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Nigeria; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/nigeria-w5-cluster/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

## Mandate: REFUTE
Your job is to find the reason this branch must NOT be merged. Default to `verdict: fail` where you are uncertain. A fix agent wrote this branch and its report; the report is a self-report and its "SCOPE DEVIATIONS: none" is a claim, not a proof. Check, by running, not by reading:
1. **Does it fix the issue?** Reproduce the issue's symptom on `origin/development` code (a second clone: `git worktree add /local/job$SLURM_JOB_ID/base origin/development` is allowed -- that is your own scratch, not the shared mirror) and show it gone on the branch, in your private root, cold where the change is a build-path change (`.venv/bin/python -m lsms_library.cli cache clear --country X` against YOUR root; NEVER against the live one).
2. **Do the claimed numbers reproduce?** Every before/after figure in the report and commit messages: re-measure and tabulate claimed vs measured. A number you cannot reproduce is a finding.
3. **Data loss / laundering.** Row counts, index uniqueness, `grain_reports()`, `null_index_reports()`, `null_read_reports()` on every touched table before vs after; any `find lsms_library/countries -name '*.parquet'` hit; any write outside the private root.
4. **Healthy countries intact.** For a framework change: the full suite under the verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > /local/job$SLURM_JOB_ID/pytest.log 2>&1`, then read the file), and `bench/feature_audit/scan.py --features <touched feature> --countries <2-3 warm ones>` where a feature is touched. For a country change: that country's tests plus `tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py`.
5. **Scope and stop-list.** `git diff --stat origin/development..HEAD` versus the task's named files; any baseline/golden/blessed/lockfile/dependency edit; any weakened, skipped or xfailed assertion (a moved pin is acceptable ONLY with before/after numbers in the commit message -- check they are there and that they match your measurement).
6. **Are the new tests meaningful?** Would each new test fail on `origin/development`? Run them there (the worktree) and say which do not.
7. **CONTENTS.org**: did the agent name the idiosyncrasy, and is what it wrote true? Anything it claims contradicts CONTENTS.org -- verify against the raw data.
8. **Cache cost claim**: is the stated re-warm scope right? (A `_/*.org`, `_/*.py`, `data_scheme.yml`, wave-module edit moves that country; `get_dataframe`/`_normalize_dataframe_index`-reachable framework edits move the corpus.)

Environment: `PYTHONPATH` is your clone; use `PYTHONSAFEPATH=1` for `-c`/`-m` runs only. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`. `GH_REPO` is set; `gh` reads only, no writes. Read `lsms_library/countries/<C>/_/CONTENTS.org` for each country concerned.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- You commit NOTHING and push NOTHING: this is a review. `git status --short` must be empty at the end (delete any scratch you made inside the clone).
- Write the report to `$STAGE/VERDICT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


## Verdict format (write to `$STAGE/VERDICT.md` AND repeat as your final message)
Begin with one line: `VERDICT: pass | pass-with-fixes | fail | needs-human` and one sentence why. Then:
1. **Refutations** -- each a claim from the report/commit, what you ran, what you got (a claim that survives is listed as "held", with the command).
2. **Claimed vs measured** table.
3. **Regression** -- suite/scan summary lines verbatim.
4. **Scope findings**.
5. **Residual risks and what a human must decide** (the branch may be right and still need a decision).
6. **Recommended follow-up commits** (concrete, with paths), if `pass-with-fixes`.

## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.

## The task the fix agent was given
## Scope note from the scrum master
Read `Nigeria/_/CONTENTS.org` first (post-planting/post-harvest: every wave dir holds TWO rounds; cluster_features is wave-grain -- see `.claude/skills/add-feature/pp-ph/SKILL.md`). W5 ships no geovars: declare df_key + df_main only, NO df_geo, and say so in CONTENTS.org. Cold-build Nigeria cluster_features in your private root; report the (t, v) count for 2023-24 and that community_prices rows now join geography.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #841 -- Nigeria cluster_features undeclared for W5 while sample and community_prices ship it: 24,184 price rows have no geography
```json
{
 "tier": "B",
 "type": "data-gap",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 2,
 "summary": "Claim holds: warm cluster_features parquet has t in {2010Q3..2019Q1} only; latest.csv grades Nigeria,cluster_features,2023Q3 and 2024Q1 `absent`; Nigeria/2023-24/_/data_info.yml declares sample (:16) and individual_education (:62) but no cluster_features (its :11 comment even says 'This wave has no cluster_features table'). The W5 cover page `Post Planting Wave 5/Household/secta_plantingw5.dta` (5,067 x 35, read via get_dataframe in this triage) carries zone, state, lga, sector, ea -- exactly W4's df_key/df_main inputs -- but NO lat/lon, and no geovariables file ships for W5 (find Nigeria/2023-24/Data -iname '*geo*' = none). So the issue's 'follow the W4 declaration' must omit df_geo; Latitude/Longitude will be NaN for W5 and Site B will warn per wave (the known country-grain `optional:` mismatch, CLAUDE.md).",
 "evidence": "warm parquet Nigeria/var/cluster_features.parquet t values; .coder/coverage/latest.csv; lsms_library/countries/Nigeria/2023-24/_/data_info.yml:9-30; Nigeria/2018-19/_/data_info.yml:44-90 (W4 pattern); get_dataframe on secta_plantingw5.dta columns; Nigeria/_/data_scheme.yml:12-40",
 "approach": "Add a `cluster_features:` block to Nigeria/2023-24/_/data_info.yml copying W4's df_key (v: [state, lga, ea, hhid]) and df_main (Region: zone, District: state, Rural: sector mapping) from `Post Planting Wave 5/Household/secta_plantingw5.dta`, no df_geo, final_index (t, i, v); confirm the pp/ph t-assignment matches sample's (2023Q3/2024Q1). Note the ea == 0 'Moved' sentinel (393 households) in the :13-15 comment -- those clusters become singletons; keep the composite key. Delete the ':11 no cluster_features' comment.",
 "verify": "cold build Nigeria cluster_features; W5 clusters should be 719 (data_info.yml:14) or the per-round count; community_prices 2024Q1 v join -> 507/507 (CONTENTS.org:814-829 table); tests/test_nigeria_cluster_identity.py; Site 2 grain audit must be lossless for Region/District/Rural (composite key). CACHE COST: 2023-24/_/data_info.yml is in Wave._input_hash for every 2023-24 table, so every Nigeria L2-country table with a 2023-24 wave re-warms (sample, community_prices, household_roster, housing, plot_features, individual_education, crop_production ...); other waves' L2-wave parquets stay.",
 "contents_org": "Nigeria/_/CONTENTS.org:805-829: v is the COMPOSITE cluster_id(state, lga, ea) (GH #323; bare ea merged distinct clusters, 890/5000 W1 households got the wrong District); the 2024Q1 row of the keyspace table shows sample() matches 507/507 and cluster_features 0 because the table is undeclared.",
 "blocked_by": "none",
 "decision_for_human": "none"
}
```


## The branch: diffstat (vs development)
```
 .../countries/Nigeria/2023-24/_/data_info.yml      | 64 ++++++++++++++++++++--
 lsms_library/countries/Nigeria/_/CONTENTS.org      | 45 +++++++++++++--
 tests/test_nigeria_cluster_identity.py             | 31 +++++++++--
 3 files changed, 126 insertions(+), 14 deletions(-)
```
## The branch: commit messages
--- df3b211f6 Nigeria CONTENTS.org: state the W5 lossless-collapse claim from measurement
The previous wording attributed W2-W4 within-cluster disagreement to
"movers" (ea == 0 households are singletons and cannot disagree) and did
not say the columns had been checked.  Measured per column on the W2/W3/W4
cover page + geovariables over real composite clusters: zone/state/sector
disagree in 0 clusters in every wave; Latitude/Longitude in 5/24/50 (the
data_scheme.yml figures).  The audit's 70/78/129 conflicting tuples add
households with no coordinate beside ones with one.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 055af9ce3 Nigeria: declare cluster_features for W5 (2023-24) from the cover page, no geovars (GH #841)
W5 shipped sample and community_prices but no cluster_features, so the
27,318 2024Q1 price rows (the issue counted 24,184 at filing) had no
Region/District/Rural to join and the (Nigeria, cluster_features,
2023Q3/2024Q1) cells graded `absent`.

Add a cluster_features block to 2023-24/_/data_info.yml copying W4's
df_key (v: [state, lga, ea, hhid] -> nigeria.cluster_id composite) and
df_main (Region: zone, District: state, Rural: sector) from
`Post Planting Wave 5/Household/secta_plantingw5.dta`, with NO df_geo:
Wave 5 ships no household geovariables file (nothing matching *geo*/*gps*
under 2023-24/Data; the cover page has no lat/lon), so Latitude/Longitude
are NaN for 2023Q3/2024Q1 by source.  Site B warns for those two waves
(the known country-grain `optional:` mismatch); not silenced.

Cold build in a private LSMS_DATA_DIR, before -> after:
  cluster_features rows             5,248 (8 quarters) -> 7,070 (10)
  (t, v) 2023Q3 / 2024Q1            absent -> 911 / 911
    = 518 real clusters (BID "518 EAs") + 393 moved-{hhid} singletons
  sample.v vs cluster_features.v    W5 orphans 0 both directions
  community_prices 2024Q1           507/507 clusters, 27,318/27,318 rows join
  Region/District/Rural nulls (W5)  0; Latitude/Longitude 1,822/1,822 NaN
  GrainCollapseWarning for W5       none (lossless: no lat/lon to disagree)
  W1-W4 counts/coordinates          unchanged (500/647/792/685 per quarter)

The W5 data_info.yml comment claimed "719 real (state, lga, ea) clusters";
719 counts the 201 (state, lga, ea=0) mover groups.  Corrected to 518 +
393 and recorded in CONTENTS.org with the no-geovars idiosyncrasy and the
2024Q1 keyspace row (0 (0%) -> 507 (100%)).

tests/test_nigeria_cluster_identity.py: CF_ROUNDS now covers all five
waves (W5 enters the cluster-set/round-agreement and keyspace tests);
the coordinate test keeps its original four waves as GEO_QUARTERS, and a
new W5 test pins Region/District/Rural fully populated with lat/lon
all-NaN, so acquiring a W5 geovars file later fails loudly.  No
assertion was weakened: 34 passed.

Cache cost (not paid here beyond the private root): 2023-24/_/data_info.yml
is in Wave._input_hash for both 2023-24 quarters, so every Nigeria
L2-country table with a 2023-24 wave re-warms on next read; other waves'
L2-wave parquets stay.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


