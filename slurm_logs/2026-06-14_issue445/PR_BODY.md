## What

Clears the pre-existing failures GH #445 surfaced in the 2026-06-14 full cold
`pytest --rebuild-caches` gate. None were introduced by the WB-parity loop;
four are stale baselines tracking intentional, already-shipped schema evolution,
and one is a framework footgun. Investigated read-only first, then fixed.

## The 5 failures and what each turned out to be

| Failure | Diagnosis | This PR |
|---|---|---|
| `test_sample[Albania]` "missing 1996" | **Framework footgun**: `Country.waves` reads a `_/{country}.py` module's `waves` attr *before* the data_scheme `Waves:` list, then falls through to a directory glob when the module defines none. `albania.py` had no `waves`, so the glob re-included `1996/` (has `Documentation/SOURCE.org`) despite data_scheme documenting it excluded (EWS, not LSMS). | Add authoritative `waves` to `albania.py` |
| `invariance[interview_date]` (×9 wave/var) | `date`→`Int_t` — intentional (`4192eb06`, closes #325) | Regenerate baseline |
| `invariance[cluster_features]` | Lat/Lon float32→float64 + GPS now populated (coords in Uganda's bbox, #161) | Regenerate baseline |
| `invariance[shocks]` | pandas-3.0 dtype migration (boolean→string, Int64→Float64; values preserved) | Regenerate baseline |
| `api_vs_replication[interview_date]` | replication baseline predates #325 (`date` vs `Int_t`) | `xfail` (matches existing food_acquired/nutrition) |
| `api_vs_replication[food_quantities]` | **Not a stale baseline** — passes deterministically warm AND cold in isolation. Its gate failure is the pre-existing **#330** parallel-cold cache-race flake. | **Left alone** (xfail would XPASS) |

## How the baseline was regenerated (defensively)

- Canonical `tests/generate_baseline.py` against a **fresh cold build** (cleared cache → built all 13 baseline tables from empty), then **restricted to the original 53-key set** so the test surface does not expand to newly-built tables.
- 13 entries changed; all three *content_hash* changes (cluster_features, shocks, nutrition) **verified deterministic across two independent cold builds** before pinning. (nutrition's hash had drifted from food-chain evolution but was masked in the gate by skip/ordering — it is reproducible.)
- 40 entries kept byte-identical.

## Verification

- Albania: `test_sample[Albania]` passes (`Albania.waves` no longer lists 1996).
- `interview_date` api → XFAIL (documented); `food_quantities` api → passes.
- Uganda invariance: **53/53 pass** against a clean cold build (run with `--no-purge` serially; conftest purges the Uganda cache at session start in default mode, so a plain run skips — `conftest.py:96-109`).
- Full cold all-country gate (`-n 24 --dist=loadfile --rebuild-caches`, all 40 countries, commit `f840977c`): **`1117 passed, 137 skipped, 11 xfailed, 1 xpassed, 0 failed, 0 errors`** (exit 0, 21m). Down from the prior gate's **5 failed**. (The extra skips vs. that gate are the read-only Uganda invariance tests skipping when no other test rebuilt the cache first — the #330 timing fragility — which is why they were verified separately via `--no-purge` above. The 1 xpassed is a pre-existing replication xfail that happened to pass under this run's cache timing; not introduced here.)

## Not in scope (maintainer follow-ups)

- **#330** — the parallel-cold cache-race flake that makes `food_quantities` (and the read-only invariance tests) timing-dependent under xdist. The invariance tests only *read* parquets, so they pass/skip depending on whether another test rebuilt Uganda's cache first. Not a correctness issue.
- The broader `Country.waves` footgun (data_scheme `Waves:` silently ignored whenever a country `_/` module exists). Albania is the only country currently affected; a framework fix would make the data_scheme list authoritative.
- Regenerating the external **replication** parquets (would let the two `interview_date`/`food_acquired`/`nutrition` xfails flip back to real comparisons).

Refs #445.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
