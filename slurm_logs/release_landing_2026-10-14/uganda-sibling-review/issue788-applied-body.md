> **Narrowed 2026-10-15.** This issue remains open for the general builder-authority choice and divergence detection.
>
> **Resolved so far:** direction (3), stopping permanent laundering, is implemented by #809 (PR #918) and #866 (PR #924). PR #1014 now aligns Uganda's country assets compiler with canonical wave concatenation, retaining `id_walk`, `Value` and `(t,i,j)`. Actual Makefile replay changes 790,688 rows with 673,841 duplicate raw keys to 116,808 real rows; explicit exact comparisons preserve canonical raw and served values. The API's 4,237 existing duplicates after label mapping remain unchanged.
>
> **Remaining:** direction (4), detecting divergent builders across the corpus, and the (1)/(2) choice of authoritative builder. Both Uganda assets builders remain. Uganda's assets script still writes hashless country output; income/nutrition can rewrite earnings/FCT siblings, which ordinary API reads rebuild under #809. The assets correction does not choose authority, add general detection, or change sibling-write policy.
>
> **Decision for @ligon:** A: framework authoritative (retire concatenate-only country scripts and their targets after accounting for operations such as `id_walk`), or B: script authoritative when `materialize: make` is declared? Preserve genuinely necessary script transformations in either design.


**Observed coupled-output freshness cycle**

Additional verified sibling-write case, October 15 release validation (`3914d8804`): ordinary trailing `fct(), earnings(), fct(), earnings()` calls leave Uganda FCT and earnings fresh, but overwrite `Uganda/var/nutrition.parquet` without its content-hash or wave-coverage footer. Repeating the two requested reads does not make the complete cache set simultaneously fresh.

`Uganda/_/Makefile:142-143` gives nutrition and FCT the same `nutrition.py` recipe. That script writes FCT at line 63 and nutrition at line 68, both without `cache_hash`/coverage arguments; the framework subsequently verifies and stamps only the requested table. The script and coupled recipe are unchanged since v0.14.0. This extends the already documented nutrition→FCT direction with the observed FCT→nutrition metadata overwrite. Earnings is not the cause of the nutrition overwrite.

The final private state is **423 fresh country parquets plus one hashless nutrition parquet**. Nutrition still has the approved #967 shape 23,777 × 15 and content fingerprint `12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a`. All 1,519 saved raw frames retain their cold-corpus shapes, columns, index names, dtypes and content fingerprints. This is frame equality, not unchanged file bytes: the coupled write changes metadata, bytes and mtime.

The release disposition is to exclude exactly `Uganda/var/nutrition.parquet` from promotion and backup-then-retire its existing shared counterpart if present, while preserving the final private evidence. There is no manual stamp or assembly of a fictional fully fresh snapshot from different call times. Under #809, the next ordinary nutrition read rebuilds; that rebuild can in turn leave FCT hashless. The disposition is not a runtime fix or a claim that nutrition is unsupported.

Keep #788 open for authoritative-builder selection, divergence detection, and coupled-output freshness/coverage handling. #809's protection against permanent trust-once laundering and #1014's exact assets correction remain valid; neither guarantees simultaneous metadata freshness for every script-written sibling. The #967 nutrition approval remains in force, with no additional scientific or fixture change from this finding. Final promotion execution and its backup/rollback evidence are separate from this review.

---
*Original text, kept for the evidence:*

A country script's output is laundered into looking framework-fresh: hashless `var/{table}.parquet` is trusted once and re-stamped

## Summary

A country-level `_/{table}.py` script is **not** what `Country(...).{table}()` runs, the two builders can produce **different data**, and — this is the part that makes it stick — a country script's output is **hashless**, so the first API read trusts it once and **re-stamps it with the framework's own hash**. From then on it grades `fresh` and is served indefinitely, indistinguishable from something the framework built.

Found while fixing #772. The Uganda instances of the symptom are fixed on `development`; the mechanism below is a design decision, so it is filed rather than patched.

> **Correction (this issue originally said the outcome was "decided by cache provenance").** That named the symptom and missed the mechanism. Which builder *runs* is determined by configuration — the Makefile prerequisite graph and which wave-level targets exist — not by cache state. What makes a divergent artifact *permanent* is the `legacy` trust-once-and-re-stamp rule, described under "Why it becomes permanent" below. That is also the fixable part.

## Two builders

`country.py`'s `load_from_waves` loops the waves calling `run_make_target(..., wave=w)` and **concatenates the wave parquets**. The country-level script is reached only via

```python
country_fallback = run_make_target(method_name, wave=None)   # country.py:3380
```

which fires **only when no wave produced anything**. So for any table with wave-level scripts, the country-level script is dead on the default path.

Verified by planting `raise RuntimeError(...)` at the top of `Uganda/_/earnings.py`:

| invocation | result |
|---|---|
| `Country('Uganda').earnings()` | returned `(22447, 1)` — sentinel **never tripped** |
| same call, `LSMS_BUILD_BACKEND=make` | **raised the sentinel** |

## Why this is not a corner case: the Makefile prerequisite graph

`income` has no wave-level scripts, so it always falls through to the country target — and `$(VAR_DIR)/income.parquet` lists `$(VAR_DIR)/enterprise_income.parquet` and `$(VAR_DIR)/earnings.parquet` as prerequisites. Building `income` therefore rewrites both through their country scripts.

Measured on Uganda, before the #772-adjacent fixes:

| what happened | `Country('Uganda').enterprise_income()` |
|---|---|
| cache without `var/enterprise_income.parquet` | **13,935** rows |
| after anything built `income` | **78,040** rows (64,105 entirely null) |

## Why it becomes permanent

`to_parquet` called from a country script does not stamp `lsms_cache_hash`. Measured on a live cache, after an `income()` build had rewritten `earnings` through its script:

```
earnings             lsms_cache_hash=NO      <- written by the country script
enterprise_income    lsms_cache_hash=yes
income               lsms_cache_hash=yes
assets               lsms_cache_hash=yes
sample               lsms_cache_hash=yes
household_roster     lsms_cache_hash=yes
```

`cache_freshness` grades a hashless parquet `legacy`, whose documented handling is trust-once + re-stamp — its own docstring says "trust-once-then-stamp migration of legacy / **script-written** parquets". So:

```
before API call: stamped = NO  | rows = 22447
API returned    : (22447, 1)            <- served as-is, no rebuild
after  API call : stamped = yes | rows = 22447
```

The hash it is stamped with is computed from the **yml/config inputs** (`data_info.yml`, `data_scheme.yml`, `_/Makefile`, `*.org`, wave/country modules, DVC sidecar md5s). Those did not change — only the *builder* did. So the content-hash mechanism is structurally unable to detect this, and the artifact grades `fresh` forever.

`Country._evict_hashless_wave_caches` exists for exactly this hazard but globs `*/_/{method_name}.parquet` — **wave** parquets only. There is no country-level counterpart for `var/{method_name}.parquet`.

**That is how #772 reached the reporter as 129,160 rows** while a cold build of the same commit produces 22,447: the make chain wrote it hashless, the next read trusted it once and stamped it, and thereafter it was "fresh".

## The builders can disagree in *schema*, not just row count

`Uganda/_/assets.py` writes a column `assets` indexed `(i, t, m)`. The served `var/assets.parquet` holds `Value` indexed `(t, i, j)` — the wave parquets concatenated. Nothing had triggered `assets.py` recently, so the concatenation is what is on disk. Whichever builder writes last wins, and they do not agree on column name or index.

Not a config difference between tables: bare `!make` desugars to `{materialize: make}` (`yaml_utils._construct_make`), identical to what `earnings` declares.

## Fixed vs. not

Fixed on `development` (`a4bbcb0`, `296d791`, `603442b`): the Uganda `earnings` and `enterprise_income` country scripts use `pd.concat` instead of the `stack()`/dict/`stack()` round trip, so the two builders **agree** for those two — 22,447 and 13,935 by either path, verified.

That is convergence by hand, one table at a time. Not addressed: `assets`, whose script still disagrees in column name and index; every other country's country-level scripts, never checked against their concatenation counterpart; and the laundering itself.

## Possible directions (maintainer's call)

1. **Make the framework authoritative** and delete country scripts that only concatenate. Some do real work (`id_walk`, cross-table joins) the concatenation path does differently or not at all.
2. **Make the script authoritative** when `materialize: make` is declared, so the declaration means what it appears to mean.
3. **Stop the laundering.** Either stamp country-level script writes, or give `var/` the country-level twin of `_evict_hashless_wave_caches`, or narrow `legacy` so it cannot apply to a table whose builder can differ. Note this interacts with the deliberate v0.8.0 no-rebuild-on-upgrade migration, so it is not free.
4. **Detect the divergence**: build both, compare shape/columns/index, warn or fail on mismatch — in the spirit of `_audit_index_collapse` / `null_read_audit`.

(3) and (4) are worth doing whichever of (1)/(2) is chosen; the failure is silent, and silence is what let it persist.

## Reproducer

```bash
rm -f ~/.local/share/lsms_library/Uganda/var/{income,earnings,enterprise_income}.parquet
python -c "import lsms_library as ll; print(ll.Country('Uganda').enterprise_income().shape)"
# -> (13935, 6)      framework wave concatenation

rm -f ~/.local/share/lsms_library/Uganda/var/{income,earnings,enterprise_income}.parquet
python -c "import lsms_library as ll; ll.Country('Uganda').income()"   # drags the make chain
python -c "import lsms_library as ll; print(ll.Country('Uganda').enterprise_income().shape)"
# -> (78040, 6)      country script, pre-fix
```

(The second returns 13,935 on `development` now that the script is fixed; revert `Uganda/_/enterprise_income.py` to reproduce.)
