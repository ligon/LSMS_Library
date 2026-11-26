# Prior-Art Ledger — #1288 default country fallback coverage

**Search tier:** git, ripgrep and inspection of the bound framework in an
offline synthetic Country with a real GNU Make target.

## §1 Task, restated

Stop the default backend from publishing an unchanged country Make/script
target as the result of a new `load_from_waves` build. In a strict subset
of country waves, the old target can survive the #866 eviction gate.
Real Make then returns zero without running the recipe. The old frame
may remain available on this call, but it is not evidence of a successful
rebuild or the newly requested waves.

## §2 Existing machinery

| Machinery | Source | Decision |
|-----------|--------|----------|
| `output_identity`, `run_make_target` | `country.py`, `_aggregate_wave_data` | Reuse the per-successful-invocation output receipt already used in the wave fallback (#1590). |
| `load_from_waves`, `load_dataframe_with_dvc` | `country.py`, `_aggregate_wave_data` | Pass a response-only outcome to both active default L2 writers. |
| `_read_l2_country_cache`, `_union_waves`, `_restrict_to_waves` | `country.py` | Retain freshness gates, the prior/request union and requested-wave selection (#891, #892). |
| `_evict_stale_country_cache`, direct Make transaction | `country.py` | Preserve the existing full-union #866 eviction and the separate direct country Make contract (#1645). |
| `Coverage1590` | `tests/test_gh1590_wave_failure_manifest.py` | Extend the existing invented survey and IO leaves. Do not exercise real survey data. |

## §3 Definitions & conventions in force

See `STANDING.md` §§3–4, CLAUDE Cache Behavior, the #1590 ledger §§3–5
and the #1288 direct Make repair ledger §§3–4. Coverage is the logical wave
set the producer **built**. It is not reconstructed from observed `t`.
An explicit empty manifest is distinct from no publication. A country
builder that actually produces an output retains its prior contract to
complete the attempted scope without inspection of its internal concat.

## §4 Invariants & assumptions

- An unchanged target can still answer this invocation under the existing
  fallback policy. Do not stamp, overwrite, record a write or evict it
  for a strict subset. Keep its original published hash and coverage.
- Compare identity around each invocation that actually succeeded so an
  earlier failed recipe's write cannot certify a later no-op. This is an
  invocation-local change receipt, not independent producer or input audit.
- Preserve constructed empty handling, response filtering and finalization,
  full-union eviction, stage output handling, direct Make and JSON paths.
  Do not force a build, change the declared waves, or infer that a prior
  false manifest can be reconstructed by examining data values.
- The orchestrator remains fingerprinted. Changes to it can invalidate
  wave and country caches under the existing registry, including past false
  manifests. Measure this without clearing or rebuilding shared caches.

## §5 Reuse decision

Use the existing receipt and cache contracts. No schema, public attr,
new sidecar, special-purpose hash exception or new rebuild route.

## §6 Open questions for the human

No new survey-policy decision is needed for the publication guard. Live
affected-cell prevalence is not known from the source-free witness. Keep
other computed-input and fallback residuals under #1288 open.

---
### Phase 3 — verification

- **OK (§2–4).** On development `47df62d49e6`, all 16 new no-op
  regressions fail specifically at file preservation. On the candidate
  they pass, including corrected retries and verified warm reads. The
  separate full-union #866 positive control passes.
- **OK (§2–4).** Independent GNU Make checks cover both writer arms with
  no-op, real rewrite and missing-target publication. The corrected oracle
  passes 10/10 against the candidate. On development all six no-op cases
  fail and all four genuine-output controls pass.
- **OK (§3–4).** All 202 country and 1,166 wave fingerprints computed in
  the eight-country inventory change through the tagged orchestrator.
  This prevents previously stamped entries from being assumed current.
  No shared cache was cleared, rebuilt, or re-stamped during verification.
- Run the `test_gh1590_wave_failure_manifest.py` suite and the sibling
  #866, #1288, #1039, #1292, #809 and build-transform hash tests without
  `requires_s3`. Initial broad run passed 417 tests, 12 skipped. The final
  targeted rerun with the added full-union control passed 18 tests.
