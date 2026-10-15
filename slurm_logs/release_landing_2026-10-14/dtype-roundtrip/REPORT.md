SCOPE DEVIATIONS

None. This is a read-only diagnosis with disposable physical copies and private source rebuilds. No tracked file, dependency, fixture, original/final/shared L2 cache, or GitHub object was changed. Original/final raw parquets were read only for byte hashes. API calls used two new private roots. Raw survey frames and warning logs remain private; this bundle contains aggregate results, source-only probes, and digests.

The release comparison can record two precisely bounded serialization exceptions: Liberia `interview_date.Int_t` seconds→milliseconds, and Mali `household_roster.MonthsSpent` object/numeric scalars→Arrow strings. Both reproduce on unchanged runtime source. This report establishes their mechanism and preservation of numeric/time values, missingness, keys and wave coverage; it does not claim general cold/warm frame equality, approve a scientific proxy, or normalize away the separate Mali/Tanzania `interview_date` coverage loss tracked under #798. No production edit is proposed for this release.

## Exact scope and reconstruction

Release implementation is `3914d8804d7b2936d75041cea98bbc9ddb6d4c9f`. The probe worktree is `framework/uganda-assets` at approved `515864c993050c73e8add567b0a36e22f55af3ef`; its entire `lsms_library/` subtree is identical to the release head. `country.py`, `local_tools.py`, and both countries' configuration/source trees are unchanged from the original corpus source `7fa05179c`. pandas is 3.0.6 and pyarrow 25.0.1. `source-and-raw-identity.json` proves all seven target country/wave parquets in original and final corpus roots are byte-identical.

The old runner saved served fingerprints, not full frames. First, ordinary APIs on a new physical copy reproduced both final warm fingerprints exactly. Then a second private root copied only dependencies and omitted the two target country/wave caches, forcing actual source reconstruction. The actual writer was wrapped to record aggregate before/return/stored types, with its implementation unchanged. A second API call read the resulting warm cache.

Mali's fresh rebuild used a different row order. Unique key sets and sorted index values/names/dtypes matched exactly before alignment; aligning fresh frames to the original warm frame's index reproduced the *original cold fingerprint exactly*, and the complete reconstructed warm frame equalled the original warm frame with `check_exact=True`. Liberia needed no order change. Alignment was used only in the investigative comparison, never applied to an API result, cache, fixture, or corpus assertion. `strict-semantics.json` is the final decisive result. Earlier `cold-result.json` records Mali's unaligned hash mismatch; `rebuild-comparison.json` and strict reconstruction explain it, rather than concealing it.

| Pair | Original cold fingerprint | Original/final warm fingerprint |
|---|---|---|
| Liberia interview_date | `12468dab89868f7c9790ab1a0fdc67d87a66d8ac2602b6e5071c17818179d7b3` | `5998082be93cb80d5bbe5e298f6cec70ef43fc5452704bc129a078ccd194707e` |
| Mali household_roster | `d7ff366add23225bbdd3fbec487ac91afb19128a692ac464ca675bef3f183da9` | `6c400824a4bd8599e7921b1049a55e914c99661b0925571db2d5b348317607de` |

## Liberia: precision metadata, identical timestamps

The served table has 2,787 rows, one `Int_t` column, index `(i,t,v)`, one wave 2018-19, no null timestamps and no duplicate keys. Cold `datetime64[s]` becomes warm `datetime64[ms]`. Every timestamp is exactly equal after converting both to nanoseconds; restoring only the original dtype reconstructs the complete original cold fingerprint. Index, order, null mask and wave counts are unchanged.

The raw wave/country tables each contain 2,986 rows including 199 missing dates; the served finalizer drops all-null rows in both cases. The actual writer receives seconds, returns seconds, and writes Parquet `timestamp[ms]`; the reloaded dataframe has milliseconds. This is physical timestamp serialization, not an altered date calculation. Source is Liberia `2018-19/_/data_info.yml` `interview_date: Int_t: interview_date1`, then `local_tools.to_parquet`/Arrow storage; there is no declared precision normalization in `_finalize_result`.

Liberia CONTENTS was read first in full, including task-state searches. Its idiosyncrasy is the forest-proximate NHFS livelihood sample, not a standard nationally representative LSMS: urban Montserrado is excluded, and planned distance strata were abandoned after a mechanical distance error. Missing strata therefore cannot be inferred as a processing omission. No date-precision claim in CONTENTS contradicted this measurement; the source's primary interview date deliberately ignores repeat-visit dates.

## Mali: scalar conversion as well as dtype

The served table retains all 220,732 rows and seven columns, unique `(i,t,v,pid)` index keys, all six other columns exactly, and all null masks. `MonthsSpent` has 43,823 missing and 176,909 nonmissing values. This is **not merely dtype metadata**: numeric Python scalars become text, which affects direct arithmetic and type-sensitive code.

| Wave | Rows | Nonmissing MonthsSpent | Cold scalar types | Warm scalar types |
|---|---:|---:|---|---|
| 2014-15 | 37,175 | 36,825 | str: 36,825 | str: 36,825 |
| 2017-18 | 94,071 | 94,070 | float: 94,070 | str: 94,070 |
| 2018-19 | 46,014 | 46,014 | int: 46,014 | str: 46,014 |
| 2021-22 | 43,472 | 0 | none | none |

Exact numeric comparison via `pd.to_numeric(errors='raise').astype('Float64')` passes for every aligned cell, with total 2,085,227.0. This total is an equality diagnostic, not an endorsement of interpreting binary eligibility proxies as actual duration. Applying the actual writer rule only to `MonthsSpent`—save original missingness, `astype(str).astype('string[pyarrow]')`, restore the mask—reproduces the entire warm frame exactly. No additional value, missingness, key, or wave-coverage discrepancy remains.

`writer-events.json` traces the source path: 2014-15 has object integers before the wave writer, whose return rebinds them as strings; 2017-18 retains float64 and 2018-19 int64 through their wave writers. Country concatenation mixes these into an object column. `local_tools.py:2348` (`to_parquet`, object block near2415) casts object values to strings while preserving the original null mask. `Wave.grab_data` at `country.py:1897` rebinds the writer's return; country cache write sites at4788/4964 discard it. The cold country result therefore retains mixed object scalars, while the stored/next warm result contains strings. `_finalize_result` has no `MonthsSpent` dtype declaration to reconcile them.

Prior art `.coder/ledger/645-to-parquet-null-coercion.md` explicitly records this rebind/discard distinction and deliberately keeps object→string conversion because mixed Arrow values require it. Its claim that cold/warm agreement is a tested invariant needs a narrower reading: `tests/test_gh645_to_parquet_null_coercion.py::test_cold_and_warm_builds_agree` checks row counts for three `individual_education` cases, not exact dataframe dtypes/scalars. The present result identifies a preexisting remaining interface difference; it does not undo #645's preservation of real nulls and legitimate strings.

Mali CONTENTS was read first in full, including task-state searches. Its idiosyncrasy is the mixture of EAC-I2014/2017 and EHCVM2018/2021 instruments; EHCVM cluster identity is `grappe`, with `vague` omitted because each cluster is visited in one phase. A scientific documentation/source contradiction must remain explicit: CONTENTS “Household Presence / MonthsSpent” says EHCVM eligibility filters “do not give a continuous duration measure” and advises using binary variables “rather than expecting a =MonthsSpent= column in =household_roster()=.” Yet existing 2018-19 YAML maps `s01q12` Oui→12/Non→0 into `MonthsSpent`; 2017-18 similarly maps `s1q13`, while 2014-15 uses continuous `s01q11` and 2021-22 supplies none. This investigation neither changes nor scientifically approves that proxy. Any follow-up must reconcile the documentation and intended feature semantics separately.

## Reproduction and limits

Private warm root: `/local/job39919092/release-work/framework/dtype-roundtrip-data`. Its 21 physical files/3,610,468 bytes were copied with distinct inodes; `copy-manifest.json` preserves their hashes. Cold root: `/local/job39919092/release-work/framework/dtype-cold-data`, with target country/wave files omitted from the dependency copy (`cold-copy-manifest.json`). The only writable-cache symlink is the permitted shared `dvc-cache` L1; no writable L2 links were used. Probe scripts assert package, countries-root and private data-root identity.

Commands used the worktree as cwd, `/local/job39919092/venv_img/bin/python`, matching `PYTHONPATH` and `LSMS_COUNTRIES_ROOT`, private `LSMS_DATA_DIR`, an empty `LSMS_CONFIG_DIR`, `LSMS_SKIP_AUTH=1`, `taskset -c 16-23`, and `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1`. `probe.py` runs against the warm copy; `cold_probe.py` against the intentionally target-absent copy; `compare_frames.py` and `strict_semantics.py` consume only private saved frames. Their fixed paths deliberately target these disposable locations. Repeating a cold proof requires a new equivalent target-absent root, not clearing an original corpus.

This is a two-pair exception, not a blanket dtype normalization or a whole-corpus pass. It does not cover the material Mali/Tanzania date-row losses under #798. No source, test, or baseline assertions were weakened. Proposed country-documentation additions are the observed date-precision roundtrip and roster scalar-type behavior, plus the quoted existing MonthsSpent contradiction; tracked CONTENTS edits are left to the parent-owned release documentation checkpoint.
