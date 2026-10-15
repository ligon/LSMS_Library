SCOPE DEVIATIONS

None. Ten ordinary single-wave `interview_date(waves=[wave])` calls ran on a new physical copy of the completed Mali and Tanzania caches, with only immutable L1 shared. All 140 source and copied parquet identities (SHA-256, size, mtime, device and inode) stayed unchanged. Every copied file has a different inode from its source. No APIs touched the original, final promotion, or shared roots.

The practical workaround succeeds on final source 3914d8804d7b2936d75041cea98bbc9ddb6d4c9f and pandas 3.0.6:

```python
country = ll.Country('Mali', preload_panel_ids=False)  # also verified for Tanzania
dates = pd.concat([country.interview_date(waves=[wave])
                   for wave in country.waves])
```

All four Mali waves return 24,939 rows in 1.09 seconds; all six Tanzania waves return 22,427 rows in 0.93 seconds (excluding the six dates already null in the raw cache). Values, index, row order and dtypes are exactly frame-equal to the pure replay that matches the original cold fingerprint. The defensive alignment was an identity operation for both countries: the concatenation already had the cold row order. No tolerances or row exclusions were used. `result.json` records both unaligned and aligned fingerprints.

This verifies consumer guidance for a complete warm cache, not a source repair or universal cold/warm equality. The ordinary all-wave API still has the independently recorded #798 loss. No cache bypass, manual metadata stamping, or `LSMS_NO_CACHE` was used. `LSMS_SKIP_AUTH=1` skips setup only; this was not a claim of a credential-free audit.

An initial attempt stopped before copying because library import had initialized an empty data root/L1 directory. The generator now explicitly permits only that empty initialization; its retry completed. Both logs are preserved privately. Aggregate result.json and copy-manifest.json provide the outcome and provenance without survey identifiers.
