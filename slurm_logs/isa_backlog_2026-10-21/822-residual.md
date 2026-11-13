Resolved so far: PR #1086 (`9d4cd807c4221972749e8732f96c597fb12fc8fd`)
preserves community-price variety, visit and observation axes. That change is
included in v0.16.0. It does not restore Mali's first two price readings.

Remaining work is the 2014-15 reading coverage:

- `Mali/2014-15/_/community_prices.py` reads only `s04q09/s04q10/s04q11`
  (reading 3), in both passages. Readings 1-2 (`s04q03` through `s04q08`)
  are explicitly left unread in the current script.
- Serve all three recorded readings on the existing native `obs` axis, with
  `obs=1,2,3`. The current fabricated `obs=1` represents reading 3 and must
  become `obs=3` for those rows. Preserve `variety` and `visit`.
- Preserve the recorded per-lot Price and `Poids (KG)` Quantity basis. Do not
  aggregate readings or reinterpret the weight as a NumberOfUnits count.
- Verify cold and warm API frames, uniqueness and row/reading conservation;
  re-grade 2014-15 and run the existing community-price Feature audit.

The historical five-source comparison measured 26,745 product-variety rows
priced only in readings 1-2; these are historical findings, not a fresh census
of the v0.16.0 output. Re-measure exact before/after counts in the fix.

The distinct missing 2018-19 community-price module remains #821. Shared
helpers may be reused, but neither issue may be closed by fixing only the
other wave.

Duplicate residual #1089 is consolidated here. This issue stays open until
the missing readings are restored and verified.

Sue
