# Prior-art ledger - #1288 Ethiopia cluster cover cache inputs

Baseline: development `47df62d49e614c9a56e319def7324145d138c3d9`.

## Scope and reusable contracts

- The science and locators are already fixed in
  `1120-ethiopia-locating-cover-v1.md`. Keep the household cover authoritative,
  add only agriculture-only EAs and use both linked covers for the W3
  crosswalk. No row, selector, weight, label or geography changes.
- Reuse the nested `file:` declaration used for Burkina and other EHCVM
  `food_acquired.unit_weights`. `Wave._input_hash` and the country hash see
  these via `local_tools._collect_file_paths_from_block`; the `dfs:` getter
  continues to read only its two existing subframes before the hook runs.
- Inherit `STANDING.md` and `CLAUDE.md` for the cache and extraction contracts.
  The change is bounded to Ethiopia's five wave YAML declarations and a
  clarifying hook docstring. It conservatively invalidates all Ethiopia wave
  hashes that share the edited inputs once. No shared-cache rebuild.
- The actual W3 hook reads its planting cover and the W2 household cover at
  pinned development. Perturbing only the W3 planting cover sidecar in a
  detached copy changed neither wave nor country `cluster_features` hashes.

## Verification

Check the actual declaration union against `CLUSTER_AGRICULTURE_SOURCES` and
verify every tracked input, including the W3 cross-wave W2 cover, perturbs the
expected fingerprints. Compare exact held-data cold/warm output and standard
focused tests. No altered data object is read during hash perturbation.

## Measured acceptance

Each planting-cover digest now moves only its own cluster wave and the
country table hash. The W2 household digest moves W2 and W3 cluster hashes
and the country table. Restoring all sidecars restores the baseline. The
candidate changes 25 Ethiopia country and 125 Ethiopia wave hashes once.
The 15 exact held input objects produce 2,179 identical Country and warmed
Feature rows, index, dtypes and attrs. A fresh warm Country process does not
reach native extraction and conserves six cache artifacts byte for byte.
Focused tests: 77 passed, 31 skipped. No shared copy or cache changed.
