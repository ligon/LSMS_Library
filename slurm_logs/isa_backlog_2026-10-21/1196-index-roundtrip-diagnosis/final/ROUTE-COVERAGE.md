# Country gate coverage for the two GH1196 cases

This is source/saved-call analysis only. No API or test was added or replayed during sealing.

At exact f8, Niger `_/data_scheme.yml:41` declares food_acquired without `materialize:make`; Uganda `_/data_scheme.yml:19` likewise declares cluster_features without that flag. Both are nonnull registered schema entries.

| Country/table | Default | Make | Runtime evidence limit |
|---|---|---|---|
| Niger food_acquired | `_aggregate_wave_data`5811–5812 -> load_with_dvc_cache -> load_dataframe_with_dvc5478 -> `_read_l2_country_cache`5498; current/fullcoverage country hit returns before wave descent | use_dvc=False5800–5803; has_data_scheme_entry=True and prefer_make_backend=False -> load_from_waves5820–5823 -> Wave.grab_data at5280–5284 and YAML wave cache gates; bypasses country gate | Two new Default warm calls plus original pinned Default cold/warm frames. No staged original/new Niger Make food return or gate trace; Make route is source-bound only |
| Uganda cluster_features | Same Default country gate; two new current/fullcoverage warm calls match frozen warm fullframes/attrs | Same bypass -> load_from_waves -> YAML Wave cache gates; this path does read harmonized wave parquets | Four physically copied, immutable manifest-gated original1198 caller endpoints: base/candidate × make-cold/make-warm-continuation, each4805rows on full(t,v) MultiIndex; extracted v display string cold and str warm. These are original saved calls, not new measurements |

The source condition `prefer_make_backend and self._has_country_builder(...)` short-circuits for both targets because prefer_make_backend=False. The presence of an unrelated country Makefile does not redirect either target into this country-gate branch. `_read_assumed_fresh_cache` is not evidence for a normal gate repair: no assume-fresh option was used or authorized. The ordinary Make route is not a cache bypass; it still uses YAML Wave read/write gates and sanctioned parquet IO.

Original1198 build-v2.py explicitly sets LSMS_BUILD_BACKEND=make from DATASET=make and calls each support method with empty kwargs, saving complete frame/warnings/endpoints before strict equality. The independent report records both-head Make cluster dtype stops. The actual parent's library is f8-exact; the candidate's crop-specific change does not change this framework route or cluster declaration. This diagnosis's new guarded calls used Default only. Their route binding is source/state/argument evidence, not newly added `_read_l2_country_cache` tracing.

A metadata-directed generic parquet-reader repair covers the already reached common reader operation on both Default and Make, as well as direct native/generic reads. A generic writer repair changes build-side behavior and must separately justify retaining the recorded policy; existing outputs already retain the needed metadata. A Country-gate-only fix could address these Default country hits but cannot resolve the actually observed Uganda Make symptom. Its simulated zero-hash cost does not establish coverage or correctness. New helper placement and actual candidate hashes require root authorization/review; there is no new exclusion or policy choice here.

Source pins, the four copied JSON endpoint receipts and their original manifest binding are in private route-evidence/results-safe.json and copy-terminal-private.json. Complete saved frames remain in the frozen original review; no payload re-unpickle, source decode, normalization or API replay was needed for this addendum.

Sue
