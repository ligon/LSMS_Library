Proposed Tier B boundary for #1058: expose the two already recorded Burkina 2021 parcel coordinates through the existing helper and native key path. No implementation is authorized or present yet.

1. In `Burkina_Faso/2021-22/_/plot_features.py`, add exactly these colmap bindings:

   ```python
   latitude = 's16aq46__latitude',
   longitude = 's16aq46__longitude',
   ```

2. In `Burkina_Faso/_/burkina_faso.py::plot_features_for_wave`, reuse the existing retained `src` rows and positional DataFrame construction. Add optional coordinate outputs before setting the existing index:

   ```python
   for source_key, output_name in [('latitude', 'Latitude'),
                                   ('longitude', 'Longitude')]:
       if source_key in c:
           df[output_name] = pd.array(src[c[source_key]], dtype='Float64')
   ```

   A declared but absent source name raises, rather than silently hiding a misspelled mapping. Absence of the optional colmap key leaves that wave's columns unchanged. The source supplies numeric double coordinates; the nullable float conversion preserves values and the independent missingness of each axis. There is no flag gate, pairwise drop, fill, range screen or interpolation. The existing source mask, formatter, plot_id, area calculations, units, tenure, soil, irrigation, order and attrs remain unchanged.

3. In the existing country plot_features declaration, add `Latitude: {type: float, optional: true}` and the same for Longitude. Canonical tables.plot_features already permits float coordinates. Optional is justified because 2018 has none and 2014 is unsupported. No canonical schema, index, Makefile, crosswalk, sample or shared reducer edit is needed.

4. Correct the false blanket GPS comments when the helper/scheme are edited, and record current source coverage and the native-versus-country id_walk distinction in CONTENTS. Cite the seven-row 2018 all-payload-missing behavior as preserved. Do not silently change the wave-specific household formatter or the concatenator route. The implementation ledger must be committed first if root approves this boundary.

Expected output: all 19,784 currently served rows and their six old columns/attrs remain exact; 2021 gains 4,180 paired positions and keeps 3,170 missing pairs. The 2018 country slice gains missing coordinate columns from concatenation; its wave helper remains six columns and its seven all-empty rows remain excluded. Unsupported 2014 stays zero-row. The measured flag alone never creates a coordinate. Accuracy, altitude and timestamp are not part of this requested mapping.

The baseline already has zero native/post-walk parcel collisions and zero public sample orphans. This fix must retain the tested old_i/country_i path in 2021, ehcvm_i in 2018, and the current id_walk/id_converted protocol. No identifier or eligibility repair is bundled.

Candidate tests should bind real source values by the unchanged native key, verify exact old payload/null/dtype/axis/logical-attrs retention, reject source-index alignment errors and q47-as-coordinate, and cover independently missing axes, both missing axes, zero/signed numeric values and coordinates alongside a non-Yes measurement flag without introducing a new filter. Verify a missing declared coordinate column raises. Pin the 2018 six-column wave and missing country coordinate slice, 2014 empty subset, ten measured-but-position-missing parcels and existing uniqueness guards. Do not change fixtures/goldens or scientific thresholds.

Verification after approval needs real fresh private baseline/candidate cold builds, separate-process warm frames, Senegal comparator bytes/full frames, exact Feature conservation and full finding accounting. All 24 Burkina hashes will invalidate because the helper/scheme are whole-file country inputs; stage the already held required sample support rather than restamp companions. The read-only dependency census anticipates 525 other-country hashes and all 39 framework fingerprints unchanged. Actual candidate hashes must be measured after implementation.

Mali, Niger and Senegal 2021 require no coordinate mapping from their currently held s16a files: none contains a coordinate header or variable label. Record the bounded negative result in the issue's readiness update. No non-ISA code edit or source acquisition is needed. No new source-authority or representation decision is required for these explicitly labelled source coordinates; the outstanding decision is root approval of this exact implementation boundary.

Sue.
