# Mali-only GH1091 retirement proposal (not implemented)

1. Commit the rooted task ledger first on an approved focused writer based on
   aba2badfe/PR1180. No framework or other-country file changes.
2. Delete `lsms_library/countries/Mali/_/community_prices.py`.
3. Remove only `$(VAR_DIR)/community_prices.parquet: community_prices.py
   ../2014-15/_/community_prices.parquet` and its recipe from Mali/_/Makefile.
   Retain the exact wave pattern/recipe at102-103. Replace the neighboring
   country-concatenator comment with framework authority/#788, the retained
   2014-15 wave source path and no-country-target explanation. Correct its
   all-EHCVM-no-grappe wording to leave unwired2018-19 #821 explicit.
4. Add a dated CONTENTS note with actual baseline layer/attrs/subset findings,
   source-backed155931/obs counts, unchanged eligibility and retained wave path.
   Preserve historical #840/#822 evidence as dated records.
5. Add focused discriminating tests and safe evidence; no fixtures/goldens,
   global enums, finalizer/driver/schema/grain/source edits.

Direct Make behavior: `make -C Mali/_ all` remains committed panel_ids.json
only; `make -C lsms_library build country=Mali` invokes that default. The
retired country var target should fail honestly with no rule, even if a
generic wrapper exists; do not substitute another country concatenator.
Users materialize through Country/CLI/generic `make materialize ...`.
That CLI delegates to the Country method; the explicit wave pattern remains
available for its normal framework build. `persist/register` concern DVC
stages and are not substitutes to run during this task.

## Discriminating verification design

- Baseline-failing static/caller controls: country script absent; exact country
  var rule absent under fresh empty-root `make -n`; retained wave rule exists;
  actual `_has_country_builder('community_prices')` False; `_is_script_path`
  remains True and required-column errors still propagate.
- Actual empty-target default + Make builds with two verified held passage
  blobs, separate physical roots, exact source/config/import identities and
  normal caches. No NO_CACHE/assume-fresh/hash stamp/read-gate bypass.
- Complete native wave frame equality (eight levels including i==v, full
  attrs/order/dtypes/nulls) and source155931 accounting: obs1=66699,
  obs2=49014, obs3=40218; passage1=66159, passage2=89772.
- Full default public frame/full attrs identical to reviewed822. Future Make
  public frame/full attrs must equal that authoritative default exactly.
  Report its deliberate id_converted=True addition versus old Make; every
  other attrs key/value/type and payload unchanged. Never strip attrs to pass.
- Full COUNTRY raw default/native/walked-layer comparisons recorded separately.
  New Make wave route should create no country parquet, so absence is expected,
  not a fabricated raw-frame equality. Default country hash/full coverage valid;
  script wave companions may legitimately remain hashless.
- Supported `waves=['2014-15']`, default/all requests and fresh-process warm
  full-frame equality. Under both backends, separately test2017-18,2018-19,
  2021-22 and their combined strict subset under cold and populated states.
  All must return declared empty frames; zero unrelated2014-15 rows. Baseline
  Make wrong-wave rows are a positive negative control, not preservation data.
- Plain Country callable and CLI materialize with repeated --wave flags follow
  the same route; no alternate concatenator. Direct missing var target and
  wave target/all behavior checked with actual GNU make under private roots.
- Actual synthetic wave-script/finalizer/Description/unknown-unit/slot tests
  remain green: focused822 controls plus relevant1039/808/866/cache-coverage
  regressions, selected by relevance with no hidden skips. Do not pin unrelated
  defects or create tests mirroring comments only.
- Source-pinned healthy comparator, default and currency=None Feature frames,
  complete scoped phases1/2 finding multisets; record every difference, including
  expected removal of duplicate-builder behavior. community_prices has no
  registered kwargs grid; do not invent one.
- Measure all549country/table hashes and39tagged fingerprints on the actual
  patch against its exact822 parent. Static dependency analysis predicts24Mali
  country hashes move because Makefile/local Python are whole-file inputs;
  other525/39 should remain exact. This diagnosis measures only24 baseline
  Mali hashes and makes NO postchange hash or unbuilt-output claim.
- Independent reviewer uses a new physical held-source/cache root and an
  immutable candidate checkout. After scoped PASS, root composes with current
  integration, then verifies actual parent provenance, unchanged other fixes,
  focused Mali default/Make/subsets/Feature/current metadata, and no shared
  cache or source/evidence mutation. No public merge is part of this diagnosis.

## Residual narrowing draft for root (apply only after a Mali PR exists)

Tier A/B per settled #788.
Title: `retire the Niger / Tanzania community_prices concatenators`

Body:
> Resolved so far: Mali country concatenator/rule retired by PR <MALI-PR>,
> based on reviewed #822/PR1180 rather than its pre-restoration parent.
> Full source/native/public155931 rows and obs1/2/3 are preserved; Make now
> uses the authoritative framework wave route, including correct unsupported
> strict subsets and explicitly documented attrs/metadata expectations.
>
> Remaining: diagnose and retire only the Niger and Tanzania concatenate-only
> country community_prices builders and targets under #788's settled
> framework-authoritative ruling. Account for every actual Make prerequisite,
> country transformation, native/walked grain and supported/unsupported wave
> subset; retain genuinely necessary wave scripts/targets and all recorded
> variety/visit/obs detail accepted in #840/PR1086. Verify actual source-backed
> cold/warm default and Make outputs/full attrs, current cache metadata and
> scoped Feature findings before removal. No blanket source/grain/unit change.
>
> The authority/grain decisions are settled; no reopening them here. Country
> scopes remain separate work, with no claim that Mali proves their payloads.

This is a future conditional draft; no Mali implementation, PR, issue update or
Niger/Tanzania primary-source claim is made now.

Sue
