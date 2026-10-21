# Draft migration note: approved framework increment

Applies to source `532d58700fa46e52a0b390198604eca1dc4de00e`; read alongside the draft release notes. This is not a version bump or release announcement.

## Keep country in pooled calculations

`seed_kg` and `nitrogen_kg` now return `(country, t, i, plot_id)` when the input has a `country` index level or column. `fertilizer_rate` retains that dimension at plot grain, or returns `(country, t, i, parcel)` at parcel grain. This applies even to one labeled country and to shaped empty results. Country-less calls retain their existing index contract; `attrs['country']` alone does not add an axis.

Update code that unpacks exactly three index levels, resets/rebuilds indexes, or joins pooled numerator and area data. Preserve country through joins and reductions so identical local identifiers in different countries remain distinct. An actual country index takes precedence over a redundant country column.

Present country labels must be non-null: the reducers raise an actionable `ValueError` before reduction when labels are missing. For `fertilizer_rate`, numerator and plot/area data must both carry country or both omit it; one-sided country labeling raises `ValueError`. Existing country-less arithmetic, nutrient/product choice, share handling, organic/unknown-unit refusals and parcel aggregation rules are preserved. Other derived transforms and price pools are not thereby guaranteed to preserve country.

## Expect an automatic cache rewarm

The #1099 aggregation-closure change moves build fingerprints globally. In the paired corpus, all 549 expected country hashes and all 1,183 observed wave-cache expected hashes changed; the latter is an observed-file set, not the complete 2,139-wave census. Ordinary hash checks rebuild affected harmonized caches on demand. Allow time and disk space for the first access; raw L1 source blobs remain reusable. No blanket cache deletion is required.

## Interpret empty frames and text units precisely

A typed empty strict-wave-subset result means those known requested waves do not declare the table, under the bounded no-country-builder contract. It does not convert unknown waves, missing tables or source failures into success.

Comma rules are explicit: `1,5L` means 1.5 L, `0,125kg` means 0.125 kg, and `1,500kg` means 1,500 kg. One or two comma digits are decimal; groups of three are thousands except a literal `0,` prefix. Mixed or malformed tokens must not be interpreted through a numeric suffix.

Provenance tools may now refuse a conflicting scalar rewrite rather than discard retained metadata. Latest catalog observations are not evidence that a different raw release has been acquired. The guards do not implement #1149 staging, overwrite or rollback.

Sue
