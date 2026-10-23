# Static options for the newly exposed empty optional-column dtypes

This is diagnosis only. No fallback or shared code change is implemented.

The actual parent and candidate share the same `country.py:1050` empty constructor and `:7103` coercer. The constructor creates every declared payload column as object. Finalization invokes the coercer at `:3953`, but that coercer recognizes scalar type strings and lists, not extended `{type: ..., optional: true}` dict declarations. The four new BF optional fields therefore remain object on the Make no-builder empty route at `:5403`, while ordinary Default slices retain the native writer's explicit nullable types. The three older scalar-declared fields coerce correctly on both sources. The parent's absence of the four new fields must remain explicit.

No existing country-local typed-empty hook was found on this route. Country module formatting functions are consumed by `Wave.column_mapping`/`grab_data` (`:1462`, `:1820`, `:1926`), including per-table `df_edit` hooks. The `_empty_declared_frame` return does not pass through a wave extraction hook, and `_finalize_result` has no country table callback for this stage. The scheme's non-column keys at `:1016` are index/materialize/backend/aggregation; there is no separate optional-column list that would permit scalar type declarations while preserving optionality.

Country-local alternatives do not preserve the released contract: making the four fields required scalar types loses optionality; a new country builder changes routing and requested-wave behavior; a categorical mapping does not supply nullable boolean/Float64 types and is outside the source boundary. Adding a new post-finalize hook would be a framework change. None is a narrow reuse of existing country-local machinery for this empty path.

For root's separate framework follow-up, two concrete small implementation candidates are:

1. Unwrap a dict declaration's `type` in the existing `_enforce_declared_dtypes` loop before dispatching through the existing scalar coercion branches. Required/optional classification remains in its existing shared helpers. This also changes coercion of nonempty optional fields, so it needs its own reviewed scope and tests rather than an empty-only claim.
2. Have `_empty_declared_frame` initialize each required and optional column using its declared type and the existing dtype map/coercion machinery, retaining current column order and absent-data semantics. This confines the behavioral change to genuinely empty no-builder responses, but must avoid a second drifting interpretation of types, constrained-string lists and datetime declarations.

Both candidates require a separate shared-code decision and validation. The BF source repair keeps the four optional fields, their explicit native nullable types and all recorded empty-frame discrepancies unchanged.

Sue
