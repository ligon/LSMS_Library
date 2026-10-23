# Make empty responses leave optional country fields as object

After a normal held-source build of Burkina Faso plot_inputs, request either unsupported wave 2014 or 2021-22. On exact candidate 3d39096d55bc2b2a8a4e387e9d939aabe8951ecd, Default and Make both return zero rows and the same seven-column order, but the four new optional fields differ in type:

| Field | Default | Make |
|---|---|---|
| Unit_purchased | string | object |
| Received_as_gift | boolean | object |
| Quantity_received_as_gift | Float64 | object |
| Unit_received_as_gift | string | object |

The order is Quantity, Purchased, Quantity_purchased, Unit_purchased, Received_as_gift, Quantity_received_as_gift, Unit_received_as_gift. The three original columns retain Float64, boolean and Float64 on both backends. There are four retained full-strict failures: each of the two unsupported singleton requests after the first normal process and after a separate fresh warm process. Source frames, operands, errors, complete axes and typed attrs are retained privately; no comparison disables dtype checks.

Actual parent 3dc9d28510dd783c648413987011832669f8bcf7 has only the original three columns. Its corresponding Default/Make comparisons pass; the new fields do not exist there. This is a newly exposed observable contract gap through existing machinery, not an identical four-column failure reproduced on the parent.

The unchanged _empty_declared_frame (country.py:1050) constructs declared payload columns as object. The Make no-country-builder path returns it for the unsupported strict subset (country.py:5403). Finalization calls _enforce_declared_dtypes (country.py:3953/7103), which recognizes scalar/list declarations but ignores the extended optional dict form. BF's native writer explicitly supplies nullable types for nonempty output; ordinary Default slices preserve them. Nonempty source/native/public/Feature and seed_kg/nitrogen_kg proof is separate from these empty cases.

No country-local empty post-finalize callback exists on this route. Wave df_edit hooks are not called here. Changing these fields to required scalar declarations loses optionality, and adding a country builder changes routing. Root can separately consider unwrapping dict.type in the shared coercer (also affects nonempty optionals), or typed empty construction using the existing type machinery without duplicating type rules. Neither change is part of BF1188; declaration order is a separate matter and the observed BF column order already agrees.

Proposed tier: C, empty-interface datatype consistency. Root owns dedup, severity and filing. The source repair retains exactly the approved four optional fields and every observed mismatch. No workaround, actual-frame cast, attrs transplantation or cache reset was used.

Sue
