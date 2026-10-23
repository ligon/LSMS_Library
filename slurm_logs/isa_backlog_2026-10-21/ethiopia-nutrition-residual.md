Split from #1062 after its Uganda-specific implementation and current source-rule census.

Tier: **B, diagnose before fixing**.

Found during #1062's read-only sibling-write census. Ethiopia/_/Makefile:187-188 lists var/food_acquired.parquet as a prerequisite of the shared fct/nutrition target and runs nutrition.py. The declared nutrition builder itself obtains quantities from Country('Ethiopia').food_quantities(), so the legacy prerequisite can independently run Ethiopia/_/food_acquired.py, whose line 69 writes var/food_acquired.parquet without a framework cache hash. The rule also names fct, but fct is not declared by Ethiopia/_/data_scheme.yml and the current nutrition script writes only nutrition. No live Ethiopia build was run here. Diagnose a private cold/default/make sequence, compare raw/served food frames exactly, and inventory sibling hash/coverage before proposing retirement under #788. Coordinate with #771's separate FCT/API dependency; do not invoke the live FDC API or change scientific inputs by accident. Fix only after demonstrating the actual current route and exact values; old static 13-rules/12-country counts are stale after retirements.

The proposed Uganda fix does not change Ethiopia. Track the actual Ethiopia route and exact residual here; preserve #771 as the separate FCT/API reproducibility issue.

Sue
