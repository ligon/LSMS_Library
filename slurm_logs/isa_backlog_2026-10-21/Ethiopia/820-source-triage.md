# Ethiopia820: source-only reconciliation

Source inspected: release `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Live issue820 and its comment were fetched on 2026-10-22; it remains open.
This is static triage, with no source decode, table build or new instrument
verification. No implementation or country worktree is assigned.

The missing read still exists in the 2011-12, 2013-14 and 2015-16 drivers:
each reads post-harvest section9, planting section4, and section11 sales;
none reads its held `sect12_ph_w{1,2,3}.dta` sidecar. All three section12
sidecars declare a size and MD5. Their presence does not establish that a
held blob passes the physical source gates or that every source field is
suitable for the existing crop table.

`Ethiopia/_/CONTENTS.org:1597` calls the missing section12 wiring a
"feature addition, not a config tweak". Its historical 2015-16 counts and
the D3 price-source report are prior measurements, not new measurements
from this triage. Treat this as backlog tier C pending a concrete design;
do not turn the existence of an unwired module into permission for a new
grain, reducer or allocation rule.

The current helper's sales-grain caveat is material:
`Ethiopia/_/ethiopia.py:1219` attaches a reported holder/crop sale only to
an unambiguous harvest plot/crop. The output schema remains
`(t,i,plot_id,j,u)`. The issue's suggested native sale-source concatenation
therefore needs reconciliation with the actual section12 source grain,
harvest coverage, unit identity, collisions between the two sales modules,
and this existing no-allocation rule. A simple second file read does not
answer those questions. No repair to existing sales joins is implied.

The scope is W1-W3. The issue comment and
`Ethiopia/_/CONTENTS.org:1820` describe ESS4/ESS5 folding perennial crops
into section11. The comment explicitly grounds that reading in EPAR code,
not newly checked questionnaires; the country prose is more categorical.
Keep that evidence distinction. The actual 2018-19 and 2021-22 drivers
already read section11, with their own column maps. Do not read a household
or community section12 as a perennial post-harvest module merely because
the number matches, and do not invent a PP/PH round dimension here.

Prior art to reuse in any subsequent diagnosis: the complete current
country CONTENTS and tasks, the PP/PH skill, the existing crop helper,
canonical schema, the price-source D3 analysis, and ledger
`852-ethiopia-shipped-factors.md`. Issue852 is closed; its conversion
loader is already implemented. No stored unit conversion, new factor
layer, volume assumption, dictionary correction or imputed plot allocation
belongs in this section12 scope. The documented absence of a purchase
value for own-production/gift food is a separate instrument fact.

A subsequent read-only diagnosis, if prioritized, must first gate the
actual held files and inspect native identities, fields and labels, then
give a checkable schema/join proposal and residual. Scientific or new
feature design goes to the maintainer before implementation.

Sue
