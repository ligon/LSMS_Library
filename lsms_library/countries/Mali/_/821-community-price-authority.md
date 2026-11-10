# Mali 2021/22 community-price authority gap (GH #821)

Source-only update, 2026-11-09. Keep `unsure` and `C1-partial;C4-notrun`.
The 2018 loader in merged PR #1356 is already implemented; this packet
neither replays it nor proposes another loader.

A new own-country primary-source lead qualifies the existing evidence.
INSTAT's August 2023 poverty report for Mali 2021 is publicly available at
[the INSTAT publication URL](https://www.instat-mali.org/laravel-filemanager/files/shares/pub/profil-determinants-pauvrete-mali-2021_pub.pdf).
Its cover identifies INSTAT and August 2023; PDF p.3 explicitly describes
second-edition EHCVM fieldwork in October 2021–July 2022. The PDF has 64
pages. Section 1.2 on PDF p.15 describes five community sections, including:

> Enfin la section 5 permet de prendre les informations sur les prix des différents produits alimentaires consommés dans la localité.

The final bullet concerns local food prices. This is a report's description
of the survey, not the community instrument or its deployment record.
The report text was inspected through the public browser PDF text layer;
visual requests for PDF pp.3 and 15 failed with cache misses. Local Poppler
was unavailable. No full-document visual review is claimed.

The public PDF snapshot saved separately for review is 1,456,311 bytes,
SHA256 `cd86f5ebf0cc20ece8ad6dd4cb7eb20a62d370bb3a08feff5069a0a3de17bcd1`.
The browser text and snapshot share the public URL; local extraction from
those exact saved bytes was not performed, so this is not a cryptographic
binding of the browser extraction to the snapshot. ROOT can close that
small documentary gate without acquiring any microdata.

## Preserve the contradictory evidence

The 2026-11-04 country entry records the own English BID, PDF p.8, as
saying “four main sections”; its PDF p.9 describes separate monthly market
collection at regional urban/rural locations. The same entry preserves the
DDI reference to local retail prices and the missing own community form.
The BID was not reopened in this dispatch. Neither document is dismissed
as stale, copied from a sibling, or authoritative merely because it is newer.

The previously inspected released s00–s04 schemas and regional price file
remain their separate evidence. The new report does not create a grappe
column, authenticate an unreleased module, or establish the exact fielded
form/version. No closing absence verdict follows from either description.
The [current GitHub discussion](https://github.com/ligon/LSMS_Library/issues/821#issuecomment-6533160758)
requires the exact community/market instrument; that requirement still holds.

## Finite next steps for ROOT

1. Authenticate the public snapshot and its cover, p.3 and p.15 visually,
   retaining the report/BID contradiction. This is document work only.
2. Obtain the exact own 2021/22 community form or CAPI project/export version,
   its deployment/change history and the version applying to each passage.
   The report's section list is not a substitute for that operand.
3. Ask the designated authority, through ROOT only, whether the food-price
   section was fielded and where any responses were released. Require the
   exact file/version and cluster/product/unit/quote key specification, or
   an explicit noncollection/nonrelease explanation. No outreach occurred.
4. Only then decide C4 and any `asked-not-distributed`/`not-asked` verdict.
   A loader proposal would additionally require an authenticated compatible
   released source and explicit source/read execution authorization.

Do not acquire the five existing community modules merely to repeat their
published schema inspection. Do not assign the regional price file to
household clusters or borrow another country's questionnaire. There is no
Country/Feature, native header/row, source acquisition, cache operation or
new runtime acceptance in this source-only packet.
