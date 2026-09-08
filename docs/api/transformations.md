# Transformations

Reusable transforms over harmonized tables. The derived tables
(`food_expenditures`, `food_prices`, `food_quantities`,
`household_characteristics`) are built from these at API time, but the module
is **also a library of standalone derived measures** — asset indices,
dependency ratios, tropical livestock units, anthropometric z-scores, the World
Bank median-price valuation ladder — that you can call directly.

!!! tip "Check here before writing a new derived measure"
    Several of these have no call site inside the library yet, so searching for
    a usage will not find them. `make api-audit` lists exactly which.

::: lsms_library.transformations
    options:
      show_root_heading: true
      show_source: false
      members_order: alphabetical
      filters: ["!^_"]
