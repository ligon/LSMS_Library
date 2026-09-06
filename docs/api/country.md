# Country

!!! warning "The data methods are not on this page"
    `Country.food_acquired()`, `.food_prices()`, `.household_roster()` and every
    other table method are **generated at attribute-access time** from the
    country's `data_scheme`, so they have no source definition for this
    reference to introspect. They — and the keyword arguments they share — are
    documented in [Data methods and their keyword arguments](../guide/data-methods.md).

    What follows is the *introspection* surface: how to ask a country what it
    has, not how to get the data out.

<!-- Deliberately no `members:` allowlist.  An allowlist is hand-maintained,
     so a newly added public method stays invisible until somebody remembers
     to list it -- the same "you had to already know it existed" failure this
     page's warning is about.  `filters` excludes privates by pattern, so new
     public members appear automatically. -->

::: lsms_library.Country
    options:
      show_root_heading: true
      show_source: false
      members_order: source
      filters: ["!^_"]
