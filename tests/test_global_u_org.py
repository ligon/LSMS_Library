"""GH #223 Layer 2 step 3: the global categorical_mapping/u.org composes
additively into each country's `u` table.

These read `Country.categorical_mapping` (org parsing only -- no microdata),
so they're fast and CI-safe.
"""
from __future__ import annotations

import lsms_library as ll


def _u_dict(country):
    u = ll.Country(country, preload_panel_ids=False).categorical_mapping["u"]
    return u.set_index("Original Label")["Preferred Label"].to_dict()


def test_global_u_org_provides_kg_variants():
    # A no-`u`-table country inherits the global table wholesale.
    d = _u_dict("Tanzania")
    assert d.get("Kg") == "Kg"
    assert d.get("kg") == "Kg"          # canonicalizes lowercase
    assert d.get("Kilogramme") == "Kg"  # and spelling variants


def test_country_u_table_inherits_global_kg_additively():
    # CotedIvoire no longer declares Kg (PoC removal); it must still be
    # present via the global table, AND its own rows must survive.
    d = _u_dict("CotedIvoire")
    assert d.get("Kg") == "Kg"      # inherited from global u.org
    assert d.get("Litre") == "Litre"   # CotedIvoire's own row preserved
    assert d.get("Unité") == "Unité"


def test_global_u_does_not_define_contested_metric_canonicals():
    # Only kilogram is globally canonicalized; gram/litre/ml are deferred
    # (Layer 3), so the global table must not force them.
    # Parse the file's TABLES rather than scanning its lines: since GH #919
    # u.org also carries `u_kg` (canonical unit -> kilograms), and a line-based
    # scan reads that table's numbers as if they were Preferred Labels.
    from lsms_library.local_tools import all_dfs_from_orgfile
    from lsms_library.paths import countries_root
    u_org = countries_root().parent / "categorical_mapping" / "u.org"
    tables = all_dfs_from_orgfile(u_org)
    prefs = set(tables["u"]["Preferred Label"].dropna().astype(str).str.strip())
    assert prefs == {"Kg"}, f"global u.org should map only to Kg, got {prefs}"
