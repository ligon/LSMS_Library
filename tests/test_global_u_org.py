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
    import re
    from pathlib import Path
    from lsms_library.paths import COUNTRIES_ROOT
    u_org = COUNTRIES_ROOT.parent / "categorical_mapping" / "u.org"
    text = u_org.read_text(encoding="utf-8")
    table = [ln for ln in text.splitlines()
             if ln.strip().startswith("|") and "Preferred Label" not in ln
             and not set(ln.strip()) <= set("|-+ ")]
    prefs = {ln.split("|")[2].strip() for ln in table}
    assert prefs == {"Kg"}, f"global u.org should map only to Kg, got {prefs}"
