#!/usr/bin/env python3
"""Derive ``conversion_to_kgs.json`` from ``units.json`` (GitHub issue #117).

``units.json`` enumerates every measurement-unit code used in the Panama
food-consumption module, with a partial "Conversion to Pounds" column.  This
script fills in the well-known standard mass/volume units (libra, arroba,
onza, quintal, litro, cuartillo, pinta, galon, ...) and emits a flat
``{unit-label: kg-factor}`` mapping, mirroring the Ethiopia/Tanzania
``conversion_to_kgs.json`` files.

Item-specific packaging units whose weight depends on the food item
(bandeja/cajeta/sobre/paquete/botella/lata/frasco/...) are left as ``null``,
exactly as the Ethiopia and Tanzania item-level conversion tables do.

Standard factors used (all uncontroversial):
  - 1 libra (pound)            = 0.45359237 kg              (1 lb)
  - 1 arroba                   = 11.33980925 kg             (25 lb)
  - 1 onza (ounce)             = 0.028349523 kg             (1/16 lb)
  - 1 quintal                  = 45.359237 kg               (100 lb)
  - 1 gramos (gram)            = 0.001 kg
  - 1 kilo (kilogram)          = 1.0 kg
  - 1 litro (liter)            = 1.0 kg                     (water-equivalent)
  - 1 galon (US gallon)        = 3.785411784 kg            (3.785411784 L)
  - 1/2 galon                  = 1.892705892 kg
  - 1 pinta (US pint)          = 0.473176473 kg            (1/8 gal)
  - 1 cuartillo (Spanish liq.) = 0.504 kg                  (~0.504 L)

Numeric "Conversion to Pounds" factors already present in ``units.json``
(e.g. "lata de 25 libras" = 25 lb, "barra de 1/4 de libra" = 0.25 lb,
package-of-N-pounds entries) are carried through and divided by 2.20462.

The CSV->JSON regeneration of ``units.json`` itself is retained below for
reference (it produced invalid JSON and required manual reformatting; do not
re-run it without re-checking the result).
"""
import json

LBS_PER_KG = 2.20462  # pounds per kilogram

# --- Standard mass units, expressed in pounds -----------------------------
# Keyed by the Spanish "Label" as it appears in units.json.  These override
# any null in the source and correct the metric/Spanish "quintal" ambiguity
# (the Spanish quintal is 100 lb, not the 100 kg metric centner).
STANDARD_POUNDS = {
    "libra": 1.0,                 # pound
    "arroba": 25.0,               # 25 lb (Spanish customary)
    "onza": 1.0 / 16.0,           # ounce = 1/16 lb
    "quintal": 100.0,             # 100 lb (Spanish quintal)
    "gramos": 1.0 / 453.59237,    # gram in lb
    "kilo": LBS_PER_KG,           # kilogram in lb
}

# --- Standard volume units, expressed directly in kg (water-equivalent) ---
STANDARD_KGS = {
    "litro": 1.0,                 # liter
    "galón": 3.785411784,    # US gallon
    "1/2 galón": 1.892705892,
    "pinta": 0.473176473,         # US pint
    "cuartillo": 0.504,           # Spanish liquid cuartillo ~0.504 L
}


def label_to_kgs(unit):
    """Return the kg factor for one units.json record, or None if not derivable."""
    label = unit["Label"]

    # Volume units defined directly in kilograms.
    if label in STANDARD_KGS:
        return STANDARD_KGS[label]

    # Mass units defined in pounds (standard table wins over source).
    if label in STANDARD_POUNDS:
        return STANDARD_POUNDS[label] / LBS_PER_KG

    # Otherwise carry through any explicit "Conversion to Pounds" already in
    # the source file (e.g. "paquete de 2 libras" = 2 lb, "barra de 1/4 de
    # libra" = 0.25 lb).  These are exact, item-independent weights.
    lbs = unit.get("Conversion to Pounds")
    if lbs is not None:
        return lbs / LBS_PER_KG

    # Item-specific packaging unit (bandeja/cajeta/sobre/...): not derivable.
    return None


def build_conversion_to_kgs(units_json="units.json",
                            out="conversion_to_kgs.json"):
    with open(units_json, "r") as f:
        units = json.load(f)["units"]

    conversion = {u["Label"]: label_to_kgs(u) for u in units}

    with open(out, "w") as f:
        json.dump(conversion, f, ensure_ascii=False, indent=1)

    return conversion


if __name__ == "__main__":
    conv = build_conversion_to_kgs()
    derivable = {k: v for k, v in conv.items() if v is not None}
    print(f"Wrote conversion_to_kgs.json: "
          f"{len(derivable)}/{len(conv)} units with kg factors.")


# --- Legacy: CSV -> units.json regeneration (reference only) ---------------
# from lsms_library.local_tools import get_dataframe
# units = get_dataframe('../1997/Data/unittable.csv')
# units.to_json('units.json', orient='records', lines=True)
