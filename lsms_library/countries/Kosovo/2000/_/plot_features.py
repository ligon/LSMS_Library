"""Build plot_features for Kosovo 2000 (OPLAND.dta, section 8a).

OPLAND.dta is the operated-land / plot roster: one row per (household,
plot).  The plot identifier is s8a_q01 ("plot code"), unique within hhid.

Per-plot area is NOT stored in hectares directly: all_ha_1 ("total area
of holding") is the HOUSEHOLD total (constant within hhid; its per-plot
sum equals all_ha_1 to machine precision).  The per-plot area is the raw
value s8a_q03a in the unit named by s8a_q03b (Square m / Hectares /
Ares), which we convert to hectares.

Canonical columns produced:
  Area        -- plot area in hectares (s8a_q03a x unit factor)
  AreaUnit    -- 'Hectares' (Area is normalized to ha)
  Tenure      -- plot status s8a_q09 (Owned / Rented / Borrowed)
  TenureSystem-- rent arrangement s8a_q12 (sparse; only rented/borrowed)
  Irrigated   -- True if irrigated in either reference period
                 (s8a_q06 1997-98 OR s8a_q07 1999-2000)

No soil-type or GPS variable exists in this module, so SoilType /
Latitude / Longitude are not emitted.  The 8 "Landless" rows (null
plot code, zero holding) are dropped -- they carry no plot.
"""
import sys

sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

WAVE = '2000'

# convert_categoricals=False keeps integer codes so we can map them to
# full, un-truncated labels (Stata stored 8-char truncated value labels).
df = get_dataframe('../Data/OPLAND.dta', convert_categoricals=False)

# Drop the landless / sentinel rows that have no plot identifier.
df = df[df['s8a_q01'].notna()].copy()

# --- index ---------------------------------------------------------------
df['t'] = WAVE
df['i'] = df['hhid'].apply(format_id)
df['plot_id'] = df['s8a_q01'].astype('Int64').apply(format_id)

# --- Area (hectares) -----------------------------------------------------
unit_to_ha = {1.0: 0.0001,   # Square metres -> ha
              2.0: 1.0,      # Hectares
              3.0: 0.01}     # Ares -> ha
df['Area'] = df['s8a_q03a'] * df['s8a_q03b'].map(unit_to_ha)
df['AreaUnit'] = 'Hectares'

# --- Tenure --------------------------------------------------------------
df['Tenure'] = df['s8a_q09'].map({1.0: 'Owned',
                                  2.0: 'Rented',
                                  3.0: 'Borrowed'})

df['TenureSystem'] = df['s8a_q12'].map({1.0: 'Rent',
                                        2.0: 'Sharecropped',
                                        3.0: 'No payment',
                                        4.0: 'Exchange',
                                        5.0: 'Other'})

# --- Irrigated -----------------------------------------------------------
# s8a_q06 / s8a_q07: 1 = Yes, 2 = No (two reference periods).
def _irrig(row):
    vals = [row['s8a_q06'], row['s8a_q07']]
    if all(v != v for v in vals):  # both NaN
        return None
    return any(v == 1.0 for v in vals)

df['Irrigated'] = df.apply(_irrig, axis=1).astype('boolean')

out = df.set_index(['t', 'i', 'plot_id'])[
    ['Area', 'AreaUnit', 'Tenure', 'TenureSystem', 'Irrigated']
]

assert out.index.is_unique, "Non-unique (t,i,plot_id) in Kosovo plot_features"
assert len(out) > 0, "Kosovo plot_features produced no rows"

to_parquet(out, 'plot_features.parquet')
