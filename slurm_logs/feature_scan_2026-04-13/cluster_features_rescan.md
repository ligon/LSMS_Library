# cluster_features — Rescan 2026-04-13

**Probe**: `Country(X).cluster_features()` via API  | **Countries probed**: 30  | **OK**: 29  | **Error**: 1  | **Total rows (ok)**: 65,259

> No prior audit found for this feature.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Albania | ok | 1,829 | `t, v` | Region, Rural |
| Azerbaijan | ok | 92 | `t, v` | Rural |
| Benin | ok | 670 | `t, v` | i, Region, Rural, Latitude, Longitude |
| Burkina_Faso | ok | 550 | `t, v` | Region, District, Rural |
| Cambodia | ok | 252 | `t, v` | Region, Rural |
| China | ok | 30 | `t, v` | Region |
| CotedIvoire | ok | 1,484 | `t, v` | Region, Rural, i, Latitude, Longitude |
| Ethiopia | ok | 866 | `t, v` | i, Region, District, Rural, Latitude, Longitude |
| GhanaLSS | ok | 3,791 | `t, v` | Region, Rural, Ecological_zone |
| Guatemala | ok | 8 | `t, v` | Rural |
| Guinea-Bissau | ok | 450 | `t, v` | i, Region, Rural, Latitude, Longitude |
| Guyana | ok | 130 | `t, v` | Region, Rural |
| India | ok | 120 | `t, v` | Region, District |
| Kazakhstan | ok | 135 | `t, v` | Region, Rural |
| Kosovo | ok | 360 | `t, v` | Region, Rural |
| Liberia | ok | 32 | `t, v` | Region, Rural, County |
| Malawi | ok | 38,103 | `i, t, v` | Region, District, Rural, Latitude, Longitude |
| Mali | ok | 3,006 | `t, v` | Region, Rural, Latitude, Longitude |
| Nepal | **error** `RuntimeError: Could not materialize Nepal/cluster_features: no wave-level build ` | — | — | — |
| Niger | ok | 1,329 | `t, v` | Region, District, Rural, i, Latitude, Longitude |
| Nigeria | ok | 820 | `t, v` | i, Region, District, Rural, Latitude, Longitude |
| Pakistan | ok | 301 | `t, v` | Region, Language |
| Senegal | ok | 1,194 | `t, v` | Region, Rural, Latitude, Longitude |
| Serbia | ok | 328 | `t, v` | Region, Rural |
| Serbia and Montenegro | ok | 919 | `t, v` | Region, Rural |
| South Africa | ok | 355 | `t, v` | Region, Rural |
| Tajikistan | ok | 767 | `t, v` | Region, Rural |
| Tanzania | ok | 2,389 | `t, v` | Rural, Region, District |
| Togo | ok | 540 | `t, v` | i, Region, Rural, Latitude, Longitude |
| Uganda | ok | 4,409 | `t, v` | Region, Rural, District, i, Latitude, Longitude |

## 2. Index Consistency

**Warning**: 2 distinct index structures across countries.

- `('t', 'v')` — Albania, Azerbaijan, Benin, Burkina_Faso, Cambodia, China, CotedIvoire, Ethiopia, GhanaLSS, Guatemala, Guinea-Bissau, Guyana, India, Kazakhstan, Kosovo, Liberia, Mali, Niger, Nigeria, Pakistan, Senegal, Serbia, Serbia and Montenegro, South Africa, Tajikistan, Tanzania, Togo, Uganda
- `('i', 't', 'v')` — Malawi

## 3. Canonical Column Checks

### Missing Required Columns

- `Region` missing in: Azerbaijan, Guatemala
- `Rural` missing in: China, India, Pakistan

### Missing Optional Columns (informational)

- `District` absent in: Albania, Azerbaijan, Benin, Cambodia, China, CotedIvoire, GhanaLSS, Guatemala, Guinea-Bissau, Guyana, Kazakhstan, Kosovo, Liberia, Mali, Pakistan, Senegal, Serbia, Serbia and Montenegro, South Africa, Tajikistan, Togo
- `Latitude` absent in: Albania, Azerbaijan, Burkina_Faso, Cambodia, China, GhanaLSS, Guatemala, Guyana, India, Kazakhstan, Kosovo, Liberia, Pakistan, Serbia, Serbia and Montenegro, South Africa, Tajikistan, Tanzania
- `Longitude` absent in: Albania, Azerbaijan, Burkina_Faso, Cambodia, China, GhanaLSS, Guatemala, Guyana, India, Kazakhstan, Kosovo, Liberia, Pakistan, Serbia, Serbia and Montenegro, South Africa, Tajikistan, Tanzania

### High Null Rate (>50%) on Canonical Columns

- Uganda / `Latitude`: 21.8% non-null
- Uganda / `Longitude`: 21.8% non-null

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `i` | Benin, CotedIvoire, Ethiopia, Guinea-Bissau, Niger, Nigeria, Togo, Uganda |
| `Ecological_zone` | GhanaLSS |
| `County` | Liberia |
| `Language` | Pakistan |

## 5. Warnings

- (29×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Albania`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Azerbaijan`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Cambodia`
- (1×) `UserWarning: Data scheme does not contain panel_ids for China`
- (1×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Guatemala`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Guyana`
- (1×) `UserWarning: Data scheme does not contain panel_ids for India`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Kazakhstan`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Kosovo`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Liberia`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Pakistan`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Serbia`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Serbia and Montenegro`
- (1×) `UserWarning: Data scheme does not contain panel_ids for South Africa`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Tajikistan`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Togo`
