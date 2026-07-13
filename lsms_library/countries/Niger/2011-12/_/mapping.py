# Formatting functions for Niger 2011-12 (ECVMA-I)
import sys
from pathlib import Path

# mapping.py is imported by the framework from an ARBITRARY cwd (importlib
# spec_from_file_location), so a relative '../../_/' does not resolve.  Anchor
# on __file__ instead: {Country}/{wave}/_/mapping.py -> parents[2] == {Country}.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))

# cluster_features is extracted from the household cover page (one row per
# household, 3968 rows / 270 clusters) but is declared at (t, v).  Collapse to
# cluster grain EXPLICITLY -- via the within-cluster majority, warning on any
# conflict -- rather than leaving it to _normalize_dataframe_index's
# groupby().first(), which picks by row order (GH #323).  Attributes happen to
# be constant within every cluster in this wave, so the values are unchanged;
# what changes is that the de-duplication is now declared instead of accidental.
from niger import cluster_features_to_cluster_grain as cluster_features  # noqa: F401,E402
