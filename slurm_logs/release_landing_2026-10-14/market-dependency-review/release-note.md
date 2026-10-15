Region-market food variants still warn and exclude GhanaSPS and Panama: both
have cluster IDs in `sample()`, but neither exposes `sample.Region` or declares
the `cluster_features` geography fallback. The relevant code and declarations
are unchanged since v0.14.0; GhanaSPS remains covered by open [#140](https://github.com/ligon/LSMS_Library/issues/140).
