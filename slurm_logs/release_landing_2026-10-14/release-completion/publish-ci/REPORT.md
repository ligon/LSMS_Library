SCOPE DEVIATIONS

None. Read-only monitoring of the release workflow initiated by the parent. No dispatch, upload, publication, tag, source, dependency or cache changes by this agent.

Release [v0.15.0](https://github.com/ligon/LSMS_Library/releases/tag/v0.15.0) is published. Automatic release-event [workflow 38767928967](https://github.com/ligon/LSMS_Library/actions/runs/38767928967) completed successfully at exact source `cb44e3522a2426ba03431ca9a218cc63fe03f6c9`. Annotated tag object `d4ff03d28ab417c5d98f8e1e7c94d1261b8331bc` peels to that source.

The build log records `Building 0.15.0 from v0.15.0` and `Building LSMS_Library (0.15.0)`. Both build and publish jobs succeeded. The uploaded and downloaded `dist` artifact SHA256 agree: `5793da7215cf91da8018a1875e747f4030a3ee92abb33f99ce037366c1f6838c`. This artifact archive digest is distinct from the individual distribution digests below.

Public [PyPI version metadata](https://pypi.org/pypi/LSMS-Library/0.15.0/json) confirms version 0.15.0 and both non-yanked files. Their SHA256 values agree with the distribution attestation subjects in the terminal publish log.

| Distribution | Bytes | SHA256 |
|---|---:|---|
| lsms_library-0.15.0-py3-none-any.whl | 8852718 | `a19b458f2d2fce0bb67c192338ffb02fb745f78af3ba77b0d5db1c3395c0f9f0` |
| lsms_library-0.15.0.tar.gz | 5300551 | `e8ed24a6609268b02220e2eb94c7809ab5c153468094614c8af2b8444c78c657` |

| Job | State | Terminal log bytes | Terminal log SHA256 |
|---|---|---:|---|
| [build / 116535902863](https://github.com/ligon/LSMS_Library/actions/runs/38767928967/job/116535902863) | success | 76974 | `7d3feb5e4d8e2e41a867065010aecc3e5c1b7c1647fe314b6c7c07f683852275` |
| [publish / 116536350142](https://github.com/ligon/LSMS_Library/actions/runs/38767928967/job/116536350142) | success | 52963 | `c88a3d70ea6fa1193104bffc66d49164ef0bcc2bcb50757175ba75398bbda7aa` |

Read-only workflow/log/API observation. Distribution digests are compared between the publish log attestation and public PyPI metadata; this monitor did not download/install the distributions or initiate publication.

Raw job logs and HTTP captures remain private. This bundle contains only aggregate outcomes, public identifiers/digests and the analysis script. Earlier master CI and local make-release validation are separate evidence bundles.
